#include "lob_capture/replay.hpp"

#include "lob_capture/csv_writer.hpp"
#include "lob_capture/order_book.hpp"
#include "lob_capture/types.hpp"

#include <fstream>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>

namespace lob_capture {
namespace {

using SnapshotKey = std::pair<std::uint64_t, std::uint64_t>;

std::filesystem::path snapshot_sidecar_for(const std::filesystem::path& market_csv) {
    std::filesystem::path stem = market_csv;
    stem.replace_extension("");
    return std::filesystem::path(stem.string() + "_snapshots.jsonl");
}

std::map<SnapshotKey, boost::json::value> load_snapshot_records(const std::filesystem::path& path) {
    std::map<SnapshotKey, boost::json::value> records;
    std::ifstream input(path);
    if (!input) return records;

    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.empty()) continue;
        try {
            const auto value = boost::json::parse(line);
            if (!value.is_object()) throw std::runtime_error("snapshot log line is not an object");
            const auto& object = value.as_object();
            const auto* epoch = object.if_contains("conn_epoch");
            const auto* seq = object.if_contains("conn_seq");
            const auto* payload = object.if_contains("payload");
            if (epoch == nullptr || seq == nullptr || payload == nullptr) {
                throw std::runtime_error("snapshot log line lacks conn_epoch, conn_seq, or payload");
            }
            // When multiple resync attempts were made for one event, the latest
            // snapshot record wins (std::map assignment replaces the earlier one).
            records[{json_u64(*epoch, "conn_epoch"), json_u64(*seq, "conn_seq")}] = *payload;
        } catch (const std::exception& error) {
            throw std::runtime_error(path.string() + ":" + std::to_string(line_number) + ": " + error.what());
        }
    }
    return records;
}

std::uint64_t parse_u64_field(const std::string& text, const char* field) {
    try {
        std::size_t used = 0;
        const auto result = std::stoull(text, &used);
        if (used != text.size()) throw std::runtime_error("extra characters");
        return result;
    } catch (const std::exception&) {
        throw std::runtime_error(std::string("invalid unsigned integer in CSV field '") + field + "'");
    }
}

std::int64_t parse_i64_field(const std::string& text, const char* field) {
    try {
        std::size_t used = 0;
        const auto result = std::stoll(text, &used);
        if (used != text.size()) throw std::runtime_error("extra characters");
        return result;
    } catch (const std::exception&) {
        throw std::runtime_error(std::string("invalid integer in CSV field '") + field + "'");
    }
}

}  // namespace

void replay_market_csv(const std::filesystem::path& market_csv,
                       const std::filesystem::path& output_dir,
                       Metrics& metrics) {
    std::ifstream input(market_csv);
    if (!input) throw std::runtime_error("cannot open replay CSV: " + market_csv.string());

    std::string line;
    if (!std::getline(input, line)) throw std::runtime_error("replay CSV is empty");
    const auto header = parse_csv_record(line);
    const auto required = parse_csv_record(market_csv_header());
    if (header != required) {
        throw std::runtime_error("replay CSV header does not match the required market-data schema");
    }

    const auto snapshot_path = snapshot_sidecar_for(market_csv);
    if (!std::filesystem::exists(snapshot_path)) {
        throw std::runtime_error("replay requires the capture's snapshot sidecar: " + snapshot_path.string());
    }
    const auto snapshots = load_snapshot_records(snapshot_path);

    std::filesystem::create_directories(output_dir);
    std::filesystem::path output_path = output_dir / (market_csv.stem().string() + "_orderbook.csv");
    std::ofstream output(output_path, std::ios::out | std::ios::trunc);
    if (!output) throw std::runtime_error("cannot create replay order-book CSV: " + output_path.string());
    output << orderbook_csv_header() << '\n';

    OrderBook book;
    std::string symbol;
    Venue venue = Venue::Spot;
    bool have_venue = false;
    bool have_epoch = false;
    std::uint64_t epoch = 0;
    std::uint64_t row_sequence = 1;
    std::size_t source_rows = 0;
    std::size_t diff_rows = 0;

    while (std::getline(input, line)) {
        if (line.empty()) continue;
        ++source_rows;
        const auto fields = parse_csv_record(line);
        if (fields.size() != 9) {
            throw std::runtime_error("market-data CSV row " + std::to_string(source_rows + 1) +
                                     " has " + std::to_string(fields.size()) + " columns; expected 9");
        }

        const auto row_venue = parse_venue(fields[2]);
        if (have_venue && row_venue != venue) throw std::runtime_error("replay CSV mixes venues");
        venue = row_venue;
        have_venue = true;
        const auto kind = parse_stream_kind(fields[3]);
        const auto row_epoch = parse_u64_field(fields[5], "conn_epoch");
        const auto row_conn_seq = parse_u64_field(fields[6], "conn_seq");
        const auto row_symbol = fields[7];
        const auto sec = parse_i64_field(fields[0], "recv_tsec");
        const auto nsec = parse_i64_field(fields[1], "recv_tnsec");
        if (nsec < 0 || nsec >= 1'000'000'000LL) throw std::runtime_error("recv_tnsec outside expected range");
        const std::int64_t recv_ns = sec * 1'000'000'000LL + nsec;
        const auto payload = boost::json::parse(fields[8]);

        if (symbol.empty()) symbol = row_symbol;
        if (row_symbol != symbol) throw std::runtime_error("a per-symbol replay CSV contains multiple symbols");
        if (!have_epoch) {
            epoch = row_epoch;
            have_epoch = true;
        } else if (row_epoch != epoch) {
            book.invalidate();
            epoch = row_epoch;
        }

        const auto snapshot_it = snapshots.find({row_epoch, row_conn_seq});
        if (snapshot_it != snapshots.end()) {
            book.install_snapshot(snapshot_it->second, venue);
        }

        MarketEvent event;
        event.recv_ns = recv_ns;
        event.venue = venue;
        event.kind = kind;
        event.conn_epoch = row_epoch;
        event.conn_seq = row_conn_seq;
        event.symbol = row_symbol;
        event.payload = payload;

        if (kind == StreamKind::Trade) {
            metrics.trade_messages.fetch_add(1);
            continue;
        }
        if (kind == StreamKind::Depth5) {
            output << format_orderbook_row(recv_ns, row_sequence++, stable_instrument_id(symbol),
                                           'S', 'N', top5_from_partial_depth(payload)) << '\n';
            metrics.orderbook_rows_written.fetch_add(1);
            metrics.depth5_messages.fetch_add(1);
            continue;
        }

        ++diff_rows;
        metrics.depth_messages.fetch_add(1);
        if (!book.has_snapshot()) {
            // During a live REST outage, the capture intentionally leaves the
            // book unsynchronized and emits no diff row. Continue until a
            // recorded snapshot sidecar entry establishes a replay baseline.
            continue;
        }
        const auto result = book.apply_diff(payload, venue);
        if (result == ApplyResult::Applied) {
            output << format_orderbook_row(recv_ns, row_sequence++, stable_instrument_id(symbol),
                                           'D', side_for_depth_event(payload), book.top5()) << '\n';
            metrics.orderbook_rows_written.fetch_add(1);
        } else if (result == ApplyResult::Gap) {
            metrics.sequence_gaps.fetch_add(1);
            book.invalidate();
            // A live capture can also fail to obtain a replacement snapshot;
            // its next successful resync is recorded in the sidecar.
        }
    }

    output.flush();
    if (!output.good()) throw std::runtime_error("failed while writing replay output: " + output_path.string());
    std::cout << "Replay completed without network calls.\n"
              << "Input rows: " << source_rows << "\n"
              << "Depth-diff rows seen: " << diff_rows << "\n"
              << "Order-book rows written: " << metrics.orderbook_rows_written.load() << "\n"
              << "Output: " << output_path << '\n';
}

}  // namespace lob_capture
