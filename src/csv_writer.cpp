#include "lob_capture/csv_writer.hpp"

#include "lob_capture/fixed_point.hpp"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <string>
#include <utility>

namespace lob_capture {
namespace {

constexpr const char* kMarketHeader =
    "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,conn_epoch,conn_seq,symbol,payload_json";
constexpr const char* kOrderbookHeader =
    "tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4";

std::pair<std::int64_t, std::int32_t> split_timestamp(std::int64_t ns) {
    constexpr std::int64_t billion = 1'000'000'000LL;
    std::int64_t seconds = ns / billion;
    std::int64_t remainder = ns % billion;
    if (remainder < 0) {
        remainder += billion;
        --seconds;
    }
    return {seconds, static_cast<std::int32_t>(remainder)};
}

void ensure_open(const std::ofstream& stream, const std::filesystem::path& path) {
    if (!stream.is_open() || !stream.good()) {
        throw std::runtime_error("CSV output write failed: " + path.string());
    }
}

}  // namespace

std::string csv_escape(std::string_view field) {
    const bool quote = field.find_first_of(",\"\r\n") != std::string_view::npos;
    if (!quote) return std::string(field);
    std::string out;
    out.reserve(field.size() + 2U);
    out.push_back('"');
    for (const char c : field) {
        if (c == '"') out.push_back('"');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

std::vector<std::string> parse_csv_record(std::string_view line) {
    std::vector<std::string> fields;
    std::string field;
    bool in_quotes = false;
    bool closed_quote = false;

    for (std::size_t i = 0; i < line.size(); ++i) {
        const char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    field.push_back('"');
                    ++i;
                } else {
                    in_quotes = false;
                    closed_quote = true;
                }
            } else {
                field.push_back(c);
            }
            continue;
        }
        if (c == ',' ) {
            fields.push_back(std::move(field));
            field.clear();
            closed_quote = false;
        } else if (c == '"' && field.empty() && !closed_quote) {
            in_quotes = true;
        } else {
            if (closed_quote && c != '\r') {
                throw std::runtime_error("invalid characters after a closing CSV quote");
            }
            if (c != '\r') field.push_back(c);
        }
    }
    if (in_quotes) throw std::runtime_error("unterminated quoted CSV field");
    fields.push_back(std::move(field));
    return fields;
}

const std::string& market_csv_header() {
    static const std::string header{kMarketHeader};
    return header;
}

const std::string& orderbook_csv_header() {
    static const std::string header{kOrderbookHeader};
    return header;
}

std::string format_orderbook_row(std::int64_t timestamp_ns, std::uint64_t seq_no,
                                 std::int32_t instrument_id, char type, char side,
                                 const TopLevels& levels) {
    const auto [seconds, nanoseconds] = split_timestamp(timestamp_ns);
    std::array<std::int64_t, 5> bid_prices{};
    std::array<std::int64_t, 5> bid_sizes{};
    std::array<std::int64_t, 5> ask_prices{};
    std::array<std::int64_t, 5> ask_sizes{};
    for (std::size_t i = 0; i < std::min<std::size_t>(5, levels.bids.size()); ++i) {
        bid_prices[i] = levels.bids[i].price;
        bid_sizes[i] = levels.bids[i].quantity;
    }
    for (std::size_t i = 0; i < std::min<std::size_t>(5, levels.asks.size()); ++i) {
        ask_prices[i] = levels.asks[i].price;
        ask_sizes[i] = levels.asks[i].quantity;
    }

    std::string row = std::to_string(seconds) + "," + std::to_string(nanoseconds) + "," +
        std::to_string(seq_no) + "," + std::to_string(instrument_id) + "," + type + "," + side;
    for (const auto value : bid_prices) row += "," + std::to_string(value);
    for (const auto value : bid_sizes) row += "," + std::to_string(value);
    for (const auto value : ask_prices) row += "," + std::to_string(value);
    for (const auto value : ask_sizes) row += "," + std::to_string(value);
    return row;
}

CsvOutputManager::CsvOutputManager(std::filesystem::path output_dir, Venue venue,
                                   const std::vector<std::string>& symbols)
    : output_dir_(std::move(output_dir)), venue_(venue), date_(utc_date_now()) {
    std::filesystem::create_directories(output_dir_);
    (void)symbols;
    std::map<std::int32_t, std::string> ids_seen;
    for (const auto& symbol : symbols) {
        const auto id = stable_instrument_id(symbol);
        const auto [it, inserted] = ids_seen.emplace(id, symbol);
        if (!inserted && it->second != symbol) {
            throw std::runtime_error("stable instrument ID hash collision between " + it->second + " and " + symbol);
        }
        instrument_ids_.emplace(symbol, id);
    }
}

CsvOutputManager::~CsvOutputManager() {
    try { flush(); } catch (...) { /* Destructors must not throw. */ }
}

CsvOutputManager::SymbolFiles& CsvOutputManager::files_for(const std::string& symbol) {
    const auto found = files_.find(symbol);
    if (found != files_.end()) return *found->second;

    auto files = std::make_unique<SymbolFiles>();
    const std::string stem = "market_data_" + to_string(venue_) + "_" + symbol + "_" + date_;
    files->market_path = output_dir_ / (stem + ".csv");
    files->orderbook_path = output_dir_ / (stem + "_orderbook.csv");
    files->snapshot_path = output_dir_ / (stem + "_snapshots.jsonl");

    files->market.open(files->market_path, std::ios::out | std::ios::trunc);
    files->orderbook.open(files->orderbook_path, std::ios::out | std::ios::trunc);
    files->snapshots.open(files->snapshot_path, std::ios::out | std::ios::trunc);
    ensure_open(files->market, files->market_path);
    ensure_open(files->orderbook, files->orderbook_path);
    ensure_open(files->snapshots, files->snapshot_path);
    files->market << kMarketHeader << '\n';
    files->orderbook << orderbook_csv_header() << '\n';
    ensure_open(files->market, files->market_path);
    ensure_open(files->orderbook, files->orderbook_path);

    auto [it, inserted] = files_.emplace(symbol, std::move(files));
    (void)inserted;
    return *it->second;
}

void CsvOutputManager::write_market(const MarketEvent& event) {
    auto& files = files_for(event.symbol);
    const auto [seconds, nanoseconds] = split_timestamp(event.recv_ns);
    files.market << seconds << ',' << nanoseconds << ','
                 << to_string(event.venue) << ',' << to_string(event.kind) << ','
                 << event.shard_id << ',' << event.conn_epoch << ',' << event.conn_seq << ','
                 << event.symbol << ',' << csv_escape(event.payload_json) << '\n';
    ensure_open(files.market, files.market_path);
}

void CsvOutputManager::write_orderbook(const MarketEvent& event, char type, char side,
                                       const TopLevels& levels) {
    auto& files = files_for(event.symbol);
    const auto id = instrument_id(event.symbol);
    if (id <= 0) throw std::runtime_error("no stable instrument ID for symbol " + event.symbol);
    files.orderbook << format_orderbook_row(event.recv_ns, files.next_orderbook_seq++, id, type, side, levels) << '\n';
    ensure_open(files.orderbook, files.orderbook_path);
}

void CsvOutputManager::write_snapshot_record(const MarketEvent& event,
                                             const boost::json::value& snapshot) {
    auto& files = files_for(event.symbol);
    boost::json::object record;
    record["conn_epoch"] = event.conn_epoch;
    record["conn_seq"] = event.conn_seq;
    record["symbol"] = event.symbol;
    record["payload"] = snapshot;
    files.snapshots << boost::json::serialize(record) << '\n';
    ensure_open(files.snapshots, files.snapshot_path);
}

void CsvOutputManager::flush() {
    for (auto& [symbol, ptr] : files_) {
        (void)symbol;
        ptr->market.flush();
        ptr->orderbook.flush();
        ptr->snapshots.flush();
        ensure_open(ptr->market, ptr->market_path);
        ensure_open(ptr->orderbook, ptr->orderbook_path);
        ensure_open(ptr->snapshots, ptr->snapshot_path);
    }
}

std::filesystem::path CsvOutputManager::market_path(const std::string& symbol) const {
    const std::string stem = "market_data_" + to_string(venue_) + "_" + symbol + "_" + date_;
    return output_dir_ / (stem + ".csv");
}

std::filesystem::path CsvOutputManager::orderbook_path(const std::string& symbol) const {
    const std::string stem = "market_data_" + to_string(venue_) + "_" + symbol + "_" + date_;
    return output_dir_ / (stem + "_orderbook.csv");
}

std::filesystem::path CsvOutputManager::snapshot_path(const std::string& symbol) const {
    const std::string stem = "market_data_" + to_string(venue_) + "_" + symbol + "_" + date_;
    return output_dir_ / (stem + "_snapshots.jsonl");
}

std::int32_t CsvOutputManager::instrument_id(const std::string& symbol) const {
    const auto it = instrument_ids_.find(symbol);
    return it == instrument_ids_.end() ? 0 : it->second;
}

}  // namespace lob_capture
