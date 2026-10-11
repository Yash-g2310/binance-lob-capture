#include "lob_capture/binance_parser.hpp"
#include "lob_capture/csv_writer.hpp"
#include "lob_capture/fixed_point.hpp"
#include "lob_capture/message_queue.hpp"
#include "lob_capture/order_book.hpp"
#include "lob_capture/types.hpp"

#include <boost/json.hpp>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

void check(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error("FAILED: " + message);
}

template <typename Function>
void check_throws(Function function, const std::string& message) {
    bool threw = false;
    try { function(); } catch (...) { threw = true; }
    check(threw, message);
}

boost::json::value parse(const std::string& text) {
    return boost::json::parse(text);
}

void test_fixed_point() {
    using lob_capture::parse_scaled_decimal;
    check(parse_scaled_decimal("65000.12345678") == 6500012345678LL,
          "price converts exactly to scale 1e8");
    check(parse_scaled_decimal("1") == 100000000LL, "integer decimal scales exactly");
    check(parse_scaled_decimal("0.00000001") == 1LL, "minimum scale tick");
    check(parse_scaled_decimal("12.340000000") == 1234000000LL,
          "extra trailing zero decimals are accepted");
    check_throws([] { (void)parse_scaled_decimal("1.000000001"); },
                 "non-zero precision beyond eight decimals is rejected");
    check_throws([] { (void)parse_scaled_decimal("not-a-price"); },
                 "invalid decimal is rejected");
    check_throws([] { (void)parse_scaled_decimal("1."); },
                 "a trailing decimal point without fractional digits is rejected");
    check_throws([] { (void)parse_scaled_decimal("-1"); },
                 "negative price/quantity is rejected");
    check_throws([] { (void)parse_scaled_decimal("92233720369"); },
                 "scaled int64 overflow is rejected");
}

void test_parser() {
    const std::string raw = R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","s":"BTCUSDT","U":11,"u":12,"b":[["100.00","1.25"]],"a":[]}})";
    lob_capture::RawMessage message{1234567890123LL, 2, 7, raw};
    const auto event = lob_capture::parse_combined_message(message, lob_capture::Venue::Spot);
    check(event.symbol == "BTCUSDT", "parser extracts uppercase symbol");
    check(event.kind == lob_capture::StreamKind::DepthDiff, "parser identifies depth stream");
    check(event.conn_epoch == 2 && event.conn_seq == 7, "parser preserves connection metadata");
    check(event.payload.is_object(), "parser extracts inner payload object");
    check(event.payload_json.find("depthUpdate") != std::string::npos,
          "parser serializes inner payload");

    const std::string snapshot = R"({"stream":"ethusdt@depth5@100ms","data":{"lastUpdateId":77,"bids":[["10","1"]],"asks":[["11","2"]]}})";
    message.text = snapshot;
    const auto depth5 = lob_capture::parse_combined_message(message, lob_capture::Venue::Spot);
    check(depth5.symbol == "ETHUSDT", "depth5 symbol falls back to stream name");
    check(depth5.kind == lob_capture::StreamKind::Depth5, "parser identifies depth5 stream");

    message.text = "not json";
    check_throws([&] { (void)lob_capture::parse_combined_message(message, lob_capture::Venue::Spot); },
                 "malformed JSON is rejected");
    message.text = R"({"stream":"btcusdt@trade"})";
    check_throws([&] { (void)lob_capture::parse_combined_message(message, lob_capture::Venue::Spot); },
                 "missing data envelope is rejected");
    message.text = R"({"stream":"btcusdt@unknown","data":{}})";
    check_throws([&] { (void)lob_capture::parse_combined_message(message, lob_capture::Venue::Spot); },
                 "unknown stream type is rejected");
}

void test_spot_order_book() {
    lob_capture::OrderBook book;
    const auto snapshot = parse(R"({"lastUpdateId":10,"bids":[["100","1"],["99","2"]],"asks":[["101","3"],["102","4"]]})");
    book.install_snapshot(snapshot, lob_capture::Venue::Spot);
    check(!book.synchronized(), "snapshot alone is provisional until a diff bridges it");

    const auto stale = parse(R"({"U":8,"u":10,"b":[],"a":[]})");
    check(book.apply_diff(stale, lob_capture::Venue::Spot) == lob_capture::ApplyResult::IgnoredStale,
          "spot ignores depth updates already contained in the snapshot");

    const auto gap = parse(R"({"U":12,"u":13,"b":[],"a":[]})");
    check(book.apply_diff(gap, lob_capture::Venue::Spot) == lob_capture::ApplyResult::Gap,
          "spot detects a sequence gap");

    const auto update = parse(R"({"U":10,"u":11,"b":[["100","3"],["99","0"]],"a":[["101","0"],["100.5","1"]]})");
    check(book.apply_diff(update, lob_capture::Venue::Spot) == lob_capture::ApplyResult::Applied,
          "spot applies update bridging snapshot ID");
    check(book.synchronized(), "book is synchronized after valid first diff");

    const auto before_malformed = book.top5();
    const auto malformed_second_side = parse(
        R"({"U":11,"u":12,"b":[["100","9"]],"a":[["not-a-price","1"]]})");
    check_throws([&] { (void)book.apply_diff(malformed_second_side, lob_capture::Venue::Spot); },
                 "malformed ask level is rejected before applying either side");
    const auto after_malformed = book.top5();
    check(after_malformed.bids.size() == before_malformed.bids.size() &&
          after_malformed.bids[0].quantity == before_malformed.bids[0].quantity,
          "malformed ask does not partially mutate the bid book");

    const auto top = book.top5();
    check(top.bids.size() == 1 && top.bids[0].price == 10000000000LL &&
          top.bids[0].quantity == 300000000LL, "bid updated and zero quantity removes level");
    check(top.asks.size() == 2 && top.asks[0].price == 10050000000LL,
          "ask zero removal and best-ask ordering");
}

void test_usdm_sequence() {
    lob_capture::OrderBook book;
    book.install_snapshot(parse(R"({"lastUpdateId":20,"bids":[["100","1"]],"asks":[["101","1"]]})"),
                          lob_capture::Venue::Usdm);
    const auto first = parse(R"({"U":19,"u":21,"pu":18,"b":[["100","2"]],"a":[]})");
    check(book.apply_diff(first, lob_capture::Venue::Usdm) == lob_capture::ApplyResult::Applied,
          "USD-M first diff overlaps the snapshot update ID");
    const auto next = parse(R"({"U":22,"u":23,"pu":21,"b":[["99","1"]],"a":[]})");
    check(book.apply_diff(next, lob_capture::Venue::Usdm) == lob_capture::ApplyResult::Applied,
          "USD-M pu equals previous final update ID");
    const auto gap = parse(R"({"U":24,"u":25,"pu":22,"b":[],"a":[]})");
    check(book.apply_diff(gap, lob_capture::Venue::Usdm) == lob_capture::ApplyResult::Gap,
          "USD-M detects a pu continuity break");
}

void test_depth5_and_csv() {
    const auto partial = parse(R"({"lastUpdateId":10,"bids":[["100","1"],["101","2"]],"asks":[["102","3"]]})");
    const auto top = lob_capture::top5_from_partial_depth(partial);
    check(top.bids.size() == 2 && top.bids[0].price == 10100000000LL,
          "depth5 bids sorted best first");
    check(top.asks.size() == 1 && top.asks[0].price == 10200000000LL,
          "depth5 asks sorted best first");

    const auto escaped = lob_capture::csv_escape("{\"x\":\"a,b\"}");
    check(escaped == "\"{\"\"x\"\":\"\"a,b\"\"}\"", "CSV quotes embedded JSON correctly");
    const auto parsed = lob_capture::parse_csv_record("1,\"a,b\",\"say \"\"hi\"\"\"");
    check(parsed.size() == 3 && parsed[1] == "a,b" && parsed[2] == "say \"hi\"",
          "CSV reader understands commas and doubled quotes inside quoted fields");

    lob_capture::TopLevels five;
    five.bids.push_back({100, 1});
    const auto row = lob_capture::format_orderbook_row(1'700'000'000'123'456'789LL, 1, 42, 'D', 'B', five);
    check(lob_capture::parse_csv_record(row).size() == 26, "order-book rows have exactly 26 columns");
    check(lob_capture::orderbook_csv_header() ==
              "tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,ask_size3,ask_size4",
          "order-book header matches exact required schema");
    check_throws([] { (void)lob_capture::parse_csv_record("1,\"unterminated"); },
                 "unterminated CSV quoted field is rejected");
}

void test_csv_output_files() {
    const auto directory = std::filesystem::temp_directory_path() / "binance_lob_csv_output_test";
    std::filesystem::remove_all(directory);

    std::filesystem::path market_path;
    std::filesystem::path orderbook_path;
    std::filesystem::path snapshots_path;
    {
        lob_capture::CsvOutputManager output(directory, lob_capture::Venue::Spot, {"BTCUSDT"});
        lob_capture::MarketEvent event;
        event.recv_ns = 1'700'000'000'123'456'789LL;
        event.venue = lob_capture::Venue::Spot;
        event.kind = lob_capture::StreamKind::Trade;
        event.conn_epoch = 2;
        event.conn_seq = 7;
        event.symbol = "BTCUSDT";
        event.payload_json = R"({"e":"trade","p":"1,2","note":"say \"hi\""})";
        event.payload = parse(R"({"lastUpdateId":10,"bids":[["100","1"]],"asks":[["101","1"]]})");

        output.write_market(event);
        output.write_orderbook(event, 'D', 'B', lob_capture::TopLevels{{{100, 5}}, {{101, 7}}});
        output.write_snapshot_record(event, event.payload);
        output.flush();
        market_path = output.market_path("BTCUSDT");
        orderbook_path = output.orderbook_path("BTCUSDT");
        snapshots_path = output.snapshot_path("BTCUSDT");
    }

    const auto read_lines = [](const std::filesystem::path& path) {
        std::ifstream in(path);
        check(static_cast<bool>(in), "output file exists and opens: " + path.string());
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(in, line)) lines.push_back(line);
        return lines;
    };

    const auto market_lines = read_lines(market_path);
    check(market_lines.size() == 2, "market CSV contains header and one event row");
    check(lob_capture::parse_csv_record(market_lines[0]).size() == 9,
          "market CSV header contains exactly nine columns");
    const auto market_row = lob_capture::parse_csv_record(market_lines[1]);
    check(market_row.size() == 9 && market_row[2] == "spot" &&
          market_row[3] == "trade" && market_row[5] == "2" && market_row[6] == "7",
          "market CSV row preserves metadata columns");
    check(market_row[8] == R"({"e":"trade","p":"1,2","note":"say \"hi\""})",
          "market CSV round-trips quoted JSON with an embedded comma");

    const auto book_lines = read_lines(orderbook_path);
    check(book_lines.size() == 2, "order-book CSV contains header and one row");
    check(lob_capture::parse_csv_record(book_lines[0]).size() == 26 &&
          lob_capture::parse_csv_record(book_lines[1]).size() == 26,
          "order-book CSV file has the exact 26-column contract");

    const auto snapshot_lines = read_lines(snapshots_path);
    check(snapshot_lines.size() == 1, "REST snapshot sidecar contains one JSONL record");
    const auto snapshot_record = parse(snapshot_lines[0]);
    check(lob_capture::json_u64(snapshot_record.as_object().at("conn_epoch"), "conn_epoch") == 2 &&
          lob_capture::json_u64(snapshot_record.as_object().at("conn_seq"), "conn_seq") == 7,
          "snapshot sidecar preserves the replay key");

    std::filesystem::remove_all(directory);
}

void test_message_queue() {
    lob_capture::MessageQueue queue(2);
    check(queue.push(lob_capture::RawMessage{1, 0, 1, "first"}), "queue accepts first message");
    check(queue.push(lob_capture::RawMessage{2, 0, 2, "second"}), "queue accepts second message");
    check(queue.high_water_mark() == 2, "queue records high-water mark");
    queue.close();
    lob_capture::RawMessage message;
    check(queue.pop(message) && message.text == "first", "queue preserves FIFO order after close");
    check(queue.pop(message) && message.text == "second", "queue drains all queued messages after close");
    check(!queue.pop(message), "closed and empty queue signals end of stream");
    check(!queue.push(lob_capture::RawMessage{}), "closed queue rejects new messages");
}

}  // namespace

int main() {
    const std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"fixed-point conversion", test_fixed_point},
        {"combined message parser", test_parser},
        {"Spot order book", test_spot_order_book},
        {"USD-M sequence continuity", test_usdm_sequence},
        {"depth5 and CSV", test_depth5_and_csv},
        {"bounded FIFO queue", test_message_queue},
        {"CSV files and snapshot sidecar", test_csv_output_files},
    };

    std::size_t passed = 0;
    for (const auto& [name, test] : tests) {
        try {
            test();
            ++passed;
            std::cout << "[PASS] " << name << '\n';
        } catch (const std::exception& error) {
            std::cerr << "[FAIL] " << name << ": " << error.what() << '\n';
            return 1;
        }
    }
    std::cout << passed << " tests passed.\n";
    return 0;
}
