#pragma once

#include <boost/json.hpp>

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace lob_capture {

enum class Venue { Spot, Usdm };
enum class StreamKind { DepthDiff, Depth5, Trade };

enum class ApplyResult { Applied, IgnoredStale, Gap, NoSnapshot };

struct RawMessage {
    std::int64_t recv_ns{};
    std::uint64_t conn_epoch{};
    std::uint64_t conn_seq{};
    std::string text;
};

struct MarketEvent {
    std::int64_t recv_ns{};
    Venue venue{Venue::Spot};
    StreamKind kind{StreamKind::Trade};
    std::uint32_t shard_id{0};
    std::uint64_t conn_epoch{};
    std::uint64_t conn_seq{};
    std::string symbol;
    std::string stream_name;
    boost::json::value payload;
    std::string payload_json;
};

struct Level {
    std::int64_t price{};
    std::int64_t quantity{};
};

struct TopLevels {
    std::vector<Level> bids;
    std::vector<Level> asks;
};

struct Metrics {
    std::atomic<std::uint64_t> messages_received{0};
    std::atomic<std::uint64_t> messages_processed{0};
    std::atomic<std::uint64_t> depth_messages{0};
    std::atomic<std::uint64_t> depth5_messages{0};
    std::atomic<std::uint64_t> trade_messages{0};
    std::atomic<std::uint64_t> parse_errors{0};
    std::atomic<std::uint64_t> sequence_gaps{0};
    std::atomic<std::uint64_t> reconnects{0};
    std::atomic<std::uint64_t> market_rows_written{0};
    std::atomic<std::uint64_t> orderbook_rows_written{0};
    std::atomic<std::uint64_t> snapshots_fetched{0};
    std::atomic<std::uint64_t> rest_errors{0};
    std::atomic<std::uint64_t> processing_errors{0};
    std::atomic<std::uint64_t> queue_high_water{0};
};

Venue parse_venue(const std::string& value);
std::string to_string(Venue venue);
std::string to_string(StreamKind kind);
StreamKind parse_stream_kind(const std::string& value);
std::int64_t wall_clock_ns();
std::string utc_date_now();
std::string uppercase_ascii(std::string text);
std::string lowercase_ascii(std::string text);
std::int32_t stable_instrument_id(const std::string& symbol);
std::uint64_t json_u64(const boost::json::value& value, const std::string& field_name);

}  // namespace lob_capture
