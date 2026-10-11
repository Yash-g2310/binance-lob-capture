#include "lob_capture/binance_parser.hpp"

#include <boost/system/system_error.hpp>

#include <stdexcept>
#include <string>

namespace lob_capture {
namespace {

std::string required_string(const boost::json::object& object, const char* key) {
    const auto* value = object.if_contains(key);
    if (value == nullptr || !value->is_string()) {
        throw std::runtime_error(std::string("missing/invalid string field '") + key + "'");
    }
    return std::string(value->as_string());
}

StreamKind kind_from_stream_name(const std::string& stream) {
    if (stream.find("@depth5") != std::string::npos) return StreamKind::Depth5;
    if (stream.find("@depth") != std::string::npos) return StreamKind::DepthDiff;
    if (stream.find("@trade") != std::string::npos) return StreamKind::Trade;
    throw std::runtime_error("unsupported Binance stream name: " + stream);
}

std::string symbol_from_stream_name(const std::string& stream) {
    const auto separator = stream.find('@');
    if (separator == std::string::npos || separator == 0) {
        throw std::runtime_error("cannot derive symbol from stream name: " + stream);
    }
    return uppercase_ascii(stream.substr(0, separator));
}

}  // namespace

MarketEvent parse_combined_message(const RawMessage& message, Venue venue) {
    boost::json::value root;
    try {
        root = boost::json::parse(message.text);
    } catch (const boost::system::system_error& e) {
        throw std::runtime_error(std::string("invalid JSON WebSocket message: ") + e.what());
    }

    if (!root.is_object()) throw std::runtime_error("WebSocket message envelope is not an object");
    const auto& envelope = root.as_object();
    const std::string stream = required_string(envelope, "stream");
    const auto* payload_ptr = envelope.if_contains("data");
    if (payload_ptr == nullptr || !payload_ptr->is_object()) {
        throw std::runtime_error("combined WebSocket message has no object-valued 'data' field");
    }

    MarketEvent event;
    event.recv_ns = message.recv_ns;
    event.venue = venue;
    event.kind = kind_from_stream_name(stream);
    event.shard_id = 0;
    event.conn_epoch = message.conn_epoch;
    event.conn_seq = message.conn_seq;
    event.stream_name = stream;
    event.symbol = symbol_from_stream_name(stream);
    event.payload = *payload_ptr;

    // Most Binance events contain their symbol in the payload. Partial depth
    // messages may omit it, so the combined-stream name is the fallback.
    const auto& payload = event.payload.as_object();
    if (const auto* symbol_value = payload.if_contains("s");
        symbol_value != nullptr && symbol_value->is_string()) {
        event.symbol = uppercase_ascii(std::string(symbol_value->as_string()));
    }
    event.payload_json = boost::json::serialize(event.payload);
    return event;
}

}  // namespace lob_capture
