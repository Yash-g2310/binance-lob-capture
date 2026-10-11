#include "lob_capture/types.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <limits>
#include <stdexcept>

namespace lob_capture {

Venue parse_venue(const std::string& value) {
    if (value == "spot") return Venue::Spot;
    if (value == "usdm") return Venue::Usdm;
    throw std::invalid_argument("venue must be 'spot' or 'usdm'");
}

std::string to_string(Venue venue) {
    return venue == Venue::Spot ? "spot" : "usdm";
}

std::string to_string(StreamKind kind) {
    switch (kind) {
        case StreamKind::DepthDiff: return "depth_diff";
        case StreamKind::Depth5: return "depth5";
        case StreamKind::Trade: return "trade";
    }
    throw std::logic_error("unknown stream kind");
}

StreamKind parse_stream_kind(const std::string& value) {
    if (value == "depth_diff" || value == "0") return StreamKind::DepthDiff;
    if (value == "depth5" || value == "1") return StreamKind::Depth5;
    if (value == "trade" || value == "2") return StreamKind::Trade;
    throw std::invalid_argument("unknown stream_kind: " + value);
}

std::int64_t wall_clock_ns() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
}

std::string utc_date_now() {
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
#if defined(_WIN32)
    gmtime_s(&utc, &now);
#else
    gmtime_r(&now, &utc);
#endif
    char buffer[16]{};
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &utc) == 0) {
        throw std::runtime_error("failed to format UTC date");
    }
    return buffer;
}

std::string uppercase_ascii(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
    });
    return text;
}

std::string lowercase_ascii(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return text;
}

std::int32_t stable_instrument_id(const std::string& symbol) {
    // 31-bit FNV-1a hash: deterministic across runs and independent of CLI order.
    std::uint32_t hash = 2166136261U;
    for (const unsigned char c : symbol) {
        hash ^= static_cast<std::uint32_t>(c);
        hash *= 16777619U;
    }
    hash &= 0x7fffffffU;
    if (hash == 0U) hash = 1U;
    return static_cast<std::int32_t>(hash);
}

std::uint64_t json_u64(const boost::json::value& value, const std::string& field_name) {
    if (value.is_uint64()) return value.as_uint64();
    if (value.is_int64() && value.as_int64() >= 0) {
        return static_cast<std::uint64_t>(value.as_int64());
    }
    throw std::runtime_error("JSON field '" + field_name + "' is not a non-negative integer");
}

}  // namespace lob_capture
