#include "lob_capture/fixed_point.hpp"

#include <limits>
#include <stdexcept>
#include <string>

namespace lob_capture {

std::int64_t parse_scaled_decimal(std::string_view value) {
    if (value.empty()) throw std::invalid_argument("empty decimal string");
    if (value.front() == '-') throw std::invalid_argument("negative prices/quantities are not supported");
    if (value.front() == '+') value.remove_prefix(1);
    if (value.empty()) throw std::invalid_argument("invalid decimal string");

    const auto dot = value.find('.');
    const std::string_view whole_part = value.substr(0, dot);
    const std::string_view fractional_part = dot == std::string_view::npos
        ? std::string_view{} : value.substr(dot + 1);
    if (whole_part.empty()) throw std::invalid_argument("decimal must have an integer part");
    if (dot != std::string_view::npos && fractional_part.empty()) {
        throw std::invalid_argument("decimal point must be followed by fractional digits");
    }

    std::uint64_t whole = 0;
    for (const char c : whole_part) {
        if (c < '0' || c > '9') throw std::invalid_argument("invalid decimal digit");
        const auto digit = static_cast<std::uint64_t>(c - '0');
        if (whole > (static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) / kDecimalScale - digit) / 10U) {
            throw std::out_of_range("decimal value exceeds int64 scaling range");
        }
        whole = whole * 10U + digit;
    }

    std::uint64_t fraction = 0;
    unsigned digits = 0;
    for (const char c : fractional_part) {
        if (c < '0' || c > '9') throw std::invalid_argument("invalid fractional digit");
        if (digits < kScaleDigits) {
            fraction = fraction * 10U + static_cast<std::uint64_t>(c - '0');
            ++digits;
        } else if (c != '0') {
            throw std::invalid_argument("decimal has precision beyond the documented 8-digit scale");
        }
    }
    while (digits < kScaleDigits) {
        fraction *= 10U;
        ++digits;
    }

    const auto max_value = static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
    if (whole > max_value / kDecimalScale) {
        throw std::out_of_range("decimal value exceeds int64 scaling range");
    }
    const std::uint64_t scaled_whole = whole * kDecimalScale;
    if (fraction > max_value - scaled_whole) {
        throw std::out_of_range("decimal value exceeds int64 scaling range");
    }
    return static_cast<std::int64_t>(scaled_whole + fraction);
}

}  // namespace lob_capture
