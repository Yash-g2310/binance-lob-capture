#pragma once

#include <cstdint>
#include <string_view>

namespace lob_capture {

// The CSV contract uses scaled integer prices and quantities; no floating point.
inline constexpr std::uint64_t kDecimalScale = 100'000'000ULL;
inline constexpr unsigned kScaleDigits = 8;

std::int64_t parse_scaled_decimal(std::string_view value);

}  // namespace lob_capture
