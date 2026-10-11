#pragma once

#include "lob_capture/types.hpp"

namespace lob_capture {

MarketEvent parse_combined_message(const RawMessage& message, Venue venue);

}  // namespace lob_capture
