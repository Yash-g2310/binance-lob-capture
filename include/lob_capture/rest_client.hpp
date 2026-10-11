#pragma once

#include "lob_capture/types.hpp"

#include <string>

namespace lob_capture {

class RestClient {
public:
    // Fetches one public market-depth snapshot. No API key is required.
    static boost::json::value fetch_depth_snapshot(Venue venue, const std::string& symbol);
};

}  // namespace lob_capture
