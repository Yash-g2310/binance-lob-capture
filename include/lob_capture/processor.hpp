#pragma once

#include "lob_capture/message_queue.hpp"
#include "lob_capture/types.hpp"

#include <filesystem>
#include <vector>

namespace lob_capture {

void process_live_queue(MessageQueue& queue, Venue venue,
                        const std::vector<std::string>& symbols,
                        const std::filesystem::path& output_dir,
                        Metrics& metrics);

}  // namespace lob_capture
