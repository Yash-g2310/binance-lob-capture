#pragma once

#include "lob_capture/message_queue.hpp"
#include "lob_capture/types.hpp"

#include <chrono>
#include <functional>
#include <string>
#include <vector>

namespace lob_capture {

class WebSocketClient {
public:
    WebSocketClient(Venue venue, std::vector<std::string> symbols);

    // Runs the reader/reconnect loop until should_stop() returns true.
    void run(MessageQueue& queue, Metrics& metrics,
             const std::function<bool()>& should_stop);

private:
    Venue venue_;
    std::vector<std::string> symbols_;
    std::string host_;
    std::string port_;
    std::string target_;

    void read_one_connection(MessageQueue& queue, Metrics& metrics,
                             std::uint64_t epoch, bool& connection_completed,
                             std::chrono::steady_clock::time_point& connected_at,
                             const std::function<bool()>& should_stop);
};

}  // namespace lob_capture
