#include "lob_capture/message_queue.hpp"
#include "lob_capture/processor.hpp"
#include "lob_capture/replay.hpp"
#include "lob_capture/types.hpp"
#include "lob_capture/websocket_client.hpp"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <chrono>
#include <csignal>
#include <exception>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

volatile std::sig_atomic_t g_stop_requested = 0;

void signal_handler(int) {
    g_stop_requested = 1;
}

struct Options {
    lob_capture::Venue venue{lob_capture::Venue::Spot};
    std::vector<std::string> symbols{"BTCUSDT"};
    std::filesystem::path output_dir{"./output"};
    std::filesystem::path replay_file;
    bool replay{false};
    bool output_dir_explicit{false};
    int duration_seconds{0};
    std::size_t queue_capacity{50'000};
};

void print_usage(const char* executable) {
    std::cout
        << "Usage: " << executable << " [options]\n\n"
        << "Live capture options:\n"
        << "  --venue spot|usdm          Binance venue (default: spot)\n"
        << "  --symbols BTCUSDT,ETHUSDT  Comma-separated symbols (default: BTCUSDT)\n"
        << "  --output-dir DIR           Output directory (default: ./output)\n"
        << "  --duration SECONDS         Stop after N seconds; omit to run until Ctrl+C\n"
        << "  --queue-capacity N         Bounded FIFO message capacity (default: 50000)\n\n"
        << "Replay options:\n"
        << "  --replay MARKET_DATA.csv   Rebuild order-book CSV offline; no network calls\n"
        << "  --output-dir DIR           Replay output directory (default: MARKET_DATA_DIR/replay_output)\n"
        << "  --help                     Show this help\n";
}

std::vector<std::string> parse_symbols(const std::string& text) {
    std::vector<std::string> symbols;
    std::size_t start = 0;
    while (start <= text.size()) {
        const auto comma = text.find(',', start);
        const auto end = comma == std::string::npos ? text.size() : comma;
        if (end == start) throw std::invalid_argument("--symbols contains an empty symbol");
        std::string symbol = lob_capture::uppercase_ascii(text.substr(start, end - start));
        for (const unsigned char c : symbol) {
            if (!((c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'))) {
                throw std::invalid_argument("invalid symbol '" + symbol + "'; use alphanumeric Binance symbols");
            }
        }
        symbols.push_back(std::move(symbol));
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    std::sort(symbols.begin(), symbols.end());
    const auto duplicate = std::adjacent_find(symbols.begin(), symbols.end());
    if (duplicate != symbols.end()) throw std::invalid_argument("duplicate symbol: " + *duplicate);
    return symbols;
}

int parse_positive_int(const std::string& value, const char* option, bool allow_zero = false) {
    std::size_t used = 0;
    long long parsed = 0;
    try {
        parsed = std::stoll(value, &used);
    } catch (...) {
        throw std::invalid_argument(std::string("invalid value for ") + option);
    }
    if (used != value.size() || parsed < (allow_zero ? 0 : 1) || parsed > 2'147'483'647LL) {
        throw std::invalid_argument(std::string("invalid value for ") + option);
    }
    return static_cast<int>(parsed);
}

Options parse_args(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto next = [&](const char* name) -> std::string {
            if (i + 1 >= argc) throw std::invalid_argument(std::string("missing value for ") + name);
            return argv[++i];
        };
        if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            std::exit(0);
        } else if (arg == "--venue") {
            options.venue = lob_capture::parse_venue(next("--venue"));
        } else if (arg == "--symbols") {
            options.symbols = parse_symbols(next("--symbols"));
        } else if (arg == "--output-dir") {
            options.output_dir = next("--output-dir");
            options.output_dir_explicit = true;
        } else if (arg == "--duration") {
            options.duration_seconds = parse_positive_int(next("--duration"), "--duration");
        } else if (arg == "--queue-capacity") {
            const int capacity = parse_positive_int(next("--queue-capacity"), "--queue-capacity");
            if (capacity > 500'000) {
                throw std::invalid_argument("--queue-capacity must not exceed 500000 messages");
            }
            options.queue_capacity = static_cast<std::size_t>(capacity);
        } else if (arg == "--replay") {
            options.replay = true;
            options.replay_file = next("--replay");
        } else {
            throw std::invalid_argument("unknown argument: " + arg);
        }
    }

    if (options.replay && options.output_dir_explicit == false) {
        options.output_dir = options.replay_file.parent_path() / "replay_output";
        if (options.output_dir.empty()) options.output_dir = "./replay_output";
    }
    return options;
}

void print_metrics(const lob_capture::Metrics& metrics, std::size_t queue_high_water) {
    std::cout << "\n========== Capture statistics ==========\n"
              << "Messages received       : " << metrics.messages_received.load() << '\n'
              << "Messages processed      : " << metrics.messages_processed.load() << '\n'
              << "Depth updates           : " << metrics.depth_messages.load() << '\n'
              << "Depth5 snapshots        : " << metrics.depth5_messages.load() << '\n'
              << "Trades                  : " << metrics.trade_messages.load() << '\n'
              << "Parse errors            : " << metrics.parse_errors.load() << '\n'
              << "Sequence gaps           : " << metrics.sequence_gaps.load() << '\n'
              << "Reconnects              : " << metrics.reconnects.load() << '\n'
              << "Market CSV rows         : " << metrics.market_rows_written.load() << '\n'
              << "Order-book CSV rows     : " << metrics.orderbook_rows_written.load() << '\n'
              << "Snapshots fetched       : " << metrics.snapshots_fetched.load() << '\n'
              << "REST errors             : " << metrics.rest_errors.load() << '\n'
              << "Processing errors       : " << metrics.processing_errors.load() << '\n'
              << "Queue high-water mark   : " << queue_high_water << '\n'
              << "========================================\n";
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parse_args(argc, argv);
        if (options.replay) {
            lob_capture::Metrics metrics;
            lob_capture::replay_market_csv(options.replay_file, options.output_dir, metrics);
            return 0;
        }

        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);

        std::cout << "Venue: " << lob_capture::to_string(options.venue) << '\n'
                  << "Symbols:";
        for (const auto& symbol : options.symbols) std::cout << ' ' << symbol;
        std::cout << "\nOutput directory: " << options.output_dir << '\n'
                  << "Price scale: 10^8; quantity scale: 10^8\n"
                  << "Press Ctrl+C to stop.\n";

        // Validate/build the combined-stream URL before starting any threads so
        // bad CLI input cannot leave a joinable worker thread behind.
        lob_capture::WebSocketClient client(options.venue, options.symbols);
        lob_capture::Metrics metrics;
        lob_capture::MessageQueue queue(options.queue_capacity);
        std::atomic<bool> processor_failed{false};
        std::exception_ptr processor_exception;
        const auto start = std::chrono::steady_clock::now();
        const auto should_stop = [&]() {
            if (g_stop_requested != 0 || processor_failed.load()) return true;
            if (options.duration_seconds > 0 &&
                std::chrono::steady_clock::now() - start >=
                    std::chrono::seconds(options.duration_seconds)) return true;
            return false;
        };

        std::thread processor([&]() {
            try {
                lob_capture::process_live_queue(queue, options.venue, options.symbols,
                                                options.output_dir, metrics);
            } catch (...) {
                processor_exception = std::current_exception();
                processor_failed.store(true);
                queue.close();
            }
        });

        client.run(queue, metrics, should_stop);
        queue.close();
        processor.join();
        if (processor_exception) std::rethrow_exception(processor_exception);
        print_metrics(metrics, queue.high_water_mark());
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Fatal error: " << error.what() << '\n';
        return 1;
    }
}
