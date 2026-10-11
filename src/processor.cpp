#include "lob_capture/processor.hpp"

#include "lob_capture/binance_parser.hpp"
#include "lob_capture/csv_writer.hpp"
#include "lob_capture/order_book.hpp"
#include "lob_capture/rest_client.hpp"

#include <chrono>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>

namespace lob_capture {

void process_live_queue(MessageQueue& queue, Venue venue,
                        const std::vector<std::string>& symbols,
                        const std::filesystem::path& output_dir,
                        Metrics& metrics) {
    CsvOutputManager output(output_dir, venue, symbols);
    std::map<std::string, OrderBook> books;
    std::uint64_t active_epoch = 0;
    bool have_epoch = false;
    std::map<std::string, std::chrono::steady_clock::time_point> next_snapshot_retry;

    RawMessage raw;
    while (queue.pop(raw)) {
        MarketEvent event;
        try {
            event = parse_combined_message(raw, venue);
        } catch (const std::exception& error) {
            metrics.parse_errors.fetch_add(1);
            std::cerr << "Skipping malformed WebSocket message (epoch=" << raw.conn_epoch
                      << ", seq=" << raw.conn_seq << "): " << error.what() << '\n';
            continue;
        }

        if (!have_epoch) {
            active_epoch = event.conn_epoch;
            have_epoch = true;
        } else if (event.conn_epoch != active_epoch) {
            std::cerr << "Connection epoch changed from " << active_epoch << " to "
                      << event.conn_epoch << "; invalidating all local books for resynchronization.\n";
            for (auto& [symbol, book] : books) {
                (void)symbol;
                book.invalidate();
            }
            active_epoch = event.conn_epoch;
        }

        // Preserve every valid logical Binance payload before touching the LOB.
        output.write_market(event);
        metrics.market_rows_written.fetch_add(1);
        metrics.messages_processed.fetch_add(1);

        try {
            if (event.kind == StreamKind::Trade) {
                metrics.trade_messages.fetch_add(1);
                continue;  // Trades are audited but do not directly mutate a diff-based LOB.
            }

            if (event.kind == StreamKind::Depth5) {
                metrics.depth5_messages.fetch_add(1);
                // depth5 is a partial top-five view. Emit it as a replacement
                // view, but do not erase the deeper REST-synchronized book.
                output.write_orderbook(event, 'S', 'N', top5_from_partial_depth(event.payload));
                metrics.orderbook_rows_written.fetch_add(1);
                continue;
            }

            metrics.depth_messages.fetch_add(1);
            auto& book = books[event.symbol];
            bool written = false;
            bool deferred_for_rest_backoff = false;
            for (int attempt = 0; attempt < 3 && !written; ++attempt) {
                if (!book.has_snapshot()) {
                    const auto retry = next_snapshot_retry.find(event.symbol);
                    if (retry != next_snapshot_retry.end() &&
                        std::chrono::steady_clock::now() < retry->second) {
                        deferred_for_rest_backoff = true;
                        break;
                    }

                    boost::json::value snapshot;
                    try {
                        snapshot = RestClient::fetch_depth_snapshot(venue, event.symbol);
                    } catch (const std::exception& error) {
                        metrics.rest_errors.fetch_add(1);
                        next_snapshot_retry[event.symbol] =
                            std::chrono::steady_clock::now() + std::chrono::seconds(2);
                        book.invalidate();
                        std::cerr << "REST snapshot failed for " << event.symbol
                                  << "; book remains unsynchronized and REST retries are throttled: "
                                  << error.what() << '\n';
                        deferred_for_rest_backoff = true;
                        break;
                    }
                    book.install_snapshot(snapshot, venue);
                    output.write_snapshot_record(event, snapshot);
                    metrics.snapshots_fetched.fetch_add(1);
                    next_snapshot_retry.erase(event.symbol);
                }

                const ApplyResult result = book.apply_diff(event.payload, venue);
                if (result == ApplyResult::Applied) {
                    output.write_orderbook(event, 'D', side_for_depth_event(event.payload), book.top5());
                    metrics.orderbook_rows_written.fetch_add(1);
                    written = true;
                } else if (result == ApplyResult::IgnoredStale) {
                    // The event is already represented by the REST snapshot.
                    written = true;
                } else if (result == ApplyResult::Gap) {
                    metrics.sequence_gaps.fetch_add(1);
                    std::cerr << "Depth sequence gap for " << event.symbol << " at epoch "
                              << event.conn_epoch << ", conn_seq " << event.conn_seq
                              << "; refetching snapshot.\n";
                    book.invalidate();
                } else {
                    book.invalidate();
                }
            }
            if (!written && !deferred_for_rest_backoff) {
                std::cerr << "Could not synchronize " << event.symbol
                          << " after repeated snapshot attempts; waiting for a later depth event.\n";
                book.invalidate();
            }
        } catch (const std::exception& error) {
            // Malformed depth payloads or other per-event issues are logged and
            // invalidate the book. File write failures are fatal: never pretend
            // the audit is complete after an output error.
            if (std::string(error.what()).find("CSV output write failed") != std::string::npos) {
                throw;
            }
            if (event.kind == StreamKind::DepthDiff) books[event.symbol].invalidate();
            metrics.processing_errors.fetch_add(1);
            std::cerr << "Event processing warning for " << event.symbol << ": "
                      << error.what() << '\n';
        }
    }

    output.flush();
    metrics.queue_high_water.store(queue.high_water_mark());
}

}  // namespace lob_capture
