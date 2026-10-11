#include "lob_capture/websocket_client.hpp"

#include "lob_capture/types.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <openssl/ssl.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <sys/socket.h>
#include <sys/time.h>
#include <utility>

namespace lob_capture {
namespace beast = boost::beast;
namespace net = boost::asio;
namespace ssl = net::ssl;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

namespace {

void set_sni(ssl::stream<tcp::socket>& stream, const std::string& host) {
    if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
        throw std::runtime_error("failed to set TLS SNI hostname for " + host);
    }
}

void set_socket_timeouts(tcp::socket& socket, long seconds) {
    const timeval timeout{seconds, 0};
    if (::setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO,
                     &timeout, sizeof(timeout)) != 0 ||
        ::setsockopt(socket.native_handle(), SOL_SOCKET, SO_SNDTIMEO,
                     &timeout, sizeof(timeout)) != 0) {
        throw std::runtime_error("failed to configure TCP socket timeouts");
    }
}

std::string make_stream_target(Venue venue, const std::vector<std::string>& symbols) {
    std::string target = venue == Venue::Spot
        ? "/stream?streams=" : "/public/stream?streams=";
    bool first = true;
    for (const auto& symbol : symbols) {
        const std::string lower = lowercase_ascii(symbol);
        const std::vector<std::string> streams = {
            lower + "@depth@100ms",
            lower + "@depth5@100ms",
            lower + "@trade"
        };
        for (const auto& stream : streams) {
            if (!first) target += '/';
            target += stream;
            first = false;
        }
    }
    if (target.size() > 8192) {
        throw std::invalid_argument("combined stream URL is too long; reduce --symbols or split the capture");
    }
    if (symbols.size() * 3U > 1024U) {
        throw std::invalid_argument("Binance combined connection supports at most 1024 streams; reduce --symbols");
    }
    return target;
}

}  // namespace

WebSocketClient::WebSocketClient(Venue venue, std::vector<std::string> symbols)
    : venue_(venue), symbols_(std::move(symbols)) {
    if (venue_ == Venue::Spot) {
        host_ = "stream.binance.com";
        port_ = "9443";
    } else {
        host_ = "fstream.binance.com";
        port_ = "443";
    }
    target_ = make_stream_target(venue_, symbols_);
}

void WebSocketClient::run(MessageQueue& queue, Metrics& metrics,
                          const std::function<bool()>& should_stop) {
    std::uint64_t epoch = 0;
    unsigned int backoff_seconds = 1;

    while (!should_stop()) {
        bool connection_completed = false;
        std::chrono::steady_clock::time_point connected_at{};
        try {
            std::cout << "Connecting to Binance " << to_string(venue_)
                      << " WebSocket (conn_epoch=" << epoch << ")...\n";
            read_one_connection(queue, metrics, epoch, connection_completed, connected_at, should_stop);
            if (should_stop()) break;
            throw std::runtime_error("WebSocket read loop ended unexpectedly");
        } catch (const std::exception& error) {
            if (should_stop()) break;
            std::cerr << "WebSocket connection error: " << error.what() << '\n';
            if (connection_completed) {
                metrics.reconnects.fetch_add(1);
                // Increment only after a previously established connection failed.
                ++epoch;
                // Reset backoff only after a connection survived for a full minute.
                // A rapid connect/disconnect loop therefore grows the delay.
                if (std::chrono::steady_clock::now() - connected_at >= std::chrono::seconds(60)) {
                    backoff_seconds = 1;
                }
            }
            for (unsigned int waited = 0; waited < backoff_seconds * 10U && !should_stop(); ++waited) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            backoff_seconds = std::min(backoff_seconds * 2U, 30U);
        }
    }
}

void WebSocketClient::read_one_connection(MessageQueue& queue, Metrics& metrics,
                                          std::uint64_t epoch, bool& connection_completed,
                                          std::chrono::steady_clock::time_point& connected_at,
                                          const std::function<bool()>& should_stop) {
    net::io_context ioc;
    ssl::context tls_context{ssl::context::tls_client};
    tls_context.set_default_verify_paths();

    tcp::resolver resolver{ioc};
    const auto endpoints = resolver.resolve(host_, port_);
    websocket::stream<ssl::stream<tcp::socket>> ws{ioc, tls_context};

    ws.next_layer().set_verify_mode(ssl::verify_peer);
    ws.next_layer().set_verify_callback(ssl::host_name_verification(host_));
    set_sni(ws.next_layer(), host_);

    const auto endpoint = net::connect(beast::get_lowest_layer(ws), endpoints);
    set_socket_timeouts(beast::get_lowest_layer(ws), 65);
    ws.next_layer().handshake(ssl::stream_base::client);

    auto timeout = websocket::stream_base::timeout::suggested(beast::role_type::client);
    timeout.idle_timeout = std::chrono::seconds(60);
    timeout.keep_alive_pings = true;
    ws.set_option(timeout);
    ws.read_message_max(8U * 1024U * 1024U);

    ws.handshake(host_ + ":" + std::to_string(endpoint.port()), target_);
    connection_completed = true;
    connected_at = std::chrono::steady_clock::now();
    std::cout << "Connected to Binance " << to_string(venue_) << " WebSocket.\n";

    std::uint64_t connection_sequence = 0;
    beast::flat_buffer buffer;  // Reuse this buffer between messages.
    while (!should_stop()) {
        ws.read(buffer);  // Synchronous: waits for one complete WebSocket message.
        RawMessage message;
        message.recv_ns = wall_clock_ns();
        message.conn_epoch = epoch;
        message.conn_seq = ++connection_sequence;
        message.text = beast::buffers_to_string(buffer.data());
        metrics.messages_received.fetch_add(1);
        if (!queue.push(std::move(message))) break;
        buffer.consume(buffer.size());
    }

    boost::system::error_code close_error;
    ws.close(websocket::close_code::normal, close_error);
    if (close_error == net::ssl::error::stream_truncated && should_stop()) {
        // Some public-stream peers close the underlying TLS socket without a
        // TLS close_notify after our WebSocket close frame. At this point the
        // capture loop has stopped and the processor drains the queue; treat
        // this specific shutdown-only condition as non-fatal, but explain it.
        std::cerr << "WebSocket shutdown note: peer ended TLS without close_notify "
                     "after capture stopped; queued messages will still be drained.\n";
    } else if (close_error && !should_stop()) {
        throw boost::system::system_error(close_error, "WebSocket close");
    } else if (close_error) {
        std::cerr << "WebSocket close note: " << close_error.message() << '\n';
    }
}

}  // namespace lob_capture
