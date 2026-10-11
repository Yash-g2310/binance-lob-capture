#include "lob_capture/rest_client.hpp"

#include <boost/system/system_error.hpp>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <openssl/ssl.h>

#include <chrono>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>

namespace lob_capture {
namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

namespace {

void set_socket_timeouts(tcp::socket& socket, long seconds) {
    const timeval timeout{seconds, 0};
    if (::setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO,
                     &timeout, sizeof(timeout)) != 0 ||
        ::setsockopt(socket.native_handle(), SOL_SOCKET, SO_SNDTIMEO,
                     &timeout, sizeof(timeout)) != 0) {
        throw std::runtime_error("failed to configure REST TCP socket timeouts");
    }
}

void set_sni(beast::ssl_stream<beast::tcp_stream>& stream, const std::string& host) {
    if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
        throw std::runtime_error("failed to set TLS SNI hostname for " + host);
    }
}

}  // namespace

boost::json::value RestClient::fetch_depth_snapshot(Venue venue, const std::string& symbol) {
    const std::string host = venue == Venue::Spot ? "api.binance.com" : "fapi.binance.com";
    const std::string port = "443";
    const std::string target = venue == Venue::Spot
        ? "/api/v3/depth?symbol=" + symbol + "&limit=5000"
        : "/fapi/v1/depth?symbol=" + symbol + "&limit=1000";

    net::io_context ioc;
    ssl::context tls_context{ssl::context::tls_client};
    tls_context.set_default_verify_paths();

    tcp::resolver resolver{ioc};
    const auto endpoints = resolver.resolve(host, port);
    beast::ssl_stream<beast::tcp_stream> stream{ioc, tls_context};
    stream.set_verify_mode(ssl::verify_peer);
    stream.set_verify_callback(ssl::host_name_verification(host));
    set_sni(stream, host);

    beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(10));
    beast::get_lowest_layer(stream).connect(endpoints);
    set_socket_timeouts(stream.next_layer().socket(), 10);
    stream.handshake(ssl::stream_base::client);

    http::request<http::empty_body> request{http::verb::get, target, 11};
    request.set(http::field::host, host);
    request.set(http::field::user_agent, "binance-lob-capture/1.0 (C++17)");
    request.set(http::field::accept, "application/json");
    request.keep_alive(false);

    beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(10));
    http::write(stream, request);
    beast::flat_buffer buffer;
    http::response<http::string_body> response;
    beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(10));
    http::read(stream, buffer, response);

    // The HTTP request uses Connection: close. Avoid blocking on a TLS
    // close_notify exchange; the socket is closed by RAII at scope exit.
    if (response.result() != http::status::ok) {
        throw std::runtime_error("depth snapshot request returned HTTP " +
                                 std::to_string(response.result_int()) + ": " + response.body());
    }
    try {
        auto snapshot = boost::json::parse(response.body());
        if (!snapshot.is_object() || !snapshot.as_object().if_contains("lastUpdateId")) {
            throw std::runtime_error("depth snapshot response missing lastUpdateId");
        }
        return snapshot;
    } catch (const boost::system::system_error& e) {
        throw std::runtime_error(std::string("could not parse depth snapshot JSON: ") + e.what());
    }
}

}  // namespace lob_capture
