#include "WsSession.hpp"
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = boost::asio::ip::tcp;

namespace {

void serveHttpThenMaybeUpgrade(tcp::socket socket) {
    auto stream = std::make_shared<beast::tcp_stream>(std::move(socket));
    auto buffer = std::make_shared<beast::flat_buffer>();
    auto req = std::make_shared<http::request<http::string_body>>();

    http::async_read(*stream, *buffer, *req, [stream, buffer, req](beast::error_code ec, std::size_t) {
        if (ec) return;

        if (beast::websocket::is_upgrade(*req)) {
            std::make_shared<WsSession>(std::move(*stream))->run(std::move(*req));
            return;
        }

        // Basic HTTP health endpoint for deployment probes (Render etc.
        // hit the plain HTTP port to confirm the service is alive).
        auto res = std::make_shared<http::response<http::string_body>>(http::status::ok, req->version());
        res->set(http::field::server, "SockTest-C++");
        res->set(http::field::content_type, "text/plain");
        res->keep_alive(false);
        res->body() = "socktest backend is running\n";
        res->prepare_payload();

        http::async_write(*stream, *res, [stream, res](beast::error_code, std::size_t) {
            beast::error_code ignored;
            stream->socket().shutdown(tcp::socket::shutdown_both, ignored);
        });
    });
}

// Owns the listening acceptor via shared_ptr so the accept loop never
// depends on a stack frame staying alive -- the previous version captured
// `acceptor`/`ioc` by reference in a recursive lambda, which only worked
// because main()'s frame happened to outlive ioc.run(). This version is
// correct regardless of how/where it's started from.
class Listener : public std::enable_shared_from_this<Listener> {
public:
    Listener(asio::io_context& ioc, tcp::endpoint endpoint) : acceptor_(ioc) {
        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen(asio::socket_base::max_listen_connections);
    }

    void run() { doAccept(); }

private:
    void doAccept() {
        auto self = shared_from_this();
        acceptor_.async_accept([this, self](beast::error_code ec, tcp::socket socket) {
            if (!ec) {
                serveHttpThenMaybeUpgrade(std::move(socket));
            }
            // Keep accepting regardless of a one-off error on a single
            // connection attempt; only stop if the acceptor itself is gone.
            if (acceptor_.is_open() || !ec) {
                doAccept();
            }
        });
    }

    tcp::acceptor acceptor_;
};

}  // namespace

int main() {
    try {
        const char* port_env = std::getenv("PORT");
        int port = port_env ? std::stoi(port_env) : 8080;
        const char* bind_ip = std::getenv("BIND_IP") ? std::getenv("BIND_IP") : "0.0.0.0";

        // Multiple worker threads sharing one io_context. This is required
        // for correctness, not just throughput: with a single thread, any
        // listener/connection that did something blocking (or just slow)
        // would stall every other listener and every other connected
        // browser tab. The SocketManager fixes were also written to be
        // safe under this concurrency (mutex-protected listener map,
        // mutex-protected write queue in WsSession).
        unsigned int threads = std::max(2u, std::thread::hardware_concurrency());

        asio::io_context ioc{static_cast<int>(threads)};

        auto endpoint = tcp::endpoint(asio::ip::make_address(bind_ip), port);
        auto listener = std::make_shared<Listener>(ioc, endpoint);
        listener->run();

        std::cout << "[Server] Listening on " << bind_ip << ":" << port
                  << " with " << threads << " worker threads" << std::endl;

        std::vector<std::thread> pool;
        pool.reserve(threads - 1);
        for (unsigned int i = 1; i < threads; ++i) {
            pool.emplace_back([&ioc] { ioc.run(); });
        }
        ioc.run();

        for (auto& t : pool) t.join();

    } catch (const std::exception& e) {
        std::cerr << "[Exception] Critical failure: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
