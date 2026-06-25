#pragma once
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/http.hpp>
#include <nlohmann/json.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace http = beast::http;
class SocketManager;

class WsSession : public std::enable_shared_from_this<WsSession> {
public:
    explicit WsSession(beast::tcp_stream&& stream);
    void run(http::request<http::string_body> req);
    void sendJson(const nlohmann::json& msg);

private:
    void onAccept(beast::error_code ec);
    void doRead();
    void onRead(beast::error_code ec, std::size_t bytes_transferred);
    void onWrite(beast::error_code ec, std::size_t bytes_transferred);
    void handleMessage(const std::string& text);
    void doWriteLocked();

    websocket::stream<beast::tcp_stream> ws_;
    beast::flat_buffer buffer_;
    std::shared_ptr<SocketManager> sock_mgr_;

    std::mutex writeMu_;
    std::vector<std::string> write_queue_;
    bool writing_ = false;
};
