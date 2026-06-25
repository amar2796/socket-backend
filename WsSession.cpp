#include "WsSession.hpp"
#include "SocketManager.hpp"

WsSession::WsSession(beast::tcp_stream&& stream)
    : ws_(std::move(stream)) {}

void WsSession::run(http::request<http::string_body> req) {
    // Beast websocket streams are not safe to use concurrently from
    // multiple threads without a strand. Binding the stream's executor to
    // a strand here makes every async op on ws_ (reads, writes, accept)
    // serialize automatically, which is required now that the io_context
    // runs with multiple worker threads.
    ws_.async_accept(req, beast::bind_front_handler(&WsSession::onAccept, shared_from_this()));
}

void WsSession::onAccept(beast::error_code ec) {
    if (ec) return;
    sock_mgr_ = std::make_shared<SocketManager>(
        static_cast<boost::asio::io_context&>(ws_.get_executor().context()),
        shared_from_this()
    );
    doRead();
}

void WsSession::doRead() {
    ws_.async_read(buffer_, beast::bind_front_handler(&WsSession::onRead, shared_from_this()));
}

void WsSession::onRead(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);
    if (ec) {
        // Covers normal closure, resets, and anything else -- in every
        // case we stop reading and let the session (and its SocketManager,
        // via its destructor) tear down cleanly. No further doRead() call.
        if (sock_mgr_) sock_mgr_->cleanup();
        return;
    }

    std::string text = beast::buffers_to_string(buffer_.data());
    buffer_.consume(buffer_.size());
    handleMessage(text);
    doRead();
}

namespace {
// Small helper so a malformed/missing field produces a clean error
// message instead of nlohmann::json throwing past our control.
bool requireFields(const nlohmann::json& msg, std::initializer_list<const char*> fields) {
    for (auto* f : fields) {
        if (!msg.contains(f)) return false;
    }
    return true;
}
}  // namespace

void WsSession::handleMessage(const std::string& text) {
    nlohmann::json msg;
    try {
        msg = nlohmann::json::parse(text);
    } catch (const std::exception&) {
        sendJson({{"type", "log"}, {"id", "global"}, {"level", "error"}, {"message", "Malformed JSON command"}});
        return;
    }

    std::string action = msg.value("action", "");

    try {
        if (action == "start_listener") {
            if (!requireFields(msg, {"id", "protocol", "ip", "port"})) {
                sendJson({{"type", "log"}, {"id", "global"}, {"level", "error"},
                          {"message", "start_listener requires id, protocol, ip, port"}});
                return;
            }
            int port = msg["port"].get<int>();
            if (port < 1 || port > 65535) {
                sendJson({{"type", "log"}, {"id", msg["id"].get<std::string>()}, {"level", "error"},
                          {"message", "Port must be between 1 and 65535"}});
                return;
            }
            sock_mgr_->startListener(msg["id"].get<std::string>(), msg["protocol"].get<std::string>(),
                                      msg["ip"].get<std::string>(), static_cast<uint16_t>(port));

        } else if (action == "stop_listener") {
            if (!requireFields(msg, {"id"})) return;
            sock_mgr_->stopListener(msg["id"].get<std::string>());

        } else if (action == "send") {
            if (!requireFields(msg, {"protocol", "ip", "port"})) {
                sendJson({{"type", "log"}, {"id", "global"}, {"level", "error"},
                          {"message", "send requires protocol, ip, port"}});
                return;
            }
            int port = msg["port"].get<int>();
            std::string reqId = msg.value("reqId", "");
            if (port < 1 || port > 65535) {
                sendJson({{"type", "sent"}, {"reqId", reqId}, {"ok", false},
                          {"error", "Port must be between 1 and 65535"}});
                return;
            }
            sock_mgr_->sendMessage(msg["protocol"].get<std::string>(), msg["ip"].get<std::string>(),
                                    static_cast<uint16_t>(port), msg.value("message", ""), reqId);

        } else {
            sendJson({{"type", "log"}, {"id", "global"}, {"level", "error"},
                      {"message", "Unknown action: " + action}});
        }
    } catch (const std::exception& e) {
        // Catches json type-mismatch errors (e.g. port sent as a string)
        // so a single bad command never takes down the session.
        sendJson({{"type", "log"}, {"id", "global"}, {"level", "error"},
                  {"message", std::string("Command error: ") + e.what()}});
    }
}

void WsSession::sendJson(const nlohmann::json& msg) {
    std::string text = msg.dump();
    auto self = shared_from_this();
    std::lock_guard<std::mutex> lk(writeMu_);
    write_queue_.push_back(std::move(text));
    if (!writing_) {
        writing_ = true;
        // Hop onto the websocket stream's own executor before touching ws_,
        // since this method can be called from SocketManager's async
        // handlers, which may run on a different worker thread.
        boost::asio::post(ws_.get_executor(), [self]() { self->doWriteLocked(); });
    }
}

void WsSession::doWriteLocked() {
    std::string next;
    {
        std::lock_guard<std::mutex> lk(writeMu_);
        if (write_queue_.empty()) {
            writing_ = false;
            return;
        }
        next = write_queue_.front();
    }
    ws_.text(true);
    ws_.async_write(boost::asio::buffer(next), beast::bind_front_handler(&WsSession::onWrite, shared_from_this()));
}

void WsSession::onWrite(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);
    {
        std::lock_guard<std::mutex> lk(writeMu_);
        if (!write_queue_.empty()) write_queue_.erase(write_queue_.begin());
    }
    if (ec) {
        std::lock_guard<std::mutex> lk(writeMu_);
        writing_ = false;
        return;
    }
    doWriteLocked();
}
