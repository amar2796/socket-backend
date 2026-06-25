#include "SocketManager.hpp"
#include "WsSession.hpp"
#include <iomanip>
#include <sstream>

namespace {

std::string toHex(const char* data, size_t len) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
        oss << std::setw(2) << (static_cast<unsigned int>(static_cast<unsigned char>(data[i])));
        if (i + 1 != len) oss << ' ';
    }
    return oss.str();
}

// Best-effort printable text: keep printable ASCII/whitespace, replace
// everything else with '.' so binary payloads never break JSON encoding
// or the browser console.
std::string toPrintable(const char* data, size_t len) {
    std::string out;
    out.reserve(len);
    for (size_t i = 0; i < len; ++i) {
        unsigned char c = static_cast<unsigned char>(data[i]);
        out.push_back((c == '\n' || c == '\r' || c == '\t' || (c >= 0x20 && c < 0x7f))
                          ? static_cast<char>(c)
                          : '.');
    }
    return out;
}

}  // namespace

SocketManager::SocketManager(asio::io_context& ioc, std::shared_ptr<WsSession> ws)
    : ioc_(ioc), ws_(ws) {}

SocketManager::~SocketManager() { cleanup(); }

void SocketManager::cleanup() {
    std::lock_guard<std::mutex> lk(mu_);
    boost::system::error_code ec;
    for (auto& [id, variant] : active_listeners_) {
        if (auto* tcpL = std::get_if<TcpListener>(&variant)) {
            if (tcpL->acceptor) tcpL->acceptor->close(ec);
            if (tcpL->activeConns) {
                for (auto& weakConn : *tcpL->activeConns) {
                    if (auto conn = weakConn.lock()) conn->close(ec);
                }
            }
        } else if (auto* udpL = std::get_if<UdpListener>(&variant)) {
            if (udpL->socket) udpL->socket->close(ec);
        }
    }
    active_listeners_.clear();
}

void SocketManager::sendLog(const std::string& id, const std::string& level, const std::string& msg) {
    if (auto ws = ws_.lock()) {
        ws->sendJson({{"type", "log"}, {"id", id}, {"level", level}, {"message", msg}});
    }
}

void SocketManager::sendData(const std::string& id, const std::string& proto,
                              const std::string& remote, const char* data, size_t len) {
    if (auto ws = ws_.lock()) {
        ws->sendJson({
            {"type", "data"},
            {"id", id},
            {"protocol", proto},
            {"remote", remote},
            {"text", toPrintable(data, len)},
            {"hex", toHex(data, len)},
            {"size", len}
        });
    }
}

void SocketManager::sendAck(const std::string& reqId, const std::string& proto, bool ok,
                             const std::string& err) {
    if (auto ws = ws_.lock()) {
        json j = {{"type", "sent"}, {"reqId", reqId}, {"protocol", proto}, {"ok", ok}};
        if (!ok) j["error"] = err;
        ws->sendJson(j);
    }
}

// ---------------------------------------------------------------------
// Listeners
// ---------------------------------------------------------------------

void SocketManager::startListener(const std::string& id, const std::string& proto,
                                   const std::string& ip_str, uint16_t port) {
    try {
        asio::ip::address addr = asio::ip::make_address(ip_str);

        if (proto == "tcp") {
            auto acceptor = std::make_shared<tcp::acceptor>(ioc_);
            tcp::endpoint ep(addr, port);
            acceptor->open(ep.protocol());
            acceptor->set_option(asio::socket_base::reuse_address(true));
            acceptor->bind(ep);
            acceptor->listen();

            auto activeConns = std::make_shared<std::vector<std::weak_ptr<tcp::socket>>>();
            {
                std::lock_guard<std::mutex> lk(mu_);
                active_listeners_[id] = TcpListener{acceptor, activeConns};
            }
            sendLog(id, "info", "TCP listener started on " + ip_str + ":" + std::to_string(port));
            doTcpAccept(id, acceptor, activeConns);

        } else if (proto == "udp") {
            auto socket = std::make_shared<udp::socket>(ioc_);
            udp::endpoint ep(addr, port);
            socket->open(ep.protocol());
            socket->set_option(asio::socket_base::reuse_address(true));
            socket->bind(ep);

            {
                std::lock_guard<std::mutex> lk(mu_);
                active_listeners_[id] = UdpListener{socket};
            }
            sendLog(id, "info", "UDP listener started on " + ip_str + ":" + std::to_string(port));

            auto buffer = std::make_shared<std::vector<char>>(65536);
            auto sender = std::make_shared<udp::endpoint>();
            doUdpReceive(id, socket, buffer, sender);

        } else {
            sendLog(id, "error", "Unknown protocol: " + proto);
        }
    } catch (const std::exception& e) {
        sendLog(id, "error", std::string("Failed to bind: ") + e.what());
    }
}

void SocketManager::stopListener(const std::string& id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = active_listeners_.find(id);
    if (it == active_listeners_.end()) return;

    boost::system::error_code ec;
    if (auto* tcpL = std::get_if<TcpListener>(&it->second)) {
        if (tcpL->acceptor) tcpL->acceptor->close(ec);
        if (tcpL->activeConns) {
            for (auto& weakConn : *tcpL->activeConns) {
                if (auto conn = weakConn.lock()) conn->close(ec);
            }
        }
    } else if (auto* udpL = std::get_if<UdpListener>(&it->second)) {
        if (udpL->socket) udpL->socket->close(ec);
    }
    active_listeners_.erase(it);
    sendLog(id, "info", "Listener stopped cleanly.");
}

void SocketManager::doTcpAccept(const std::string& id, std::shared_ptr<tcp::acceptor> acceptor,
                                 std::shared_ptr<std::vector<std::weak_ptr<tcp::socket>>> activeConns) {
    auto self = shared_from_this();
    auto socket = std::make_shared<tcp::socket>(ioc_);
    acceptor->async_accept(*socket, [this, self, id, acceptor, activeConns, socket](boost::system::error_code ec) {
        if (!ec) {
            activeConns->push_back(socket);

            boost::system::error_code epEc;
            auto ep = socket->remote_endpoint(epEc);
            std::string remote = epEc ? "unknown" : ep.address().to_string() + ":" + std::to_string(ep.port());
            sendLog(id, "info", "Accepted connection from " + remote);

            doTcpRead(id, socket, remote);
            doTcpAccept(id, acceptor, activeConns);  // keep accepting further connections
        } else if (ec != asio::error::operation_aborted) {
            sendLog(id, "error", "Accept error: " + ec.message());
            // Don't re-arm on a real error (e.g. acceptor was closed by
            // stopListener) -- operation_aborted means we're shutting down
            // intentionally, anything else means the acceptor is unusable.
        }
    });
}

void SocketManager::doTcpRead(const std::string& id, std::shared_ptr<tcp::socket> socket, std::string remote) {
    auto self = shared_from_this();
    auto buffer = std::make_shared<std::vector<char>>(8192);
    socket->async_read_some(
        asio::buffer(*buffer),
        [this, self, id, socket, buffer, remote](boost::system::error_code ec, std::size_t bytes_transferred) {
            if (!ec && bytes_transferred > 0) {
                sendData(id, "tcp", remote, buffer->data(), bytes_transferred);
                doTcpRead(id, socket, remote);  // keep reading from this connection

            } else if (ec == asio::error::eof) {
                sendLog(id, "info", "Client " + remote + " disconnected cleanly.");
            } else if (ec != asio::error::operation_aborted) {
                // Covers connection_reset and every other real read error.
                // Previously this branch didn't exist, so resets vanished
                // with zero feedback in the UI.
                sendLog(id, "error", "Read error from " + remote + ": " + ec.message());
            }
        });
}

void SocketManager::doUdpReceive(const std::string& id, std::shared_ptr<udp::socket> socket,
                                  std::shared_ptr<std::vector<char>> buffer,
                                  std::shared_ptr<udp::endpoint> sender) {
    auto self = shared_from_this();
    socket->async_receive_from(
        asio::buffer(*buffer), *sender,
        [this, self, id, socket, buffer, sender](boost::system::error_code ec, std::size_t bytes_transferred) {
            if (!ec) {
                std::string remote = sender->address().to_string() + ":" + std::to_string(sender->port());
                sendData(id, "udp", remote, buffer->data(), bytes_transferred);
                doUdpReceive(id, socket, buffer, sender);  // keep listening
            } else if (ec != asio::error::operation_aborted) {
                sendLog(id, "error", "Read error: " + ec.message());
            }
        });
}

// ---------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------
//
// Both branches are now FULLY asynchronous. Nothing here ever calls a
// blocking asio::write/send_to -- that was the root cause of the backend
// freezing (and then crashing) on a slow or rude TCP peer, since the old
// code ran on a single io_context thread with no exception safety around
// the blocking call.

void SocketManager::sendMessage(const std::string& proto, const std::string& ip_str, uint16_t port,
                                 const std::string& msg, const std::string& reqId) {
    asio::ip::address addr;
    try {
        addr = asio::ip::make_address(ip_str);
    } catch (const std::exception& e) {
        sendAck(reqId, proto, false, std::string("Invalid IP address: ") + e.what());
        return;
    }

    auto self = shared_from_this();

    if (proto == "tcp") {
        auto socket = std::make_shared<tcp::socket>(ioc_);
        auto target = std::make_shared<tcp::endpoint>(addr, port);
        auto payload = std::make_shared<std::string>(msg);

        socket->async_connect(*target, [this, self, socket, payload, proto, reqId, ip_str, port](boost::system::error_code ec) {
            if (ec) {
                sendAck(reqId, proto, false, "Connect failed: " + ec.message());
                return;
            }
            asio::async_write(*socket, asio::buffer(*payload),
                [this, self, socket, payload, proto, reqId](boost::system::error_code wec, std::size_t) {
                    boost::system::error_code shutEc;
                    socket->shutdown(tcp::socket::shutdown_send, shutEc);
                    // shutdown errors are not fatal -- the peer may have
                    // already closed its side; ignore and report on the
                    // write result only.
                    if (wec) {
                        sendAck(reqId, proto, false, "Write failed: " + wec.message());
                    } else {
                        sendAck(reqId, proto, true, "");
                    }
                });
        });

    } else if (proto == "udp") {
        auto socket = std::make_shared<udp::socket>(ioc_);
        try {
            socket->open(udp::v4());
        } catch (const std::exception& e) {
            sendAck(reqId, proto, false, std::string("Failed to open socket: ") + e.what());
            return;
        }
        auto target = std::make_shared<udp::endpoint>(addr, port);
        auto payload = std::make_shared<std::string>(msg);

        socket->async_send_to(asio::buffer(*payload), *target,
            [this, self, socket, payload, proto, reqId](boost::system::error_code ec, std::size_t) {
                if (ec) {
                    sendAck(reqId, proto, false, "Send failed: " + ec.message());
                } else {
                    sendAck(reqId, proto, true, "");
                }
            });

    } else {
        sendAck(reqId, proto, false, "Unknown protocol: " + proto);
    }
}
