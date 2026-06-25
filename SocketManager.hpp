#pragma once
#include <boost/asio.hpp>
#include <nlohmann/json.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

using json = nlohmann::json;
namespace asio = boost::asio;
using asio::ip::tcp;
using asio::ip::udp;

class WsSession; // Forward declaration

// SocketManager owns every listener (TCP/UDP) started by one browser tab's
// WebSocket session, plus any sockets opened for outbound "send" requests.
// Every operation is fully asynchronous -- nothing here ever blocks the
// io_context thread, so one slow/stuck peer can never freeze the backend
// or any other connected client.
class SocketManager : public std::enable_shared_from_this<SocketManager> {
public:
    explicit SocketManager(asio::io_context& ioc, std::shared_ptr<WsSession> ws);
    ~SocketManager();

    void startListener(const std::string& id, const std::string& proto, const std::string& ip_str, uint16_t port);
    void stopListener(const std::string& id);

    // Fire-and-forget from the caller's point of view -- all real work
    // (connect, write, shutdown, error handling) happens asynchronously.
    // reqId is echoed back in the ack so the frontend can match it up.
    void sendMessage(const std::string& proto, const std::string& ip_str, uint16_t port,
                      const std::string& msg, const std::string& reqId);

    void cleanup();

private:
    struct TcpListener {
        std::shared_ptr<tcp::acceptor> acceptor;
        // Track live accepted connections so stopListener can close them
        // immediately instead of waiting for them to time out on their own.
        std::shared_ptr<std::vector<std::weak_ptr<tcp::socket>>> activeConns;
    };
    struct UdpListener {
        std::shared_ptr<udp::socket> socket;
    };

    void doTcpAccept(const std::string& id, std::shared_ptr<tcp::acceptor> acceptor,
                      std::shared_ptr<std::vector<std::weak_ptr<tcp::socket>>> activeConns);
    void doTcpRead(const std::string& id, std::shared_ptr<tcp::socket> socket, std::string remote);
    void doUdpReceive(const std::string& id, std::shared_ptr<udp::socket> socket,
                       std::shared_ptr<std::vector<char>> buffer,
                       std::shared_ptr<udp::endpoint> sender);

    void sendLog(const std::string& id, const std::string& level, const std::string& msg);
    void sendData(const std::string& id, const std::string& proto, const std::string& remote,
                   const char* data, size_t len);
    void sendAck(const std::string& reqId, const std::string& proto, bool ok, const std::string& err);

    asio::io_context& ioc_;
    std::weak_ptr<WsSession> ws_;

    std::mutex mu_;
    std::unordered_map<std::string, std::variant<TcpListener, UdpListener>> active_listeners_;
};
