// Tests for mini-tcp-tunnel: the byte transformer, the fd RAII wrapper and
// full client -> tunnel -> target round trips over loopback sockets.

#include "Logger.hpp"
#include "TcpTunnel.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

#ifdef MSG_NOSIGNAL
constexpr int MSG_NOSIGNAL_FLAG = MSG_NOSIGNAL;
#else
constexpr int MSG_NOSIGNAL_FLAG = 0;  // macOS: SIGPIPE is ignored in main() instead.
#endif

int g_failures = 0;
int g_checks = 0;

#define CHECK(condition)                                                                   \
    do {                                                                                   \
        ++g_checks;                                                                        \
        if (!(condition)) {                                                                \
            ++g_failures;                                                                  \
            std::cerr << "  FAILED " << __FILE__ << ":" << __LINE__ << ": " #condition "\n"; \
        }                                                                                  \
    } while (false)

#define CHECK_EQ(actual, expected)                                                    \
    do {                                                                              \
        ++g_checks;                                                                   \
        const auto& actualValue = (actual);                                           \
        const auto& expectedValue = (expected);                                       \
        if (!(actualValue == expectedValue)) {                                        \
            ++g_failures;                                                             \
            std::cerr << "  FAILED " << __FILE__ << ":" << __LINE__ << ": " #actual   \
                      << " == " #expected "\n    actual:   " << actualValue           \
                      << "\n    expected: " << expectedValue << "\n";                 \
        }                                                                             \
    } while (false)

struct TestCase {
    const char* name;
    void (*body)();
};

std::vector<TestCase>& registry() {
    static std::vector<TestCase> tests;
    return tests;
}

struct Registrar {
    Registrar(const char* name, void (*body)()) { registry().push_back({name, body}); }
};

#define TEST(name)                                  \
    void name();                                    \
    const Registrar registrar_##name(#name, &name); \
    void name()

// ---------------------------------------------------------------------------
// Socket helpers
// ---------------------------------------------------------------------------

tunnel::UniqueFd listenOnLoopback(std::uint16_t& port) {
    tunnel::UniqueFd fd(::socket(AF_INET, SOCK_STREAM, 0));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    ::bind(fd.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address));
    ::listen(fd.get(), 16);
    socklen_t length = sizeof(address);
    ::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&address), &length);
    port = ntohs(address.sin_port);
    return fd;
}

tunnel::UniqueFd connectToLoopback(std::uint16_t port) {
    tunnel::UniqueFd fd(::socket(AF_INET, SOCK_STREAM, 0));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(fd.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        return tunnel::UniqueFd();
    }
    return fd;
}

bool waitReadable(int fd, int timeoutMs) {
    pollfd item{fd, POLLIN, 0};
    return ::poll(&item, 1, timeoutMs) > 0;
}

bool sendAll(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t written = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL_FLAG);
        if (written <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(written);
    }
    return true;
}

std::string readUntilEof(int fd, std::chrono::milliseconds timeout = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string data;
    char buffer[16 * 1024];
    while (std::chrono::steady_clock::now() < deadline) {
        if (!waitReadable(fd, 50)) {
            continue;
        }
        const ssize_t received = ::recv(fd, buffer, sizeof(buffer), 0);
        if (received <= 0) {
            break;
        }
        data.append(buffer, static_cast<std::size_t>(received));
    }
    return data;
}

// Target server: echoes everything back and remembers the raw bytes it saw.
// Each connection gets its own thread so concurrent sessions never wait on
// each other.
class EchoTarget {
public:
    EchoTarget() : listener_(listenOnLoopback(port_)) { thread_ = std::thread(&EchoTarget::serve, this); }

    ~EchoTarget() {
        running_ = false;
        thread_.join();
        for (auto& worker : workers_) {
            worker.join();
        }
    }

    EchoTarget(const EchoTarget&) = delete;
    EchoTarget& operator=(const EchoTarget&) = delete;

    std::uint16_t port() const { return port_; }

    std::string lastReceived() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return lastReceived_;
    }

private:
    void serve() {
        while (running_) {
            if (!waitReadable(listener_.get(), 50)) {
                continue;
            }
            tunnel::UniqueFd client(::accept(listener_.get(), nullptr, nullptr));
            if (!client.valid()) {
                continue;
            }
            workers_.emplace_back(&EchoTarget::echo, this, std::move(client));
        }
    }

    void echo(tunnel::UniqueFd client) {
        const std::string data = readUntilEof(client.get());
        {
            std::lock_guard<std::mutex> lock(mutex_);
            lastReceived_ = data;
        }
        sendAll(client.get(), data);
    }

    std::uint16_t port_ = 0;
    tunnel::UniqueFd listener_;
    std::atomic<bool> running_{true};
    mutable std::mutex mutex_;
    std::string lastReceived_;
    std::thread thread_;
    std::vector<std::thread> workers_;  // Only touched by the accept thread until join.
};

// Runs a TcpTunnel on a background thread for the lifetime of the object.
class RunningTunnel {
public:
    RunningTunnel(std::uint16_t targetPort, std::optional<std::uint8_t> xorKey) {
        tunnel::TunnelConfig config;
        config.listenAddress = "127.0.0.1";
        config.listenPort = 0;
        config.targetHost = "127.0.0.1";
        config.targetPort = targetPort;
        config.xorKey = xorKey;
        tunnel_ = std::make_unique<tunnel::TcpTunnel>(std::move(config));
        thread_ = std::thread([this] { tunnel_->run(); });
        for (int i = 0; i < 500 && tunnel_->boundPort() == 0; ++i) {
            std::this_thread::sleep_for(10ms);
        }
    }

    ~RunningTunnel() {
        tunnel_->stop();
        thread_.join();
    }

    RunningTunnel(const RunningTunnel&) = delete;
    RunningTunnel& operator=(const RunningTunnel&) = delete;

    std::uint16_t port() const { return tunnel_->boundPort(); }

private:
    std::unique_ptr<tunnel::TcpTunnel> tunnel_;
    std::thread thread_;
};

std::string roundTrip(std::uint16_t port, const std::string& payload) {
    tunnel::UniqueFd client = connectToLoopback(port);
    if (!client.valid()) {
        return "<connect failed>";
    }
    sendAll(client.get(), payload);
    ::shutdown(client.get(), SHUT_WR);
    return readUntilEof(client.get());
}

std::string makePayload(std::size_t size) {
    std::string payload(size, '\0');
    for (std::size_t i = 0; i < size; ++i) {
        payload[i] = static_cast<char>((i * 31 + 7) % 256);
    }
    return payload;
}

std::string xorAll(std::string data, std::uint8_t key) {
    for (char& c : data) {
        c = static_cast<char>(static_cast<std::uint8_t>(c) ^ key);
    }
    return data;
}

// ---------------------------------------------------------------------------
// ByteTransformer
// ---------------------------------------------------------------------------

TEST(passThroughTransformerLeavesBytesAlone) {
    const tunnel::ByteTransformer transformer(std::nullopt);
    CHECK(transformer.isIdentity());
    std::uint8_t data[4] = {1, 2, 3, 4};
    transformer.apply(data, sizeof(data));
    CHECK(data[0] == 1 && data[1] == 2 && data[2] == 3 && data[3] == 4);
    CHECK(transformer.describe().find("pass-through") != std::string::npos);
}

TEST(xorTransformerIsItsOwnInverse) {
    const tunnel::ByteTransformer transformer(std::uint8_t{0x5A});
    CHECK(!transformer.isIdentity());
    CHECK(transformer.describe().find("0x5a") != std::string::npos);

    std::uint8_t data[3] = {0x00, 0x5A, 0xFF};
    transformer.apply(data, sizeof(data));
    CHECK(data[0] == 0x5A && data[1] == 0x00 && data[2] == 0xA5);
    transformer.apply(data, sizeof(data));
    CHECK(data[0] == 0x00 && data[1] == 0x5A && data[2] == 0xFF);
}

// ---------------------------------------------------------------------------
// UniqueFd
// ---------------------------------------------------------------------------

TEST(uniqueFdOwnsAndReleasesDescriptors) {
    int fds[2] = {-1, -1};
    CHECK(::pipe(fds) == 0);

    tunnel::UniqueFd reader(fds[0]);
    tunnel::UniqueFd writer(fds[1]);
    CHECK(reader.valid());

    tunnel::UniqueFd moved(std::move(reader));
    CHECK(!reader.valid());
    CHECK_EQ(moved.get(), fds[0]);

    const int raw = writer.release();
    CHECK(!writer.valid());
    CHECK_EQ(raw, fds[1]);
    ::close(raw);

    moved.reset();
    CHECK(!moved.valid());
    // The descriptor really was closed by reset().
    CHECK(::fcntl(fds[0], F_GETFD) == -1);

    tunnel::UniqueFd empty;
    CHECK(!empty.valid());
}

// ---------------------------------------------------------------------------
// End-to-end
// ---------------------------------------------------------------------------

TEST(relaysDataInBothDirections) {
    EchoTarget target;
    RunningTunnel tunnelUnderTest(target.port(), std::nullopt);
    CHECK(tunnelUnderTest.port() != 0);

    const std::string payload = makePayload(512 * 1024);
    const std::string reply = roundTrip(tunnelUnderTest.port(), payload);
    CHECK_EQ(reply.size(), payload.size());
    CHECK(reply == payload);
    CHECK(target.lastReceived() == payload);
}

TEST(xorModeObfuscatesTrafficOnTheWire) {
    constexpr std::uint8_t key = 0x2C;
    EchoTarget target;
    RunningTunnel tunnelUnderTest(target.port(), key);

    const std::string payload = makePayload(64 * 1024);
    const std::string reply = roundTrip(tunnelUnderTest.port(), payload);

    // The target sees transformed bytes ...
    CHECK(target.lastReceived() == xorAll(payload, key));
    // ... and the echo is transformed again on the way back, restoring it.
    CHECK(reply == payload);
}

TEST(servesSeveralClientsConcurrently) {
    EchoTarget target;
    RunningTunnel tunnelUnderTest(target.port(), std::nullopt);

    std::vector<tunnel::UniqueFd> clients;
    for (int i = 0; i < 5; ++i) {
        clients.push_back(connectToLoopback(tunnelUnderTest.port()));
        CHECK(clients.back().valid());
    }
    // All connections are open at the same time; finish them in reverse order.
    for (int i = 4; i >= 0; --i) {
        const std::string message = "client-" + std::to_string(i);
        sendAll(clients[static_cast<std::size_t>(i)].get(), message);
        ::shutdown(clients[static_cast<std::size_t>(i)].get(), SHUT_WR);
        CHECK_EQ(readUntilEof(clients[static_cast<std::size_t>(i)].get()), message);
    }
}

TEST(closesClientWhenTargetIsUnreachable) {
    std::uint16_t deadPort = 0;
    { const tunnel::UniqueFd closed = listenOnLoopback(deadPort); }

    RunningTunnel tunnelUnderTest(deadPort, std::nullopt);
    tunnel::UniqueFd client = connectToLoopback(tunnelUnderTest.port());
    CHECK(client.valid());
    const auto started = std::chrono::steady_clock::now();
    const std::string reply = readUntilEof(client.get(), 5s);
    CHECK(reply.empty());
    CHECK(std::chrono::steady_clock::now() - started < 4s);
}

}  // namespace

int main() {
    std::signal(SIGPIPE, SIG_IGN);
    tunnel::logger::setVerbose(false);
    for (const auto& test : registry()) {
        const int failuresBefore = g_failures;
        test.body();
        std::cout << (g_failures == failuresBefore ? "[ PASS ] " : "[ FAIL ] ") << test.name << '\n';
    }
    std::cout << '\n'
              << registry().size() << " tests, " << g_checks << " checks, " << g_failures
              << " failure(s)\n";
    return g_failures == 0 ? 0 : 1;
}
