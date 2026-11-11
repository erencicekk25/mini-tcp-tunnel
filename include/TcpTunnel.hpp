#pragma once

// mini-tcp-tunnel: a single-threaded, select()-based, bi-directional TCP tunnel.
//
//   client  <--TCP-->  [ TcpTunnel (listen port) ]  <--TCP-->  target server
//
// Every accepted client connection gets its own outbound connection to the
// target. Bytes are relayed in both directions through bounded per-direction
// buffers, and a ByteTransformer (optional XOR mask) is applied to every chunk
// that passes through the tunnel.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include <sys/select.h>
#include <sys/socket.h>

namespace tunnel {

// ---------------------------------------------------------------------------
// UniqueFd: RAII owner of a POSIX file descriptor (move-only).
// ---------------------------------------------------------------------------
class UniqueFd {
public:
    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}
    ~UniqueFd() { reset(); }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(other.release()) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept
    {
        if (this != &other) {
            reset(other.release());
        }
        return *this;
    }

    [[nodiscard]] int get() const noexcept { return fd_; }
    [[nodiscard]] bool valid() const noexcept { return fd_ >= 0; }

    // Gives up ownership without closing the descriptor.
    [[nodiscard]] int release() noexcept
    {
        const int fd = fd_;
        fd_ = -1;
        return fd;
    }

    // Closes the currently owned descriptor (if any) and takes ownership of `fd`.
    void reset(int fd = -1) noexcept;

private:
    int fd_ = -1;
};

// ---------------------------------------------------------------------------
// ByteTransformer: the "middleware" applied to every relayed chunk.
//
// XOR is symmetric (x ^ k ^ k == x), so two tunnels with the same key placed
// back-to-back restore the original stream. NOTE: XOR is obfuscation, not
// encryption.
// ---------------------------------------------------------------------------
class ByteTransformer {
public:
    explicit ByteTransformer(std::optional<std::uint8_t> xorKey) noexcept;

    void apply(std::uint8_t* data, std::size_t length) const noexcept;

    [[nodiscard]] bool isIdentity() const noexcept;
    [[nodiscard]] std::string describe() const;

private:
    std::optional<std::uint8_t> xorKey_;
};

// ---------------------------------------------------------------------------
// TunnelConfig: everything needed to start a tunnel.
// ---------------------------------------------------------------------------
struct TunnelConfig {
    std::string listenAddress = "0.0.0.0";
    std::uint16_t listenPort = 0;
    std::string targetHost;
    std::uint16_t targetPort = 0;
    std::optional<std::uint8_t> xorKey;          // std::nullopt => pass-through
    std::size_t maxSessions = 64;                // concurrent client connections
    std::size_t bufferCapacity = 64 * 1024;      // bytes per direction per session
    bool verbose = false;                        // per-chunk logging
};

namespace detail {
struct Flow;
} // namespace detail

// ---------------------------------------------------------------------------
// TcpTunnel: owns the listening socket, the sessions and the event loop.
// ---------------------------------------------------------------------------
class TcpTunnel {
public:
    // Throws std::invalid_argument if the configuration is inconsistent.
    explicit TcpTunnel(TunnelConfig config);
    ~TcpTunnel();

    TcpTunnel(const TcpTunnel&) = delete;
    TcpTunnel& operator=(const TcpTunnel&) = delete;
    TcpTunnel(TcpTunnel&&) = delete;
    TcpTunnel& operator=(TcpTunnel&&) = delete;

    // Resolves the target, opens the listening socket and runs the event loop
    // until stop() is called. Throws std::system_error / std::runtime_error on
    // fatal errors (e.g. port already in use, unresolvable target host).
    void run();

    // Requests a graceful shutdown. Async-signal-safe: may be called from a
    // signal handler.
    void stop() noexcept;

    [[nodiscard]] const TunnelConfig& config() const noexcept { return config_; }

    // Port the listener is bound to; 0 until run() has opened it. Thread-safe,
    // so callers can wait for readiness when listenPort is 0 (ephemeral).
    [[nodiscard]] std::uint16_t boundPort() const noexcept { return boundPort_.load(); }
    [[nodiscard]] std::size_t activeSessionCount() const noexcept { return sessions_.size(); }

private:
    struct Session;

    // Setup
    void resolveTarget();
    void openListener();

    // Event loop
    void eventLoop();
    [[nodiscard]] int registerInterest(fd_set& readSet, fd_set& writeSet) const;
    void acceptPendingClients();
    void serviceSession(Session& session, const fd_set& readSet, const fd_set& writeSet);

    // Session lifecycle
    void beginTargetConnect(Session& session);
    void finishTargetConnect(Session& session);
    void closeSession(Session& session, const std::string& reason, bool isError);
    void reapClosedSessions();
    void shutdownAllSessions();

    // Data pumping (return false if the session was closed)
    [[nodiscard]] bool pumpRead(Session& session, detail::Flow& flow, int sourceFd);
    [[nodiscard]] bool pumpWrite(Session& session, detail::Flow& flow, int sinkFd);
    void propagateEof(const Session& session, detail::Flow& flow, int sinkFd) const;

    TunnelConfig config_;
    ByteTransformer transformer_;

    UniqueFd listenFd_;
    std::string listenDescription_;

    sockaddr_storage targetAddress_{};
    socklen_t targetAddressLength_ = 0;
    std::string targetDescription_;

    std::map<std::uint64_t, std::unique_ptr<Session>> sessions_;
    std::uint64_t nextSessionId_ = 1;

    std::uint64_t totalSessions_ = 0;
    std::uint64_t totalBytesUpstream_ = 0;
    std::uint64_t totalBytesDownstream_ = 0;

    std::atomic<bool> stopRequested_{false};
    std::atomic<std::uint16_t> boundPort_{0};
    static_assert(std::atomic<bool>::is_always_lock_free,
                  "stop() must be async-signal-safe, which requires a lock-free atomic");
};

} // namespace tunnel
