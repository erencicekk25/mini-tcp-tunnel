#include "TcpTunnel.hpp"

#include "Logger.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace tunnel {

// ===========================================================================
// Internal helpers
// ===========================================================================
namespace {

#ifdef MSG_NOSIGNAL
// Linux: do not raise SIGPIPE when writing to a socket whose peer has gone.
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
// macOS/BSD: SIGPIPE is ignored process-wide in main() instead.
constexpr int kSendFlags = 0;
#endif

constexpr int kSelectTimeoutMs = 500;          // how often the loop re-checks stop()
constexpr std::size_t kHexPreviewBytes = 16;   // bytes shown per chunk in verbose mode

using AddrInfoPtr = std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)>;

[[nodiscard]] std::string errnoMessage(const int error)
{
    return std::generic_category().message(error);
}

[[noreturn]] void throwSystemError(const int error, const std::string& what)
{
    throw std::system_error(error, std::generic_category(), what);
}

[[nodiscard]] bool isTransientError(const int error) noexcept
{
    return error == EAGAIN || error == EWOULDBLOCK || error == EINTR;
}

// Puts a descriptor into non-blocking + close-on-exec mode. Returns 0 on success
// or the errno value of the failing call.
[[nodiscard]] int makeNonBlocking(const int fd) noexcept
{
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return errno;
    }
    const int fdFlags = ::fcntl(fd, F_GETFD, 0);
    if (fdFlags < 0 || ::fcntl(fd, F_SETFD, fdFlags | FD_CLOEXEC) < 0) {
        return errno;
    }
    return 0;
}

// Disables Nagle's algorithm: a proxy should forward small chunks immediately.
// Failure is not fatal, so it is only reported in verbose mode.
void enableTcpNoDelay(const int fd) noexcept
{
    const int enabled = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) < 0) {
        logger::debug("setsockopt(TCP_NODELAY) failed on fd ", fd, ": ", errnoMessage(errno));
    }
}

// Converts a socket address to "ip:port" (IPv4) or "[ip]:port" (IPv6).
[[nodiscard]] std::string formatAddress(const sockaddr* const address, const socklen_t length)
{
    std::array<char, NI_MAXHOST> host{};
    std::array<char, NI_MAXSERV> service{};

    const int rc = ::getnameinfo(address, length,
                                 host.data(), static_cast<socklen_t>(host.size()),
                                 service.data(), static_cast<socklen_t>(service.size()),
                                 NI_NUMERICHOST | NI_NUMERICSERV);
    if (rc != 0) {
        return std::string("<unknown address: ") + ::gai_strerror(rc) + ">";
    }

    if (address->sa_family == AF_INET6) {
        return "[" + std::string(host.data()) + "]:" + service.data();
    }
    return std::string(host.data()) + ":" + service.data();
}

// Resolves host/port to a list of TCP socket addresses.
[[nodiscard]] AddrInfoPtr resolveAddress(const std::string& host, const std::uint16_t port, const bool passive)
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_NUMERICSERV;
    if (passive) {
        hints.ai_flags |= AI_PASSIVE | AI_NUMERICHOST;  // listen address must be a literal IP
    }

    const std::string service = std::to_string(port);
    addrinfo* raw = nullptr;
    const int rc = ::getaddrinfo(host.c_str(), service.c_str(), &hints, &raw);
    if (rc != 0) {
        const std::string reason = (rc == EAI_SYSTEM) ? errnoMessage(errno) : ::gai_strerror(rc);
        throw std::runtime_error("cannot resolve '" + host + "': " + reason);
    }
    if (raw == nullptr) {
        throw std::runtime_error("cannot resolve '" + host + "': no addresses returned");
    }
    return AddrInfoPtr(raw, &::freeaddrinfo);
}

// "47 45 54 20 2f ..." – a short hex dump of the beginning of a chunk.
[[nodiscard]] std::string hexPreview(const std::uint8_t* const data, const std::size_t shown,
                                     const std::size_t total)
{
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (std::size_t i = 0; i < shown; ++i) {
        if (i != 0) {
            out << ' ';
        }
        out << std::setw(2) << static_cast<unsigned>(data[i]);
    }
    if (total > shown) {
        out << " ...";
    }
    return out.str();
}

[[nodiscard]] std::string formatBytes(const std::uint64_t bytes)
{
    constexpr std::array<const char*, 4> units{"B", "KiB", "MiB", "GiB"};

    auto value = static_cast<double>(bytes);
    std::size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < units.size()) {
        value /= 1024.0;
        ++unit;
    }

    std::ostringstream out;
    if (unit == 0) {
        out << bytes << ' ' << units[unit];
    } else {
        out << std::fixed << std::setprecision(2) << value << ' ' << units[unit];
    }
    return out.str();
}

[[nodiscard]] long long millisecondsSince(const std::chrono::steady_clock::time_point start)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - start)
        .count();
}

} // namespace

// ===========================================================================
// detail: per-session data structures
// ===========================================================================
namespace detail {

// Fixed-capacity byte buffer for one direction of a session.
//
//   storage_: [ consumed | readable (pending to send) | writable (free) ]
//              0         begin_                       end_              capacity
class ChannelBuffer {
public:
    explicit ChannelBuffer(const std::size_t capacity) : storage_(capacity) {}

    [[nodiscard]] const std::uint8_t* readableData() const noexcept { return storage_.data() + begin_; }
    [[nodiscard]] std::size_t readableSize() const noexcept { return end_ - begin_; }

    [[nodiscard]] std::uint8_t* writableData() noexcept { return storage_.data() + end_; }
    [[nodiscard]] std::size_t writableSize() const noexcept { return storage_.size() - end_; }

    [[nodiscard]] bool empty() const noexcept { return begin_ == end_; }
    [[nodiscard]] bool full() const noexcept { return readableSize() == storage_.size(); }

    // Marks `count` bytes (written into writableData()) as readable.
    void commit(const std::size_t count) noexcept { end_ += count; }

    // Drops `count` bytes from the front after they were sent.
    void consume(const std::size_t count) noexcept
    {
        begin_ += count;
        if (begin_ == end_) {
            begin_ = 0;
            end_ = 0;
        }
    }

    // Moves pending bytes to the front when the tail has no free space left.
    void compact() noexcept
    {
        if (begin_ == 0 || writableSize() != 0) {
            return;
        }
        std::memmove(storage_.data(), storage_.data() + begin_, readableSize());
        end_ -= begin_;
        begin_ = 0;
    }

private:
    std::vector<std::uint8_t> storage_;
    std::size_t begin_ = 0;
    std::size_t end_ = 0;
};

// One direction of a session: source socket -> buffer -> sink socket.
struct Flow {
    Flow(const char* source, const char* sink, const std::size_t capacity)
        : sourceName(source), sinkName(sink), buffer(capacity)
    {
    }

    const char* sourceName;          // "client" or "target"
    const char* sinkName;            // "target" or "client"
    ChannelBuffer buffer;            // bytes read from source, already transformed
    bool sourceEof = false;          // source sent FIN (recv() returned 0)
    bool sinkShutdown = false;       // we forwarded the FIN to the sink (shutdown(SHUT_WR))
    std::uint64_t totalBytes = 0;
    std::uint64_t chunkCount = 0;
};

enum class SessionState { Connecting, Established, Closed };

} // namespace detail

// ===========================================================================
// Session
// ===========================================================================
struct TcpTunnel::Session {
    Session(const std::uint64_t sessionId, std::string peerAddress, UniqueFd client,
            const std::size_t bufferCapacity)
        : id(sessionId),
          peer(std::move(peerAddress)),
          clientFd(std::move(client)),
          upstream("client", "target", bufferCapacity),
          downstream("target", "client", bufferCapacity),
          startedAt(std::chrono::steady_clock::now())
    {
    }

    const std::uint64_t id;
    const std::string peer;
    UniqueFd clientFd;
    UniqueFd targetFd;
    detail::SessionState state = detail::SessionState::Connecting;
    detail::Flow upstream;     // client -> target
    detail::Flow downstream;   // target -> client
    const std::chrono::steady_clock::time_point startedAt;
};

// ===========================================================================
// UniqueFd
// ===========================================================================
void UniqueFd::reset(const int fd) noexcept
{
    if (fd_ >= 0) {
        // On Linux the descriptor is released even if close() reports EINTR,
        // so retrying would be wrong. There is nothing useful to do on error.
        ::close(fd_);
    }
    fd_ = fd;
}

// ===========================================================================
// ByteTransformer
// ===========================================================================
ByteTransformer::ByteTransformer(const std::optional<std::uint8_t> xorKey) noexcept
    : xorKey_(xorKey)
{
}

void ByteTransformer::apply(std::uint8_t* const data, const std::size_t length) const noexcept
{
    if (!xorKey_) {
        return;
    }
    const std::uint8_t key = *xorKey_;
    std::for_each(data, data + length, [key](std::uint8_t& byte) {
        byte = static_cast<std::uint8_t>(byte ^ key);
    });
}

bool ByteTransformer::isIdentity() const noexcept
{
    return !xorKey_.has_value();
}

std::string ByteTransformer::describe() const
{
    if (!xorKey_) {
        return "pass-through (no transformation)";
    }
    std::ostringstream out;
    out << "XOR mask 0x" << std::hex << std::setw(2) << std::setfill('0')
        << static_cast<unsigned>(*xorKey_);
    return out.str();
}

// ===========================================================================
// TcpTunnel: construction / public API
// ===========================================================================
TcpTunnel::TcpTunnel(TunnelConfig config)
    : config_(std::move(config)), transformer_(config_.xorKey)
{
    if (config_.targetHost.empty()) {
        throw std::invalid_argument("target host must not be empty");
    }
    if (config_.targetPort == 0) {
        throw std::invalid_argument("target port must be in range 1-65535");
    }
    if (config_.maxSessions == 0) {
        throw std::invalid_argument("max sessions must be at least 1");
    }
    if (config_.bufferCapacity == 0) {
        throw std::invalid_argument("buffer capacity must be at least 1 byte");
    }
}

// Defined here because Session is an incomplete type in the header.
TcpTunnel::~TcpTunnel() = default;

void TcpTunnel::stop() noexcept
{
    stopRequested_.store(true, std::memory_order_relaxed);
}

void TcpTunnel::run()
{
    resolveTarget();
    openListener();

    logger::info("tunnel ready: ", listenDescription_, " ==> ", targetDescription_);
    logger::info("transform: ", transformer_.describe(),
                 " | max sessions: ", config_.maxSessions,
                 " | buffer: ", formatBytes(config_.bufferCapacity), " per direction",
                 " | multiplexer: select() (FD_SETSIZE=", FD_SETSIZE, ")");
    logger::info("press Ctrl+C to stop");

    try {
        eventLoop();
    } catch (...) {
        shutdownAllSessions();
        listenFd_.reset();
        throw;
    }

    logger::info("stop requested, shutting down");
    shutdownAllSessions();
    listenFd_.reset();
    logger::info("tunnel stopped | sessions served: ", totalSessions_,
                 " | client -> target: ", formatBytes(totalBytesUpstream_),
                 " | target -> client: ", formatBytes(totalBytesDownstream_));
}

// ===========================================================================
// Setup
// ===========================================================================
void TcpTunnel::resolveTarget()
{
    const AddrInfoPtr result = resolveAddress(config_.targetHost, config_.targetPort, false);
    const addrinfo& first = *result;

    if (first.ai_addrlen > sizeof(targetAddress_)) {
        throw std::runtime_error("resolved target address does not fit in sockaddr_storage");
    }
    std::memcpy(&targetAddress_, first.ai_addr, first.ai_addrlen);
    targetAddressLength_ = first.ai_addrlen;

    const std::string numeric = formatAddress(first.ai_addr, first.ai_addrlen);
    const std::string requested = config_.targetHost + ":" + std::to_string(config_.targetPort);
    targetDescription_ = (numeric == requested) ? numeric : requested + " (" + numeric + ")";

    logger::debug("target resolved to ", numeric);
}

void TcpTunnel::openListener()
{
    const AddrInfoPtr result = resolveAddress(config_.listenAddress, config_.listenPort, true);
    const addrinfo& address = *result;

    UniqueFd fd(::socket(address.ai_family, address.ai_socktype, address.ai_protocol));
    if (!fd.valid()) {
        throwSystemError(errno, "socket() for listener failed");
    }
    if (fd.get() >= FD_SETSIZE) {
        throw std::runtime_error("listener descriptor exceeds FD_SETSIZE");
    }

    // Allows an immediate restart while old connections are still in TIME_WAIT.
    const int reuse = 1;
    if (::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        throwSystemError(errno, "setsockopt(SO_REUSEADDR) failed");
    }

    if (::bind(fd.get(), address.ai_addr, address.ai_addrlen) < 0) {
        const int error = errno;
        throwSystemError(error, "bind() to " + formatAddress(address.ai_addr, address.ai_addrlen) +
                                    " failed");
    }
    if (::listen(fd.get(), SOMAXCONN) < 0) {
        throwSystemError(errno, "listen() failed");
    }
    if (const int error = makeNonBlocking(fd.get()); error != 0) {
        throwSystemError(error, "making listener non-blocking failed");
    }

    // Report the address we actually got: with port 0 the kernel picks one.
    sockaddr_storage bound{};
    socklen_t boundLength = sizeof(bound);
    if (::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&bound), &boundLength) == 0) {
        listenDescription_ = formatAddress(reinterpret_cast<const sockaddr*>(&bound), boundLength);
        const in_port_t port = bound.ss_family == AF_INET6
                                   ? reinterpret_cast<const sockaddr_in6*>(&bound)->sin6_port
                                   : reinterpret_cast<const sockaddr_in*>(&bound)->sin_port;
        boundPort_.store(ntohs(port));
    } else {
        listenDescription_ = formatAddress(address.ai_addr, address.ai_addrlen);
        boundPort_.store(config_.listenPort);
    }
    listenFd_ = std::move(fd);
}

// ===========================================================================
// Event loop
// ===========================================================================
void TcpTunnel::eventLoop()
{
    while (!stopRequested_.load(std::memory_order_relaxed)) {
        fd_set readSet;
        fd_set writeSet;
        FD_ZERO(&readSet);
        FD_ZERO(&writeSet);

        // 1) Declare interest. The listener is only watched while there is room
        //    for another session; otherwise new clients wait in the kernel's
        //    accept backlog (natural back-pressure).
        const bool acceptEnabled = sessions_.size() < config_.maxSessions;
        int maxFd = -1;
        if (acceptEnabled) {
            FD_SET(listenFd_.get(), &readSet);
            maxFd = listenFd_.get();
        }
        maxFd = std::max(maxFd, registerInterest(readSet, writeSet));

        // 2) Sleep until at least one descriptor is ready, or the timeout
        //    expires so that stop() is noticed even when the tunnel is idle.
        timeval timeout{};
        timeout.tv_sec = 0;
        timeout.tv_usec = static_cast<decltype(timeout.tv_usec)>(kSelectTimeoutMs * 1000);

        const int ready = ::select(maxFd + 1, &readSet, &writeSet, nullptr, &timeout);
        if (ready < 0) {
            const int error = errno;
            if (error == EINTR) {
                continue;  // interrupted by a signal (e.g. Ctrl+C): re-check stop flag
            }
            throwSystemError(error, "select() failed");
        }
        if (ready == 0) {
            continue;  // timeout, nothing to do
        }

        // 3) Service existing sessions first, then release closed ones, and only
        //    then accept new clients. New descriptors therefore never collide
        //    with stale bits in this round's fd_sets.
        for (auto& entry : sessions_) {
            serviceSession(*entry.second, readSet, writeSet);
        }
        reapClosedSessions();

        if (acceptEnabled && FD_ISSET(listenFd_.get(), &readSet)) {
            acceptPendingClients();
        }
    }
}

int TcpTunnel::registerInterest(fd_set& readSet, fd_set& writeSet) const
{
    int maxFd = -1;
    const auto watch = [&maxFd](const int fd, fd_set& set) {
        FD_SET(fd, &set);
        maxFd = std::max(maxFd, fd);
    };

    for (const auto& entry : sessions_) {
        const Session& session = *entry.second;
        if (session.state == detail::SessionState::Closed) {
            continue;
        }
        const int clientFd = session.clientFd.get();
        const int targetFd = session.targetFd.get();

        // The client may already send data while the target connect is in
        // flight; it is buffered until the connection is established.
        if (!session.upstream.sourceEof && !session.upstream.buffer.full()) {
            watch(clientFd, readSet);
        }

        if (session.state == detail::SessionState::Connecting) {
            // A non-blocking connect() completes when the socket becomes writable.
            watch(targetFd, writeSet);
            continue;
        }

        if (!session.downstream.sourceEof && !session.downstream.buffer.full()) {
            watch(targetFd, readSet);
        }
        if (!session.upstream.buffer.empty()) {
            watch(targetFd, writeSet);
        }
        if (!session.downstream.buffer.empty()) {
            watch(clientFd, writeSet);
        }
    }
    return maxFd;
}

void TcpTunnel::acceptPendingClients()
{
    // The listener is non-blocking: drain every pending connection.
    for (;;) {
        sockaddr_storage peerAddress{};
        socklen_t peerLength = sizeof(peerAddress);

        UniqueFd client(::accept(listenFd_.get(), reinterpret_cast<sockaddr*>(&peerAddress), &peerLength));
        if (!client.valid()) {
            const int error = errno;
            if (error == EAGAIN || error == EWOULDBLOCK) {
                return;  // backlog drained
            }
            if (error == EINTR || error == ECONNABORTED || error == EPROTO) {
                continue;  // client gave up before we accepted it; try the next one
            }
            if (error == EMFILE || error == ENFILE || error == ENOBUFS || error == ENOMEM) {
                logger::error("accept() failed, out of resources: ", errnoMessage(error));
                return;
            }
            throwSystemError(error, "accept() failed");
        }

        const std::string peer = formatAddress(reinterpret_cast<const sockaddr*>(&peerAddress), peerLength);

        if (sessions_.size() >= config_.maxSessions) {
            logger::warn("rejecting ", peer, ": session limit (", config_.maxSessions, ") reached");
            continue;  // UniqueFd closes the socket
        }
        if (client.get() >= FD_SETSIZE) {
            logger::warn("rejecting ", peer, ": descriptor ", client.get(), " exceeds FD_SETSIZE");
            continue;
        }
        if (const int error = makeNonBlocking(client.get()); error != 0) {
            logger::warn("rejecting ", peer, ": cannot make socket non-blocking: ", errnoMessage(error));
            continue;
        }
        enableTcpNoDelay(client.get());

        const std::uint64_t id = nextSessionId_++;
        ++totalSessions_;
        auto session = std::make_unique<Session>(id, peer, std::move(client), config_.bufferCapacity);

        logger::info("[#", id, "] accepted client ", peer,
                     " (active sessions: ", sessions_.size() + 1, "/", config_.maxSessions, ")");

        beginTargetConnect(*session);
        if (session->state != detail::SessionState::Closed) {
            sessions_.emplace(id, std::move(session));
        }
    }
}

void TcpTunnel::serviceSession(Session& session, const fd_set& readSet, const fd_set& writeSet)
{
    if (session.state == detail::SessionState::Closed) {
        return;
    }
    const int clientFd = session.clientFd.get();
    const int targetFd = session.targetFd.get();

    if (session.state == detail::SessionState::Connecting && FD_ISSET(targetFd, &writeSet)) {
        finishTargetConnect(session);
        if (session.state == detail::SessionState::Closed) {
            return;
        }
    }

    // client -> buffer (allowed while connecting)
    if (FD_ISSET(clientFd, &readSet) && !pumpRead(session, session.upstream, clientFd)) {
        return;
    }

    if (session.state != detail::SessionState::Established) {
        return;
    }

    // target -> buffer
    if (FD_ISSET(targetFd, &readSet) && !pumpRead(session, session.downstream, targetFd)) {
        return;
    }
    // buffer -> target / buffer -> client
    if (FD_ISSET(targetFd, &writeSet) && !pumpWrite(session, session.upstream, targetFd)) {
        return;
    }
    if (FD_ISSET(clientFd, &writeSet) && !pumpWrite(session, session.downstream, clientFd)) {
        return;
    }

    // Half-close handling: forward a FIN once a side finished sending and
    // every byte it sent has been delivered.
    propagateEof(session, session.upstream, targetFd);
    propagateEof(session, session.downstream, clientFd);

    if (session.upstream.sinkShutdown && session.downstream.sinkShutdown) {
        closeSession(session, "both directions finished", false);
    }
}

// ===========================================================================
// Session lifecycle
// ===========================================================================
void TcpTunnel::beginTargetConnect(Session& session)
{
    UniqueFd fd(::socket(targetAddress_.ss_family, SOCK_STREAM, IPPROTO_TCP));
    if (!fd.valid()) {
        closeSession(session, "socket() for target failed: " + errnoMessage(errno), true);
        return;
    }
    if (fd.get() >= FD_SETSIZE) {
        closeSession(session, "target descriptor exceeds FD_SETSIZE", true);
        return;
    }
    if (const int error = makeNonBlocking(fd.get()); error != 0) {
        closeSession(session, "cannot make target socket non-blocking: " + errnoMessage(error), true);
        return;
    }
    session.targetFd = std::move(fd);

    const int rc = ::connect(session.targetFd.get(),
                             reinterpret_cast<const sockaddr*>(&targetAddress_),
                             targetAddressLength_);
    if (rc == 0) {
        // Possible for loopback targets: the handshake finished immediately.
        session.state = detail::SessionState::Established;
        enableTcpNoDelay(session.targetFd.get());
        logger::info("[#", session.id, "] connected to target ", targetDescription_,
                     " immediately, tunnel established");
        return;
    }

    const int error = errno;
    if (error == EINPROGRESS || error == EINTR) {
        // Normal case: the TCP handshake continues in the background.
        logger::debug("[#", session.id, "] connecting to target ", targetDescription_, " ...");
        return;
    }
    closeSession(session, "connect() to " + targetDescription_ + " failed: " + errnoMessage(error), true);
}

void TcpTunnel::finishTargetConnect(Session& session)
{
    // A writable socket only means "connect finished": SO_ERROR tells whether
    // it succeeded or failed (e.g. ECONNREFUSED, ETIMEDOUT, EHOSTUNREACH).
    int socketError = 0;
    socklen_t length = sizeof(socketError);
    if (::getsockopt(session.targetFd.get(), SOL_SOCKET, SO_ERROR, &socketError, &length) < 0) {
        socketError = errno;
    }
    if (socketError != 0) {
        closeSession(session,
                     "connect() to " + targetDescription_ + " failed: " + errnoMessage(socketError),
                     true);
        return;
    }

    session.state = detail::SessionState::Established;
    enableTcpNoDelay(session.targetFd.get());
    logger::info("[#", session.id, "] connected to target ", targetDescription_, " in ",
                 millisecondsSince(session.startedAt), " ms, tunnel established");
}

void TcpTunnel::closeSession(Session& session, const std::string& reason, const bool isError)
{
    if (session.state == detail::SessionState::Closed) {
        return;
    }
    session.state = detail::SessionState::Closed;

    // Closing both sockets: a pending FIN/RST is sent by the kernel.
    session.clientFd.reset();
    session.targetFd.reset();

    totalBytesUpstream_ += session.upstream.totalBytes;
    totalBytesDownstream_ += session.downstream.totalBytes;

    std::ostringstream summary;
    summary << "[#" << session.id << "] session closed (" << reason << ")"
            << " | client -> target: " << formatBytes(session.upstream.totalBytes)
            << " in " << session.upstream.chunkCount << " chunks"
            << " | target -> client: " << formatBytes(session.downstream.totalBytes)
            << " in " << session.downstream.chunkCount << " chunks"
            << " | duration: " << millisecondsSince(session.startedAt) << " ms";

    if (isError) {
        logger::warn(summary.str());
    } else {
        logger::info(summary.str());
    }
}

void TcpTunnel::reapClosedSessions()
{
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        if (it->second->state == detail::SessionState::Closed) {
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
}

void TcpTunnel::shutdownAllSessions()
{
    for (auto& entry : sessions_) {
        closeSession(*entry.second, "tunnel shutting down", false);
    }
    sessions_.clear();
}

// ===========================================================================
// Data pumping
// ===========================================================================
bool TcpTunnel::pumpRead(Session& session, detail::Flow& flow, const int sourceFd)
{
    flow.buffer.compact();
    std::uint8_t* const destination = flow.buffer.writableData();
    const std::size_t room = flow.buffer.writableSize();
    if (room == 0) {
        return true;  // buffer full: back-pressure, wait for the sink to drain it
    }

    const ssize_t received = ::recv(sourceFd, destination, room, 0);
    if (received < 0) {
        const int error = errno;
        if (isTransientError(error)) {
            return true;  // spurious wake-up or signal: try again next round
        }
        closeSession(session,
                     std::string("recv() from ") + flow.sourceName + " failed: " + errnoMessage(error),
                     true);
        return false;
    }

    if (received == 0) {
        // Orderly shutdown (FIN) from the source. Remaining buffered bytes are
        // still delivered; propagateEof() forwards the FIN afterwards.
        flow.sourceEof = true;
        logger::info("[#", session.id, "] ", flow.sourceName, " closed its sending side (EOF), ",
                     flow.buffer.readableSize(), " bytes still pending for ", flow.sinkName);
        return true;
    }

    const auto count = static_cast<std::size_t>(received);
    const std::size_t shown = std::min(count, kHexPreviewBytes);

    // Keep a copy of the first bytes so verbose mode can show before/after.
    std::array<std::uint8_t, kHexPreviewBytes> original{};
    std::copy_n(destination, shown, original.begin());

    transformer_.apply(destination, count);  // the "middleware" step
    flow.buffer.commit(count);
    flow.totalBytes += count;
    ++flow.chunkCount;

    if (logger::isVerbose()) {
        std::ostringstream line;
        line << "[#" << session.id << "] " << flow.sourceName << " -> " << flow.sinkName
             << " | chunk #" << flow.chunkCount << " | " << count << " bytes"
             << " | in: " << hexPreview(original.data(), shown, count);
        if (!transformer_.isIdentity()) {
            line << " | out: " << hexPreview(destination, shown, count);
        }
        logger::debug(line.str());
    }
    return true;
}

bool TcpTunnel::pumpWrite(Session& session, detail::Flow& flow, const int sinkFd)
{
    if (flow.buffer.empty()) {
        return true;
    }

    const std::size_t pending = flow.buffer.readableSize();
    const ssize_t sent = ::send(sinkFd, flow.buffer.readableData(), pending, kSendFlags);
    if (sent < 0) {
        const int error = errno;
        if (isTransientError(error)) {
            return true;  // socket send buffer full: retry when writable again
        }
        // Typical: EPIPE / ECONNRESET when the peer vanished.
        closeSession(session,
                     std::string("send() to ") + flow.sinkName + " failed: " + errnoMessage(error),
                     true);
        return false;
    }

    const auto written = static_cast<std::size_t>(sent);
    flow.buffer.consume(written);

    if (written < pending) {
        // Partial write: the remainder stays buffered and is retried later.
        logger::debug("[#", session.id, "] ", flow.sourceName, " -> ", flow.sinkName,
                      " | partial send: ", written, "/", pending, " bytes, ",
                      pending - written, " bytes buffered");
    }
    return true;
}

void TcpTunnel::propagateEof(const Session& session, detail::Flow& flow, const int sinkFd) const
{
    if (!flow.sourceEof || flow.sinkShutdown || !flow.buffer.empty()) {
        return;
    }

    // Half-close: tell the sink "no more data" while still allowing the
    // opposite direction to keep flowing.
    if (::shutdown(sinkFd, SHUT_WR) < 0) {
        const int error = errno;
        if (error != ENOTCONN) {
            logger::warn("[#", session.id, "] shutdown(SHUT_WR) on ", flow.sinkName,
                         " failed: ", errnoMessage(error));
        }
    }
    flow.sinkShutdown = true;
    logger::info("[#", session.id, "] forwarded EOF to ", flow.sinkName, " (",
                 flow.sourceName, " -> ", flow.sinkName, " finished, ",
                 formatBytes(flow.totalBytes), " total)");
}

} // namespace tunnel
