// mini-tcp-tunnel command line interface.
//
// Example:
//   mini-tcp-tunnel --listen-port 8080 --target-host 127.0.0.1 --target-port 9000 --xor-key 0x5A -v

#include "Logger.hpp"
#include "TcpTunnel.hpp"

#include <atomic>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <signal.h>
#include <sys/select.h>

namespace {

constexpr int kExitSuccess = 0;
constexpr int kExitRuntimeError = 1;
constexpr int kExitUsageError = 2;

constexpr std::size_t kMinBufferSize = 1024;
constexpr std::size_t kMaxBufferSize = 16 * 1024 * 1024;

// Every session uses two descriptors (client + target); keep a few spare for
// the listener and standard streams so that all fit into an fd_set.
constexpr std::size_t kMaxSessionsLimit = (FD_SETSIZE - 16) / 2;

// ---------------------------------------------------------------------------
// Signal handling
// ---------------------------------------------------------------------------

// The signal handler may only touch lock-free atomics.
std::atomic<tunnel::TcpTunnel*> g_activeTunnel{nullptr};
static_assert(std::atomic<tunnel::TcpTunnel*>::is_always_lock_free,
              "signal handler requires a lock-free atomic pointer");

void handleTerminationSignal(int /*signalNumber*/)
{
    if (tunnel::TcpTunnel* const active = g_activeTunnel.load()) {
        active->stop();
    }
}

void installSignalHandlers()
{
    struct sigaction action {};
    action.sa_handler = handleTerminationSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;  // no SA_RESTART: let select() return EINTR promptly

    if (::sigaction(SIGINT, &action, nullptr) < 0 || ::sigaction(SIGTERM, &action, nullptr) < 0) {
        throw std::system_error(errno, std::generic_category(), "sigaction() failed");
    }

    // Writing to a socket whose peer has disconnected must yield EPIPE
    // instead of killing the process.
    struct sigaction ignore {};
    ignore.sa_handler = SIG_IGN;
    sigemptyset(&ignore.sa_mask);
    if (::sigaction(SIGPIPE, &ignore, nullptr) < 0) {
        throw std::system_error(errno, std::generic_category(), "sigaction(SIGPIPE) failed");
    }
}

// Publishes the tunnel to the signal handler for exactly its lifetime.
class ActiveTunnelGuard {
public:
    explicit ActiveTunnelGuard(tunnel::TcpTunnel& active) noexcept { g_activeTunnel.store(&active); }
    ~ActiveTunnelGuard() { g_activeTunnel.store(nullptr); }

    ActiveTunnelGuard(const ActiveTunnelGuard&) = delete;
    ActiveTunnelGuard& operator=(const ActiveTunnelGuard&) = delete;
    ActiveTunnelGuard(ActiveTunnelGuard&&) = delete;
    ActiveTunnelGuard& operator=(ActiveTunnelGuard&&) = delete;
};

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------
class UsageError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

void printUsage(std::ostream& out, const std::string_view program)
{
    out << "mini-tcp-tunnel - educational bi-directional TCP tunnel (select() based)\n"
           "\n"
           "Usage:\n"
           "  " << program << " -l <port> -t <host> -p <port> [options]\n"
           "\n"
           "Required:\n"
           "  -l, --listen-port <port>    local port to accept clients on (1-65535)\n"
           "  -t, --target-host <host>    target server hostname or IP address\n"
           "  -p, --target-port <port>    target server port (1-65535)\n"
           "\n"
           "Options:\n"
           "  -a, --listen-addr <ip>      local IP address to bind (default: 0.0.0.0)\n"
           "  -x, --xor-key <byte>        XOR every relayed byte with this key\n"
           "                              (decimal or hex, e.g. 90 or 0x5A; 1-255)\n"
           "  -m, --max-sessions <n>      maximum concurrent sessions (default: 64, max: "
        << kMaxSessionsLimit << ")\n"
           "  -b, --buffer-size <bytes>   buffer size per direction (default: 65536)\n"
           "  -v, --verbose               log every chunk with size and hex preview\n"
           "  -h, --help                  show this help and exit\n"
           "\n"
           "Example:\n"
           "  " << program << " -l 8080 -t 127.0.0.1 -p 9000 -x 0x5A -v\n";
}

// Parses an unsigned integer in decimal or "0x"-prefixed hexadecimal form and
// checks that it lies within [minValue, maxValue].
[[nodiscard]] unsigned long long parseUnsigned(const std::string_view option, const std::string& text,
                                               const unsigned long long minValue,
                                               const unsigned long long maxValue)
{
    const auto fail = [&]() -> UsageError {
        return UsageError("invalid value '" + text + "' for " + std::string(option) + " (expected " +
                          std::to_string(minValue) + "-" + std::to_string(maxValue) + ")");
    };

    if (text.empty() || !std::isdigit(static_cast<unsigned char>(text.front()))) {
        throw fail();  // rejects "", "-1", "+1", " 1"
    }

    const bool isHex = text.size() > 2 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X');
    const int base = isHex ? 16 : 10;

    errno = 0;
    char* end = nullptr;
    const unsigned long long value = std::strtoull(text.c_str(), &end, base);
    if (errno == ERANGE || end == text.c_str() || *end != '\0' || value < minValue || value > maxValue) {
        throw fail();
    }
    return value;
}

[[nodiscard]] std::uint16_t parsePort(const std::string_view option, const std::string& text)
{
    return static_cast<std::uint16_t>(parseUnsigned(option, text, 1, 65535));
}

// Returns std::nullopt when --help was requested.
[[nodiscard]] std::optional<tunnel::TunnelConfig> parseArguments(const int argc, char* const argv[])
{
    const std::vector<std::string_view> args(argv + 1, argv + argc);
    tunnel::TunnelConfig config;

    for (std::size_t i = 0; i < args.size(); ++i) {
        const std::string_view option = args[i];

        const auto requireValue = [&]() -> std::string {
            if (i + 1 >= args.size()) {
                throw UsageError("option '" + std::string(option) + "' requires a value");
            }
            ++i;
            return std::string(args[i]);
        };

        if (option == "-h" || option == "--help") {
            return std::nullopt;
        }
        if (option == "-l" || option == "--listen-port") {
            config.listenPort = parsePort(option, requireValue());
        } else if (option == "-a" || option == "--listen-addr") {
            config.listenAddress = requireValue();
            if (config.listenAddress.empty()) {
                throw UsageError("listen address must not be empty");
            }
        } else if (option == "-t" || option == "--target-host") {
            config.targetHost = requireValue();
            if (config.targetHost.empty()) {
                throw UsageError("target host must not be empty");
            }
        } else if (option == "-p" || option == "--target-port") {
            config.targetPort = parsePort(option, requireValue());
        } else if (option == "-x" || option == "--xor-key") {
            config.xorKey = static_cast<std::uint8_t>(parseUnsigned(option, requireValue(), 1, 255));
        } else if (option == "-m" || option == "--max-sessions") {
            config.maxSessions = static_cast<std::size_t>(
                parseUnsigned(option, requireValue(), 1, kMaxSessionsLimit));
        } else if (option == "-b" || option == "--buffer-size") {
            config.bufferCapacity = static_cast<std::size_t>(
                parseUnsigned(option, requireValue(), kMinBufferSize, kMaxBufferSize));
        } else if (option == "-v" || option == "--verbose") {
            config.verbose = true;
        } else {
            throw UsageError("unknown option '" + std::string(option) + "'");
        }
    }

    if (config.listenPort == 0) {
        throw UsageError("missing required option --listen-port");
    }
    if (config.targetHost.empty()) {
        throw UsageError("missing required option --target-host");
    }
    if (config.targetPort == 0) {
        throw UsageError("missing required option --target-port");
    }
    return config;
}

} // namespace

int main(int argc, char* argv[])
{
    const std::string_view program = (argc > 0 && argv[0] != nullptr) ? argv[0] : "mini-tcp-tunnel";

    std::optional<tunnel::TunnelConfig> config;
    try {
        config = parseArguments(argc, argv);
    } catch (const UsageError& error) {
        std::cerr << "error: " << error.what() << "\n\n";
        printUsage(std::cerr, program);
        return kExitUsageError;
    }

    if (!config) {
        printUsage(std::cout, program);
        return kExitSuccess;
    }

    tunnel::logger::setVerbose(config->verbose);

    try {
        tunnel::TcpTunnel tcpTunnel(std::move(*config));
        const ActiveTunnelGuard guard(tcpTunnel);
        installSignalHandlers();
        tcpTunnel.run();
    } catch (const std::exception& error) {
        tunnel::logger::error("fatal: ", error.what());
        return kExitRuntimeError;
    }

    return kExitSuccess;
}
