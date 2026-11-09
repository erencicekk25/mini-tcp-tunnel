#pragma once

// Minimal, header-only, timestamped console logger.
//
// Info/Debug messages go to stdout, Warn/Error messages go to stderr.
// Debug messages are only printed when verbose mode is enabled.

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>

namespace tunnel::logger {

enum class Level { Debug, Info, Warn, Error };

namespace detail {

inline std::atomic<bool>& verboseFlag() noexcept
{
    static std::atomic<bool> flag{false};
    return flag;
}

[[nodiscard]] inline std::string timestamp()
{
    using Clock = std::chrono::system_clock;

    const Clock::time_point now = Clock::now();
    const std::time_t seconds = Clock::to_time_t(now);
    const auto millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;

    std::tm local{};
    localtime_r(&seconds, &local);

    std::ostringstream out;
    out << std::put_time(&local, "%H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << millis;
    return out.str();
}

[[nodiscard]] constexpr std::string_view label(const Level level) noexcept
{
    switch (level) {
    case Level::Debug: return "[DEBUG]";
    case Level::Info:  return "[INFO ]";
    case Level::Warn:  return "[WARN ]";
    case Level::Error: return "[ERROR]";
    }
    return "[?????]";
}

} // namespace detail

inline void setVerbose(const bool enabled) noexcept
{
    detail::verboseFlag().store(enabled, std::memory_order_relaxed);
}

[[nodiscard]] inline bool isVerbose() noexcept
{
    return detail::verboseFlag().load(std::memory_order_relaxed);
}

template <typename... Args>
void write(const Level level, const Args&... args)
{
    if (level == Level::Debug && !isVerbose()) {
        return;
    }

    // Build the whole line first so that a single write reaches the stream.
    std::ostringstream line;
    line << detail::timestamp() << ' ' << detail::label(level) << ' ';
    (line << ... << args);
    line << '\n';

    std::ostream& out = (level >= Level::Warn) ? std::cerr : std::cout;
    out << line.str() << std::flush;
}

template <typename... Args> void debug(const Args&... args) { write(Level::Debug, args...); }
template <typename... Args> void info(const Args&... args)  { write(Level::Info, args...); }
template <typename... Args> void warn(const Args&... args)  { write(Level::Warn, args...); }
template <typename... Args> void error(const Args&... args) { write(Level::Error, args...); }

} // namespace tunnel::logger
