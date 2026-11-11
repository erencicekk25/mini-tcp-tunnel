cmake_minimum_required(VERSION 3.16)

project(mini_tcp_tunnel
    VERSION 1.0.0
    DESCRIPTION "Educational bi-directional TCP tunnel built on raw Berkeley sockets"
    LANGUAGES CXX
)

# ---------------------------------------------------------------------------
# Platform check: the project intentionally uses raw POSIX/Berkeley sockets
# (sys/socket.h, netinet/in.h, select()). Native MSVC/Winsock is not supported.
# ---------------------------------------------------------------------------
if(WIN32 AND NOT CYGWIN AND NOT MSYS)
    message(FATAL_ERROR
        "mini-tcp-tunnel requires POSIX sockets. "
        "Build it on Linux, macOS or inside WSL on Windows.")
endif()

# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

option(TUNNEL_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)
option(TUNNEL_ENABLE_SANITIZERS "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

# ---------------------------------------------------------------------------
# Shared compile options (interface target)
# ---------------------------------------------------------------------------
add_library(tunnel_project_options INTERFACE)
target_compile_features(tunnel_project_options INTERFACE cxx_std_17)
target_compile_options(tunnel_project_options INTERFACE
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wnon-virtual-dtor
    -Wcast-align
    -Wunused
    -Woverloaded-virtual
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
)

if(TUNNEL_WARNINGS_AS_ERRORS)
    target_compile_options(tunnel_project_options INTERFACE -Werror)
endif()

if(TUNNEL_ENABLE_SANITIZERS)
    target_compile_options(tunnel_project_options INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(tunnel_project_options INTERFACE -fsanitize=address,undefined)
endif()

# ---------------------------------------------------------------------------
# Core library: tunnelling logic
# ---------------------------------------------------------------------------
add_library(tcptunnel STATIC
    src/TcpTunnel.cpp
)
target_include_directories(tcptunnel
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(tcptunnel PRIVATE tunnel_project_options)
set_target_properties(tcptunnel PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

# ---------------------------------------------------------------------------
# Executable: command line interface
# ---------------------------------------------------------------------------
add_executable(mini-tcp-tunnel
    src/main.cpp
)
target_link_libraries(mini-tcp-tunnel
    PRIVATE
        tcptunnel
        tunnel_project_options
)
set_target_properties(mini-tcp-tunnel PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

install(TARGETS mini-tcp-tunnel RUNTIME DESTINATION bin)

# ---------------------------------------------------------------------------
# Tests: transformer, fd wrapper and loopback round trips through the tunnel
# ---------------------------------------------------------------------------
option(TUNNEL_BUILD_TESTS "Build the test suite" ON)
if(TUNNEL_BUILD_TESTS)
    enable_testing()
    find_package(Threads REQUIRED)
    add_executable(tunnel_tests tests/TunnelTests.cpp)
    target_link_libraries(tunnel_tests PRIVATE tcptunnel tunnel_project_options Threads::Threads)
    set_target_properties(tunnel_tests PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
    add_test(NAME tunnel_tests COMMAND tunnel_tests)
    set_tests_properties(tunnel_tests PROPERTIES TIMEOUT 120)
endif()
