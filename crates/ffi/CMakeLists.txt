cmake_minimum_required(VERSION 3.16)
project(potatodb-cpp-examples LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

enable_testing()

# ---------------------------------------------------------------------------
# The Rust static library is built by Cargo. Point CARGO_TARGET_DIR at the
# workspace target directory (the default when built from the repo root).
# Override on the command line if your layout differs:
#
#   cmake -B build -DCARGO_TARGET_DIR=../../target
# ---------------------------------------------------------------------------
set(CARGO_TARGET_DIR "${CMAKE_SOURCE_DIR}/../../target" CACHE PATH
    "Path to the Cargo target directory")

set(POTATODB_LIB_DIR "${CARGO_TARGET_DIR}/release")

# Imported static library produced by `cargo build --release -p potatodb-ffi`
add_library(potatodb_ffi STATIC IMPORTED)

if(MSVC)
    set_target_properties(potatodb_ffi PROPERTIES
        IMPORTED_LOCATION "${POTATODB_LIB_DIR}/potatodb_ffi.lib"
    )
else()
    set_target_properties(potatodb_ffi PROPERTIES
        IMPORTED_LOCATION "${POTATODB_LIB_DIR}/libpotatodb_ffi.a"
    )
endif()

# System libraries required by the Rust static lib
find_package(Threads REQUIRED)
target_link_libraries(potatodb_ffi INTERFACE Threads::Threads)

if(MSVC)
    # Windows system libraries needed by Rust runtime
    target_link_libraries(potatodb_ffi INTERFACE
        ws2_32 userenv advapi32 bcrypt ntdll synchronization
    )
elseif(APPLE)
    # macOS: Rust staticlib uses rustls -> security-framework
    target_link_libraries(potatodb_ffi INTERFACE
        ${CMAKE_DL_LIBS}
        m
        "-framework Security"
        "-framework CoreFoundation"
        "-framework Foundation"
    )
else()
    target_link_libraries(potatodb_ffi INTERFACE
        ${CMAKE_DL_LIBS}   # -ldl
        m                   # -lm
    )
endif()

# ---------------------------------------------------------------------------
# Example executables
# ---------------------------------------------------------------------------

# Original combined example
add_executable(potatodb_example examples/main.cpp)
target_include_directories(potatodb_example PRIVATE include)
target_link_libraries(potatodb_example PRIVATE potatodb_ffi)

# Basic CRUD operations (C++ API)
add_executable(potatodb_basic examples/basic.cpp)
target_include_directories(potatodb_basic PRIVATE include)
target_link_libraries(potatodb_basic PRIVATE potatodb_ffi)

# Prepared statements (C++ API)
add_executable(potatodb_prepared examples/prepared.cpp)
target_include_directories(potatodb_prepared PRIVATE include)
target_link_libraries(potatodb_prepared PRIVATE potatodb_ffi)

# Analytical queries (C++ API)
add_executable(potatodb_analytics examples/analytics.cpp)
target_include_directories(potatodb_analytics PRIVATE include)
target_link_libraries(potatodb_analytics PRIVATE potatodb_ffi)

# Plain C API usage
add_executable(potatodb_c_api examples/c_api.cpp)
target_include_directories(potatodb_c_api PRIVATE include)
target_link_libraries(potatodb_c_api PRIVATE potatodb_ffi)

# ---------------------------------------------------------------------------
# Tests (doctest)
# ---------------------------------------------------------------------------

add_executable(potatodb_tests tests/test_ffi.cpp)
target_include_directories(potatodb_tests PRIVATE include tests)
target_link_libraries(potatodb_tests PRIVATE potatodb_ffi)

add_test(NAME potatodb_ffi_tests COMMAND potatodb_tests)
