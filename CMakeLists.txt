cmake_minimum_required(VERSION 3.16)
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()
project(BinanceLOBCapture LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Boost 1.75 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

option(ENABLE_SANITIZERS "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

add_library(lob_core STATIC
    src/json_impl.cpp
    src/types.cpp
    src/fixed_point.cpp
    src/binance_parser.cpp
    src/order_book.cpp
    src/csv_writer.cpp
)
target_include_directories(lob_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(lob_core PUBLIC Boost::headers)
target_compile_definitions(lob_core PUBLIC BOOST_ERROR_CODE_HEADER_ONLY)
target_compile_options(lob_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(binance_capture
    src/main.cpp
    src/websocket_client.cpp
    src/rest_client.cpp
    src/processor.cpp
    src/replay.cpp
)
target_link_libraries(binance_capture PRIVATE lob_core OpenSSL::SSL OpenSSL::Crypto Threads::Threads)
target_compile_options(binance_capture PRIVATE -Wall -Wextra -Wpedantic)

if(ENABLE_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(lob_core PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(lob_core PRIVATE -fsanitize=address,undefined)
    target_compile_options(binance_capture PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(binance_capture PRIVATE -fsanitize=address,undefined)
endif()

enable_testing()
add_executable(unit_tests tests/unit_tests.cpp)
target_link_libraries(unit_tests PRIVATE lob_core)
target_compile_options(unit_tests PRIVATE -Wall -Wextra -Wpedantic)
if(ENABLE_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(unit_tests PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(unit_tests PRIVATE -fsanitize=address,undefined)
endif()
add_test(NAME unit_tests COMMAND unit_tests)

# The included real capture is also an offline regression fixture. This checks
# that replay reconstructs the same order-book CSV without any network access.
set(SAMPLE_MARKET_CSV
    "${CMAKE_CURRENT_SOURCE_DIR}/sample/market_data_spot_BTCUSDT_2026-10-10.csv")
set(SAMPLE_ORDERBOOK_CSV
    "${CMAKE_CURRENT_SOURCE_DIR}/sample/market_data_spot_BTCUSDT_2026-10-10_orderbook.csv")
set(SAMPLE_REPLAY_DIR "${CMAKE_CURRENT_BINARY_DIR}/sample-replay")
if(EXISTS "${SAMPLE_MARKET_CSV}" AND EXISTS "${SAMPLE_ORDERBOOK_CSV}" AND
   EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/sample/market_data_spot_BTCUSDT_2026-10-10_snapshots.jsonl")
    add_test(NAME replay_real_capture
        COMMAND binance_capture --replay "${SAMPLE_MARKET_CSV}" --output-dir "${SAMPLE_REPLAY_DIR}")
    add_test(NAME replay_real_capture_matches
        COMMAND ${CMAKE_COMMAND} -E compare_files
            "${SAMPLE_ORDERBOOK_CSV}"
            "${SAMPLE_REPLAY_DIR}/market_data_spot_BTCUSDT_2026-10-10_orderbook.csv")
    set_tests_properties(replay_real_capture_matches PROPERTIES DEPENDS replay_real_capture)
endif()
