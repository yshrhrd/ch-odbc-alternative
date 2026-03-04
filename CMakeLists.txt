cmake_minimum_required(VERSION 3.20)
project(clickhouse-odbc-alternative VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Windows ODBC libraries
if(WIN32)
    find_library(ODBC32_LIB odbc32)
    find_library(ODBCCP32_LIB odbccp32)
endif()

# cpp-httplib (header-only) - for ClickHouse HTTP connection
include(FetchContent)

FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG        v0.18.3
)
FetchContent_MakeAvailable(httplib)

FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
)
FetchContent_MakeAvailable(nlohmann_json)

add_subdirectory(src)
