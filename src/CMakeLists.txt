set(CLICKHOUSE_ODBC_SOURCES
    dllmain.cpp
    clickhouse_odbc.cpp
    handle.cpp
    connection.cpp
    statement.cpp
    result.cpp
    catalog.cpp
    info.cpp
    clickhouse_client.cpp
    type_mapping.cpp
    util.cpp
)

add_library(clickhouse_odbc SHARED ${CLICKHOUSE_ODBC_SOURCES})

target_include_directories(clickhouse_odbc PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(clickhouse_odbc PRIVATE
    httplib::httplib
    nlohmann_json::nlohmann_json
)

if(WIN32)
    target_link_libraries(clickhouse_odbc PRIVATE odbc32 odbccp32 ws2_32)
    set_target_properties(clickhouse_odbc PROPERTIES
        OUTPUT_NAME "clickhouse_odbc"
        SUFFIX ".dll"
    )
    # Use .def file for ODBC exports
    target_sources(clickhouse_odbc PRIVATE clickhouse_odbc.def)
endif()

# Install
install(TARGETS clickhouse_odbc
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
)
