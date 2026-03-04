#pragma once

#include "handle.h"
#include <string>
#include <vector>
#include <memory>
#include <nlohmann/json.hpp>
#include <httplib.h>

namespace clickhouse_odbc {

class ClickHouseClient {
public:
    ClickHouseClient() = default;
    ~ClickHouseClient() = default;

    // Connect to ClickHouse
    bool Connect(const std::string &host, uint16_t port, const std::string &database,
                 const std::string &user, const std::string &password);

    // Disconnect
    void Disconnect();

    // Execute query, populate result set
    bool ExecuteQuery(const std::string &query, ResultSet &result_set, std::string &error_msg);

    // Execute SELECT count() to get total row count
    bool ExecuteCountQuery(const std::string &base_query, size_t &out_count, std::string &error_msg);

    // Fetch data page by page (execute query with LIMIT/OFFSET, does not set column metadata)
    bool ExecutePageQuery(const std::string &page_query, std::vector<ResultRow> &out_rows, std::string &error_msg);

    // Ping (health check)
    bool Ping();

    bool IsConnected() const { return connected_; }

    // Get server version
    std::string GetServerVersion();

    // Timeout configuration (in seconds, 0 = no timeout)
    void SetConnectionTimeout(unsigned int seconds) { connection_timeout_ = seconds; }
    void SetQueryTimeout(unsigned int seconds) { query_timeout_ = seconds; }
    unsigned int GetConnectionTimeout() const { return connection_timeout_; }
    unsigned int GetQueryTimeout() const { return query_timeout_; }

    // Retry configuration
    void SetRetryCount(unsigned int count) { retry_count_ = count; }
    void SetRetryDelayMs(unsigned int ms) { retry_delay_ms_ = ms; }
    unsigned int GetRetryCount() const { return retry_count_; }

    // HTTP compression setting
    void SetCompressionEnabled(bool enabled) { compression_enabled_ = enabled; }
    bool IsCompressionEnabled() const { return compression_enabled_; }

    // SSL/TLS configuration
    void SetSSLEnabled(bool enabled) { ssl_enabled_ = enabled; }
    bool IsSSLEnabled() const { return ssl_enabled_; }
    void SetSSLVerify(bool verify) { ssl_verify_ = verify; }
    bool IsSSLVerify() const { return ssl_verify_; }

    // Client-side row limit (0 = unlimited)
    void SetMaxRows(SQLULEN max_rows) { max_rows_ = max_rows; }
    SQLULEN GetMaxRows() const { return max_rows_; }

    // Map ClickHouse type to ODBC ColumnInfo
    static ColumnInfo MapClickHouseType(const std::string &name, const std::string &ch_type);

    // Threshold (bytes) for auto-selecting DOM/SAX parser based on response size
    static constexpr size_t kSaxParserThreshold = 4 * 1024 * 1024;  // 4MB

private:
    std::string host_;
    uint16_t port_ = 8123;
    std::string database_;
    std::string user_;
    std::string password_;
    bool connected_ = false;

    // Timeout settings (seconds)
    unsigned int connection_timeout_ = 30;
    unsigned int query_timeout_ = 300;

    // Retry settings
    unsigned int retry_count_ = 0;     // 0 = no retry (single attempt)
    unsigned int retry_delay_ms_ = 1000;

    // HTTP compression (enabled by default: uses gzip decompression for responses)
    bool compression_enabled_ = true;

    // SSL/TLS settings
    bool ssl_enabled_ = false;
    bool ssl_verify_ = true;

    // Client-side row limit (0 = unlimited, applied in ParseJsonResponse)
    SQLULEN max_rows_ = 0;

    // Create an httplib::Client configured with SSL/timeout settings
    std::unique_ptr<httplib::Client> CreateHttpClient(int read_timeout_sec = 0);

    // Parse ClickHouse JSON response (DOM-based, for small/medium results)
    bool ParseJsonResponse(const std::string &body, ResultSet &result_set, std::string &error_msg);

    // Parse ClickHouse JSON response (SAX-based streaming, for large results)
    bool ParseJsonResponseSAX(const std::string &body, ResultSet &result_set, std::string &error_msg);
};

} // namespace clickhouse_odbc
