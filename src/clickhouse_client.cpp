#include "include/clickhouse_client.h"
#include "include/type_mapping.h"
#include "include/trace.h"

#include <nlohmann/json.hpp>

#include <sstream>
#include <iomanip>
#include <algorithm>
#include <thread>
#include <chrono>

namespace clickhouse_odbc {

// Create httplib::Client with SSL/timeout/compression configuration
std::unique_ptr<httplib::Client> ClickHouseClient::CreateHttpClient(int read_timeout_sec) {
    std::unique_ptr<httplib::Client> cli;

#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    if (ssl_enabled_) {
        std::string url = "https://" + host_ + ":" + std::to_string(port_);
        cli = std::make_unique<httplib::Client>(url);
        if (!ssl_verify_) {
            cli->enable_server_certificate_verification(false);
            cli->enable_server_hostname_verification(false);
        }
    } else {
        cli = std::make_unique<httplib::Client>(host_, port_);
    }
#else
    cli = std::make_unique<httplib::Client>(host_, port_);
#endif

    int conn_timeout = connection_timeout_ > 0 ? (int)connection_timeout_ : 5;
    cli->set_connection_timeout(conn_timeout, 0);
    if (read_timeout_sec > 0) {
        cli->set_read_timeout(read_timeout_sec, 0);
    } else {
        cli->set_read_timeout(conn_timeout, 0);
    }

#ifdef CPPHTTPLIB_ZLIB_SUPPORT
    cli->set_decompress(true);
#endif

    return cli;
}

bool ClickHouseClient::Connect(const std::string &host, uint16_t port, const std::string &database,
                               const std::string &user, const std::string &password) {
    host_ = host;
    port_ = port;
    database_ = database;
    user_ = user;
    password_ = password;

    // Attempt connection with retries
    unsigned int attempts = 1 + retry_count_;
    for (unsigned int i = 0; i < attempts; i++) {
        if (Ping()) {
            connected_ = true;
            return true;
        }
        // Wait before retry (skip delay on last attempt)
        if (i + 1 < attempts && retry_delay_ms_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms_));
        }
    }

    connected_ = false;
    return false;
}

void ClickHouseClient::Disconnect() {
    connected_ = false;
}

bool ClickHouseClient::Ping() {
    try {
        auto cli = CreateHttpClient();
        auto res = cli->Get("/ping");
        return res && res->status == 200;
    } catch (...) {
        return false;
    }
}

std::string ClickHouseClient::GetServerVersion() {
    try {
        auto cli = CreateHttpClient();

        httplib::Params params;
        params.emplace("user", user_);
        if (!password_.empty()) {
            params.emplace("password", password_);
        }
        params.emplace("query", "SELECT version()");

        auto res = cli->Get("/", params, httplib::Headers());
        if (res && res->status == 200) {
            std::string ver = res->body;
            // Trim trailing newline
            while (!ver.empty() && (ver.back() == '\n' || ver.back() == '\r')) {
                ver.pop_back();
            }
            return ver;
        }
    } catch (...) {
    }
    return "unknown";
}

bool ClickHouseClient::ExecuteQuery(const std::string &query, ResultSet &result_set, std::string &error_msg) {
    result_set.Reset();

    unsigned int attempts = 1 + retry_count_;
    for (unsigned int attempt = 0; attempt < attempts; attempt++) {
        try {
            int qry_timeout = query_timeout_ > 0 ? (int)query_timeout_ : 300;
            auto cli = CreateHttpClient(qry_timeout);

            // Send query via POST with FORMAT JSONCompact
            std::string full_query = query;

            // Check if query already has FORMAT clause
            std::string upper_query = query;
            std::transform(upper_query.begin(), upper_query.end(), upper_query.begin(), ::toupper);

            bool is_select = (upper_query.find("SELECT") != std::string::npos ||
                              upper_query.find("SHOW") != std::string::npos ||
                              upper_query.find("DESCRIBE") != std::string::npos ||
                              upper_query.find("EXISTS") != std::string::npos);

            if (is_select && upper_query.find("FORMAT") == std::string::npos) {
                full_query += " FORMAT JSONCompact";
            }

            std::string query_string = "/?";
            httplib::Params url_params;
            url_params.emplace("user", user_);
            if (!password_.empty()) {
                url_params.emplace("password", password_);
            }
            url_params.emplace("database", database_);

            // If HTTP compression is enabled, request compressed responses from ClickHouse
            if (compression_enabled_) {
                url_params.emplace("enable_http_compression", "1");
            }

            query_string += httplib::detail::params_to_query_str(url_params);

            TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "ExecuteQuery",
                      std::string(ssl_enabled_ ? "HTTPS " : "HTTP ") +
                      "POST " + host_ + ":" + std::to_string(port_) + query_string +
                      " compression=" + (compression_enabled_ ? "on" : "off"));
            TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "ExecuteQuery",
                      "Query: " + full_query);

            // Build request headers
            httplib::Headers headers;
#ifdef CPPHTTPLIB_ZLIB_SUPPORT
            if (compression_enabled_) {
                // Set Accept-Encoding to request gzip responses from ClickHouse
                // cpp-httplib automatically decompresses gzip/deflate responses
                headers.emplace("Accept-Encoding", "gzip, deflate");
            }
#endif

            auto res = cli->Post(query_string, headers, full_query, "text/plain");

            if (!res) {
                error_msg = "Connection failed: unable to reach ClickHouse server at " + host_ + ":" + std::to_string(port_);
                TRACE_LOG(clickhouse_odbc::TraceLevel::Error, "ExecuteQuery", error_msg);
                // Retry on connection failure
                if (attempt + 1 < attempts && retry_delay_ms_ > 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms_));
                    result_set.Reset();
                    continue;
                }
                return false;
            }

            if (res->status != 200) {
                error_msg = "ClickHouse error: " + res->body;
                TRACE_LOG(clickhouse_odbc::TraceLevel::Error, "ExecuteQuery",
                          "HTTP " + std::to_string(res->status) + ": " + res->body.substr(0, 500));
                // Don't retry on server-side errors (query errors)
                return false;
            }

            // Non-select queries don't return result set
            if (!is_select) {
                TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "ExecuteQuery", "Non-SELECT OK");
                return true;
            }

            TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "ExecuteQuery",
                      "Response (" + std::to_string(res->body.size()) + " bytes): " +
                      res->body.substr(0, 300));

            // Automatically select parser based on response size
            // Large responses use SAX parser to avoid DOM construction for memory efficiency
            if (res->body.size() > kSaxParserThreshold) {
                TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "ExecuteQuery",
                          "Using SAX parser for large response (" +
                          std::to_string(res->body.size()) + " bytes)");
                return ParseJsonResponseSAX(res->body, result_set, error_msg);
            }
            return ParseJsonResponse(res->body, result_set, error_msg);

        } catch (const std::exception &e) {
            error_msg = std::string("Exception: ") + e.what();
            if (attempt + 1 < attempts && retry_delay_ms_ > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms_));
                result_set.Reset();
                continue;
            }
            return false;
        }
    }

    return false;
}

bool ClickHouseClient::ExecuteCountQuery(const std::string &base_query, size_t &out_count, std::string &error_msg) {
    // Generate a count() query from base_query
    // e.g.: "SELECT * FROM table WHERE x > 1" -> "SELECT count() FROM (SELECT * FROM table WHERE x > 1)"
    std::string count_query = "SELECT count() FROM (" + base_query + ")";

    TRACE_LOG(TraceLevel::Info, "ExecuteCountQuery", "Query: " + count_query);

    try {
        int qry_timeout = query_timeout_ > 0 ? (int)query_timeout_ : 300;
        auto cli = CreateHttpClient(qry_timeout);

        httplib::Params url_params;
        url_params.emplace("user", user_);
        if (!password_.empty()) {
            url_params.emplace("password", password_);
        }
        url_params.emplace("database", database_);
        if (compression_enabled_) {
            url_params.emplace("enable_http_compression", "1");
        }

        std::string query_string = "/?" + httplib::detail::params_to_query_str(url_params);

        httplib::Headers headers;
#ifdef CPPHTTPLIB_ZLIB_SUPPORT
        if (compression_enabled_) {
            headers.emplace("Accept-Encoding", "gzip, deflate");
        }
#endif

        auto res = cli->Post(query_string, headers, count_query, "text/plain");
        if (!res) {
            error_msg = "Connection failed during count query";
            return false;
        }
        if (res->status != 200) {
            error_msg = "Count query error: " + res->body;
            return false;
        }

        // Response contains only a number (e.g., "12345\n")
        std::string body = res->body;
        while (!body.empty() && (body.back() == '\n' || body.back() == '\r')) {
            body.pop_back();
        }
        out_count = std::stoull(body);
        TRACE_LOG(TraceLevel::Info, "ExecuteCountQuery", "Count: " + std::to_string(out_count));
        return true;

    } catch (const std::exception &e) {
        error_msg = std::string("Count query exception: ") + e.what();
        return false;
    }
}

bool ClickHouseClient::ExecutePageQuery(const std::string &page_query, std::vector<ResultRow> &out_rows, std::string &error_msg) {
    // Execute page query with FORMAT JSONCompact and retrieve row data only
    TRACE_LOG(TraceLevel::Debug, "ExecutePageQuery", "Query: " + page_query.substr(0, 300));

    try {
        int qry_timeout = query_timeout_ > 0 ? (int)query_timeout_ : 300;
        auto cli = CreateHttpClient(qry_timeout);

        std::string full_query = page_query + " FORMAT JSONCompact";

        httplib::Params url_params;
        url_params.emplace("user", user_);
        if (!password_.empty()) {
            url_params.emplace("password", password_);
        }
        url_params.emplace("database", database_);
        if (compression_enabled_) {
            url_params.emplace("enable_http_compression", "1");
        }

        std::string query_string = "/?" + httplib::detail::params_to_query_str(url_params);

        httplib::Headers headers;
#ifdef CPPHTTPLIB_ZLIB_SUPPORT
        if (compression_enabled_) {
            headers.emplace("Accept-Encoding", "gzip, deflate");
        }
#endif

        auto res = cli->Post(query_string, headers, full_query, "text/plain");
        if (!res) {
            error_msg = "Connection failed during page query";
            return false;
        }
        if (res->status != 200) {
            error_msg = "Page query error: " + res->body;
            return false;
        }

        // JSON parse (retrieve data array only)
        auto json = nlohmann::json::parse(res->body);
        if (json.contains("data")) {
            for (const auto &row : json["data"]) {
                ResultRow result_row;
                for (size_t i = 0; i < row.size(); i++) {
                    if (row[i].is_null()) {
                        result_row.push_back(std::nullopt);
                    } else if (row[i].is_boolean()) {
                        result_row.push_back(row[i].get<bool>() ? std::string("1") : std::string("0"));
                    } else if (row[i].is_string()) {
                        result_row.push_back(row[i].get<std::string>());
                    } else if (row[i].is_number_integer()) {
                        if (row[i].is_number_unsigned()) {
                            result_row.push_back(std::to_string(row[i].get<uint64_t>()));
                        } else {
                            result_row.push_back(std::to_string(row[i].get<int64_t>()));
                        }
                    } else if (row[i].is_number_float()) {
                        std::ostringstream oss;
                        oss << std::setprecision(17) << row[i].get<double>();
                        result_row.push_back(oss.str());
                    } else {
                        result_row.push_back(row[i].dump());
                    }
                }
                out_rows.push_back(std::move(result_row));
            }
        }

        TRACE_LOG(TraceLevel::Debug, "ExecutePageQuery",
                  "Fetched " + std::to_string(out_rows.size()) + " rows");
        return true;

    } catch (const std::exception &e) {
        error_msg = std::string("Page query exception: ") + e.what();
        return false;
    }
}

bool ClickHouseClient::ParseJsonResponse(const std::string &body, ResultSet &result_set, std::string &error_msg) {
    try {
        auto json = nlohmann::json::parse(body);

        // Parse metadata (column names and types)
        if (json.contains("meta")) {
            for (const auto &col : json["meta"]) {
                std::string name = col["name"].get<std::string>();
                std::string type = col["type"].get<std::string>();
                result_set.columns.push_back(MapClickHouseType(name, type));
            }
        }

        // Parse data rows (JSONCompact format: array of arrays)
        if (json.contains("data")) {
            size_t row_limit = (max_rows_ > 0) ? static_cast<size_t>(max_rows_) : SIZE_MAX;
            for (const auto &row : json["data"]) {
                if (result_set.rows.size() >= row_limit) {
                    TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "ParseJsonResponse",
                              "Row limit reached: " + std::to_string(row_limit));
                    break;
                }
                ResultRow result_row;
                for (size_t i = 0; i < row.size(); i++) {
                    if (row[i].is_null()) {
                        result_row.push_back(std::nullopt);
                    } else if (row[i].is_boolean()) {
                        // Convert boolean to "1"/"0" for consistent BIT/Bool handling
                        result_row.push_back(row[i].get<bool>() ? std::string("1") : std::string("0"));
                    } else if (row[i].is_string()) {
                        result_row.push_back(row[i].get<std::string>());
                    } else if (row[i].is_number_integer()) {
                        // Integer numbers: use to_string to avoid JSON formatting artifacts
                        if (row[i].is_number_unsigned()) {
                            result_row.push_back(std::to_string(row[i].get<uint64_t>()));
                        } else {
                            result_row.push_back(std::to_string(row[i].get<int64_t>()));
                        }
                    } else if (row[i].is_number_float()) {
                        // Float numbers: use high-precision string representation
                        std::ostringstream oss;
                        oss << std::setprecision(17) << row[i].get<double>();
                        result_row.push_back(oss.str());
                    } else {
                        // Complex types (arrays, objects) -> dump as JSON string
                        result_row.push_back(row[i].dump());
                    }
                }
                result_set.rows.push_back(std::move(result_row));
            }
        }

        return true;

    } catch (const nlohmann::json::exception &e) {
        error_msg = std::string("JSON parse error: ") + e.what();
        return false;
    }
}

ColumnInfo ClickHouseClient::MapClickHouseType(const std::string &name, const std::string &ch_type) {
    ColumnInfo info;
    info.name = name;
    info.clickhouse_type = ch_type;
    info.sql_type = ClickHouseTypeToSqlType(ch_type);
    info.column_size = GetColumnSizeForType(ch_type, info.sql_type);
    info.decimal_digits = GetDecimalDigitsForType(ch_type, info.sql_type);

    // Check nullable
    if (ch_type.find("Nullable") != std::string::npos) {
        info.nullable = SQL_NULLABLE;
    } else {
        info.nullable = SQL_NO_NULLS;
    }

    return info;
}

// ============================================================================
// SAX-based streaming JSON parser
// Processes JSONCompact format token-by-token without building a DOM.
// Keeps memory usage low even for large result sets.
//
// JSONCompact format:
//   {"meta": [{"name":"x","type":"Int32"}, ...],
//    "data": [[1,"a"], [2,"b"], ...],
//    "rows": 2}
// ============================================================================
class JsonCompactSaxHandler : public nlohmann::json_sax<nlohmann::json> {
public:
    JsonCompactSaxHandler(ResultSet &rs, size_t max_rows)
        : result_set_(rs), max_rows_(max_rows > 0 ? max_rows : SIZE_MAX) {}

    // --- SAX callbacks ---
    bool null() override {
        if (in_data_ && in_row_) {
            current_row_.push_back(std::nullopt);
        } else if (in_meta_ && in_meta_obj_) {
            // ignore null in meta
        }
        return !row_limit_reached_;
    }

    bool boolean(bool val) override {
        if (in_data_ && in_row_) {
            current_row_.push_back(val ? std::string("1") : std::string("0"));
        }
        return !row_limit_reached_;
    }

    bool number_integer(number_integer_t val) override {
        if (in_data_ && in_row_) {
            current_row_.push_back(std::to_string(val));
        }
        return !row_limit_reached_;
    }

    bool number_unsigned(number_unsigned_t val) override {
        if (in_data_ && in_row_) {
            current_row_.push_back(std::to_string(val));
        }
        return !row_limit_reached_;
    }

    bool number_float(number_float_t val, const string_t &) override {
        if (in_data_ && in_row_) {
            std::ostringstream oss;
            oss << std::setprecision(17) << val;
            current_row_.push_back(oss.str());
        }
        return !row_limit_reached_;
    }

    bool string(string_t &val) override {
        if (in_meta_ && in_meta_obj_) {
            if (current_key_ == "name") {
                current_meta_name_ = val;
            } else if (current_key_ == "type") {
                current_meta_type_ = val;
            }
        } else if (in_data_ && in_row_) {
            current_row_.push_back(std::move(val));
        }
        return !row_limit_reached_;
    }

    bool binary(binary_t &) override { return !row_limit_reached_; }

    bool key(string_t &key) override {
        current_key_ = key;

        // Top-level key detection
        if (depth_ == 1) {
            if (key == "meta") {
                in_meta_ = true;
                in_data_ = false;
            } else if (key == "data") {
                in_data_ = true;
                in_meta_ = false;
            } else {
                in_meta_ = false;
                in_data_ = false;
            }
        }
        return !row_limit_reached_;
    }

    bool start_object(std::size_t) override {
        depth_++;
        if (in_meta_ && depth_ == 3) {
            // Start of a meta object: {"name":"...", "type":"..."}
            in_meta_obj_ = true;
            current_meta_name_.clear();
            current_meta_type_.clear();
        }
        return !row_limit_reached_;
    }

    bool end_object() override {
        if (in_meta_ && in_meta_obj_ && depth_ == 3) {
            // End of meta object — register column
            result_set_.columns.push_back(
                ClickHouseClient::MapClickHouseType(current_meta_name_, current_meta_type_));
            in_meta_obj_ = false;
        }
        depth_--;
        return !row_limit_reached_;
    }

    bool start_array(std::size_t) override {
        depth_++;
        if (in_data_ && depth_ == 3) {
            // Start of a data row: [val, val, ...]
            in_row_ = true;
            current_row_.clear();
        }
        return !row_limit_reached_;
    }

    bool end_array() override {
        if (in_data_ && in_row_ && depth_ == 3) {
            // End of a data row — commit it
            result_set_.rows.push_back(std::move(current_row_));
            current_row_.clear();
            in_row_ = false;

            if (result_set_.rows.size() >= max_rows_) {
                row_limit_reached_ = true;
                return false;  // Abort SAX parser
            }
        }
        depth_--;
        return !row_limit_reached_;
    }

    bool parse_error(std::size_t, const std::string &, const nlohmann::detail::exception &ex) override {
        error_msg_ = std::string("SAX parse error: ") + ex.what();
        return false;
    }

    bool HasError() const { return !error_msg_.empty(); }
    const std::string &GetError() const { return error_msg_; }
    bool WasLimitReached() const { return row_limit_reached_; }

private:
    ResultSet &result_set_;
    size_t max_rows_;

    // Parser state tracking
    int depth_ = 0;           // JSON nesting depth (0 = outside root)
    std::string current_key_;  // Last seen key at any level
    bool in_meta_ = false;     // Inside "meta" array
    bool in_data_ = false;     // Inside "data" array
    bool in_meta_obj_ = false; // Inside a meta object {"name":..., "type":...}
    bool in_row_ = false;      // Inside a data row [...]

    // Meta accumulation
    std::string current_meta_name_;
    std::string current_meta_type_;

    // Row accumulation
    ResultRow current_row_;

    // Termination
    bool row_limit_reached_ = false;
    std::string error_msg_;
};

bool ClickHouseClient::ParseJsonResponseSAX(const std::string &body, ResultSet &result_set, std::string &error_msg) {
    JsonCompactSaxHandler handler(result_set, static_cast<size_t>(max_rows_));
    bool ok = nlohmann::json::sax_parse(body, &handler);

    if (!ok && handler.HasError()) {
        error_msg = handler.GetError();
        return false;
    }

    // Treat as success even if SAX parser was aborted at max_rows
    if (handler.WasLimitReached()) {
        TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "ParseJsonResponseSAX",
                  "Stopped at max_rows=" + std::to_string(max_rows_) +
                  " rows parsed=" + std::to_string(result_set.rows.size()));
    }

    return true;
}

} // namespace clickhouse_odbc
