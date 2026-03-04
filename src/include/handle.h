#pragma once
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <map>
#include <mutex>
#include <variant>
#include <optional>
#include <functional>

namespace clickhouse_odbc {

// Forward declarations
struct OdbcEnvironment;
struct OdbcConnection;
struct OdbcStatement;
struct OdbcDescriptor;

// Diagnostic record
struct DiagRecord {
    std::string sql_state;
    SQLINTEGER native_error = 0;
    std::string message;
};

// Base handle with diagnostic support and thread safety
struct OdbcHandle {
    SQLSMALLINT handle_type;
    std::vector<DiagRecord> diag_records;
    mutable std::recursive_mutex handle_mutex;

    explicit OdbcHandle(SQLSMALLINT type) : handle_type(type) {}
    virtual ~OdbcHandle() = default;

    // Non-copyable due to mutex
    OdbcHandle(const OdbcHandle &) = delete;
    OdbcHandle &operator=(const OdbcHandle &) = delete;

    void ClearDiagRecords() {
        std::lock_guard<std::recursive_mutex> lock(handle_mutex);
        diag_records.clear();
    }

    void AddDiagRecord(const std::string &sql_state, SQLINTEGER native_error, const std::string &message) {
        std::lock_guard<std::recursive_mutex> lock(handle_mutex);
        diag_records.push_back({sql_state, native_error, message});
    }

    void SetError(const std::string &sql_state, const std::string &message, SQLINTEGER native_error = 0) {
        std::lock_guard<std::recursive_mutex> lock(handle_mutex);
        diag_records.clear();
        diag_records.push_back({sql_state, native_error, message});
    }
};

// RAII lock guard for handle operations
class HandleLock {
public:
    explicit HandleLock(OdbcHandle *handle) : lock_(handle->handle_mutex) {}
private:
    std::lock_guard<std::recursive_mutex> lock_;
};

// Column metadata
struct ColumnInfo {
    std::string name;
    std::string clickhouse_type;
    SQLSMALLINT sql_type = SQL_VARCHAR;
    SQLULEN column_size = 0;
    SQLSMALLINT decimal_digits = 0;
    SQLSMALLINT nullable = SQL_NULLABLE_UNKNOWN;
};

// Bound column
struct BoundColumn {
    SQLSMALLINT target_type = SQL_C_DEFAULT;
    SQLPOINTER target_value = nullptr;
    SQLLEN buffer_length = 0;
    SQLLEN *str_len_or_ind = nullptr;
};

// Bound parameter
struct BoundParameter {
    SQLSMALLINT input_output_type = SQL_PARAM_INPUT;
    SQLSMALLINT value_type = SQL_C_DEFAULT;
    SQLSMALLINT parameter_type = SQL_VARCHAR;
    SQLULEN column_size = 0;
    SQLSMALLINT decimal_digits = 0;
    SQLPOINTER parameter_value = nullptr;
    SQLLEN buffer_length = 0;
    SQLLEN *str_len_or_ind = nullptr;
};

// Result set row (vector of nullable string values)
using ResultRow = std::vector<std::optional<std::string>>;

// Callback type for page fetching
// query: query with LIMIT/OFFSET, out_rows: fetched rows, error: error message
// Returns: true=success, false=failure
using PageFetcher = std::function<bool(const std::string &query,
                                       std::vector<ResultRow> &out_rows,
                                       std::string &error)>;

// Result set (normal mode + lazy paging mode)
struct ResultSet {
    std::vector<ColumnInfo> columns;
    std::vector<ResultRow> rows;      // for normal mode
    SQLLEN current_row = -1;          // before first row

    // --- Lazy paging mode ---
    bool lazy = false;                // true = lazy paging enabled
    size_t total_row_count = 0;       // total row count obtained via SELECT count()
    size_t page_size = 10000;         // rows per page
    std::string base_query;           // original SELECT query (without LIMIT/OFFSET)
    PageFetcher page_fetcher;         // page fetch callback
    std::map<size_t, std::vector<ResultRow>> page_cache; // page number → row data
    size_t max_cached_pages = 10;     // max number of cached pages
    size_t prefetch_pages = 5;        // pages to fetch per HTTP request (batch prefetch)
    std::vector<size_t> page_access_order; // access order for LRU tracking
    mutable std::recursive_mutex page_mutex;  // mutex for page cache protection (recursive)

    bool HasData() const { return !columns.empty(); }

    // Total row count (normal mode: rows.size(), lazy mode: total_row_count)
    size_t RowCount() const;

    // Get specified row (may trigger page fetch in lazy mode)
    // Returns: reference to the row. Returns empty static row if not available
    const ResultRow &GetRow(size_t idx);

    // Check if the specified row is accessible, fetch page if necessary
    bool EnsureRow(size_t idx);

    bool Fetch();
    void Reset();       // Full reset: clears columns, rows, cursor
    void CloseCursor();  // Cursor close: clears rows and cursor, preserves column metadata (IRD)

private:
    void EvictOldPages();
    void TouchPage(size_t page_num);
    static ResultRow empty_row_;  // safe return value when GetRow fails
};

// Environment handle
struct OdbcEnvironment : OdbcHandle {
    SQLINTEGER odbc_version = SQL_OV_ODBC3;
    SQLUINTEGER connection_pooling = SQL_CP_OFF;       // SQL_ATTR_CONNECTION_POOLING
    SQLUINTEGER cp_match = SQL_CP_STRICT_MATCH;        // SQL_ATTR_CP_MATCH
    std::vector<OdbcConnection *> connections;

    OdbcEnvironment() : OdbcHandle(SQL_HANDLE_ENV) {}
};

// Connection handle
struct OdbcConnection : OdbcHandle {
    OdbcEnvironment *env = nullptr;
    std::vector<OdbcStatement *> statements;

    // Connection parameters
    std::string host = "localhost";
    uint16_t port = 8123;
    std::string database = "default";
    std::string user = "default";
    std::string password;
    bool connected = false;

    // HTTP compression setting (enabled by default: gzip response compression)
    bool compression = true;

    // Connection-level default max rows (0 = unlimited)
    // When DefaultMaxRows=N is specified in DSN/connection string,
    // it becomes the initial value of SQL_ATTR_MAX_ROWS for new statements
    SQLULEN default_max_rows = 0;

    // Page size for lazy paging (default 10000 rows)
    // Can be specified via PageSize=N in DSN/connection string
    size_t page_size = 10000;

    // Enable/disable lazy paging (enabled by default)
    // Can be disabled via LazyPaging=0 in DSN/connection string
    bool lazy_paging = true;

    // Maximum rows exposed in lazy paging mode (0 = unlimited, default)
    // When set, prevents MS Access from hitting its 2GB temporary database limit
    // when navigating to the last row of a very large table.
    // Can be specified via MaxLazyRows=N in DSN/connection string
    size_t max_lazy_rows = 0;

    // SSL/TLS settings
    // SSL=1 or SSL=true in connection string enables HTTPS
    bool ssl = false;
    // SSL certificate verification (default: true)
    // SSL_VERIFY=0 disables certificate verification (for self-signed certs)
    bool ssl_verify = true;

    // Connection attributes
    SQLUINTEGER access_mode = SQL_MODE_READ_WRITE;
    SQLUINTEGER autocommit = SQL_AUTOCOMMIT_ON;
    SQLUINTEGER login_timeout = 0;
    SQLUINTEGER connection_timeout = 0;
    std::string current_catalog;

    // Additional connection attributes
    SQLUINTEGER metadata_id = SQL_FALSE;                // SQL_ATTR_METADATA_ID
    SQLUINTEGER txn_isolation = SQL_TXN_READ_COMMITTED; // SQL_ATTR_TXN_ISOLATION
    SQLUINTEGER packet_size = 0;                        // SQL_ATTR_PACKET_SIZE
    SQLULEN async_enable = SQL_ASYNC_ENABLE_OFF;        // SQL_ATTR_ASYNC_ENABLE
    SQLHWND quiet_mode = nullptr;                       // SQL_ATTR_QUIET_MODE (parent window)
    SQLULEN odbc_cursors = SQL_CUR_USE_DRIVER;          // SQL_ATTR_ODBC_CURSORS
    std::string translate_lib;                          // SQL_ATTR_TRANSLATE_LIB
    SQLUINTEGER translate_option = 0;                   // SQL_ATTR_TRANSLATE_OPTION

    // Trace settings
    bool trace_enabled = false;
    std::string trace_file;

    OdbcConnection() : OdbcHandle(SQL_HANDLE_DBC) {}
};

// Descriptor handle
enum class DescriptorType {
    APD,  // Application Parameter Descriptor
    IPD,  // Implementation Parameter Descriptor
    ARD,  // Application Row Descriptor
    IRD   // Implementation Row Descriptor
};

// Descriptor record (one per column/parameter)
struct DescriptorRecord {
    // Common fields
    SQLSMALLINT type = SQL_VARCHAR;              // SQL_DESC_TYPE
    SQLSMALLINT concise_type = SQL_VARCHAR;      // SQL_DESC_CONCISE_TYPE
    SQLULEN length = 0;                          // SQL_DESC_LENGTH / SQL_DESC_OCTET_LENGTH
    SQLSMALLINT precision = 0;                   // SQL_DESC_PRECISION
    SQLSMALLINT scale = 0;                       // SQL_DESC_SCALE
    SQLSMALLINT nullable = SQL_NULLABLE;         // SQL_DESC_NULLABLE
    std::string name;                            // SQL_DESC_NAME
    std::string base_column_name;                // SQL_DESC_BASE_COLUMN_NAME
    std::string base_table_name;                 // SQL_DESC_BASE_TABLE_NAME
    std::string table_name;                      // SQL_DESC_TABLE_NAME
    std::string type_name;                       // SQL_DESC_TYPE_NAME
    std::string label;                           // SQL_DESC_LABEL
    SQLSMALLINT unnamed = SQL_NAMED;             // SQL_DESC_UNNAMED
    SQLSMALLINT searchable = SQL_PRED_SEARCHABLE;// SQL_DESC_SEARCHABLE
    SQLULEN display_size = 0;                    // SQL_DESC_DISPLAY_SIZE
    SQLLEN octet_length = 0;                     // SQL_DESC_OCTET_LENGTH
    SQLSMALLINT fixed_prec_scale = SQL_FALSE;    // SQL_DESC_FIXED_PREC_SCALE
    SQLSMALLINT case_sensitive = SQL_TRUE;       // SQL_DESC_CASE_SENSITIVE
    SQLSMALLINT updatable = SQL_ATTR_READONLY;   // SQL_DESC_UPDATABLE
    SQLSMALLINT auto_unique_value = SQL_FALSE;   // SQL_DESC_AUTO_UNIQUE_VALUE

    // Application descriptor fields (ARD/APD)
    SQLSMALLINT data_type = SQL_C_DEFAULT;       // SQL_DESC_DATA_TYPE (C type for ARD/APD)
    SQLPOINTER data_ptr = nullptr;               // SQL_DESC_DATA_PTR
    SQLLEN *indicator_ptr = nullptr;             // SQL_DESC_INDICATOR_PTR
    SQLLEN *octet_length_ptr = nullptr;          // SQL_DESC_OCTET_LENGTH_PTR
    SQLLEN buffer_length = 0;                    // SQL_DESC_BUFFER_LENGTH (for app descriptors)
    SQLUSMALLINT parameter_type = SQL_PARAM_INPUT; // SQL_DESC_PARAMETER_TYPE (IPD only)
};

struct OdbcDescriptor : OdbcHandle {
    OdbcConnection *conn = nullptr;
    DescriptorType desc_type = DescriptorType::ARD;
    bool is_auto = true;  // auto-allocated descriptor (implicit with stmt)
    SQLSMALLINT count = 0; // SQL_DESC_COUNT
    std::unordered_map<SQLUSMALLINT, DescriptorRecord> records; // 1-based index

    OdbcDescriptor() : OdbcHandle(SQL_HANDLE_DESC) {}
    explicit OdbcDescriptor(DescriptorType type) : OdbcHandle(SQL_HANDLE_DESC), desc_type(type) {}
};

// Statement handle
struct OdbcStatement : OdbcHandle {
    OdbcConnection *conn = nullptr;

    // Query
    std::string query;
    std::string prepared_query;  // holds original template (with ? placeholders)
    bool prepared = false;

    // Source table name (extracted from query for SQL_DESC_TABLE_NAME / SQL_DESC_BASE_TABLE_NAME)
    std::string source_table;

    // Result
    ResultSet result_set;

    // Bound columns and parameters
    std::unordered_map<SQLUSMALLINT, BoundColumn> bound_columns;
    std::unordered_map<SQLUSMALLINT, BoundParameter> bound_parameters;

    // Auto-allocated descriptors (owned by statement)
    std::unique_ptr<OdbcDescriptor> auto_apd;
    std::unique_ptr<OdbcDescriptor> auto_ipd;
    std::unique_ptr<OdbcDescriptor> auto_ard;
    std::unique_ptr<OdbcDescriptor> auto_ird;

    // Current descriptor pointers (may point to auto or user-allocated)
    OdbcDescriptor *apd = nullptr;
    OdbcDescriptor *ipd = nullptr;
    OdbcDescriptor *ard = nullptr;
    OdbcDescriptor *ird = nullptr;

    // Statement attributes
    SQLULEN max_rows = 0;
    SQLULEN query_timeout = 0;
    SQLULEN cursor_type = SQL_CURSOR_FORWARD_ONLY;
    SQLULEN rowset_size = 1;
    SQLULEN row_bind_type = SQL_BIND_BY_COLUMN;   // SQL_ATTR_ROW_BIND_TYPE
    SQLULEN paramset_size = 1;                     // SQL_ATTR_PARAMSET_SIZE
    SQLULEN param_bind_type = SQL_PARAM_BIND_BY_COLUMN; // SQL_ATTR_PARAM_BIND_TYPE
    SQLULEN noscan = SQL_NOSCAN_OFF;               // SQL_ATTR_NOSCAN
    SQLULEN concurrency = SQL_CONCUR_READ_ONLY;    // SQL_ATTR_CONCURRENCY
    SQLULEN max_length = 0;                        // SQL_ATTR_MAX_LENGTH
    SQLULEN retrieve_data = SQL_RD_ON;             // SQL_ATTR_RETRIEVE_DATA
    SQLULEN use_bookmarks = SQL_UB_OFF;            // SQL_ATTR_USE_BOOKMARKS

    // Row status and rows fetched (for block cursors / SQLExtendedFetch)
    SQLUSMALLINT *row_status_ptr = nullptr;
    SQLULEN *rows_fetched_ptr = nullptr;
    SQLULEN last_rowset_count = 0;          // actual number of rows fetched in last SQLFetch/SQLExtendedFetch
    SQLLEN rowset_start_row = 0;            // absolute row index of the first row in the current rowset

    // Parameter array execution support
    SQLUSMALLINT *param_status_ptr = nullptr;      // SQL_ATTR_PARAM_STATUS_PTR
    SQLULEN *params_processed_ptr = nullptr;        // SQL_ATTR_PARAMS_PROCESSED_PTR

    // Cursor name (auto-generated if not set)
    std::string cursor_name;
    bool cursor_name_set = false;
    static inline int next_cursor_id = 0;

    // Data-at-execution support (SQLParamData/SQLPutData)
    bool need_data = false;                                        // true while waiting for DAE params
    std::vector<SQLUSMALLINT> pending_dae_params;                  // ordered list of DAE param indices
    int current_dae_index = -1;                                    // index into pending_dae_params
    std::unordered_map<SQLUSMALLINT, std::string> dae_buffers;     // accumulated data per param

    // Piecemeal SQLGetData support
    SQLUSMALLINT getdata_col = 0;      // last column read by SQLGetData (0 = none)
    SQLLEN getdata_offset = 0;          // byte offset within current column value

    // Affected rows for INSERT/UPDATE/DELETE (non-SELECT)
    SQLLEN affected_rows = -1;          // -1 = unknown/not applicable

    // Cursor scrollable/sensitivity attributes
    SQLULEN cursor_scrollable = SQL_NONSCROLLABLE;     // SQL_ATTR_CURSOR_SCROLLABLE
    SQLULEN cursor_sensitivity = SQL_INSENSITIVE;      // SQL_ATTR_CURSOR_SENSITIVITY

    OdbcStatement() : OdbcHandle(SQL_HANDLE_STMT) {
        cursor_name = "SQL_CUR" + std::to_string(++next_cursor_id);
        // Initialize auto-allocated descriptors
        auto_apd = std::make_unique<OdbcDescriptor>(DescriptorType::APD);
        auto_ipd = std::make_unique<OdbcDescriptor>(DescriptorType::IPD);
        auto_ard = std::make_unique<OdbcDescriptor>(DescriptorType::ARD);
        auto_ird = std::make_unique<OdbcDescriptor>(DescriptorType::IRD);
        apd = auto_apd.get();
        ipd = auto_ipd.get();
        ard = auto_ard.get();
        ird = auto_ird.get();
    }

    void ResetDAE() {
        need_data = false;
        pending_dae_params.clear();
        current_dae_index = -1;
        dae_buffers.clear();
    }
};

// Validation helpers
inline bool IsValidEnvHandle(SQLHANDLE handle) {
    if (!handle) return false;
    auto *h = static_cast<OdbcHandle *>(handle);
    return h->handle_type == SQL_HANDLE_ENV;
}

inline bool IsValidDbcHandle(SQLHANDLE handle) {
    if (!handle) return false;
    auto *h = static_cast<OdbcHandle *>(handle);
    return h->handle_type == SQL_HANDLE_DBC;
}

inline bool IsValidStmtHandle(SQLHANDLE handle) {
    if (!handle) return false;
    auto *h = static_cast<OdbcHandle *>(handle);
    return h->handle_type == SQL_HANDLE_STMT;
}

// Sync all auto-descriptors from statement's bound columns/parameters/result_set
void SyncDescriptorsFromStatement(OdbcStatement *stmt);

} // namespace clickhouse_odbc
