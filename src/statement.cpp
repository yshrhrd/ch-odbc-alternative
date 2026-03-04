#include "include/handle.h"
#include "include/clickhouse_client.h"
#include "include/util.h"
#include "include/type_mapping.h"
#include "include/trace.h"

#include <sql.h>
#include <sqlext.h>
#include <cstring>
#include <algorithm>

#ifdef UNICODE
#undef SQLColAttribute
#undef SQLColAttributes
#undef SQLDescribeCol
#undef SQLExecDirect
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLNativeSql
#undef SQLPrepare
#undef SQLDescribeParam
#undef SQLGetCursorName
#undef SQLSetCursorName
#endif

namespace clickhouse_odbc {
extern ClickHouseClient *GetClient(OdbcConnection *conn);
}

using namespace clickhouse_odbc;

// Helper: Check if a bound parameter uses data-at-execution
static bool IsDataAtExec(const BoundParameter &bp) {
    if (!bp.str_len_or_ind) return false;
    SQLLEN ind = *bp.str_len_or_ind;
    return (ind == SQL_DATA_AT_EXEC || ind <= SQL_LEN_DATA_AT_EXEC_OFFSET);
}

// ============================================================================
// SQLExecDirect / SQLExecDirectW
// ============================================================================
static SQLRETURN ExecDirectImpl(OdbcStatement *stmt, const std::string &query) {
    TRACE_ENTRY("ExecDirectImpl", "query=" + query.substr(0, 200));
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();
    stmt->affected_rows = -1;
    stmt->source_table.clear();

    // Process ODBC escape sequences before execution
    std::string processed_query = ProcessOdbcEscapeSequences(query);

    // Convert MS Access "SELECT TOP N" syntax to ClickHouse "LIMIT N"
    processed_query = ConvertTopToLimit(processed_query);

    stmt->query = processed_query;
    TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "ExecDirectImpl",
              "stmt=" + std::to_string(reinterpret_cast<uintptr_t>(stmt)) +
              " processed_query=" + processed_query);

    auto *client = GetClient(stmt->conn);
    if (!client) {
        stmt->SetError("08003", "Connection not open");
        TRACE_EXIT("ExecDirectImpl", SQL_ERROR);
        return SQL_ERROR;
    }

    // Apply statement-level query timeout to the client
    if (stmt->query_timeout > 0) {
        client->SetQueryTimeout(static_cast<unsigned int>(stmt->query_timeout));
    }

    // Query type detection (used for lazy paging and max_rows application)
    std::string upper_q = processed_query;
    std::transform(upper_q.begin(), upper_q.end(), upper_q.begin(), ::toupper);
    bool is_select = (upper_q.find("SELECT") != std::string::npos ||
                      upper_q.find("SHOW") != std::string::npos ||
                      upper_q.find("DESCRIBE") != std::string::npos);
    bool has_limit = (upper_q.find("LIMIT") != std::string::npos);

    // --- Lazy paging mode decision ---
    // Conditions: SELECT-type, no LIMIT, max_rows not set, lazy_paging enabled
    bool try_lazy = is_select && !has_limit && stmt->max_rows == 0 &&
                    stmt->conn && stmt->conn->lazy_paging;

    if (try_lazy) {
        size_t total_count = 0;
        std::string error_msg;
        if (client->ExecuteCountQuery(processed_query, total_count, error_msg)) {
            size_t page_sz = stmt->conn->page_size;
            TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "ExecDirectImpl",
                      "Lazy paging: count=" + std::to_string(total_count) +
                      " page_size=" + std::to_string(page_sz));

            if (total_count > page_sz) {
                // Start in lazy paging mode
                // First get metadata (column info) with LIMIT 0
                std::string meta_query = processed_query + " LIMIT 0";
                if (!client->ExecuteQuery(meta_query, stmt->result_set, error_msg)) {
                    stmt->SetError("HY000", "Metadata query failed: " + error_msg);
                    TRACE_EXIT("ExecDirectImpl", SQL_ERROR);
                    return SQL_ERROR;
                }

                // Apply MaxLazyRows cap to prevent MS Access 2GB limit
                size_t max_lazy = stmt->conn->max_lazy_rows;
                size_t capped_count = total_count;
                if (max_lazy > 0 && total_count > max_lazy) {
                    capped_count = max_lazy;
                    TRACE_LOG(clickhouse_odbc::TraceLevel::Warning, "ExecDirectImpl",
                              "Row count capped by MaxLazyRows: " + std::to_string(total_count) +
                              " -> " + std::to_string(capped_count));
                }

                // Lazy paging settings
                stmt->result_set.lazy = true;
                stmt->result_set.total_row_count = capped_count;
                stmt->result_set.page_size = page_sz;
                stmt->result_set.base_query = processed_query;
                // Ensure cache can hold at least 2 prefetch batches
                if (stmt->result_set.max_cached_pages < stmt->result_set.prefetch_pages * 2) {
                    stmt->result_set.max_cached_pages = stmt->result_set.prefetch_pages * 2;
                }

                // page_fetcher: fetch data page by page using ClickHouseClient
                auto *cli_ptr = client;
                stmt->result_set.page_fetcher = [cli_ptr](const std::string &query,
                                                           std::vector<ResultRow> &out_rows,
                                                           std::string &error) -> bool {
                    return cli_ptr->ExecutePageQuery(query, out_rows, error);
                };

                TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "ExecDirectImpl",
                          "Lazy paging enabled: " + std::to_string(capped_count) + " total rows" +
                          (capped_count < total_count ? " (capped from " + std::to_string(total_count) + ")" : ""));

                // Source table name extraction (common processing below)
                goto extract_source_table;
            }
            // count <= page_size: fall through to normal mode
        } else {
            // count() failed: fall through to normal mode (non-fatal)
            TRACE_LOG(clickhouse_odbc::TraceLevel::Warning, "ExecDirectImpl",
                      "Count query failed, falling back to normal mode: " + error_msg);
        }
    }

    // SQL_ATTR_MAX_ROWS: Apply LIMIT on server side to prevent large data transfers
    if (stmt->max_rows > 0) {
        if (is_select && !has_limit) {
            processed_query += " LIMIT " + std::to_string(stmt->max_rows);
            TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "ExecDirectImpl",
                      "Applied max_rows LIMIT " + std::to_string(stmt->max_rows));
        }
    }

    // Set client-side row limit (safety net for when server ignores LIMIT)
    {
        SQLULEN effective_max = stmt->max_rows;
        client->SetMaxRows(effective_max);

        std::string error_msg;
        if (!client->ExecuteQuery(processed_query, stmt->result_set, error_msg)) {
            // Check if error looks like a timeout
            if (error_msg.find("Connection failed") != std::string::npos ||
                error_msg.find("timeout") != std::string::npos) {
                stmt->SetError("HYT00", "Timeout expired: " + error_msg);
            } else {
                stmt->SetError("HY000", error_msg);
            }
            TRACE_EXIT("ExecDirectImpl", SQL_ERROR);
            return SQL_ERROR;
        }
    }

extract_source_table:

    // For non-SELECT queries (INSERT/UPDATE/DELETE), set affected_rows
    if (!stmt->result_set.HasData()) {
        // Detect INSERT with row count from query structure
        std::string upper_q = processed_query;
        std::transform(upper_q.begin(), upper_q.end(), upper_q.begin(), ::toupper);
        if (upper_q.find("INSERT") != std::string::npos ||
            upper_q.find("UPDATE") != std::string::npos ||
            upper_q.find("DELETE") != std::string::npos ||
            upper_q.find("ALTER") != std::string::npos) {
            stmt->affected_rows = 0;  // ClickHouse HTTP doesn't report exact affected count
        }
    }

    // Extract source table name from SELECT query for SQL_DESC_TABLE_NAME
    // MS Access needs this to construct row re-fetch queries
    {
        std::string upper_q = processed_query;
        std::transform(upper_q.begin(), upper_q.end(), upper_q.begin(), ::toupper);
        auto from_pos = upper_q.find("FROM");
        if (from_pos != std::string::npos) {
            size_t start = from_pos + 4;
            // Skip whitespace
            while (start < processed_query.size() && (processed_query[start] == ' ' || processed_query[start] == '\t' || processed_query[start] == '\n' || processed_query[start] == '\r'))
                start++;
            // Extract table name (may be quoted with backticks or double quotes)
            size_t end = start;
            if (end < processed_query.size() && (processed_query[end] == '`' || processed_query[end] == '"')) {
                char quote = processed_query[end];
                end++;
                while (end < processed_query.size() && processed_query[end] != quote) end++;
                stmt->source_table = processed_query.substr(start + 1, end - start - 1);
            } else {
                while (end < processed_query.size() && processed_query[end] != ' ' && processed_query[end] != '\t' &&
                       processed_query[end] != '\n' && processed_query[end] != '\r' &&
                       processed_query[end] != ',' && processed_query[end] != ';' &&
                       processed_query[end] != ')') end++;
                stmt->source_table = processed_query.substr(start, end - start);
            }
            // Remove database prefix if present (e.g. "default.table_name" -> "table_name")
            auto dot_pos = stmt->source_table.find('.');
            if (dot_pos != std::string::npos) {
                stmt->source_table = stmt->source_table.substr(dot_pos + 1);
            }
            // Remove backticks from the extracted name
            std::string cleaned;
            for (char c : stmt->source_table) {
                if (c != '`' && c != '"') cleaned += c;
            }
            stmt->source_table = cleaned;
        }
    }

    TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "ExecDirectImpl",
              "rows=" + std::to_string(stmt->result_set.RowCount()) +
              " cols=" + std::to_string(stmt->result_set.columns.size()) +
              " table=" + stmt->source_table +
              " lazy=" + std::to_string(stmt->result_set.lazy));
    TRACE_EXIT("ExecDirectImpl", SQL_SUCCESS);
    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR *StatementText,
                                            SQLINTEGER TextLength) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    std::string query;
    if (StatementText) {
        if (TextLength == SQL_NTS) {
            query = reinterpret_cast<const char *>(StatementText);
        } else {
            query = std::string(reinterpret_cast<const char *>(StatementText), TextLength);
        }
    }

    return ExecDirectImpl(stmt, query);
}

extern "C" SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT StatementHandle, SQLWCHAR *StatementText,
                                             SQLINTEGER TextLength) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    std::string query;
    if (StatementText) {
        query = WideToUtf8(StatementText, (SQLSMALLINT)TextLength);
    }

    return ExecDirectImpl(stmt, query);
}

// ============================================================================
// SQLPrepare / SQLPrepareW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR *StatementText,
                                         SQLINTEGER TextLength) {
    TRACE_ENTRY("SQLPrepare", "");
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    if (StatementText) {
        std::string raw;
        if (TextLength == SQL_NTS) {
            raw = reinterpret_cast<const char *>(StatementText);
        } else {
            raw = std::string(reinterpret_cast<const char *>(StatementText), TextLength);
        }
        stmt->query = ProcessOdbcEscapeSequences(raw);
        stmt->prepared_query = stmt->query;
        TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "SQLPrepare",
                  "Query: " + stmt->query);
    }
    stmt->prepared = true;
    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLPrepareW(SQLHSTMT StatementHandle, SQLWCHAR *StatementText,
                                          SQLINTEGER TextLength) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    if (StatementText) {
        std::string raw = WideToUtf8(StatementText, (SQLSMALLINT)TextLength);
        stmt->query = ProcessOdbcEscapeSequences(raw);
        stmt->prepared_query = stmt->query;
        TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "SQLPrepareW",
                  "Query: " + stmt->query);
    }
    stmt->prepared = true;
    return SQL_SUCCESS;
}

// ============================================================================
// SQLExecute
// ============================================================================

// Helper: Get element size for a C type (for column-wise parameter array binding)
static SQLLEN GetCTypeSize(SQLSMALLINT c_type) {
    switch (c_type) {
    case SQL_C_CHAR:            return 0; // variable length, use buffer_length
    case SQL_C_WCHAR:           return 0;
    case SQL_C_SSHORT:
    case SQL_C_SHORT:           return sizeof(short);
    case SQL_C_USHORT:          return sizeof(unsigned short);
    case SQL_C_SLONG:
    case SQL_C_LONG:            return sizeof(SQLINTEGER);
    case SQL_C_ULONG:           return sizeof(SQLUINTEGER);
    case SQL_C_SBIGINT:         return sizeof(int64_t);
    case SQL_C_UBIGINT:         return sizeof(uint64_t);
    case SQL_C_FLOAT:           return sizeof(float);
    case SQL_C_DOUBLE:          return sizeof(double);
    case SQL_C_BIT:
    case SQL_C_TINYINT:
    case SQL_C_STINYINT:        return sizeof(signed char);
    case SQL_C_UTINYINT:        return sizeof(unsigned char);
    case SQL_C_TYPE_DATE:
    case SQL_C_DATE:            return sizeof(SQL_DATE_STRUCT);
    case SQL_C_TYPE_TIME:
    case SQL_C_TIME:            return sizeof(SQL_TIME_STRUCT);
    case SQL_C_TYPE_TIMESTAMP:
    case SQL_C_TIMESTAMP:       return sizeof(SQL_TIMESTAMP_STRUCT);
    case SQL_C_GUID:            return sizeof(SQLGUID);
    case SQL_C_BINARY:          return 0; // variable length
    default:                    return 0;
    }
}

// Helper: Create an offset copy of BoundParameter for array element i
static BoundParameter OffsetParameter(const BoundParameter &orig, SQLULEN i,
                                       SQLULEN param_bind_type) {
    BoundParameter bp = orig;
    if (i == 0) return bp;

    if (param_bind_type == SQL_PARAM_BIND_BY_COLUMN) {
        // Column-wise: advance by element size or buffer_length
        SQLLEN elem_size = GetCTypeSize(orig.value_type);
        if (elem_size == 0) {
            elem_size = orig.buffer_length;
        }
        if (bp.parameter_value && elem_size > 0) {
            bp.parameter_value = static_cast<char *>(const_cast<void *>(
                static_cast<const void *>(orig.parameter_value))) + i * elem_size;
        }
        if (bp.str_len_or_ind) {
            bp.str_len_or_ind = orig.str_len_or_ind + i;
        }
    } else {
        // Row-wise: advance by param_bind_type (struct size)
        if (bp.parameter_value) {
            bp.parameter_value = static_cast<char *>(const_cast<void *>(
                static_cast<const void *>(orig.parameter_value))) + i * param_bind_type;
        }
        if (bp.str_len_or_ind) {
            bp.str_len_or_ind = reinterpret_cast<SQLLEN *>(
                reinterpret_cast<char *>(orig.str_len_or_ind) + i * param_bind_type);
        }
    }
    return bp;
}

extern "C" SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    if (!stmt->prepared || stmt->query.empty()) {
        stmt->SetError("HY010", "Function sequence error: no prepared statement");
        return SQL_ERROR;
    }

    // Check for data-at-execution parameters
    stmt->ResetDAE();
    if (!stmt->bound_parameters.empty()) {
        for (auto &[idx, bp] : stmt->bound_parameters) {
            if (IsDataAtExec(bp)) {
                stmt->pending_dae_params.push_back(idx);
            }
        }
        if (!stmt->pending_dae_params.empty()) {
            // Sort by parameter index for deterministic order
            std::sort(stmt->pending_dae_params.begin(), stmt->pending_dae_params.end());
            stmt->need_data = true;
            stmt->current_dae_index = -1;
            return SQL_NEED_DATA;
        }
    }

    // Parameter array execution (paramset_size > 1)
    if (stmt->paramset_size > 1 && !stmt->bound_parameters.empty()) {
        TRACE_LOG(TraceLevel::Debug, "SQLExecute",
                  "Array execution: paramset_size=" + std::to_string(stmt->paramset_size));

        SQLULEN params_processed = 0;
        SQLRETURN overall_ret = SQL_SUCCESS;
        bool any_success = false;

        for (SQLULEN i = 0; i < stmt->paramset_size; ++i) {
            // Build offset parameters for this set
            std::unordered_map<SQLUSMALLINT, BoundParameter> offset_params;
            for (const auto &[idx, bp] : stmt->bound_parameters) {
                offset_params[idx] = OffsetParameter(bp, i, stmt->param_bind_type);
            }

            std::string error_msg;
            std::string array_template = stmt->prepared_query.empty() ? stmt->query : stmt->prepared_query;
            std::string exec_query = SubstituteParameters(array_template, offset_params, error_msg);
            if (exec_query.empty() && !error_msg.empty()) {
                if (stmt->param_status_ptr) {
                    stmt->param_status_ptr[i] = SQL_PARAM_ERROR;
                }
                overall_ret = SQL_SUCCESS_WITH_INFO;
                params_processed++;
                continue;
            }

            SQLRETURN ret = ExecDirectImpl(stmt, exec_query);
            params_processed++;

            if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                any_success = true;
                if (stmt->param_status_ptr) {
                    stmt->param_status_ptr[i] = SQL_PARAM_SUCCESS;
                }
            } else {
                if (stmt->param_status_ptr) {
                    stmt->param_status_ptr[i] = SQL_PARAM_ERROR;
                }
                if (overall_ret == SQL_SUCCESS) {
                    overall_ret = SQL_SUCCESS_WITH_INFO;
                }
            }
        }

        if (stmt->params_processed_ptr) {
            *stmt->params_processed_ptr = params_processed;
        }

        if (!any_success) return SQL_ERROR;
        return overall_ret;
    }

    // Single parameter set execution
    // Use original template (with ? placeholders) from prepared_query
    // ExecDirectImpl overwrites stmt->query, so substitute from prepared_query
    std::string template_query = stmt->prepared_query.empty() ? stmt->query : stmt->prepared_query;
    std::string exec_query = template_query;
    if (!stmt->bound_parameters.empty()) {
        TRACE_LOG(TraceLevel::Debug, "SQLExecute",
                  "Substituting " + std::to_string(stmt->bound_parameters.size()) + " parameter(s)");
        std::string error_msg;
        exec_query = SubstituteParameters(template_query, stmt->bound_parameters, error_msg);
        if (exec_query.empty() && !error_msg.empty()) {
            stmt->SetError("07002", error_msg);
            return SQL_ERROR;
        }
        TRACE_LOG(TraceLevel::Debug, "SQLExecute",
                  "Substituted query: " + exec_query);
    }

    // Set params_processed for single execution
    if (stmt->params_processed_ptr) {
        *stmt->params_processed_ptr = 1;
    }
    if (stmt->param_status_ptr) {
        stmt->param_status_ptr[0] = SQL_PARAM_SUCCESS;
    }

    return ExecDirectImpl(stmt, exec_query);
}

// ============================================================================
// Prepared statement metadata discovery
// When SQLPrepare is called, the driver stores the query but does not execute it.
// MS Access (and other apps) call SQLNumResultCols / SQLDescribeCol BEFORE
// SQLExecute to inspect result set metadata. This helper executes the query
// with "LIMIT 0" to populate column metadata without fetching rows.
// ============================================================================
static void FetchPreparedMetadata(OdbcStatement *stmt) {
    // Only for prepared statements that have no result columns yet
    if (!stmt->prepared) return;
    if (!stmt->result_set.columns.empty()) return;

    // Use prepared_query (ExecDirectImpl overwrites stmt->query)
    const std::string &base_query = stmt->prepared_query.empty() ? stmt->query : stmt->prepared_query;
    if (base_query.empty()) return;

    // Only for SELECT-like queries (not INSERT/UPDATE/DELETE)
    std::string upper_q = base_query;
    std::transform(upper_q.begin(), upper_q.end(), upper_q.begin(), ::toupper);
    bool is_select = (upper_q.find("SELECT") != std::string::npos ||
                      upper_q.find("SHOW") != std::string::npos ||
                      upper_q.find("DESCRIBE") != std::string::npos);
    if (!is_select) return;

    auto *client = GetClient(stmt->conn);
    if (!client) return;

    // Build metadata query: replace ? placeholders with NULL so ClickHouse can parse
    // the query structure, then add LIMIT 0 for zero-row metadata-only execution.
    std::string metadata_query = base_query;
    {
        bool in_sq = false, in_dq = false, in_bt = false;
        size_t pos = 0;
        while (pos < metadata_query.size()) {
            char c = metadata_query[pos];
            if (c == '\'' && !in_dq && !in_bt) {
                in_sq = !in_sq;
            } else if (c == '"' && !in_sq && !in_bt) {
                in_dq = !in_dq;
            } else if (c == '`' && !in_sq && !in_dq) {
                in_bt = !in_bt;
            } else if (c == '?' && !in_sq && !in_dq && !in_bt) {
                metadata_query.replace(pos, 1, "NULL");
                pos += 4; // skip past "NULL"
                continue;
            }
            pos++;
        }
    }
    // Recompute upper_q after substitution
    std::string upper_mq = metadata_query;
    std::transform(upper_mq.begin(), upper_mq.end(), upper_mq.begin(), ::toupper);
    if (upper_mq.find("LIMIT") == std::string::npos) {
        metadata_query += " LIMIT 0";
    }

    ResultSet temp;
    std::string error_msg;
    if (client->ExecuteQuery(metadata_query, temp, error_msg)) {
        // Copy only column metadata, not rows
        stmt->result_set.columns = temp.columns;
    }
}

// ============================================================================
// SQLNumResultCols
// ============================================================================
extern "C" SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT StatementHandle, SQLSMALLINT *ColumnCount) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    // Lazy metadata discovery for prepared statements (needed by MS Access)
    FetchPreparedMetadata(stmt);

    if (ColumnCount) {
        *ColumnCount = static_cast<SQLSMALLINT>(stmt->result_set.columns.size());
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLDescribeCol / SQLDescribeColW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                                              SQLCHAR *ColumnName, SQLSMALLINT BufferLength,
                                              SQLSMALLINT *NameLength, SQLSMALLINT *DataType,
                                              SQLULEN *ColumnSize, SQLSMALLINT *DecimalDigits,
                                              SQLSMALLINT *Nullable) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    // Lazy metadata discovery for prepared statements
    FetchPreparedMetadata(stmt);

    if (ColumnNumber < 1 || ColumnNumber > stmt->result_set.columns.size()) {
        stmt->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    const auto &col = stmt->result_set.columns[ColumnNumber - 1];

    if (ColumnName && BufferLength > 0) {
        CopyStringToBuffer(col.name, ColumnName, BufferLength, NameLength);
    }
    if (DataType) *DataType = col.sql_type;
    if (ColumnSize) *ColumnSize = col.column_size;
    if (DecimalDigits) *DecimalDigits = col.decimal_digits;
    if (Nullable) *Nullable = col.nullable;

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLDescribeColW(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                                               SQLWCHAR *ColumnName, SQLSMALLINT BufferLength,
                                               SQLSMALLINT *NameLength, SQLSMALLINT *DataType,
                                               SQLULEN *ColumnSize, SQLSMALLINT *DecimalDigits,
                                               SQLSMALLINT *Nullable) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    // Lazy metadata discovery for prepared statements
    FetchPreparedMetadata(stmt);

    if (ColumnNumber < 1 || ColumnNumber > stmt->result_set.columns.size()) {
        stmt->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    const auto &col = stmt->result_set.columns[ColumnNumber - 1];

    if (ColumnName && BufferLength > 0) {
        CopyStringToBufferW(col.name, ColumnName, BufferLength, NameLength);
    }
    if (DataType) *DataType = col.sql_type;
    if (ColumnSize) *ColumnSize = col.column_size;
    if (DecimalDigits) *DecimalDigits = col.decimal_digits;
    if (Nullable) *Nullable = col.nullable;

    return SQL_SUCCESS;
}

// ============================================================================
// SQLColAttribute / SQLColAttributeW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLColAttribute(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                                               SQLUSMALLINT FieldIdentifier, SQLPOINTER CharacterAttribute,
                                               SQLSMALLINT BufferLength, SQLSMALLINT *StringLength,
                                               SQLLEN *NumericAttribute) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    // Lazy metadata discovery for prepared statements
    FetchPreparedMetadata(stmt);

    if (ColumnNumber < 1 || ColumnNumber > stmt->result_set.columns.size()) {
        stmt->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    const auto &col = stmt->result_set.columns[ColumnNumber - 1];

    switch (FieldIdentifier) {
    case SQL_DESC_NAME:
    case SQL_DESC_LABEL:
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBuffer(col.name, (SQLCHAR *)CharacterAttribute, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DESC_BASE_COLUMN_NAME:
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBuffer(col.name, (SQLCHAR *)CharacterAttribute, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DESC_TYPE:
    case SQL_DESC_CONCISE_TYPE:
        if (NumericAttribute) *NumericAttribute = col.sql_type;
        return SQL_SUCCESS;
    case SQL_DESC_LENGTH:
        if (NumericAttribute) *NumericAttribute = (SQLLEN)col.column_size;
        return SQL_SUCCESS;
    case SQL_DESC_DISPLAY_SIZE:
        if (NumericAttribute) *NumericAttribute = (SQLLEN)GetSqlTypeDisplaySize(col.sql_type);
        return SQL_SUCCESS;
    case SQL_DESC_OCTET_LENGTH:
        if (NumericAttribute) *NumericAttribute = (SQLLEN)GetSqlTypeOctetLength(col.sql_type);
        return SQL_SUCCESS;
    case SQL_DESC_PRECISION:
        if (NumericAttribute) *NumericAttribute = (SQLLEN)col.column_size;
        return SQL_SUCCESS;
    case SQL_DESC_SCALE:
        if (NumericAttribute) *NumericAttribute = col.decimal_digits;
        return SQL_SUCCESS;
    case SQL_DESC_NULLABLE:
        if (NumericAttribute) *NumericAttribute = col.nullable;
        return SQL_SUCCESS;
    case SQL_DESC_UNSIGNED:
        if (NumericAttribute) *NumericAttribute = IsUnsignedType(col.clickhouse_type) ? SQL_TRUE : SQL_FALSE;
        return SQL_SUCCESS;
    case SQL_DESC_AUTO_UNIQUE_VALUE:
        if (NumericAttribute) *NumericAttribute = SQL_FALSE;
        return SQL_SUCCESS;
    case SQL_DESC_SEARCHABLE:
        if (NumericAttribute) *NumericAttribute = SQL_PRED_SEARCHABLE;
        return SQL_SUCCESS;
    case SQL_DESC_UPDATABLE:
        if (NumericAttribute) *NumericAttribute = SQL_ATTR_READONLY;
        return SQL_SUCCESS;
    case SQL_DESC_CASE_SENSITIVE:
        // String types are case sensitive, numeric types are not
        if (NumericAttribute) {
            switch (col.sql_type) {
            case SQL_CHAR: case SQL_VARCHAR: case SQL_LONGVARCHAR:
            case SQL_WCHAR: case SQL_WVARCHAR: case SQL_WLONGVARCHAR:
                *NumericAttribute = SQL_TRUE;
                break;
            default:
                *NumericAttribute = SQL_FALSE;
                break;
            }
        }
        return SQL_SUCCESS;
    case SQL_DESC_FIXED_PREC_SCALE:
        if (NumericAttribute) *NumericAttribute = SQL_FALSE;
        return SQL_SUCCESS;
    case SQL_DESC_TYPE_NAME:
        if (CharacterAttribute && BufferLength > 0) {
            std::string type_name = GetSqlTypeName(col.sql_type);
            CopyStringToBuffer(type_name, (SQLCHAR *)CharacterAttribute, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DESC_LOCAL_TYPE_NAME:
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBuffer(col.clickhouse_type, (SQLCHAR *)CharacterAttribute, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_BASE_TABLE_NAME: {
        // Return the source table name so Access can construct re-fetch queries
        std::string tbl = stmt->source_table;
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBuffer(tbl, (SQLCHAR *)CharacterAttribute, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    }
    case SQL_DESC_SCHEMA_NAME:
    case SQL_DESC_CATALOG_NAME:
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBuffer("", (SQLCHAR *)CharacterAttribute, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DESC_LITERAL_PREFIX:
        if (CharacterAttribute && BufferLength > 0) {
            std::string prefix;
            switch (col.sql_type) {
            case SQL_CHAR: case SQL_VARCHAR: case SQL_LONGVARCHAR:
            case SQL_WCHAR: case SQL_WVARCHAR: case SQL_WLONGVARCHAR:
            case SQL_TYPE_DATE: case SQL_TYPE_TIMESTAMP: case SQL_TYPE_TIME:
            case SQL_GUID:
                prefix = "'";
                break;
            case SQL_BINARY: case SQL_VARBINARY:
                prefix = "0x";
                break;
            default:
                break;
            }
            CopyStringToBuffer(prefix, (SQLCHAR *)CharacterAttribute, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DESC_LITERAL_SUFFIX:
        if (CharacterAttribute && BufferLength > 0) {
            std::string suffix;
            switch (col.sql_type) {
            case SQL_CHAR: case SQL_VARCHAR: case SQL_LONGVARCHAR:
            case SQL_WCHAR: case SQL_WVARCHAR: case SQL_WLONGVARCHAR:
            case SQL_TYPE_DATE: case SQL_TYPE_TIMESTAMP: case SQL_TYPE_TIME:
            case SQL_GUID:
                suffix = "'";
                break;
            default:
                break;
            }
            CopyStringToBuffer(suffix, (SQLCHAR *)CharacterAttribute, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DESC_NUM_PREC_RADIX:
        if (NumericAttribute) {
            switch (col.sql_type) {
            case SQL_TINYINT: case SQL_SMALLINT: case SQL_INTEGER: case SQL_BIGINT:
            case SQL_REAL: case SQL_FLOAT: case SQL_DOUBLE:
            case SQL_DECIMAL: case SQL_NUMERIC:
                *NumericAttribute = 10;
                break;
            default:
                *NumericAttribute = 0;
                break;
            }
        }
        return SQL_SUCCESS;
    case SQL_DESC_COUNT:
        if (NumericAttribute) *NumericAttribute = (SQLLEN)stmt->result_set.columns.size();
        return SQL_SUCCESS;
    default:
        if (NumericAttribute) *NumericAttribute = 0;
        return SQL_SUCCESS;
    }
}

extern "C" SQLRETURN SQL_API SQLColAttributeW(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                                                SQLUSMALLINT FieldIdentifier, SQLPOINTER CharacterAttribute,
                                                SQLSMALLINT BufferLength, SQLSMALLINT *StringLength,
                                                SQLLEN *NumericAttribute) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    // Lazy metadata discovery for prepared statements
    FetchPreparedMetadata(stmt);

    if (ColumnNumber < 1 || ColumnNumber > stmt->result_set.columns.size()) {
        stmt->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    const auto &col = stmt->result_set.columns[ColumnNumber - 1];

    // String fields -> wide
    switch (FieldIdentifier) {
    case SQL_DESC_NAME:
    case SQL_DESC_LABEL:
    case SQL_DESC_BASE_COLUMN_NAME:
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBufferW(col.name, (SQLWCHAR *)CharacterAttribute, BufferLength / (SQLSMALLINT)sizeof(SQLWCHAR), StringLength);
            if (StringLength) *StringLength *= sizeof(SQLWCHAR);
        }
        return SQL_SUCCESS;
    case SQL_DESC_TYPE_NAME: {
        std::string type_name = GetSqlTypeName(col.sql_type);
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBufferW(type_name, (SQLWCHAR *)CharacterAttribute, BufferLength / (SQLSMALLINT)sizeof(SQLWCHAR), StringLength);
            if (StringLength) *StringLength *= sizeof(SQLWCHAR);
        }
        return SQL_SUCCESS;
    }
    case SQL_DESC_LOCAL_TYPE_NAME:
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBufferW(col.clickhouse_type, (SQLWCHAR *)CharacterAttribute, BufferLength / (SQLSMALLINT)sizeof(SQLWCHAR), StringLength);
            if (StringLength) *StringLength *= sizeof(SQLWCHAR);
        }
        return SQL_SUCCESS;
    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_BASE_TABLE_NAME: {
        // Return the source table name so Access can construct re-fetch queries
        std::string tbl = stmt->source_table;
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBufferW(tbl, (SQLWCHAR *)CharacterAttribute, BufferLength / (SQLSMALLINT)sizeof(SQLWCHAR), StringLength);
            if (StringLength) *StringLength *= sizeof(SQLWCHAR);
        }
        return SQL_SUCCESS;
    }
    case SQL_DESC_SCHEMA_NAME:
    case SQL_DESC_CATALOG_NAME:
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBufferW("", (SQLWCHAR *)CharacterAttribute, BufferLength / (SQLSMALLINT)sizeof(SQLWCHAR), StringLength);
            if (StringLength) *StringLength *= sizeof(SQLWCHAR);
        }
        return SQL_SUCCESS;
    case SQL_DESC_LITERAL_PREFIX:
    case SQL_DESC_LITERAL_SUFFIX: {
        std::string val;
        switch (col.sql_type) {
        case SQL_CHAR: case SQL_VARCHAR: case SQL_LONGVARCHAR:
        case SQL_WCHAR: case SQL_WVARCHAR: case SQL_WLONGVARCHAR:
        case SQL_TYPE_DATE: case SQL_TYPE_TIMESTAMP: case SQL_TYPE_TIME:
        case SQL_GUID:
            val = "'";
            break;
        case SQL_BINARY: case SQL_VARBINARY:
            if (FieldIdentifier == SQL_DESC_LITERAL_PREFIX) val = "0x";
            break;
        default:
            break;
        }
        if (CharacterAttribute && BufferLength > 0) {
            CopyStringToBufferW(val, (SQLWCHAR *)CharacterAttribute, BufferLength / (SQLSMALLINT)sizeof(SQLWCHAR), StringLength);
            if (StringLength) *StringLength *= sizeof(SQLWCHAR);
        }
        return SQL_SUCCESS;
    }
    default:
        return SQLColAttribute(StatementHandle, ColumnNumber, FieldIdentifier, CharacterAttribute,
                               BufferLength, StringLength, NumericAttribute);
    }
}

// ============================================================================
// SQLBindCol
// ============================================================================
extern "C" SQLRETURN SQL_API SQLBindCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                                         SQLSMALLINT TargetType, SQLPOINTER TargetValue,
                                         SQLLEN BufferLength, SQLLEN *StrLen_or_Ind) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    if (TargetValue == nullptr && StrLen_or_Ind == nullptr) {
        // Unbind column
        stmt->bound_columns.erase(ColumnNumber);
    } else {
        BoundColumn bc;
        bc.target_type = TargetType;
        bc.target_value = TargetValue;
        bc.buffer_length = BufferLength;
        bc.str_len_or_ind = StrLen_or_Ind;
        stmt->bound_columns[ColumnNumber] = bc;
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLBindParameter
// ============================================================================
extern "C" SQLRETURN SQL_API SQLBindParameter(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber,
                                                SQLSMALLINT InputOutputType, SQLSMALLINT ValueType,
                                                SQLSMALLINT ParameterType, SQLULEN ColumnSize,
                                                SQLSMALLINT DecimalDigits, SQLPOINTER ParameterValuePtr,
                                                SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {
    TRACE_ENTRY("SQLBindParameter", "Param#" + std::to_string(ParameterNumber) +
                " ValueType=" + std::to_string(ValueType) + " ParamType=" + std::to_string(ParameterType));
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    BoundParameter bp;
    bp.input_output_type = InputOutputType;
    bp.value_type = ValueType;
    bp.parameter_type = ParameterType;
    bp.column_size = ColumnSize;
    bp.decimal_digits = DecimalDigits;
    bp.parameter_value = ParameterValuePtr;
    bp.buffer_length = BufferLength;
    bp.str_len_or_ind = StrLen_or_IndPtr;

    stmt->bound_parameters[ParameterNumber] = bp;
    return SQL_SUCCESS;
}

// ============================================================================
// SQLRowCount
// ============================================================================
extern "C" SQLRETURN SQL_API SQLRowCount(SQLHSTMT StatementHandle, SQLLEN *RowCount) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    if (RowCount) {
        if (stmt->affected_rows >= 0) {
            // Non-SELECT statement: return affected row count
            *RowCount = stmt->affected_rows;
        } else {
            // SELECT statement: return total rows in result set
            *RowCount = static_cast<SQLLEN>(stmt->result_set.RowCount());
        }
    }

    return SQL_SUCCESS;
}

// ============================================================================
// Statement attributes
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute,
                                             SQLPOINTER Value, SQLINTEGER BufferLength,
                                             SQLINTEGER *StringLength) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    switch (Attribute) {
    case SQL_ATTR_MAX_ROWS:
        *(SQLULEN *)Value = stmt->max_rows;
        return SQL_SUCCESS;
    case SQL_ATTR_QUERY_TIMEOUT:
        *(SQLULEN *)Value = stmt->query_timeout;
        return SQL_SUCCESS;
    case SQL_ATTR_CURSOR_TYPE:
        *(SQLULEN *)Value = stmt->cursor_type;
        return SQL_SUCCESS;
    case SQL_ATTR_ROW_NUMBER:
        *(SQLULEN *)Value = (stmt->result_set.current_row >= 0) ? (SQLULEN)(stmt->result_set.current_row + 1) : 0;
        return SQL_SUCCESS;
    case SQL_ROWSET_SIZE:
    case SQL_ATTR_ROW_ARRAY_SIZE:
        *(SQLULEN *)Value = stmt->rowset_size;
        return SQL_SUCCESS;
    case SQL_ATTR_ROW_STATUS_PTR:
        *(SQLPOINTER *)Value = (SQLPOINTER)stmt->row_status_ptr;
        return SQL_SUCCESS;
    case SQL_ATTR_ROWS_FETCHED_PTR:
        *(SQLPOINTER *)Value = (SQLPOINTER)stmt->rows_fetched_ptr;
        return SQL_SUCCESS;
    case SQL_ATTR_ROW_BIND_TYPE:
        *(SQLULEN *)Value = stmt->row_bind_type;
        return SQL_SUCCESS;
    case SQL_ATTR_PARAMSET_SIZE:
        *(SQLULEN *)Value = stmt->paramset_size;
        return SQL_SUCCESS;
    case SQL_ATTR_PARAM_BIND_TYPE:
        *(SQLULEN *)Value = stmt->param_bind_type;
        return SQL_SUCCESS;
    case SQL_ATTR_PARAM_STATUS_PTR:
        *(SQLPOINTER *)Value = (SQLPOINTER)stmt->param_status_ptr;
        return SQL_SUCCESS;
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
        *(SQLPOINTER *)Value = (SQLPOINTER)stmt->params_processed_ptr;
        return SQL_SUCCESS;
    case SQL_ATTR_NOSCAN:
        *(SQLULEN *)Value = stmt->noscan;
        return SQL_SUCCESS;
    case SQL_ATTR_CONCURRENCY:
        *(SQLULEN *)Value = stmt->concurrency;
        return SQL_SUCCESS;
    case SQL_ATTR_MAX_LENGTH:
        *(SQLULEN *)Value = stmt->max_length;
        return SQL_SUCCESS;
    case SQL_ATTR_RETRIEVE_DATA:
        *(SQLULEN *)Value = stmt->retrieve_data;
        return SQL_SUCCESS;
    case SQL_ATTR_USE_BOOKMARKS:
        *(SQLULEN *)Value = stmt->use_bookmarks;
        return SQL_SUCCESS;
    case SQL_ATTR_CURSOR_SCROLLABLE:
        *(SQLULEN *)Value = stmt->cursor_scrollable;
        return SQL_SUCCESS;
    case SQL_ATTR_CURSOR_SENSITIVITY:
        *(SQLULEN *)Value = stmt->cursor_sensitivity;
        return SQL_SUCCESS;
    case SQL_ATTR_IMP_ROW_DESC:
        *(SQLPOINTER *)Value = (SQLPOINTER)stmt->ird;
        return SQL_SUCCESS;
    case SQL_ATTR_IMP_PARAM_DESC:
        *(SQLPOINTER *)Value = (SQLPOINTER)stmt->ipd;
        return SQL_SUCCESS;
    case SQL_ATTR_APP_ROW_DESC:
        *(SQLPOINTER *)Value = (SQLPOINTER)stmt->ard;
        return SQL_SUCCESS;
    case SQL_ATTR_APP_PARAM_DESC:
        *(SQLPOINTER *)Value = (SQLPOINTER)stmt->apd;
        return SQL_SUCCESS;
    default:
        if (Value) *(SQLULEN *)Value = 0;
        return SQL_SUCCESS;
    }
}

extern "C" SQLRETURN SQL_API SQLGetStmtAttrW(SQLHSTMT StatementHandle, SQLINTEGER Attribute,
                                              SQLPOINTER Value, SQLINTEGER BufferLength,
                                              SQLINTEGER *StringLength) {
    return SQLGetStmtAttr(StatementHandle, Attribute, Value, BufferLength, StringLength);
}

extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute,
                                             SQLPOINTER Value, SQLINTEGER StringLength) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    switch (Attribute) {
    case SQL_ATTR_MAX_ROWS:
        stmt->max_rows = (SQLULEN)(uintptr_t)Value;
        TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLSetStmtAttr",
                  "SQL_ATTR_MAX_ROWS=" + std::to_string(stmt->max_rows));
        return SQL_SUCCESS;
    case SQL_ATTR_QUERY_TIMEOUT:
        stmt->query_timeout = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_CURSOR_TYPE: {
        SQLULEN requested = (SQLULEN)(uintptr_t)Value;
        TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLSetStmtAttr",
                  "SQL_ATTR_CURSOR_TYPE requested=" + std::to_string(requested));
        // Downgrade unsupported cursor types to static (Access requests keyset-driven)
        if (requested == SQL_CURSOR_KEYSET_DRIVEN || requested == SQL_CURSOR_DYNAMIC) {
            stmt->cursor_type = SQL_CURSOR_STATIC;
            stmt->AddDiagRecord("01S02", 0, "Option value changed: cursor type downgraded to static");
            return SQL_SUCCESS_WITH_INFO;
        }
        stmt->cursor_type = requested;
        return SQL_SUCCESS;
    }
    case SQL_ROWSET_SIZE:
    case SQL_ATTR_ROW_ARRAY_SIZE:
        stmt->rowset_size = (SQLULEN)(uintptr_t)Value;
        if (stmt->rowset_size < 1) stmt->rowset_size = 1;
        TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLSetStmtAttr",
                  "rowset_size=" + std::to_string(stmt->rowset_size));
        return SQL_SUCCESS;
    case SQL_ATTR_ROW_STATUS_PTR:
        stmt->row_status_ptr = (SQLUSMALLINT *)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_ROWS_FETCHED_PTR:
        stmt->rows_fetched_ptr = (SQLULEN *)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_ROW_BIND_TYPE:
        stmt->row_bind_type = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_PARAMSET_SIZE:
        stmt->paramset_size = (SQLULEN)(uintptr_t)Value;
        if (stmt->paramset_size < 1) stmt->paramset_size = 1;
        return SQL_SUCCESS;
    case SQL_ATTR_PARAM_BIND_TYPE:
        stmt->param_bind_type = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_PARAM_STATUS_PTR:
        stmt->param_status_ptr = (SQLUSMALLINT *)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_PARAMS_PROCESSED_PTR:
        stmt->params_processed_ptr = (SQLULEN *)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_NOSCAN:
        stmt->noscan = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_CONCURRENCY:
        stmt->concurrency = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_MAX_LENGTH:
        stmt->max_length = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_RETRIEVE_DATA:
        stmt->retrieve_data = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_USE_BOOKMARKS:
        stmt->use_bookmarks = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_CURSOR_SCROLLABLE:
        stmt->cursor_scrollable = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_CURSOR_SENSITIVITY:
        stmt->cursor_sensitivity = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_APP_ROW_DESC:
        if (Value == SQL_NULL_HDESC || Value == nullptr) {
            stmt->ard = stmt->auto_ard.get();
        } else {
            stmt->ard = static_cast<OdbcDescriptor *>(Value);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_APP_PARAM_DESC:
        if (Value == SQL_NULL_HDESC || Value == nullptr) {
            stmt->apd = stmt->auto_apd.get();
        } else {
            stmt->apd = static_cast<OdbcDescriptor *>(Value);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_IMP_ROW_DESC:
    case SQL_ATTR_IMP_PARAM_DESC:
        // Implementation descriptors cannot be set by the application
        stmt->SetError("HY017", "Invalid use of an automatically allocated descriptor handle");
        return SQL_ERROR;
    default:
        // Silently accept for compatibility
        return SQL_SUCCESS;
    }
}

extern "C" SQLRETURN SQL_API SQLSetStmtAttrW(SQLHSTMT StatementHandle, SQLINTEGER Attribute,
                                              SQLPOINTER Value, SQLINTEGER StringLength) {
    return SQLSetStmtAttr(StatementHandle, Attribute, Value, StringLength);
}

// ============================================================================
// SQLNumParams
// ============================================================================
extern "C" SQLRETURN SQL_API SQLNumParams(SQLHSTMT StatementHandle, SQLSMALLINT *ParameterCountPtr) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    if (ParameterCountPtr) {
        // Use prepared_query (ExecDirectImpl overwrites stmt->query)
        const std::string &q = stmt->prepared_query.empty() ? stmt->query : stmt->prepared_query;
        SQLSMALLINT count = 0;
        bool in_quote = false;
        for (size_t i = 0; i < q.size(); i++) {
            char c = q[i];
            if (c == '\'' && !in_quote) {
                in_quote = true;
            } else if (c == '\'' && in_quote) {
                // Check for escaped quote ''
                if (i + 1 < q.size() && q[i + 1] == '\'') {
                    i++; // skip escaped quote
                } else {
                    in_quote = false;
                }
            } else if (c == '?' && !in_quote) {
                count++;
            }
        }
        *ParameterCountPtr = count;
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLDescribeParam
// ============================================================================
extern "C" SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber,
                                               SQLSMALLINT *DataTypePtr, SQLULEN *ParameterSizePtr,
                                               SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    if (ParameterNumber < 1) {
        stmt->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    // Count parameters in query (use prepared_query: ExecDirectImpl overwrites stmt->query)
    const std::string &q = stmt->prepared_query.empty() ? stmt->query : stmt->prepared_query;
    SQLSMALLINT param_count = 0;
    bool in_quote = false;
    for (size_t i = 0; i < q.size(); i++) {
        char c = q[i];
        if (c == '\'' && !in_quote) {
            in_quote = true;
        } else if (c == '\'' && in_quote) {
            if (i + 1 < q.size() && q[i + 1] == '\'') {
                i++;
            } else {
                in_quote = false;
            }
        } else if (c == '?' && !in_quote) {
            param_count++;
        }
    }

    if (ParameterNumber > (SQLUSMALLINT)param_count) {
        stmt->SetError("07009", "Invalid descriptor index: parameter " +
                        std::to_string(ParameterNumber) + " exceeds count " +
                        std::to_string(param_count));
        return SQL_ERROR;
    }

    // If the parameter has been bound, use its type info
    auto it = stmt->bound_parameters.find(ParameterNumber);
    if (it != stmt->bound_parameters.end()) {
        if (DataTypePtr) *DataTypePtr = it->second.parameter_type;
        if (ParameterSizePtr) *ParameterSizePtr = it->second.column_size;
        if (DecimalDigitsPtr) *DecimalDigitsPtr = it->second.decimal_digits;
    } else {
        // Default: SQL_VARCHAR with generous size (common for MS Access)
        if (DataTypePtr) *DataTypePtr = SQL_VARCHAR;
        if (ParameterSizePtr) *ParameterSizePtr = 255;
        if (DecimalDigitsPtr) *DecimalDigitsPtr = 0;
    }

    // All parameters are nullable in ClickHouse
    if (NullablePtr) *NullablePtr = SQL_NULLABLE;

    return SQL_SUCCESS;
}

// ============================================================================
// SQLCancel
// ============================================================================
extern "C" SQLRETURN SQL_API SQLCancel(SQLHSTMT StatementHandle) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    // Reset Data-at-Execution state if active
    if (stmt->need_data) {
        stmt->ResetDAE();
    }

    // Reset piecemeal SQLGetData state
    stmt->getdata_col = 0;
    stmt->getdata_offset = 0;

    // ClickHouse HTTP queries can't be cancelled mid-flight,
    // but we reset pending driver-side state
    return SQL_SUCCESS;
}

// ============================================================================
// SQLNativeSql / SQLNativeSqlW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLNativeSql(SQLHDBC ConnectionHandle, SQLCHAR *InStatementText,
                                           SQLINTEGER TextLength1, SQLCHAR *OutStatementText,
                                           SQLINTEGER BufferLength, SQLINTEGER *TextLength2Ptr) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    std::string sql;
    if (InStatementText) {
        if (TextLength1 == SQL_NTS) {
            sql = reinterpret_cast<const char *>(InStatementText);
        } else {
            sql = std::string(reinterpret_cast<const char *>(InStatementText), TextLength1);
        }
    }

    // Process ODBC escape sequences
    std::string native_sql = ProcessOdbcEscapeSequences(sql);

    if (OutStatementText && BufferLength > 0) {
        CopyStringToBuffer(native_sql, OutStatementText, (SQLSMALLINT)BufferLength, (SQLSMALLINT *)TextLength2Ptr);
    }
    if (TextLength2Ptr) {
        *TextLength2Ptr = (SQLINTEGER)native_sql.size();
    }

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLNativeSqlW(SQLHDBC ConnectionHandle, SQLWCHAR *InStatementText,
                                             SQLINTEGER TextLength1, SQLWCHAR *OutStatementText,
                                             SQLINTEGER BufferLength, SQLINTEGER *TextLength2Ptr) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    std::string sql;
    if (InStatementText) {
        sql = WideToUtf8(InStatementText, (SQLSMALLINT)TextLength1);
    }

    // Process ODBC escape sequences
    std::string native_sql = ProcessOdbcEscapeSequences(sql);

    if (OutStatementText && BufferLength > 0) {
        CopyStringToBufferW(native_sql, OutStatementText, (SQLSMALLINT)BufferLength, (SQLSMALLINT *)TextLength2Ptr);
    }
    if (TextLength2Ptr) {
        std::wstring wide = Utf8ToWide(native_sql);
        *TextLength2Ptr = (SQLINTEGER)(wide.size() * sizeof(SQLWCHAR));
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLParamData / SQLPutData  (Data-at-Execution)
// ============================================================================

// Helper: Execute query with DAE buffers substituted
static SQLRETURN ExecuteWithDAE(OdbcStatement *stmt) {
    // Build a temporary bound_parameters map with DAE buffers as values
    auto params_copy = stmt->bound_parameters;
    for (auto &[idx, buf] : stmt->dae_buffers) {
        auto it = params_copy.find(idx);
        if (it != params_copy.end()) {
            // Point the parameter at our DAE buffer
            it->second.parameter_value = const_cast<char *>(buf.data());
            it->second.buffer_length = static_cast<SQLLEN>(buf.size());
            SQLLEN len = static_cast<SQLLEN>(buf.size());
            it->second.str_len_or_ind = nullptr; // will use buffer_length
        }
    }

    // Substitute parameters (use prepared_query: ExecDirectImpl overwrites stmt->query)
    std::string template_query = stmt->prepared_query.empty() ? stmt->query : stmt->prepared_query;
    std::string exec_query = template_query;
    if (!params_copy.empty()) {
        std::string error_msg;
        exec_query = SubstituteParameters(template_query, params_copy, error_msg);
        if (exec_query.empty() && !error_msg.empty()) {
            stmt->SetError("07002", error_msg);
            stmt->ResetDAE();
            return SQL_ERROR;
        }
    }

    stmt->ResetDAE();
    return ExecDirectImpl(stmt, exec_query);
}

extern "C" SQLRETURN SQL_API SQLParamData(SQLHSTMT StatementHandle, SQLPOINTER *Value) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    if (!stmt->need_data) {
        stmt->SetError("HY010", "Function sequence error: no data-at-execution in progress");
        return SQL_ERROR;
    }

    // If current_dae_index >= 0, advance to next param (current one is done)
    if (stmt->current_dae_index >= 0) {
        stmt->current_dae_index++;
    } else {
        // First call — start with index 0
        stmt->current_dae_index = 0;
    }

    // Check if all DAE params have been provided
    if (stmt->current_dae_index >= static_cast<int>(stmt->pending_dae_params.size())) {
        // All data provided — execute the query
        return ExecuteWithDAE(stmt);
    }

    // Return the pointer to the next DAE parameter's ParameterValuePtr (token)
    SQLUSMALLINT param_idx = stmt->pending_dae_params[stmt->current_dae_index];
    auto it = stmt->bound_parameters.find(param_idx);
    if (it != stmt->bound_parameters.end() && Value) {
        *Value = it->second.parameter_value;
    }

    return SQL_NEED_DATA;
}

extern "C" SQLRETURN SQL_API SQLPutData(SQLHSTMT StatementHandle, SQLPOINTER Data, SQLLEN StrLen_or_Ind) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    if (!stmt->need_data || stmt->current_dae_index < 0 ||
        stmt->current_dae_index >= static_cast<int>(stmt->pending_dae_params.size())) {
        stmt->SetError("HY010", "Function sequence error: no data-at-execution parameter active");
        return SQL_ERROR;
    }

    if (StrLen_or_Ind == SQL_NULL_DATA) {
        // Store NULL marker
        SQLUSMALLINT param_idx = stmt->pending_dae_params[stmt->current_dae_index];
        stmt->dae_buffers[param_idx] = "";  // will be treated as null when substituted
        return SQL_SUCCESS;
    }

    if (!Data) {
        stmt->SetError("HY009", "Invalid use of null pointer");
        return SQL_ERROR;
    }

    SQLUSMALLINT param_idx = stmt->pending_dae_params[stmt->current_dae_index];
    SQLLEN data_len = StrLen_or_Ind;
    if (data_len == SQL_NTS) {
        data_len = static_cast<SQLLEN>(strlen(static_cast<const char *>(Data)));
    }

    // Append data to the buffer (multiple SQLPutData calls accumulate)
    if (data_len > 0) {
        stmt->dae_buffers[param_idx].append(static_cast<const char *>(Data), static_cast<size_t>(data_len));
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLSetPos / SQLBulkOperations
// ============================================================================
extern "C" SQLRETURN SQL_API SQLSetPos(SQLHSTMT StatementHandle, SQLSETPOSIROW RowNumber,
                                        SQLUSMALLINT Operation, SQLUSMALLINT LockType) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    switch (Operation) {
    case SQL_POSITION:
        // SQL_POSITION: position cursor on RowNumber within current rowset (1-based)
        // RowNumber=0 means "current row" (no-op)
        if (RowNumber == 0) {
            return SQL_SUCCESS;
        }
        // Allow positioning to any row within the current rowset.
        // MS Access calls SQLSetPos to position on individual rows within a fetched rowset.
        if (RowNumber >= 1 && RowNumber <= static_cast<SQLSETPOSIROW>(stmt->rowset_size)) {
            // Use the stored rowset start position (set at the beginning of each SQLFetch).
            // This is stable across multiple SQLSetPos calls within the same rowset.
            SQLLEN target = stmt->rowset_start_row + static_cast<SQLLEN>(RowNumber) - 1;
            if (target >= 0 && target < static_cast<SQLLEN>(stmt->result_set.RowCount())) {
                stmt->result_set.current_row = target;
                // Reset piecemeal SQLGetData state for the new row position
                stmt->getdata_col = 0;
                stmt->getdata_offset = 0;
                return SQL_SUCCESS;
            }
        }
        stmt->SetError("HY109", "Invalid cursor position");
        return SQL_ERROR;
    case SQL_REFRESH:
        // Refresh: re-read current row — no-op for HTTP-based driver
        return SQL_SUCCESS;
    case SQL_UPDATE:
    case SQL_DELETE:
    case SQL_ADD:
        stmt->SetError("HYC00", "Optional feature not implemented: positioned update/delete/add");
        return SQL_ERROR;
    default:
        stmt->SetError("HY092", "Invalid attribute/option identifier");
        return SQL_ERROR;
    }
}

extern "C" SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT StatementHandle, SQLSMALLINT Operation) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    switch (Operation) {
    case SQL_ADD: {
        // Build INSERT statement from result_set columns and bound column values
        if (stmt->result_set.columns.empty()) {
            stmt->SetError("HY010", "No result set available for bulk insert");
            return SQL_ERROR;
        }
        if (stmt->bound_columns.empty()) {
            stmt->SetError("HY010", "No columns bound for bulk insert");
            return SQL_ERROR;
        }

        // Extract table name from query (simple heuristic: look for FROM <table>)
        std::string table_name;
        {
            std::string q = stmt->query;
            // Try "FROM tablename" pattern
            auto pos = q.find("FROM ");
            if (pos == std::string::npos) pos = q.find("from ");
            if (pos == std::string::npos) pos = q.find("From ");
            if (pos != std::string::npos) {
                pos += 5;
                // Skip whitespace
                while (pos < q.size() && (q[pos] == ' ' || q[pos] == '`' || q[pos] == '"')) pos++;
                size_t start = pos;
                while (pos < q.size() && q[pos] != ' ' && q[pos] != '`' && q[pos] != '"'
                       && q[pos] != '\r' && q[pos] != '\n' && q[pos] != ';') pos++;
                table_name = q.substr(start, pos - start);
            }
        }

        if (table_name.empty()) {
            stmt->SetError("HY000", "Cannot determine table name for INSERT");
            return SQL_ERROR;
        }

        // Build INSERT INTO table (col1, col2, ...) VALUES (val1, val2, ...)
        std::string insert_query = "INSERT INTO " + table_name + " (";
        std::string values_clause = " VALUES (";

        bool first = true;
        for (SQLUSMALLINT i = 1; i <= stmt->result_set.columns.size(); i++) {
            auto bc_it = stmt->bound_columns.find(i);
            if (bc_it == stmt->bound_columns.end()) continue;

            if (!first) {
                insert_query += ", ";
                values_clause += ", ";
            }
            first = false;

            insert_query += stmt->result_set.columns[i - 1].name;

            // Convert bound column to a parameter-like value extraction
            const auto &bc = bc_it->second;
            BoundParameter temp_bp;
            temp_bp.value_type = bc.target_type;
            temp_bp.parameter_value = bc.target_value;
            temp_bp.buffer_length = bc.buffer_length;
            temp_bp.str_len_or_ind = bc.str_len_or_ind;
            values_clause += ExtractParameterValue(temp_bp);
        }

        insert_query += ")";
        values_clause += ")";
        insert_query += values_clause;

        return ExecDirectImpl(stmt, insert_query);
    }
    case SQL_UPDATE_BY_BOOKMARK:
    case SQL_DELETE_BY_BOOKMARK:
    case SQL_FETCH_BY_BOOKMARK:
        stmt->SetError("HYC00", "Bookmark operations not supported");
        return SQL_ERROR;
    default:
        stmt->SetError("HY092", "Invalid operation identifier");
        return SQL_ERROR;
    }
}

// ============================================================================
// SQLGetCursorName / SQLGetCursorNameW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT StatementHandle, SQLCHAR *CursorName,
                                               SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    SQLSMALLINT len = static_cast<SQLSMALLINT>(stmt->cursor_name.size());
    if (NameLengthPtr) {
        *NameLengthPtr = len;
    }

    if (CursorName && BufferLength > 0) {
        SQLSMALLINT copy_len = (len < BufferLength - 1) ? len : (BufferLength - 1);
        memcpy(CursorName, stmt->cursor_name.c_str(), copy_len);
        CursorName[copy_len] = '\0';
        if (len >= BufferLength) {
            stmt->AddDiagRecord("01004", 0, "String data, right truncated");
            return SQL_SUCCESS_WITH_INFO;
        }
    }

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLGetCursorNameW(SQLHSTMT StatementHandle, SQLWCHAR *CursorName,
                                                SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    std::wstring wide = Utf8ToWide(stmt->cursor_name);
    SQLSMALLINT len = static_cast<SQLSMALLINT>(wide.size());
    if (NameLengthPtr) {
        *NameLengthPtr = len;
    }

    if (CursorName && BufferLength > 0) {
        SQLSMALLINT copy_len = (len < BufferLength - 1) ? len : (BufferLength - 1);
        memcpy(CursorName, wide.c_str(), copy_len * sizeof(SQLWCHAR));
        CursorName[copy_len] = L'\0';
        if (len >= BufferLength) {
            stmt->AddDiagRecord("01004", 0, "String data, right truncated");
            return SQL_SUCCESS_WITH_INFO;
        }
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLSetCursorName / SQLSetCursorNameW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT StatementHandle, SQLCHAR *CursorName,
                                               SQLSMALLINT NameLength) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    if (!CursorName) {
        stmt->SetError("HY009", "Invalid use of null pointer");
        return SQL_ERROR;
    }

    std::string name;
    if (NameLength == SQL_NTS) {
        name = reinterpret_cast<const char *>(CursorName);
    } else {
        name = std::string(reinterpret_cast<const char *>(CursorName), NameLength);
    }

    if (name.empty()) {
        stmt->SetError("HY090", "Invalid string or buffer length");
        return SQL_ERROR;
    }

    // Cursor name max length is 18 chars per ODBC spec (SQL_MAX_CURSOR_NAME_LEN)
    if (name.size() > 128) {
        stmt->SetError("3C000", "Duplicate cursor name");
        return SQL_ERROR;
    }

    stmt->cursor_name = name;
    stmt->cursor_name_set = true;
    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLSetCursorNameW(SQLHSTMT StatementHandle, SQLWCHAR *CursorName,
                                                SQLSMALLINT NameLength) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    if (!CursorName) {
        stmt->SetError("HY009", "Invalid use of null pointer");
        return SQL_ERROR;
    }

    std::string name = WideToUtf8(CursorName, NameLength);

    if (name.empty()) {
        stmt->SetError("HY090", "Invalid string or buffer length");
        return SQL_ERROR;
    }

    if (name.size() > 128) {
        stmt->SetError("3C000", "Duplicate cursor name");
        return SQL_ERROR;
    }

    stmt->cursor_name = name;
    stmt->cursor_name_set = true;
    return SQL_SUCCESS;
}

// ============================================================================
// SQLColAttributes (ODBC 2.x backward compatibility)
// ============================================================================
extern "C" SQLRETURN SQL_API SQLColAttributes(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber,
                                               SQLUSMALLINT FieldIdentifier, SQLPOINTER CharacterAttribute,
                                               SQLSMALLINT BufferLength, SQLSMALLINT *StringLength,
                                               SQLLEN *NumericAttribute) {
    // Map ODBC 2.x field identifiers to ODBC 3.x equivalents
    SQLUSMALLINT mapped = FieldIdentifier;
    switch (FieldIdentifier) {
    case SQL_COLUMN_NAME:        mapped = SQL_DESC_NAME; break;
    case SQL_COLUMN_TYPE:        mapped = SQL_DESC_TYPE; break;
    case SQL_COLUMN_LENGTH:      mapped = SQL_DESC_LENGTH; break;
    case SQL_COLUMN_PRECISION:   mapped = SQL_DESC_PRECISION; break;
    case SQL_COLUMN_SCALE:       mapped = SQL_DESC_SCALE; break;
    case SQL_COLUMN_NULLABLE:    mapped = SQL_DESC_NULLABLE; break;
    case SQL_COLUMN_DISPLAY_SIZE: mapped = SQL_DESC_DISPLAY_SIZE; break;
    case SQL_COLUMN_UNSIGNED:    mapped = SQL_DESC_UNSIGNED; break;
    case SQL_COLUMN_UPDATABLE:   mapped = SQL_DESC_UPDATABLE; break;
    case SQL_COLUMN_AUTO_INCREMENT: mapped = SQL_DESC_AUTO_UNIQUE_VALUE; break;
    case SQL_COLUMN_CASE_SENSITIVE: mapped = SQL_DESC_CASE_SENSITIVE; break;
    case SQL_COLUMN_SEARCHABLE:  mapped = SQL_DESC_SEARCHABLE; break;
    case SQL_COLUMN_TYPE_NAME:   mapped = SQL_DESC_TYPE_NAME; break;
    case SQL_COLUMN_TABLE_NAME:  mapped = SQL_DESC_TABLE_NAME; break;
    case SQL_COLUMN_LABEL:       mapped = SQL_DESC_LABEL; break;
    case SQL_COLUMN_COUNT:       mapped = SQL_DESC_COUNT; break;
    default: break;
    }
    return SQLColAttribute(StatementHandle, ColumnNumber, mapped, CharacterAttribute,
                           BufferLength, StringLength, NumericAttribute);
}

// ============================================================================
// SQLSetScrollOptions (ODBC 2.x deprecated stub)
// ============================================================================
extern "C" SQLRETURN SQL_API SQLSetScrollOptions(SQLHSTMT StatementHandle, SQLUSMALLINT Concurrency,
                                                  SQLLEN KeysetSize, SQLUSMALLINT RowsetSize) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    // Map to modern equivalents
    if (RowsetSize > 0) {
        stmt->rowset_size = RowsetSize;
    }

    // Set cursor type based on KeysetSize
    if (KeysetSize == SQL_SCROLL_FORWARD_ONLY) {
        stmt->cursor_type = SQL_CURSOR_FORWARD_ONLY;
    } else if (KeysetSize == SQL_SCROLL_STATIC) {
        stmt->cursor_type = SQL_CURSOR_STATIC;
    } else {
        // Default: treat as static for ClickHouse
        stmt->cursor_type = SQL_CURSOR_STATIC;
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLGetStmtOption / SQLSetStmtOption (ODBC 2.x)
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT StatementHandle, SQLUSMALLINT Option,
                                               SQLPOINTER Value) {
    // ODBC 2.x option IDs map directly to 3.x attribute IDs for common options
    return SQLGetStmtAttr(StatementHandle, static_cast<SQLINTEGER>(Option),
                          Value, SQL_IS_UINTEGER, nullptr);
}

extern "C" SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT StatementHandle, SQLUSMALLINT Option,
                                               SQLULEN Value) {
    return SQLSetStmtAttr(StatementHandle, static_cast<SQLINTEGER>(Option),
                          reinterpret_cast<SQLPOINTER>(Value), SQL_IS_UINTEGER);
}
