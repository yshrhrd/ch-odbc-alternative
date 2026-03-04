#include "include/handle.h"
#include "include/util.h"
#include "include/type_mapping.h"
#include "include/trace.h"

#include <sql.h>
#include <sqlext.h>
#include <cstring>
#include <cstdlib>
#include <cmath>
#include <climits>

using namespace clickhouse_odbc;

// Helper: convert string value to bound column target type
static SQLRETURN CopyValueToTarget(const std::optional<std::string> &value, SQLSMALLINT target_type,
                                   SQLPOINTER target, SQLLEN buffer_length, SQLLEN *str_len_or_ind,
                                   OdbcStatement *stmt = nullptr) {
    if (!value.has_value()) {
        // NULL value
        if (str_len_or_ind) {
            *str_len_or_ind = SQL_NULL_DATA;
        } else {
            // ODBC spec: if no indicator and value is NULL, return SQL_ERROR (22002)
            if (stmt) {
                stmt->SetError("22002", "Indicator variable required but not supplied");
            }
            return SQL_ERROR;
        }
        return SQL_SUCCESS;
    }

    const std::string &val = value.value();

    switch (target_type) {
    case SQL_C_CHAR:
    case SQL_C_DEFAULT: {
        SQLLEN len = static_cast<SQLLEN>(val.size());
        if (str_len_or_ind) {
            *str_len_or_ind = len;
        }
        if (target && buffer_length > 0) {
            SQLLEN copy_len = (len < buffer_length - 1) ? len : (buffer_length - 1);
            memcpy(target, val.c_str(), copy_len);
            ((char *)target)[copy_len] = '\0';
            if (len >= buffer_length) {
                if (stmt) stmt->AddDiagRecord("01004", 0, "String data, right truncated");
                return SQL_SUCCESS_WITH_INFO;
            }
        }
        return SQL_SUCCESS;
    }
    case SQL_C_WCHAR: {
        std::wstring wide = Utf8ToWide(val);
        SQLLEN char_count = static_cast<SQLLEN>(wide.size());
        if (str_len_or_ind) {
            *str_len_or_ind = char_count * sizeof(SQLWCHAR);
        }
        if (target && buffer_length > 0) {
            SQLLEN max_chars = (buffer_length / sizeof(SQLWCHAR)) - 1;
            SQLLEN copy_chars = (char_count < max_chars) ? char_count : max_chars;
            memcpy(target, wide.c_str(), copy_chars * sizeof(SQLWCHAR));
            ((SQLWCHAR *)target)[copy_chars] = L'\0';
            if (char_count > max_chars) {
                if (stmt) stmt->AddDiagRecord("01004", 0, "String data, right truncated");
                return SQL_SUCCESS_WITH_INFO;
            }
        }
        return SQL_SUCCESS;
    }
    case SQL_C_SLONG:
    case SQL_C_LONG: {
        if (!ValidateNumericRange(val, SQL_C_SLONG) && stmt) {
            stmt->SetError("22003", "Numeric value out of range for SQL_C_SLONG");
            return SQL_ERROR;
        }
        SQLINTEGER v = (SQLINTEGER)atol(val.c_str());
        if (target) *(SQLINTEGER *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLINTEGER);
        return SQL_SUCCESS;
    }
    case SQL_C_ULONG: {
        SQLUINTEGER v = (SQLUINTEGER)strtoul(val.c_str(), nullptr, 10);
        if (target) *(SQLUINTEGER *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLUINTEGER);
        return SQL_SUCCESS;
    }
    case SQL_C_SSHORT:
    case SQL_C_SHORT: {
        SQLSMALLINT v = (SQLSMALLINT)atoi(val.c_str());
        if (target) *(SQLSMALLINT *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    }
    case SQL_C_USHORT: {
        SQLUSMALLINT v = (SQLUSMALLINT)atoi(val.c_str());
        if (target) *(SQLUSMALLINT *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLUSMALLINT);
        return SQL_SUCCESS;
    }
    case SQL_C_STINYINT:
    case SQL_C_TINYINT: {
        SQLSCHAR v = (SQLSCHAR)atoi(val.c_str());
        if (target) *(SQLSCHAR *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLSCHAR);
        return SQL_SUCCESS;
    }
    case SQL_C_UTINYINT: {
        SQLCHAR v = (SQLCHAR)atoi(val.c_str());
        if (target) *(SQLCHAR *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLCHAR);
        return SQL_SUCCESS;
    }
    case SQL_C_SBIGINT: {
        SQLBIGINT v = (SQLBIGINT)_atoi64(val.c_str());
        if (target) *(SQLBIGINT *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLBIGINT);
        return SQL_SUCCESS;
    }
    case SQL_C_UBIGINT: {
        SQLUBIGINT v = (SQLUBIGINT)_strtoui64(val.c_str(), nullptr, 10);
        if (target) *(SQLUBIGINT *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLUBIGINT);
        return SQL_SUCCESS;
    }
    case SQL_C_FLOAT: {
        SQLREAL v = (SQLREAL)atof(val.c_str());
        if (target) *(SQLREAL *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLREAL);
        return SQL_SUCCESS;
    }
    case SQL_C_DOUBLE: {
        SQLDOUBLE v = atof(val.c_str());
        if (target) *(SQLDOUBLE *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLDOUBLE);
        return SQL_SUCCESS;
    }
    case SQL_C_BIT: {
        SQLCHAR v = (val == "1" || val == "true" || val == "True") ? 1 : 0;
        if (target) *(SQLCHAR *)target = v;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLCHAR);
        return SQL_SUCCESS;
    }
    case SQL_C_NUMERIC: {
        // SQL_NUMERIC_STRUCT: precision, scale, sign (1=positive,0=negative), val[16] (LE)
        SQL_NUMERIC_STRUCT ns = {};
        ns.precision = 38;
        ns.scale = 0;
        ns.sign = 1; // positive

        std::string num_str = val;
        // Remove leading whitespace
        size_t start_pos = num_str.find_first_not_of(" \t");
        if (start_pos != std::string::npos) num_str = num_str.substr(start_pos);

        // Handle sign
        if (!num_str.empty() && num_str[0] == '-') {
            ns.sign = 0;
            num_str = num_str.substr(1);
        } else if (!num_str.empty() && num_str[0] == '+') {
            num_str = num_str.substr(1);
        }

        // Split integer and fraction parts
        std::string int_part = num_str;
        std::string frac_part;
        auto dot_pos = num_str.find('.');
        if (dot_pos != std::string::npos) {
            int_part = num_str.substr(0, dot_pos);
            frac_part = num_str.substr(dot_pos + 1);
            // Remove trailing zeros from fraction for scale
            while (!frac_part.empty() && frac_part.back() == '0') frac_part.pop_back();
        }
        ns.scale = (SQLSCHAR)frac_part.size();

        // Combine integer + fraction digits into a big integer (little-endian bytes)
        std::string all_digits = int_part + frac_part;
        // Remove leading zeros
        size_t nz = all_digits.find_first_not_of('0');
        if (nz != std::string::npos) all_digits = all_digits.substr(nz);
        else all_digits = "0";

        // Convert decimal string to little-endian 128-bit value in ns.val[16]
        // Long multiplication: repeatedly divide by 256
        memset(ns.val, 0, sizeof(ns.val));
        // Process digit by digit using carry-based approach
        // Start with 0, for each digit: val = val * 10 + digit
        for (char ch : all_digits) {
            int digit = ch - '0';
            // Multiply ns.val by 10 and add digit
            unsigned int carry = (unsigned int)digit;
            for (int i = 0; i < SQL_MAX_NUMERIC_LEN; i++) {
                unsigned int tmp = (unsigned int)ns.val[i] * 10 + carry;
                ns.val[i] = (SQLCHAR)(tmp & 0xFF);
                carry = tmp >> 8;
            }
        }
        ns.precision = (SQLCHAR)(int_part.size() + frac_part.size());
        if (ns.precision < 1) ns.precision = 1;

        if (target) *(SQL_NUMERIC_STRUCT *)target = ns;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQL_NUMERIC_STRUCT);
        return SQL_SUCCESS;
    }
    case SQL_C_GUID: {
        // Parse UUID string: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        SQLGUID guid = {};
        if (val.size() >= 36) {
            // Parse Data1 (8 hex chars)
            guid.Data1 = (unsigned long)strtoul(val.substr(0, 8).c_str(), nullptr, 16);
            // Parse Data2 (4 hex chars)
            guid.Data2 = (unsigned short)strtoul(val.substr(9, 4).c_str(), nullptr, 16);
            // Parse Data3 (4 hex chars)
            guid.Data3 = (unsigned short)strtoul(val.substr(14, 4).c_str(), nullptr, 16);
            // Parse Data4 (first 2 hex chars from group 4)
            guid.Data4[0] = (unsigned char)strtoul(val.substr(19, 2).c_str(), nullptr, 16);
            guid.Data4[1] = (unsigned char)strtoul(val.substr(21, 2).c_str(), nullptr, 16);
            // Parse Data4 (6 hex chars from group 5)
            for (int i = 0; i < 6; i++) {
                guid.Data4[2 + i] = (unsigned char)strtoul(val.substr(24 + i * 2, 2).c_str(), nullptr, 16);
            }
        }
        if (target) *(SQLGUID *)target = guid;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQLGUID);
        return SQL_SUCCESS;
    }
    case SQL_C_TYPE_TIME:
    case SQL_C_TIME: {
        // Parse HH:MM:SS from timestamp or time string
        SQL_TIME_STRUCT ts = {};
        std::string time_str = val;
        // If it looks like a full timestamp, extract time part
        if (val.size() >= 19 && val[10] == ' ') {
            time_str = val.substr(11);
        }
        if (time_str.size() >= 8) {
            ts.hour = (SQLUSMALLINT)atoi(time_str.substr(0, 2).c_str());
            ts.minute = (SQLUSMALLINT)atoi(time_str.substr(3, 2).c_str());
            ts.second = (SQLUSMALLINT)atoi(time_str.substr(6, 2).c_str());
        }
        if (target) *(SQL_TIME_STRUCT *)target = ts;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQL_TIME_STRUCT);
        return SQL_SUCCESS;
    }
    case SQL_C_BINARY: {
        SQLLEN len = static_cast<SQLLEN>(val.size());
        if (str_len_or_ind) *str_len_or_ind = len;
        if (target && buffer_length > 0) {
            SQLLEN copy_len = (len < buffer_length) ? len : buffer_length;
            memcpy(target, val.data(), copy_len);
            if (len > buffer_length) {
                if (stmt) stmt->AddDiagRecord("01004", 0, "String data, right truncated");
                return SQL_SUCCESS_WITH_INFO;
            }
        }
        return SQL_SUCCESS;
    }
    case SQL_C_TYPE_DATE:
    case SQL_C_DATE: {
        // Parse YYYY-MM-DD
        SQL_DATE_STRUCT ds = {};
        if (val.size() >= 10) {
            ds.year = (SQLSMALLINT)atoi(val.substr(0, 4).c_str());
            ds.month = (SQLUSMALLINT)atoi(val.substr(5, 2).c_str());
            ds.day = (SQLUSMALLINT)atoi(val.substr(8, 2).c_str());
        }
        if (target) *(SQL_DATE_STRUCT *)target = ds;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQL_DATE_STRUCT);
        return SQL_SUCCESS;
    }
    case SQL_C_TYPE_TIMESTAMP:
    case SQL_C_TIMESTAMP: {
        // Parse YYYY-MM-DD HH:MM:SS
        SQL_TIMESTAMP_STRUCT ts = {};
        if (val.size() >= 10) {
            ts.year = (SQLSMALLINT)atoi(val.substr(0, 4).c_str());
            ts.month = (SQLUSMALLINT)atoi(val.substr(5, 2).c_str());
            ts.day = (SQLUSMALLINT)atoi(val.substr(8, 2).c_str());
        }
        if (val.size() >= 19) {
            ts.hour = (SQLUSMALLINT)atoi(val.substr(11, 2).c_str());
            ts.minute = (SQLUSMALLINT)atoi(val.substr(14, 2).c_str());
            ts.second = (SQLUSMALLINT)atoi(val.substr(17, 2).c_str());
        }
        // Parse fractional seconds if present
        if (val.size() > 20 && val[19] == '.') {
            std::string frac = val.substr(20);
            while (frac.size() < 9) frac += "0";
            ts.fraction = (SQLUINTEGER)atol(frac.substr(0, 9).c_str());
        }
        if (target) *(SQL_TIMESTAMP_STRUCT *)target = ts;
        if (str_len_or_ind) *str_len_or_ind = sizeof(SQL_TIMESTAMP_STRUCT);
        return SQL_SUCCESS;
    }
    default:
        // Fall back to string
        if (target && buffer_length > 0) {
            SQLLEN len = static_cast<SQLLEN>(val.size());
            SQLLEN copy_len = (len < buffer_length - 1) ? len : (buffer_length - 1);
            memcpy(target, val.c_str(), copy_len);
            ((char *)target)[copy_len] = '\0';
            if (str_len_or_ind) *str_len_or_ind = len;
        }
        return SQL_SUCCESS;
    }
}

// ============================================================================
// SQLFetch
// ============================================================================
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle) {
    TRACE_ENTRY("SQLFetch", "");
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();

    // Reset piecemeal SQLGetData state on new fetch
    stmt->getdata_col = 0;
    stmt->getdata_offset = 0;

    if (!stmt->result_set.HasData()) {
        stmt->SetError("24000", "Invalid cursor state: no result set available");
        TRACE_EXIT("SQLFetch", SQL_ERROR);
        return SQL_ERROR;
    }

    SQLULEN rowset = stmt->rowset_size;
    if (rowset < 1) rowset = 1;
    TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLFetch",
              "rowset_size=" + std::to_string(rowset) +
              " current_row=" + std::to_string(stmt->result_set.current_row) +
              " total_rows=" + std::to_string(stmt->result_set.RowCount()) +
              " bound_cols=" + std::to_string(stmt->bound_columns.size()));

    // Record the start of this rowset (the next row to be fetched)
    stmt->rowset_start_row = stmt->result_set.current_row + 1;

    SQLULEN rows_fetched = 0;
    SQLRETURN overall_ret = SQL_SUCCESS;
    bool any_success = false;

    for (SQLULEN r = 0; r < rowset; ++r) {
        // Fast-path: SQL_RD_OFF + lazy mode → advance cursor without page fetching
        // Access uses SQL_RD_OFF to quickly position through the entire result set
        // (e.g., background idle-time loading, Ctrl+End navigation).
        // In lazy mode, skipping page fetches avoids HTTP round-trips entirely.
        if (stmt->retrieve_data == SQL_RD_OFF && stmt->result_set.lazy) {
            SQLLEN next = stmt->result_set.current_row + 1;
            if (next >= static_cast<SQLLEN>(stmt->result_set.RowCount())) {
                if (stmt->row_status_ptr) {
                    for (SQLULEN k = r; k < rowset; ++k)
                        stmt->row_status_ptr[k] = SQL_ROW_NOROW;
                }
                break;
            }
            stmt->result_set.current_row = next;
            rows_fetched++;
            any_success = true;
            if (stmt->row_status_ptr)
                stmt->row_status_ptr[r] = SQL_ROW_SUCCESS;
            continue;
        }

        if (!stmt->result_set.Fetch()) {
            // No more rows
            if (stmt->row_status_ptr && r < rowset) {
                for (SQLULEN k = r; k < rowset; ++k) {
                    stmt->row_status_ptr[k] = SQL_ROW_NOROW;
                }
            }
            break;
        }

        rows_fetched++;
        any_success = true;

        // SQL_ATTR_RETRIEVE_DATA: when SQL_RD_OFF, only position cursor without copying data
        // Access uses this for background positioning during idle-time loading
        if (stmt->retrieve_data == SQL_RD_OFF) {
            if (stmt->row_status_ptr) {
                stmt->row_status_ptr[r] = SQL_ROW_SUCCESS;
            }
            continue;
        }

        SQLLEN row_idx = stmt->result_set.current_row;
        const auto &row = stmt->result_set.GetRow(static_cast<size_t>(row_idx));
        SQLRETURN row_ret = SQL_SUCCESS;

        // Trace: log each row's data
        if (clickhouse_odbc::TraceLog::Instance().IsEnabled() &&
            clickhouse_odbc::TraceLog::Instance().GetLevel() >= clickhouse_odbc::TraceLevel::Debug) {
            std::string row_data = "row[" + std::to_string(row_idx) + "] cols=" + std::to_string(row.size());
            // Display first 3 column values (for debugging)
            for (size_t c = 0; c < row.size() && c < 3; ++c) {
                row_data += " c" + std::to_string(c) + "=";
                if (row[c].has_value()) {
                    const auto &v = row[c].value();
                    row_data += (v.size() > 40 ? v.substr(0, 40) + "..." : v);
                } else {
                    row_data += "NULL";
                }
            }
            TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLFetch", row_data);
        }

        for (auto &[col_num, bc] : stmt->bound_columns) {
            if (col_num < 1 || col_num > row.size()) continue;

            SQLSMALLINT target_type = bc.target_type;
            if (target_type == SQL_C_DEFAULT && col_num <= stmt->result_set.columns.size()) {
                target_type = GetDefaultCType(stmt->result_set.columns[col_num - 1].sql_type);
            }

            // Compute target pointer and indicator for row r
            SQLPOINTER target = bc.target_value;
            SQLLEN *indicator = bc.str_len_or_ind;

            if (r > 0 && target) {
                if (stmt->row_bind_type == SQL_BIND_BY_COLUMN) {
                    // Column-wise binding: advance by buffer_length per row
                    target = static_cast<char *>(bc.target_value) + r * bc.buffer_length;
                    if (indicator) indicator = reinterpret_cast<SQLLEN *>(
                        reinterpret_cast<char *>(bc.str_len_or_ind) + r * sizeof(SQLLEN));
                } else {
                    // Row-wise binding: advance by row_bind_type (struct size) per row
                    target = static_cast<char *>(bc.target_value) + r * stmt->row_bind_type;
                    if (indicator) indicator = reinterpret_cast<SQLLEN *>(
                        reinterpret_cast<char *>(bc.str_len_or_ind) + r * stmt->row_bind_type);
                }
            }

            SQLRETURN ret = CopyValueToTarget(row[col_num - 1], target_type, target,
                                              bc.buffer_length, indicator, stmt);
            if (ret == SQL_ERROR) {
                row_ret = SQL_ERROR;
            } else if (ret == SQL_SUCCESS_WITH_INFO && row_ret != SQL_ERROR) {
                row_ret = SQL_SUCCESS_WITH_INFO;
            }
        }

        if (stmt->row_status_ptr) {
            stmt->row_status_ptr[r] = (row_ret == SQL_ERROR) ? SQL_ROW_ERROR : SQL_ROW_SUCCESS;
        }
        if (row_ret == SQL_ERROR) {
            overall_ret = SQL_ERROR;
        } else if (row_ret == SQL_SUCCESS_WITH_INFO && overall_ret != SQL_ERROR) {
            overall_ret = SQL_SUCCESS_WITH_INFO;
        }
    }

    if (stmt->rows_fetched_ptr) {
        *stmt->rows_fetched_ptr = rows_fetched;
    }
    stmt->last_rowset_count = rows_fetched;

    if (!any_success) {
        TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLFetch",
                  "no rows fetched, returning SQL_NO_DATA");
        TRACE_EXIT("SQLFetch", SQL_NO_DATA);
        return SQL_NO_DATA;
    }

    TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLFetch",
              "fetched=" + std::to_string(rows_fetched) +
              " current_row=" + std::to_string(stmt->result_set.current_row) +
              " ret=" + clickhouse_odbc::TraceLog::SqlReturnToString(overall_ret));
    TRACE_EXIT("SQLFetch", overall_ret);
    return overall_ret;
}

// ============================================================================
// SQLFetchScroll
// ============================================================================
extern "C" SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT StatementHandle, SQLSMALLINT FetchOrientation,
                                             SQLLEN FetchOffset) {
    TRACE_ENTRY("SQLFetchScroll",
                "orientation=" + std::to_string(FetchOrientation) +
                " offset=" + std::to_string(FetchOffset));
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLFetchScroll",
              "current_row=" + std::to_string(stmt->result_set.current_row) +
              " total_rows=" + std::to_string(stmt->result_set.RowCount()));

    SQLLEN row_count = static_cast<SQLLEN>(stmt->result_set.RowCount());

    switch (FetchOrientation) {
    case SQL_FETCH_NEXT:
        return SQLFetch(StatementHandle);
    case SQL_FETCH_FIRST:
        stmt->result_set.current_row = -1;
        return SQLFetch(StatementHandle);
    case SQL_FETCH_LAST:
        if (row_count == 0) return SQL_NO_DATA;
        stmt->result_set.current_row = row_count - 2;
        return SQLFetch(StatementHandle);
    case SQL_FETCH_ABSOLUTE: {
        // ODBC spec: FetchOffset > 0 → row FetchOffset (1-based)
        //            FetchOffset < 0 → row (RowCount + FetchOffset + 1) from end
        //            FetchOffset = 0 → before start (SQL_NO_DATA)
        if (FetchOffset == 0) return SQL_NO_DATA;
        SQLLEN target_row;
        if (FetchOffset > 0) {
            target_row = FetchOffset - 1; // 0-based
        } else {
            // Negative: count from end. -1 = last row, -2 = second-to-last, etc.
            target_row = row_count + FetchOffset; // 0-based
        }
        if (target_row < 0 || target_row >= row_count) return SQL_NO_DATA;
        stmt->result_set.current_row = target_row - 1; // SQLFetch will increment
        return SQLFetch(StatementHandle);
    }
    case SQL_FETCH_RELATIVE: {
        // ODBC spec: move FetchOffset rows from current position
        // current_row is the last-fetched row (0-based); next fetch reads current_row+1
        SQLLEN current_pos = stmt->result_set.current_row; // last fetched (0-based), or -1 if before start
        SQLLEN target_row = current_pos + FetchOffset;     // 0-based target
        if (target_row < 0 || target_row >= row_count) return SQL_NO_DATA;
        stmt->result_set.current_row = target_row - 1; // SQLFetch will increment
        return SQLFetch(StatementHandle);
    }
    default:
        stmt->SetError("HY106", "Fetch type out of range");
        return SQL_ERROR;
    }
}

// ============================================================================
// SQLExtendedFetch (ODBC 2.x backward compatibility)
// ============================================================================
extern "C" SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT StatementHandle, SQLUSMALLINT FetchOrientation,
                                                SQLLEN FetchOffset, SQLULEN *RowCountPtr,
                                                SQLUSMALLINT *RowStatusArray) {
    TRACE_ENTRY("SQLExtendedFetch",
                "orientation=" + std::to_string(FetchOrientation) +
                " offset=" + std::to_string(FetchOffset));
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLExtendedFetch",
              "current_row=" + std::to_string(stmt->result_set.current_row) +
              " total_rows=" + std::to_string(stmt->result_set.RowCount()) +
              " rowset_size=" + std::to_string(stmt->rowset_size));

    // Temporarily set rows_fetched_ptr and row_status_ptr for SQLFetch multi-row support
    SQLULEN *saved_rows_fetched = stmt->rows_fetched_ptr;
    SQLUSMALLINT *saved_row_status = stmt->row_status_ptr;

    SQLULEN fetched_count = 0;
    stmt->rows_fetched_ptr = &fetched_count;
    if (RowStatusArray) {
        stmt->row_status_ptr = RowStatusArray;
    }

    // Position cursor and fetch
    SQLRETURN ret;
    SQLLEN row_count = static_cast<SQLLEN>(stmt->result_set.RowCount());
    switch (FetchOrientation) {
    case SQL_FETCH_NEXT:
        ret = SQLFetch(StatementHandle);
        break;
    case SQL_FETCH_FIRST:
        stmt->result_set.current_row = -1;
        ret = SQLFetch(StatementHandle);
        break;
    case SQL_FETCH_LAST:
        if (row_count == 0) {
            ret = SQL_NO_DATA;
        } else {
            stmt->result_set.current_row = row_count - 2;
            ret = SQLFetch(StatementHandle);
        }
        break;
    case SQL_FETCH_ABSOLUTE: {
        if (FetchOffset == 0) {
            ret = SQL_NO_DATA;
        } else {
            SQLLEN target_row;
            if (FetchOffset > 0) {
                target_row = FetchOffset - 1;
            } else {
                target_row = row_count + FetchOffset;
            }
            if (target_row < 0 || target_row >= row_count) {
                ret = SQL_NO_DATA;
            } else {
                stmt->result_set.current_row = target_row - 1;
                ret = SQLFetch(StatementHandle);
            }
        }
        break;
    }
    case SQL_FETCH_RELATIVE: {
        SQLLEN current_pos = stmt->result_set.current_row;
        SQLLEN target_row = current_pos + FetchOffset;
        if (target_row < 0 || target_row >= row_count) {
            ret = SQL_NO_DATA;
        } else {
            stmt->result_set.current_row = target_row - 1;
            ret = SQLFetch(StatementHandle);
        }
        break;
    }
    default:
        stmt->SetError("HY106", "Fetch type out of range");
        ret = SQL_ERROR;
        break;
    }

    // Restore saved pointers
    stmt->rows_fetched_ptr = saved_rows_fetched;
    stmt->row_status_ptr = saved_row_status;

    // Set output parameters
    if (RowCountPtr) {
        *RowCountPtr = fetched_count;
    }

    TRACE_LOG(clickhouse_odbc::TraceLevel::Debug, "SQLExtendedFetch",
              "fetched=" + std::to_string(fetched_count) +
              " ret=" + clickhouse_odbc::TraceLog::SqlReturnToString(ret));
    TRACE_EXIT("SQLExtendedFetch", ret);
    return ret;
}

// ============================================================================
// SQLGetData (with piecemeal retrieval support)
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT StatementHandle, SQLUSMALLINT Col_or_Param_Num,
                                         SQLSMALLINT TargetType, SQLPOINTER TargetValue,
                                         SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {
    TRACE_ENTRY("SQLGetData", "Col=" + std::to_string(Col_or_Param_Num) +
                " TargetType=" + std::to_string(TargetType) +
                " BufLen=" + std::to_string(BufferLength));
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    SQLLEN row_idx = stmt->result_set.current_row;
    TRACE_LOG(clickhouse_odbc::TraceLevel::Verbose, "SQLGetData",
              "row_idx=" + std::to_string(row_idx) +
              " total_rows=" + std::to_string(stmt->result_set.RowCount()) +
              " getdata_offset=" + std::to_string(stmt->getdata_offset));
    if (row_idx < 0 || row_idx >= static_cast<SQLLEN>(stmt->result_set.RowCount())) {
        stmt->SetError("24000", "Invalid cursor state");
        return SQL_ERROR;
    }

    const auto &getdata_row_check = stmt->result_set.GetRow(static_cast<size_t>(row_idx));
    if (Col_or_Param_Num < 1 || Col_or_Param_Num > getdata_row_check.size()) {
        stmt->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    // Reset offset when switching to a different column
    if (stmt->getdata_col != Col_or_Param_Num) {
        stmt->getdata_col = Col_or_Param_Num;
        stmt->getdata_offset = 0;
    } else if (stmt->getdata_offset == -1) {
        // Column was fully read in previous call(s)
        return SQL_NO_DATA;
    }

    SQLSMALLINT effective_type = TargetType;
    if (effective_type == SQL_C_DEFAULT && Col_or_Param_Num <= stmt->result_set.columns.size()) {
        effective_type = GetDefaultCType(stmt->result_set.columns[Col_or_Param_Num - 1].sql_type);
    }

    const auto &value = stmt->result_set.GetRow(static_cast<size_t>(row_idx))[Col_or_Param_Num - 1];

    // Piecemeal retrieval for string/binary types
    if (value.has_value() && stmt->getdata_offset > 0 &&
        (effective_type == SQL_C_CHAR || effective_type == SQL_C_DEFAULT ||
         effective_type == SQL_C_WCHAR || effective_type == SQL_C_BINARY)) {

        const std::string &val = value.value();

        if (effective_type == SQL_C_WCHAR) {
            std::wstring wide = Utf8ToWide(val);
            SQLLEN char_offset = stmt->getdata_offset / (SQLLEN)sizeof(SQLWCHAR);
            SQLLEN remaining_chars = static_cast<SQLLEN>(wide.size()) - char_offset;
            if (remaining_chars <= 0) {
                stmt->getdata_offset = -1;
                return SQL_NO_DATA;
            }
            if (StrLen_or_IndPtr) *StrLen_or_IndPtr = remaining_chars * sizeof(SQLWCHAR);
            if (TargetValue && BufferLength > (SQLLEN)sizeof(SQLWCHAR)) {
                SQLLEN max_chars = (BufferLength / (SQLLEN)sizeof(SQLWCHAR)) - 1;
                SQLLEN copy_chars = (remaining_chars < max_chars) ? remaining_chars : max_chars;
                memcpy(TargetValue, wide.c_str() + char_offset, copy_chars * sizeof(SQLWCHAR));
                ((SQLWCHAR *)TargetValue)[copy_chars] = L'\0';
                stmt->getdata_offset += copy_chars * sizeof(SQLWCHAR);
                if (remaining_chars > max_chars) {
                    stmt->AddDiagRecord("01004", 0, "String data, right truncated");
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
            stmt->getdata_offset = -1;  // Mark column as fully read
            return SQL_SUCCESS;
        }

        if (effective_type == SQL_C_BINARY) {
            SQLLEN remaining = static_cast<SQLLEN>(val.size()) - stmt->getdata_offset;
            if (remaining <= 0) {
                stmt->getdata_offset = -1;
                return SQL_NO_DATA;
            }
            if (StrLen_or_IndPtr) *StrLen_or_IndPtr = remaining;
            if (TargetValue && BufferLength > 0) {
                SQLLEN copy_len = (remaining < BufferLength) ? remaining : BufferLength;
                memcpy(TargetValue, val.data() + stmt->getdata_offset, copy_len);
                stmt->getdata_offset += copy_len;
                if (remaining > BufferLength) {
                    stmt->AddDiagRecord("01004", 0, "String data, right truncated");
                    return SQL_SUCCESS_WITH_INFO;
                }
            }
            stmt->getdata_offset = -1;  // Mark column as fully read
            return SQL_SUCCESS;
        }

        // SQL_C_CHAR / SQL_C_DEFAULT
        SQLLEN remaining = static_cast<SQLLEN>(val.size()) - stmt->getdata_offset;
        if (remaining <= 0) {
            stmt->getdata_offset = -1;
            return SQL_NO_DATA;
        }
        if (StrLen_or_IndPtr) *StrLen_or_IndPtr = remaining;
        if (TargetValue && BufferLength > 1) {
            SQLLEN copy_len = (remaining < BufferLength - 1) ? remaining : (BufferLength - 1);
            memcpy(TargetValue, val.c_str() + stmt->getdata_offset, copy_len);
            ((char *)TargetValue)[copy_len] = '\0';
            stmt->getdata_offset += copy_len;
            if (remaining > BufferLength - 1) {
                stmt->AddDiagRecord("01004", 0, "String data, right truncated");
                return SQL_SUCCESS_WITH_INFO;
            }
        }
        stmt->getdata_offset = -1;  // Mark column as fully read
        return SQL_SUCCESS;
    }

    // First call (or non-string types): use CopyValueToTarget
    SQLRETURN ret = CopyValueToTarget(value, effective_type, TargetValue, BufferLength, StrLen_or_IndPtr, stmt);

    // Track offset for piecemeal retrieval on truncation (string/binary only)
    if (ret == SQL_SUCCESS_WITH_INFO && value.has_value() &&
        (effective_type == SQL_C_CHAR || effective_type == SQL_C_DEFAULT ||
         effective_type == SQL_C_WCHAR || effective_type == SQL_C_BINARY)) {
        if (effective_type == SQL_C_WCHAR) {
            SQLLEN max_chars = (BufferLength / (SQLLEN)sizeof(SQLWCHAR)) - 1;
            if (max_chars > 0) stmt->getdata_offset = max_chars * sizeof(SQLWCHAR);
        } else if (effective_type == SQL_C_BINARY) {
            stmt->getdata_offset = BufferLength;
        } else {
            stmt->getdata_offset = (BufferLength > 1) ? BufferLength - 1 : 0;
        }
    }
    // Note: Do NOT set getdata_offset = -1 on SQL_SUCCESS.
    // Access (and other apps) may call SQLGetData multiple times for the same
    // column (e.g., to re-verify row data). Keeping getdata_offset at 0 allows
    // repeated reads. The piecemeal completion paths (above) already set -1
    // when all chunks of a truncated string/binary value have been delivered.

    return ret;
}

// ============================================================================
// SQLCloseCursor
// ============================================================================
extern "C" SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT StatementHandle) {
    if (!IsValidStmtHandle(StatementHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    // ODBC spec: return 24000 if no cursor is open (no result set)
    if (!stmt->result_set.HasData()) {
        stmt->SetError("24000", "Invalid cursor state: no cursor is open");
        return SQL_ERROR;
    }

    stmt->result_set.CloseCursor();
    return SQL_SUCCESS;
}

// ============================================================================
// SQLMoreResults
// ============================================================================
extern "C" SQLRETURN SQL_API SQLMoreResults(SQLHSTMT StatementHandle) {
    // ClickHouse doesn't support multiple result sets
    return SQL_NO_DATA;
}

// ============================================================================
// SQLGetFunctions
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC ConnectionHandle, SQLUSMALLINT FunctionId,
                                               SQLUSMALLINT *Supported) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);

    if (!Supported) return SQL_ERROR;

    // ODBC 2.x: SQL_API_ALL_FUNCTIONS — fill 100-element SQLUSMALLINT array
    if (FunctionId == SQL_API_ALL_FUNCTIONS) {
        memset(Supported, 0, 100 * sizeof(SQLUSMALLINT));
        // Core ODBC 2.x/1.x functions (IDs < 100)
        Supported[SQL_API_SQLALLOCCONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLALLOCENV] = SQL_TRUE;
        Supported[SQL_API_SQLALLOCSTMT] = SQL_TRUE;
        Supported[SQL_API_SQLBINDCOL] = SQL_TRUE;
        Supported[SQL_API_SQLCANCEL] = SQL_TRUE;
        Supported[SQL_API_SQLCOLATTRIBUTES] = SQL_TRUE;
        Supported[SQL_API_SQLCONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLDESCRIBECOL] = SQL_TRUE;
        Supported[SQL_API_SQLDISCONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLERROR] = SQL_TRUE;
        Supported[SQL_API_SQLEXECDIRECT] = SQL_TRUE;
        Supported[SQL_API_SQLEXECUTE] = SQL_TRUE;
        Supported[SQL_API_SQLFETCH] = SQL_TRUE;
        Supported[SQL_API_SQLFREECONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLFREEENV] = SQL_TRUE;
        Supported[SQL_API_SQLFREESTMT] = SQL_TRUE;
        Supported[SQL_API_SQLGETCONNECTOPTION] = SQL_TRUE;
        Supported[SQL_API_SQLGETCURSORNAME] = SQL_TRUE;
        Supported[SQL_API_SQLGETDATA] = SQL_TRUE;
        Supported[SQL_API_SQLGETFUNCTIONS] = SQL_TRUE;
        Supported[SQL_API_SQLGETINFO] = SQL_TRUE;
        Supported[SQL_API_SQLGETSTMTOPTION] = SQL_TRUE;
        Supported[SQL_API_SQLGETTYPEINFO] = SQL_TRUE;
        Supported[SQL_API_SQLNUMRESULTCOLS] = SQL_TRUE;
        Supported[SQL_API_SQLPARAMDATA] = SQL_TRUE;
        Supported[SQL_API_SQLPREPARE] = SQL_TRUE;
        Supported[SQL_API_SQLPUTDATA] = SQL_TRUE;
        Supported[SQL_API_SQLROWCOUNT] = SQL_TRUE;
        Supported[SQL_API_SQLSETCONNECTOPTION] = SQL_TRUE;
        Supported[SQL_API_SQLSETCURSORNAME] = SQL_TRUE;
        Supported[SQL_API_SQLSETSTMTOPTION] = SQL_TRUE;
        Supported[SQL_API_SQLSPECIALCOLUMNS] = SQL_TRUE;
        Supported[SQL_API_SQLSTATISTICS] = SQL_TRUE;
        Supported[SQL_API_SQLTABLES] = SQL_TRUE;
        Supported[SQL_API_SQLTRANSACT] = SQL_TRUE;
        Supported[SQL_API_SQLCOLUMNS] = SQL_TRUE;
        Supported[SQL_API_SQLDRIVERCONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLBINDPARAMETER] = SQL_TRUE;
        Supported[SQL_API_SQLNUMPARAMS] = SQL_TRUE;
        Supported[SQL_API_SQLBROWSECONNECT] = SQL_TRUE;
        Supported[SQL_API_SQLEXTENDEDFETCH] = SQL_TRUE;
        Supported[SQL_API_SQLFOREIGNKEYS] = SQL_TRUE;
        Supported[SQL_API_SQLMORERESULTS] = SQL_TRUE;
        Supported[SQL_API_SQLNATIVESQL] = SQL_TRUE;
        Supported[SQL_API_SQLPRIMARYKEYS] = SQL_TRUE;
        Supported[SQL_API_SQLPROCEDURECOLUMNS] = SQL_TRUE;
        Supported[SQL_API_SQLPROCEDURES] = SQL_TRUE;
        Supported[SQL_API_SQLSETPOS] = SQL_TRUE;
        Supported[SQL_API_SQLSETSCROLLOPTIONS] = SQL_TRUE;
        Supported[SQL_API_SQLTABLEPRIVILEGES] = SQL_TRUE;
        Supported[SQL_API_SQLCOLUMNPRIVILEGES] = SQL_TRUE;
        Supported[SQL_API_SQLDESCRIBEPARAM] = SQL_TRUE;
        return SQL_SUCCESS;
    }

    if (FunctionId == SQL_API_ODBC3_ALL_FUNCTIONS) {
        // Bitmap of all supported functions (4000 bits)
        memset(Supported, 0, SQL_API_ODBC3_ALL_FUNCTIONS_SIZE * sizeof(SQLUSMALLINT));

        auto setFunc = [&](SQLUSMALLINT id) {
            Supported[id >> 4] |= (1 << (id & 0x000F));
        };

        setFunc(SQL_API_SQLALLOCHANDLE);
        setFunc(SQL_API_SQLFREEHANDLE);
        setFunc(SQL_API_SQLCONNECT);
        setFunc(SQL_API_SQLDISCONNECT);
        setFunc(SQL_API_SQLDRIVERCONNECT);
        setFunc(SQL_API_SQLEXECDIRECT);
        setFunc(SQL_API_SQLPREPARE);
        setFunc(SQL_API_SQLEXECUTE);
        setFunc(SQL_API_SQLFETCH);
        setFunc(SQL_API_SQLFETCHSCROLL);
        setFunc(SQL_API_SQLGETDATA);
        setFunc(SQL_API_SQLNUMRESULTCOLS);
        setFunc(SQL_API_SQLDESCRIBECOL);
        setFunc(SQL_API_SQLCOLATTRIBUTE);
        setFunc(SQL_API_SQLBINDCOL);
        setFunc(SQL_API_SQLBINDPARAMETER);
        setFunc(SQL_API_SQLROWCOUNT);
        setFunc(SQL_API_SQLGETINFO);
        setFunc(SQL_API_SQLGETCONNECTATTR);
        setFunc(SQL_API_SQLSETCONNECTATTR);
        setFunc(SQL_API_SQLGETENVATTR);
        setFunc(SQL_API_SQLSETENVATTR);
        setFunc(SQL_API_SQLGETSTMTATTR);
        setFunc(SQL_API_SQLSETSTMTATTR);
        setFunc(SQL_API_SQLGETDIAGREC);
        setFunc(SQL_API_SQLGETDIAGFIELD);
        setFunc(SQL_API_SQLTABLES);
        setFunc(SQL_API_SQLCOLUMNS);
        setFunc(SQL_API_SQLGETTYPEINFO);
        setFunc(SQL_API_SQLPRIMARYKEYS);
        setFunc(SQL_API_SQLSTATISTICS);
        setFunc(SQL_API_SQLSPECIALCOLUMNS);
        setFunc(SQL_API_SQLFOREIGNKEYS);
        setFunc(SQL_API_SQLPROCEDURES);
        setFunc(SQL_API_SQLPROCEDURECOLUMNS);
        setFunc(SQL_API_SQLENDTRAN);
        setFunc(SQL_API_SQLNATIVESQL);
        setFunc(SQL_API_SQLCLOSECURSOR);
        setFunc(SQL_API_SQLMORERESULTS);
        setFunc(SQL_API_SQLGETFUNCTIONS);
        setFunc(SQL_API_SQLCANCEL);
        setFunc(SQL_API_SQLNUMPARAMS);
        setFunc(SQL_API_SQLDESCRIBEPARAM);
        setFunc(SQL_API_SQLFREESTMT);
        setFunc(SQL_API_SQLSETPOS);
        setFunc(SQL_API_SQLGETCURSORNAME);
        setFunc(SQL_API_SQLSETCURSORNAME);
        setFunc(SQL_API_SQLEXTENDEDFETCH);
        setFunc(SQL_API_SQLCOLATTRIBUTES);
        setFunc(SQL_API_SQLSETSCROLLOPTIONS);
        setFunc(SQL_API_SQLCOLUMNPRIVILEGES);
        setFunc(SQL_API_SQLTABLEPRIVILEGES);
        setFunc(SQL_API_SQLSTATISTICS);
        setFunc(SQL_API_SQLSPECIALCOLUMNS);
        setFunc(SQL_API_SQLFOREIGNKEYS);
        setFunc(SQL_API_SQLPROCEDURES);
        setFunc(SQL_API_SQLPROCEDURECOLUMNS);
        setFunc(SQL_API_SQLBULKOPERATIONS);
        setFunc(SQL_API_SQLNUMPARAMS);
        setFunc(SQL_API_SQLPARAMDATA);
        setFunc(SQL_API_SQLPUTDATA);
        setFunc(SQL_API_SQLERROR);
        setFunc(SQL_API_SQLGETCONNECTOPTION);
        setFunc(SQL_API_SQLSETCONNECTOPTION);
        setFunc(SQL_API_SQLGETSTMTOPTION);
        setFunc(SQL_API_SQLSETSTMTOPTION);
        setFunc(SQL_API_SQLTRANSACT);
        setFunc(SQL_API_SQLGETDESCFIELD);
        setFunc(SQL_API_SQLSETDESCFIELD);
        setFunc(SQL_API_SQLGETDESCREC);
        setFunc(SQL_API_SQLSETDESCREC);
        setFunc(SQL_API_SQLCOPYDESC);
        setFunc(SQL_API_SQLBROWSECONNECT);

        return SQL_SUCCESS;
    }

    // Individual function check
    switch (FunctionId) {
    case SQL_API_SQLALLOCHANDLE:
    case SQL_API_SQLFREEHANDLE:
    case SQL_API_SQLCONNECT:
    case SQL_API_SQLDISCONNECT:
    case SQL_API_SQLDRIVERCONNECT:
    case SQL_API_SQLEXECDIRECT:
    case SQL_API_SQLPREPARE:
    case SQL_API_SQLEXECUTE:
    case SQL_API_SQLFETCH:
    case SQL_API_SQLFETCHSCROLL:
    case SQL_API_SQLGETDATA:
    case SQL_API_SQLNUMRESULTCOLS:
    case SQL_API_SQLDESCRIBECOL:
    case SQL_API_SQLCOLATTRIBUTE:
    case SQL_API_SQLBINDCOL:
    case SQL_API_SQLBINDPARAMETER:
    case SQL_API_SQLROWCOUNT:
    case SQL_API_SQLGETINFO:
    case SQL_API_SQLGETCONNECTATTR:
    case SQL_API_SQLSETCONNECTATTR:
    case SQL_API_SQLGETENVATTR:
    case SQL_API_SQLSETENVATTR:
    case SQL_API_SQLGETSTMTATTR:
    case SQL_API_SQLSETSTMTATTR:
    case SQL_API_SQLGETDIAGREC:
    case SQL_API_SQLGETDIAGFIELD:
    case SQL_API_SQLNATIVESQL:
    case SQL_API_SQLTABLES:
    case SQL_API_SQLCOLUMNS:
    case SQL_API_SQLGETTYPEINFO:
    case SQL_API_SQLPRIMARYKEYS:
    case SQL_API_SQLENDTRAN:
    case SQL_API_SQLCLOSECURSOR:
    case SQL_API_SQLMORERESULTS:
    case SQL_API_SQLGETFUNCTIONS:
    case SQL_API_SQLCANCEL:
    case SQL_API_SQLDESCRIBEPARAM:
    case SQL_API_SQLFREESTMT:
    case SQL_API_SQLSETPOS:
    case SQL_API_SQLGETCURSORNAME:
    case SQL_API_SQLSETCURSORNAME:
    case SQL_API_SQLEXTENDEDFETCH:
    // SQL_API_SQLCOLATTRIBUTES == SQL_API_SQLCOLATTRIBUTE (both value 6), already listed above
    case SQL_API_SQLSETSCROLLOPTIONS:
    case SQL_API_SQLCOLUMNPRIVILEGES:
    case SQL_API_SQLTABLEPRIVILEGES:
    case SQL_API_SQLSTATISTICS:
    case SQL_API_SQLSPECIALCOLUMNS:
    case SQL_API_SQLFOREIGNKEYS:
    case SQL_API_SQLPROCEDURES:
    case SQL_API_SQLPROCEDURECOLUMNS:
    case SQL_API_SQLBULKOPERATIONS:
    case SQL_API_SQLNUMPARAMS:
    case SQL_API_SQLPARAMDATA:
    case SQL_API_SQLPUTDATA:
    case SQL_API_SQLERROR:
    case SQL_API_SQLGETCONNECTOPTION:
    case SQL_API_SQLSETCONNECTOPTION:
    case SQL_API_SQLGETSTMTOPTION:
    case SQL_API_SQLSETSTMTOPTION:
    case SQL_API_SQLTRANSACT:
    case SQL_API_SQLGETDESCFIELD:
    case SQL_API_SQLSETDESCFIELD:
    case SQL_API_SQLGETDESCREC:
    case SQL_API_SQLSETDESCREC:
    case SQL_API_SQLCOPYDESC:
    case SQL_API_SQLBROWSECONNECT:
    case SQL_API_SQLALLOCENV:
    case SQL_API_SQLFREEENV:
    case SQL_API_SQLALLOCCONNECT:
    case SQL_API_SQLFREECONNECT:
    case SQL_API_SQLALLOCSTMT:
        *Supported = SQL_TRUE;
        break;
    default:
        *Supported = SQL_FALSE;
        break;
    }

    return SQL_SUCCESS;
}
