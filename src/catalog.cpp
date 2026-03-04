#include "include/handle.h"
#include "include/clickhouse_client.h"
#include "include/util.h"
#include "include/type_mapping.h"
#include "include/trace.h"

#include <sql.h>
#include <sqlext.h>
#include <cstring>
#include <algorithm>

// Undefine ODBC UNICODE macros to allow defining both ANSI and Wide versions
#ifdef UNICODE
#undef SQLTables
#undef SQLColumns
#undef SQLPrimaryKeys
#undef SQLStatistics
#undef SQLSpecialColumns
#undef SQLForeignKeys
#undef SQLProcedures
#undef SQLProcedureColumns
#undef SQLGetTypeInfo
#undef SQLColumnPrivileges
#undef SQLTablePrivileges
#endif

namespace clickhouse_odbc {
extern ClickHouseClient *GetClient(OdbcConnection *conn);
}

using namespace clickhouse_odbc;

// ODBC search pattern escape handling.
// ODBC catalog functions treat '_' and '%' as wildcards in pattern arguments.
// The escape character (SQL_SEARCH_PATTERN_ESCAPE = '\') precedes '_' or '%'
// to treat them as literals. e.g. Access sends "M\_ITEM" meaning literal "M_ITEM".

// Check if pattern has unescaped (actual) wildcards
static bool HasUnescapedWildcards(const std::string &pattern) {
    for (size_t i = 0; i < pattern.size(); i++) {
        if (pattern[i] == '\\' && i + 1 < pattern.size()) {
            i++; // skip escaped character
            continue;
        }
        if (pattern[i] == '%' || pattern[i] == '_') return true;
    }
    return false;
}

// Strip ODBC search pattern escape characters to get the literal value.
// "M\_ITEM" -> "M_ITEM", "test\%" -> "test%", "abc" -> "abc"
static std::string StripSearchPatternEscape(const std::string &pattern) {
    std::string result;
    result.reserve(pattern.size());
    for (size_t i = 0; i < pattern.size(); i++) {
        if (pattern[i] == '\\' && i + 1 < pattern.size() &&
            (pattern[i + 1] == '_' || pattern[i + 1] == '%')) {
            result += pattern[i + 1];
            i++; // skip the escaped wildcard
        } else {
            result += pattern[i];
        }
    }
    return result;
}

// Build a SQL WHERE condition for a catalog pattern argument.
// Handles: empty/"%"=match all, unescaped wildcards=LIKE, escaped-only=exact match.
static std::string BuildPatternCondition(const std::string &column, const std::string &pattern) {
    if (pattern.empty() || pattern == "%") {
        return ""; // no filter
    }
    if (HasUnescapedWildcards(pattern)) {
        // Has actual wildcards — use LIKE. ClickHouse LIKE also uses '\' as escape,
        // so the ODBC-escaped pattern can be passed directly.
        return " AND " + column + " LIKE '" + pattern + "'";
    }
    // No wildcards (or only escaped wildcards) — exact match with stripped escapes
    std::string literal = StripSearchPatternEscape(pattern);
    return " AND " + column + " = '" + literal + "'";
}

// Helper: build a result set with specified columns (for catalog functions)
static void InitCatalogColumns(ResultSet &rs, const std::vector<std::pair<std::string, SQLSMALLINT>> &cols) {
    rs.Reset();
    for (const auto &[name, type] : cols) {
        ColumnInfo ci;
        ci.name = name;
        ci.sql_type = type;
        ci.column_size = (type == SQL_VARCHAR || type == SQL_WVARCHAR) ? 256 : GetSqlTypeDisplaySize(type);
        ci.nullable = SQL_NULLABLE;
        rs.columns.push_back(ci);
    }
}

static void AddRow(ResultSet &rs, const std::vector<std::optional<std::string>> &values) {
    rs.rows.push_back(values);
}

// ============================================================================
// SQLTables / SQLTablesW
// ============================================================================
static SQLRETURN TablesImpl(OdbcStatement *stmt, const std::string &catalog, const std::string &schema,
                            const std::string &table_name, const std::string &table_type) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "TablesImpl",
              "catalog='" + catalog + "' schema='" + schema + "' table='" + table_name + "' type='" + table_type + "'");

    auto *client = GetClient(stmt->conn);
    if (!client) {
        stmt->SetError("08003", "Connection not open");
        return SQL_ERROR;
    }

    // Standard SQLTables result columns
    InitCatalogColumns(stmt->result_set, {
        {"TABLE_CAT", SQL_VARCHAR},
        {"TABLE_SCHEM", SQL_VARCHAR},
        {"TABLE_NAME", SQL_VARCHAR},
        {"TABLE_TYPE", SQL_VARCHAR},
        {"REMARKS", SQL_VARCHAR}
    });

    // Special case: enumerate table types
    if (table_type == "%%" || table_type == "SQL_ALL_TABLE_TYPES") {
        AddRow(stmt->result_set, {std::nullopt, std::nullopt, std::nullopt, std::string("TABLE"), std::nullopt});
        AddRow(stmt->result_set, {std::nullopt, std::nullopt, std::nullopt, std::string("VIEW"), std::nullopt});
        AddRow(stmt->result_set, {std::nullopt, std::nullopt, std::nullopt, std::string("SYSTEM TABLE"), std::nullopt});
        return SQL_SUCCESS;
    }

    // Special case: enumerate catalogs
    if (catalog == "%" && schema.empty() && table_name.empty()) {
        ResultSet temp;
        std::string error_msg;
        if (client->ExecuteQuery("SELECT name FROM system.databases ORDER BY name", temp, error_msg)) {
            for (const auto &row : temp.rows) {
                if (!row.empty() && row[0].has_value()) {
                    AddRow(stmt->result_set, {row[0], std::nullopt, std::nullopt, std::nullopt, std::nullopt});
                }
            }
        }
        return SQL_SUCCESS;
    }

    // Query tables from system.tables
    std::string query = "SELECT database, name, engine FROM system.tables WHERE 1=1";

    // When catalog is empty, default to the current database (standard ODBC behavior).
    // This prevents system/information_schema tables from appearing by default.
    std::string db = catalog.empty() ? stmt->conn->database : catalog;
    if (catalog != "%") {
        query += " AND database = '" + db + "'";
    }
    if (!table_name.empty() && table_name != "%") {
        // Handle ODBC search pattern escapes (e.g. Access sends "M\_ITEM" for literal "M_ITEM")
        query += BuildPatternCondition("name", table_name);
    }
    query += " ORDER BY database, name";

    ResultSet temp;
    std::string error_msg;
    if (!client->ExecuteQuery(query, temp, error_msg)) {
        stmt->SetError("HY000", error_msg);
        return SQL_ERROR;
    }

    // Determine which table types are requested
    bool want_tables = table_type.empty() || table_type.find("TABLE") != std::string::npos ||
                       table_type.find("'TABLE'") != std::string::npos || table_type == "%";
    bool want_views = table_type.empty() || table_type.find("VIEW") != std::string::npos ||
                      table_type.find("'VIEW'") != std::string::npos || table_type == "%";
    // SYSTEM TABLE is only returned when explicitly requested (not by default)
    bool want_system = table_type.find("SYSTEM TABLE") != std::string::npos ||
                       table_type.find("'SYSTEM TABLE'") != std::string::npos ||
                       table_type == "%";
    // Avoid false positive: "TABLE" contains "TABLE" but not "SYSTEM TABLE"
    // If want_tables is true due to "TABLE" match, ensure it's not just "SYSTEM TABLE"
    if (want_tables && !want_system) {
        // "SYSTEM TABLE" also matches "TABLE", so check if the only match is "SYSTEM TABLE"
        std::string tt_upper = ToUpper(table_type);
        if (tt_upper.find("SYSTEM TABLE") != std::string::npos) {
            // Check if there's a standalone TABLE (not SYSTEM TABLE)
            std::string cleaned = tt_upper;
            size_t pos;
            while ((pos = cleaned.find("SYSTEM TABLE")) != std::string::npos) {
                cleaned.erase(pos, 12);
            }
            want_tables = cleaned.find("TABLE") != std::string::npos;
        }
    }

    for (const auto &row : temp.rows) {
        if (row.size() < 3) continue;

        std::string row_db = row[0].value_or("");
        std::string engine = row[2].value_or("");
        bool is_system_db = (row_db == "system" || row_db == "information_schema" ||
                             row_db == "INFORMATION_SCHEMA");
        std::string type_str;

        if (engine == "View" || engine == "MaterializedView") {
            if (!want_views) continue;
            type_str = "VIEW";
        } else if (is_system_db) {
            if (!want_system) continue;
            type_str = "SYSTEM TABLE";
        } else {
            if (!want_tables) continue;
            type_str = "TABLE";
        }

        AddRow(stmt->result_set, {
            row[0],                    // TABLE_CAT (database)
            std::nullopt,              // TABLE_SCHEM
            row[1],                    // TABLE_NAME
            std::string(type_str),     // TABLE_TYPE
            std::nullopt               // REMARKS
        });
    }

    TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "TablesImpl",
              "Returning " + std::to_string(stmt->result_set.RowCount()) + " rows");
    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLTables(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                        SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                        SQLCHAR *TableName, SQLSMALLINT NameLength3,
                                        SQLCHAR *TableType, SQLSMALLINT NameLength4) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    auto toString = [](SQLCHAR *s, SQLSMALLINT len) -> std::string {
        if (!s) return "";
        if (len == SQL_NTS) return reinterpret_cast<const char *>(s);
        return std::string(reinterpret_cast<const char *>(s), len);
    };

    return TablesImpl(stmt, toString(CatalogName, NameLength1), toString(SchemaName, NameLength2),
                      toString(TableName, NameLength3), toString(TableType, NameLength4));
}

extern "C" SQLRETURN SQL_API SQLTablesW(SQLHSTMT StatementHandle, SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                         SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                         SQLWCHAR *TableName, SQLSMALLINT NameLength3,
                                         SQLWCHAR *TableType, SQLSMALLINT NameLength4) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    auto toString = [](SQLWCHAR *s, SQLSMALLINT len) -> std::string {
        if (!s) return "";
        return WideToUtf8(s, len);
    };

    return TablesImpl(stmt, toString(CatalogName, NameLength1), toString(SchemaName, NameLength2),
                      toString(TableName, NameLength3), toString(TableType, NameLength4));
}

// ============================================================================
// SQLColumns / SQLColumnsW
// ============================================================================
static SQLRETURN ColumnsImpl(OdbcStatement *stmt, const std::string &catalog, const std::string &schema,
                             const std::string &table_name, const std::string &column_name) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    TRACE_LOG(clickhouse_odbc::TraceLevel::Info, "ColumnsImpl",
              "catalog='" + catalog + "' schema='" + schema + "' table='" + table_name + "' column='" + column_name + "'");

    auto *client = GetClient(stmt->conn);
    if (!client) {
        stmt->SetError("08003", "Connection not open");
        return SQL_ERROR;
    }

    // Standard SQLColumns result columns
    InitCatalogColumns(stmt->result_set, {
        {"TABLE_CAT", SQL_VARCHAR},
        {"TABLE_SCHEM", SQL_VARCHAR},
        {"TABLE_NAME", SQL_VARCHAR},
        {"COLUMN_NAME", SQL_VARCHAR},
        {"DATA_TYPE", SQL_SMALLINT},
        {"TYPE_NAME", SQL_VARCHAR},
        {"COLUMN_SIZE", SQL_INTEGER},
        {"BUFFER_LENGTH", SQL_INTEGER},
        {"DECIMAL_DIGITS", SQL_SMALLINT},
        {"NUM_PREC_RADIX", SQL_SMALLINT},
        {"NULLABLE", SQL_SMALLINT},
        {"REMARKS", SQL_VARCHAR},
        {"COLUMN_DEF", SQL_VARCHAR},
        {"SQL_DATA_TYPE", SQL_SMALLINT},
        {"SQL_DATETIME_SUB", SQL_SMALLINT},
        {"CHAR_OCTET_LENGTH", SQL_INTEGER},
        {"ORDINAL_POSITION", SQL_INTEGER},
        {"IS_NULLABLE", SQL_VARCHAR}
    });

    std::string db = catalog.empty() ? stmt->conn->database : catalog;

    std::string query = "SELECT database, table, name, type, position FROM system.columns WHERE 1=1";

    if (!db.empty() && db != "%") {
        query += " AND database = '" + StripSearchPatternEscape(db) + "'";
    }
    if (!table_name.empty() && table_name != "%") {
        // Handle ODBC search pattern escapes (e.g. Access sends "M\_ITEM" for literal "M_ITEM")
        query += BuildPatternCondition("table", table_name);
    }
    if (!column_name.empty() && column_name != "%") {
        query += BuildPatternCondition("name", column_name);
    }
    query += " ORDER BY database, table, position";

    ResultSet temp;
    std::string error_msg;
    if (!client->ExecuteQuery(query, temp, error_msg)) {
        stmt->SetError("HY000", error_msg);
        return SQL_ERROR;
    }

    for (const auto &row : temp.rows) {
        if (row.size() < 5) continue;

        std::string ch_type = row[3].value_or("String");
        SQLSMALLINT sql_type = ClickHouseTypeToSqlType(ch_type);
        SQLULEN col_size = GetColumnSizeForType(ch_type, sql_type);
        SQLULEN octet_len = GetSqlTypeOctetLength(sql_type);
        std::string type_name = GetSqlTypeName(sql_type);
        SQLSMALLINT dec_digits = GetDecimalDigitsForType(ch_type, sql_type);
        bool nullable = ch_type.find("Nullable") != std::string::npos;

        // NUM_PREC_RADIX: 10 for numeric types, NULL for others
        std::optional<std::string> num_prec_radix;
        switch (sql_type) {
        case SQL_TINYINT: case SQL_SMALLINT: case SQL_INTEGER: case SQL_BIGINT:
        case SQL_REAL: case SQL_FLOAT: case SQL_DOUBLE:
        case SQL_DECIMAL: case SQL_NUMERIC:
            num_prec_radix = "10";
            break;
        default:
            num_prec_radix = std::nullopt;
            break;
        }

        // SQL_DATETIME_SUB for date/time types
        std::optional<std::string> datetime_sub;
        switch (sql_type) {
        case SQL_TYPE_DATE:
            datetime_sub = std::to_string(SQL_CODE_DATE);
            break;
        case SQL_TYPE_TIMESTAMP:
            datetime_sub = std::to_string(SQL_CODE_TIMESTAMP);
            break;
        default:
            datetime_sub = std::nullopt;
            break;
        }

        // CHAR_OCTET_LENGTH: only for character/binary types
        std::optional<std::string> char_octet_len;
        switch (sql_type) {
        case SQL_CHAR: case SQL_VARCHAR: case SQL_LONGVARCHAR:
        case SQL_WCHAR: case SQL_WVARCHAR: case SQL_WLONGVARCHAR:
        case SQL_BINARY: case SQL_VARBINARY:
            char_octet_len = std::to_string(octet_len);
            break;
        default:
            char_octet_len = std::nullopt;
            break;
        }

        // SQL_DATA_TYPE: for datetime types, use SQL_DATETIME (9) instead of concise type
        SQLSMALLINT sql_data_type = sql_type;
        switch (sql_type) {
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
        case SQL_TYPE_TIMESTAMP:
            sql_data_type = SQL_DATETIME;
            break;
        }

        AddRow(stmt->result_set, {
            row[0],                                         // TABLE_CAT
            std::nullopt,                                   // TABLE_SCHEM
            row[1],                                         // TABLE_NAME
            row[2],                                         // COLUMN_NAME
            std::to_string(sql_type),                       // DATA_TYPE
            std::string(type_name),                         // TYPE_NAME
            std::to_string(col_size),                       // COLUMN_SIZE
            std::to_string(octet_len),                      // BUFFER_LENGTH
            std::to_string(dec_digits),                     // DECIMAL_DIGITS
            num_prec_radix,                                 // NUM_PREC_RADIX
            std::string(nullable ? "1" : "0"),              // NULLABLE (SQL_NULLABLE or SQL_NO_NULLS)
            std::nullopt,                                   // REMARKS
            std::nullopt,                                   // COLUMN_DEF
            std::to_string(sql_data_type),                  // SQL_DATA_TYPE
            datetime_sub,                                   // SQL_DATETIME_SUB
            char_octet_len,                                 // CHAR_OCTET_LENGTH
            row[4],                                         // ORDINAL_POSITION
            std::string(nullable ? "YES" : "NO")            // IS_NULLABLE
        });
    }

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLColumns(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                         SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                         SQLCHAR *TableName, SQLSMALLINT NameLength3,
                                         SQLCHAR *ColumnName, SQLSMALLINT NameLength4) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    auto toString = [](SQLCHAR *s, SQLSMALLINT len) -> std::string {
        if (!s) return "";
        if (len == SQL_NTS) return reinterpret_cast<const char *>(s);
        return std::string(reinterpret_cast<const char *>(s), len);
    };

    return ColumnsImpl(stmt, toString(CatalogName, NameLength1), toString(SchemaName, NameLength2),
                       toString(TableName, NameLength3), toString(ColumnName, NameLength4));
}

extern "C" SQLRETURN SQL_API SQLColumnsW(SQLHSTMT StatementHandle, SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                          SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                          SQLWCHAR *TableName, SQLSMALLINT NameLength3,
                                          SQLWCHAR *ColumnName, SQLSMALLINT NameLength4) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    auto toString = [](SQLWCHAR *s, SQLSMALLINT len) -> std::string {
        if (!s) return "";
        return WideToUtf8(s, len);
    };

    return ColumnsImpl(stmt, toString(CatalogName, NameLength1), toString(SchemaName, NameLength2),
                       toString(TableName, NameLength3), toString(ColumnName, NameLength4));
}

// ============================================================================
// SQLPrimaryKeys / SQLPrimaryKeysW
// ============================================================================
static SQLRETURN PrimaryKeysImpl(OdbcStatement *stmt, const std::string &catalog, const std::string &schema,
                                 const std::string &table_name) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    InitCatalogColumns(stmt->result_set, {
        {"TABLE_CAT", SQL_VARCHAR},
        {"TABLE_SCHEM", SQL_VARCHAR},
        {"TABLE_NAME", SQL_VARCHAR},
        {"COLUMN_NAME", SQL_VARCHAR},
        {"KEY_SEQ", SQL_SMALLINT},
        {"PK_NAME", SQL_VARCHAR}
    });

    if (table_name.empty()) {
        return SQL_SUCCESS;
    }

    auto *client = GetClient(stmt->conn);
    if (!client) {
        return SQL_SUCCESS;
    }

    std::string db = catalog.empty() ? stmt->conn->database : catalog;

    // Query the sorting key from system.tables (ClickHouse equivalent of primary key)
    std::string query = "SELECT sorting_key FROM system.tables WHERE database = '" + db +
                        "' AND name = '" + StripSearchPatternEscape(table_name) + "'";

    ResultSet temp;
    std::string error_msg;
    if (!client->ExecuteQuery(query, temp, error_msg) || temp.rows.empty()) {
        return SQL_SUCCESS;
    }

    std::string sorting_key = temp.rows[0][0].value_or("");
    if (sorting_key.empty()) {
        return SQL_SUCCESS;
    }

    // Parse comma-separated column names from sorting_key
    // e.g. "id" or "id, name" or "id, toDate(timestamp)"
    std::vector<std::string> key_columns;
    std::string current;
    int paren_depth = 0;
    for (char ch : sorting_key) {
        if (ch == '(') { paren_depth++; current += ch; }
        else if (ch == ')') { paren_depth--; current += ch; }
        else if (ch == ',' && paren_depth == 0) {
            // Trim whitespace
            size_t s = current.find_first_not_of(" \t");
            size_t e = current.find_last_not_of(" \t");
            if (s != std::string::npos) {
                std::string col = current.substr(s, e - s + 1);
                // Only include simple column names (skip expressions with parentheses)
                if (col.find('(') == std::string::npos) {
                    key_columns.push_back(col);
                }
            }
            current.clear();
        }
        else { current += ch; }
    }
    // Last token
    {
        size_t s = current.find_first_not_of(" \t");
        size_t e = current.find_last_not_of(" \t");
        if (s != std::string::npos) {
            std::string col = current.substr(s, e - s + 1);
            if (col.find('(') == std::string::npos) {
                key_columns.push_back(col);
            }
        }
    }

    for (int i = 0; i < static_cast<int>(key_columns.size()); ++i) {
        AddRow(stmt->result_set, {
            std::string(db),                        // TABLE_CAT
            std::nullopt,                           // TABLE_SCHEM
            std::string(table_name),                // TABLE_NAME
            key_columns[i],                         // COLUMN_NAME
            std::to_string(i + 1),                  // KEY_SEQ
            std::string("PRIMARY")                  // PK_NAME
        });
    }

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                             SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                             SQLCHAR *TableName, SQLSMALLINT NameLength3) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    auto toString = [](SQLCHAR *s, SQLSMALLINT len) -> std::string {
        if (!s) return "";
        if (len == SQL_NTS) return reinterpret_cast<const char *>(s);
        return std::string(reinterpret_cast<const char *>(s), len);
    };

    return PrimaryKeysImpl(stmt, toString(CatalogName, NameLength1), toString(SchemaName, NameLength2),
                           toString(TableName, NameLength3));
}

extern "C" SQLRETURN SQL_API SQLPrimaryKeysW(SQLHSTMT StatementHandle, SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                              SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                              SQLWCHAR *TableName, SQLSMALLINT NameLength3) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    auto toString = [](SQLWCHAR *s, SQLSMALLINT len) -> std::string {
        if (!s) return "";
        return WideToUtf8(s, len);
    };

    return PrimaryKeysImpl(stmt, toString(CatalogName, NameLength1), toString(SchemaName, NameLength2),
                           toString(TableName, NameLength3));
}

// ============================================================================
// SQLStatistics / SQLStatisticsW
// ============================================================================
static SQLRETURN StatisticsImpl(OdbcStatement *stmt) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    InitCatalogColumns(stmt->result_set, {
        {"TABLE_CAT", SQL_VARCHAR},
        {"TABLE_SCHEM", SQL_VARCHAR},
        {"TABLE_NAME", SQL_VARCHAR},
        {"NON_UNIQUE", SQL_SMALLINT},
        {"INDEX_QUALIFIER", SQL_VARCHAR},
        {"INDEX_NAME", SQL_VARCHAR},
        {"TYPE", SQL_SMALLINT},
        {"ORDINAL_POSITION", SQL_SMALLINT},
        {"COLUMN_NAME", SQL_VARCHAR},
        {"ASC_OR_DESC", SQL_CHAR},
        {"CARDINALITY", SQL_INTEGER},
        {"PAGES", SQL_INTEGER},
        {"FILTER_CONDITION", SQL_VARCHAR}
    });

    // Return empty - ClickHouse doesn't have traditional indexes
    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLStatistics(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                            SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                            SQLCHAR *TableName, SQLSMALLINT NameLength3,
                                            SQLUSMALLINT Unique, SQLUSMALLINT Reserved) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return StatisticsImpl(stmt);
}

extern "C" SQLRETURN SQL_API SQLStatisticsW(SQLHSTMT StatementHandle, SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                             SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                             SQLWCHAR *TableName, SQLSMALLINT NameLength3,
                                             SQLUSMALLINT Unique, SQLUSMALLINT Reserved) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return StatisticsImpl(stmt);
}

// ============================================================================
// SQLSpecialColumns / SQLSpecialColumnsW
// ============================================================================
static SQLRETURN SpecialColumnsImpl(OdbcStatement *stmt, SQLUSMALLINT identifier_type,
                                    const std::string &catalog, const std::string &schema,
                                    const std::string &table_name) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    InitCatalogColumns(stmt->result_set, {
        {"SCOPE", SQL_SMALLINT},
        {"COLUMN_NAME", SQL_VARCHAR},
        {"DATA_TYPE", SQL_SMALLINT},
        {"TYPE_NAME", SQL_VARCHAR},
        {"COLUMN_SIZE", SQL_INTEGER},
        {"BUFFER_LENGTH", SQL_INTEGER},
        {"DECIMAL_DIGITS", SQL_SMALLINT},
        {"PSEUDO_COLUMN", SQL_SMALLINT}
    });

    // SQL_ROWVER: ClickHouse has no row-version pseudo-column → return empty
    if (identifier_type != SQL_BEST_ROWID) {
        return SQL_SUCCESS;
    }

    if (table_name.empty()) {
        return SQL_SUCCESS;
    }

    auto *client = GetClient(stmt->conn);
    if (!client) {
        return SQL_SUCCESS;
    }

    std::string db = catalog.empty() ? stmt->conn->database : catalog;
    std::string stripped_table = StripSearchPatternEscape(table_name);

    // First, try to get sorting key columns (ClickHouse equivalent of primary key).
    // These are the best candidates for uniquely identifying rows.
    std::string sk_query = "SELECT sorting_key FROM system.tables WHERE database = '" + db +
                           "' AND name = '" + stripped_table + "'";
    ResultSet sk_temp;
    std::string error_msg;
    std::vector<std::string> key_columns;
    if (client->ExecuteQuery(sk_query, sk_temp, error_msg) && !sk_temp.rows.empty()) {
        std::string sorting_key = sk_temp.rows[0][0].value_or("");
        if (!sorting_key.empty()) {
            // Parse comma-separated column names, skip expressions with parentheses
            std::string current;
            int paren_depth = 0;
            for (char ch : sorting_key) {
                if (ch == '(') { paren_depth++; current += ch; }
                else if (ch == ')') { paren_depth--; current += ch; }
                else if (ch == ',' && paren_depth == 0) {
                    size_t s = current.find_first_not_of(" \t");
                    size_t e = current.find_last_not_of(" \t");
                    if (s != std::string::npos) {
                        std::string col = current.substr(s, e - s + 1);
                        if (col.find('(') == std::string::npos) key_columns.push_back(col);
                    }
                    current.clear();
                } else { current += ch; }
            }
            size_t s = current.find_first_not_of(" \t");
            size_t e = current.find_last_not_of(" \t");
            if (s != std::string::npos) {
                std::string col = current.substr(s, e - s + 1);
                if (col.find('(') == std::string::npos) key_columns.push_back(col);
            }
        }
    }

    if (!key_columns.empty()) {
        // Return sorting key columns — query their types from system.columns
        for (const auto &key_col : key_columns) {
            std::string col_query = "SELECT type FROM system.columns WHERE database = '" + db +
                                    "' AND table = '" + stripped_table +
                                    "' AND name = '" + key_col + "'";
            ResultSet col_temp;
            if (client->ExecuteQuery(col_query, col_temp, error_msg) && !col_temp.rows.empty()) {
                std::string ch_type = col_temp.rows[0][0].value_or("String");
                SQLSMALLINT sql_type = ClickHouseTypeToSqlType(ch_type);
                SQLULEN col_size = GetColumnSizeForType(ch_type, sql_type);
                SQLULEN octet_len = GetSqlTypeOctetLength(sql_type);
                SQLSMALLINT dec_digits = GetDecimalDigitsForType(ch_type, sql_type);
                std::string type_name = GetSqlTypeName(sql_type);

                AddRow(stmt->result_set, {
                    std::to_string(SQL_SCOPE_SESSION),
                    std::string(key_col),
                    std::to_string(sql_type),
                    std::string(type_name),
                    std::to_string(col_size),
                    std::to_string(octet_len),
                    std::to_string(dec_digits),
                    std::to_string(SQL_PC_NOT_PSEUDO)
                });
            }
        }
    } else {
        // No sorting key — return all non-Nullable columns as fallback
        std::string query = "SELECT name, type FROM system.columns WHERE database = '" + db +
                            "' AND table = '" + stripped_table +
                            "' AND type NOT LIKE 'Nullable%' ORDER BY position";
        ResultSet temp;
        if (client->ExecuteQuery(query, temp, error_msg) && !temp.rows.empty()) {
            for (const auto &row : temp.rows) {
                if (row.size() < 2) continue;
                std::string col_name = row[0].value_or("");
                std::string ch_type = row[1].value_or("String");

                SQLSMALLINT sql_type = ClickHouseTypeToSqlType(ch_type);
                SQLULEN col_size = GetColumnSizeForType(ch_type, sql_type);
                SQLULEN octet_len = GetSqlTypeOctetLength(sql_type);
                SQLSMALLINT dec_digits = GetDecimalDigitsForType(ch_type, sql_type);
                std::string type_name = GetSqlTypeName(sql_type);

                AddRow(stmt->result_set, {
                    std::to_string(SQL_SCOPE_SESSION),
                    std::string(col_name),
                    std::to_string(sql_type),
                    std::string(type_name),
                    std::to_string(col_size),
                    std::to_string(octet_len),
                    std::to_string(dec_digits),
                    std::to_string(SQL_PC_NOT_PSEUDO)
                });
            }
        }
    }

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT StatementHandle, SQLUSMALLINT IdentifierType,
                                                 SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                                 SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                                 SQLCHAR *TableName, SQLSMALLINT NameLength3,
                                                 SQLUSMALLINT Scope, SQLUSMALLINT Nullable) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    auto toString = [](SQLCHAR *s, SQLSMALLINT len) -> std::string {
        if (!s) return "";
        if (len == SQL_NTS) return reinterpret_cast<const char *>(s);
        return std::string(reinterpret_cast<const char *>(s), len);
    };

    return SpecialColumnsImpl(stmt, IdentifierType, toString(CatalogName, NameLength1),
                              toString(SchemaName, NameLength2), toString(TableName, NameLength3));
}

extern "C" SQLRETURN SQL_API SQLSpecialColumnsW(SQLHSTMT StatementHandle, SQLUSMALLINT IdentifierType,
                                                  SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                                  SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                                  SQLWCHAR *TableName, SQLSMALLINT NameLength3,
                                                  SQLUSMALLINT Scope, SQLUSMALLINT Nullable) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    auto toString = [](SQLWCHAR *s, SQLSMALLINT len) -> std::string {
        if (!s) return "";
        return WideToUtf8(s, len);
    };

    return SpecialColumnsImpl(stmt, IdentifierType, toString(CatalogName, NameLength1),
                              toString(SchemaName, NameLength2), toString(TableName, NameLength3));
}

// ============================================================================
// SQLForeignKeys / SQLForeignKeysW
// ============================================================================
static SQLRETURN ForeignKeysImpl(OdbcStatement *stmt) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    InitCatalogColumns(stmt->result_set, {
        {"PKTABLE_CAT", SQL_VARCHAR},
        {"PKTABLE_SCHEM", SQL_VARCHAR},
        {"PKTABLE_NAME", SQL_VARCHAR},
        {"PKCOLUMN_NAME", SQL_VARCHAR},
        {"FKTABLE_CAT", SQL_VARCHAR},
        {"FKTABLE_SCHEM", SQL_VARCHAR},
        {"FKTABLE_NAME", SQL_VARCHAR},
        {"FKCOLUMN_NAME", SQL_VARCHAR},
        {"KEY_SEQ", SQL_SMALLINT},
        {"UPDATE_RULE", SQL_SMALLINT},
        {"DELETE_RULE", SQL_SMALLINT},
        {"FK_NAME", SQL_VARCHAR},
        {"PK_NAME", SQL_VARCHAR},
        {"DEFERRABILITY", SQL_SMALLINT}
    });

    // ClickHouse doesn't support foreign keys
    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT StatementHandle,
                                             SQLCHAR *PKCatalogName, SQLSMALLINT NameLength1,
                                             SQLCHAR *PKSchemaName, SQLSMALLINT NameLength2,
                                             SQLCHAR *PKTableName, SQLSMALLINT NameLength3,
                                             SQLCHAR *FKCatalogName, SQLSMALLINT NameLength4,
                                             SQLCHAR *FKSchemaName, SQLSMALLINT NameLength5,
                                             SQLCHAR *FKTableName, SQLSMALLINT NameLength6) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return ForeignKeysImpl(stmt);
}

extern "C" SQLRETURN SQL_API SQLForeignKeysW(SQLHSTMT StatementHandle,
                                              SQLWCHAR *PKCatalogName, SQLSMALLINT NameLength1,
                                              SQLWCHAR *PKSchemaName, SQLSMALLINT NameLength2,
                                              SQLWCHAR *PKTableName, SQLSMALLINT NameLength3,
                                              SQLWCHAR *FKCatalogName, SQLSMALLINT NameLength4,
                                              SQLWCHAR *FKSchemaName, SQLSMALLINT NameLength5,
                                              SQLWCHAR *FKTableName, SQLSMALLINT NameLength6) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return ForeignKeysImpl(stmt);
}

// ============================================================================
// SQLProcedures / SQLProceduresW
// ============================================================================
static SQLRETURN ProceduresImpl(OdbcStatement *stmt) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    InitCatalogColumns(stmt->result_set, {
        {"PROCEDURE_CAT", SQL_VARCHAR},
        {"PROCEDURE_SCHEM", SQL_VARCHAR},
        {"PROCEDURE_NAME", SQL_VARCHAR},
        {"NUM_INPUT_PARAMS", SQL_INTEGER},
        {"NUM_OUTPUT_PARAMS", SQL_INTEGER},
        {"NUM_RESULT_SETS", SQL_INTEGER},
        {"REMARKS", SQL_VARCHAR},
        {"PROCEDURE_TYPE", SQL_SMALLINT}
    });

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLProcedures(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                            SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                            SQLCHAR *ProcName, SQLSMALLINT NameLength3) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return ProceduresImpl(stmt);
}

extern "C" SQLRETURN SQL_API SQLProceduresW(SQLHSTMT StatementHandle, SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                             SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                             SQLWCHAR *ProcName, SQLSMALLINT NameLength3) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return ProceduresImpl(stmt);
}

// ============================================================================
// SQLProcedureColumns / SQLProcedureColumnsW
// ============================================================================
static SQLRETURN ProcedureColumnsImpl(OdbcStatement *stmt) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    InitCatalogColumns(stmt->result_set, {
        {"PROCEDURE_CAT", SQL_VARCHAR},
        {"PROCEDURE_SCHEM", SQL_VARCHAR},
        {"PROCEDURE_NAME", SQL_VARCHAR},
        {"COLUMN_NAME", SQL_VARCHAR},
        {"COLUMN_TYPE", SQL_SMALLINT},
        {"DATA_TYPE", SQL_SMALLINT},
        {"TYPE_NAME", SQL_VARCHAR},
        {"COLUMN_SIZE", SQL_INTEGER},
        {"BUFFER_LENGTH", SQL_INTEGER},
        {"DECIMAL_DIGITS", SQL_SMALLINT},
        {"NUM_PREC_RADIX", SQL_SMALLINT},
        {"NULLABLE", SQL_SMALLINT},
        {"REMARKS", SQL_VARCHAR},
        {"COLUMN_DEF", SQL_VARCHAR},
        {"SQL_DATA_TYPE", SQL_SMALLINT},
        {"SQL_DATETIME_SUB", SQL_SMALLINT},
        {"CHAR_OCTET_LENGTH", SQL_INTEGER},
        {"ORDINAL_POSITION", SQL_INTEGER},
        {"IS_NULLABLE", SQL_VARCHAR}
    });

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLProcedureColumns(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                                  SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                                  SQLCHAR *ProcName, SQLSMALLINT NameLength3,
                                                  SQLCHAR *ColumnName, SQLSMALLINT NameLength4) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return ProcedureColumnsImpl(stmt);
}

extern "C" SQLRETURN SQL_API SQLProcedureColumnsW(SQLHSTMT StatementHandle, SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                                   SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                                   SQLWCHAR *ProcName, SQLSMALLINT NameLength3,
                                                   SQLWCHAR *ColumnName, SQLSMALLINT NameLength4) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return ProcedureColumnsImpl(stmt);
}

// ============================================================================
// SQLGetTypeInfo / SQLGetTypeInfoW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT StatementHandle, SQLSMALLINT DataType) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);
    stmt->ClearDiagRecords();
    BuildTypeInfoResultSet(stmt->result_set, DataType);
    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLGetTypeInfoW(SQLHSTMT StatementHandle, SQLSMALLINT DataType) {
    return SQLGetTypeInfo(StatementHandle, DataType);
}

// ============================================================================
// SQLColumnPrivileges / SQLColumnPrivilegesW
// ============================================================================
static SQLRETURN ColumnPrivilegesImpl(OdbcStatement *stmt) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    InitCatalogColumns(stmt->result_set, {
        {"TABLE_CAT", SQL_VARCHAR},
        {"TABLE_SCHEM", SQL_VARCHAR},
        {"TABLE_NAME", SQL_VARCHAR},
        {"COLUMN_NAME", SQL_VARCHAR},
        {"GRANTOR", SQL_VARCHAR},
        {"GRANTEE", SQL_VARCHAR},
        {"PRIVILEGE", SQL_VARCHAR},
        {"IS_GRANTABLE", SQL_VARCHAR}
    });

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLColumnPrivileges(SQLHSTMT StatementHandle,
                                                  SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                                  SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                                  SQLCHAR *TableName, SQLSMALLINT NameLength3,
                                                  SQLCHAR *ColumnName, SQLSMALLINT NameLength4) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return ColumnPrivilegesImpl(stmt);
}

extern "C" SQLRETURN SQL_API SQLColumnPrivilegesW(SQLHSTMT StatementHandle,
                                                   SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                                   SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                                   SQLWCHAR *TableName, SQLSMALLINT NameLength3,
                                                   SQLWCHAR *ColumnName, SQLSMALLINT NameLength4) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return ColumnPrivilegesImpl(stmt);
}

// ============================================================================
// SQLTablePrivileges / SQLTablePrivilegesW
// ============================================================================
static SQLRETURN TablePrivilegesImpl(OdbcStatement *stmt) {
    stmt->ClearDiagRecords();
    stmt->result_set.Reset();

    InitCatalogColumns(stmt->result_set, {
        {"TABLE_CAT", SQL_VARCHAR},
        {"TABLE_SCHEM", SQL_VARCHAR},
        {"TABLE_NAME", SQL_VARCHAR},
        {"GRANTOR", SQL_VARCHAR},
        {"GRANTEE", SQL_VARCHAR},
        {"PRIVILEGE", SQL_VARCHAR},
        {"IS_GRANTABLE", SQL_VARCHAR}
    });

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLTablePrivileges(SQLHSTMT StatementHandle,
                                                 SQLCHAR *CatalogName, SQLSMALLINT NameLength1,
                                                 SQLCHAR *SchemaName, SQLSMALLINT NameLength2,
                                                 SQLCHAR *TableName, SQLSMALLINT NameLength3) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return TablePrivilegesImpl(stmt);
}

extern "C" SQLRETURN SQL_API SQLTablePrivilegesW(SQLHSTMT StatementHandle,
                                                  SQLWCHAR *CatalogName, SQLSMALLINT NameLength1,
                                                  SQLWCHAR *SchemaName, SQLSMALLINT NameLength2,
                                                  SQLWCHAR *TableName, SQLSMALLINT NameLength3) {
    if (!IsValidStmtHandle(StatementHandle)) return SQL_INVALID_HANDLE;
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle); HandleLock lock(stmt); return TablePrivilegesImpl(stmt);
}
