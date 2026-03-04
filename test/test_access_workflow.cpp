// MS Access workflow tests
// Test suite simulating real use cases when working with accdb files
// Reproduces the ODBC API call sequences that Access issues during
// linked table creation, data read/write, and filtering operations

#include "test_framework.h"

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include <string>
#include <cstring>
#include <vector>

// Include driver headers
#include "../src/include/handle.h"
#include "../src/include/util.h"
#include "../src/include/type_mapping.h"

#ifdef UNICODE
#undef SQLTables
#undef SQLColumns
#undef SQLPrimaryKeys
#undef SQLStatistics
#undef SQLSpecialColumns
#undef SQLForeignKeys
#undef SQLGetTypeInfo
#undef SQLGetInfo
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLExecDirect
#undef SQLPrepare
#undef SQLDescribeCol
#undef SQLColAttribute
#undef SQLColAttributes
#undef SQLDriverConnect
#undef SQLGetDiagRec
#undef SQLGetDiagField
#undef SQLNativeSql
#undef SQLGetCursorName
#undef SQLSetCursorName
#undef SQLBrowseConnect
#undef SQLProcedures
#undef SQLColumnPrivileges
#undef SQLTablePrivileges
#endif

#pragma warning(disable: 4996)

// Forward declarations - all ODBC functions used by Access
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetInfo(SQLHDBC, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLTables(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                        SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLColumns(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                         SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                             SQLCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLStatistics(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                            SQLCHAR *, SQLSMALLINT, SQLUSMALLINT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLSpecialColumns(SQLHSTMT, SQLUSMALLINT, SQLCHAR *, SQLSMALLINT,
                                                SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                                SQLUSMALLINT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                             SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                             SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLProcedures(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                            SQLCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLBindCol(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLBindParameter(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                               SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER,
                                               SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLExecute(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLExecDirect(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT, SQLUSMALLINT, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *,
                                             SQLSMALLINT *, SQLULEN *, SQLSMALLINT *, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLColAttribute(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER,
                                              SQLSMALLINT, SQLSMALLINT *, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLRowCount(SQLHSTMT, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLDisconnect(SQLHDBC);
extern "C" SQLRETURN SQL_API SQLNativeSql(SQLHDBC, SQLCHAR *, SQLINTEGER, SQLCHAR *, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLCHAR *,
                                            SQLINTEGER *, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLSMALLINT,
                                              SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT, SQLUSMALLINT, SQLLEN, SQLULEN *, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLColAttributes(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER,
                                               SQLSMALLINT, SQLSMALLINT *, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLCancel(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLMoreResults(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLEndTran(SQLSMALLINT, SQLHANDLE, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLSetPos(SQLHSTMT, SQLSETPOSIROW, SQLUSMALLINT, SQLUSMALLINT);

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// Test helpers
// ============================================================================

// Create handles reproducing Access initial connection sequence
static void CreateAccessHandles(SQLHENV &env, SQLHDBC &dbc) {
    // Access first creates an environment handle
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    AssertTrue(ret == SQL_SUCCESS, "ENV alloc");

    // Set ODBC 3.x version (Access operates with ODBC 3.x)
    ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    AssertTrue(ret == SQL_SUCCESS, "Set ODBC version");

    // Create connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    AssertTrue(ret == SQL_SUCCESS, "DBC alloc");

    // For testing: mark as connected
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    conn->host = "localhost";
    conn->port = 8123;
    conn->database = "default";
    conn->user = "default";
}

static SQLHSTMT AllocTestStmt(SQLHDBC dbc) {
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    return stmt;
}

static void FreeAccessHandles(SQLHENV env, SQLHDBC dbc) {
    if (dbc) {
        auto *conn = static_cast<OdbcConnection *>(dbc);
        conn->connected = false;
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// Helper to directly build a result set for testing
static void PopulateMultiColumnResultSet(SQLHSTMT hstmt,
    const std::vector<std::pair<std::string, SQLSMALLINT>> &cols,
    const std::vector<std::vector<std::optional<std::string>>> &rows) {
    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    stmt->result_set.Reset();
    for (const auto &[name, type] : cols) {
        ColumnInfo ci;
        ci.name = name;
        ci.sql_type = type;
        ci.column_size = (type == SQL_VARCHAR) ? 256 : 10;
        stmt->result_set.columns.push_back(ci);
    }
    for (const auto &row : rows) {
        stmt->result_set.rows.push_back(row);
    }
    stmt->result_set.current_row = -1;
}

// ============================================================================
// Scenario 1: Table link creation workflow
// Reproduces the API call sequence when Access links a table via
// "Get External Data" -> "ODBC Database"
// ============================================================================

// 1-1: Driver info query at Access startup
TEST(AccessWorkflow, DriverInfoQueryOnStartup) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    SQLSMALLINT str_len;
    char buf[512];

    // Access checks driver name and version
    SQLRETURN ret = SQLGetInfo(dbc, SQL_DRIVER_NAME, buf, sizeof(buf), &str_len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_DRIVER_NAME");
    AssertTrue(str_len > 0, "Driver name not empty");

    ret = SQLGetInfo(dbc, SQL_DRIVER_VER, buf, sizeof(buf), &str_len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_DRIVER_VER");

    ret = SQLGetInfo(dbc, SQL_DRIVER_ODBC_VER, buf, sizeof(buf), &str_len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_DRIVER_ODBC_VER");

    // DBMS information
    ret = SQLGetInfo(dbc, SQL_DBMS_NAME, buf, sizeof(buf), &str_len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_DBMS_NAME");

    ret = SQLGetInfo(dbc, SQL_DBMS_VER, buf, sizeof(buf), &str_len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_DBMS_VER");

    FreeAccessHandles(env, dbc);
}

// 1-2: SQLGetFunctions for Access to check driver capabilities
TEST(AccessWorkflow, GetFunctionsSupportedCheck) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    // Access checks all function support at once via SQL_API_ODBC3_ALL_FUNCTIONS
    SQLUSMALLINT supported[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE];
    memset(supported, 0, sizeof(supported));
    SQLRETURN ret = SQLGetFunctions(dbc, SQL_API_ODBC3_ALL_FUNCTIONS, supported);
    AssertTrue(ret == SQL_SUCCESS, "SQLGetFunctions SQL_API_ODBC3_ALL_FUNCTIONS");

    // Check functions required by Access
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLTABLES), "SQLTables supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLCOLUMNS), "SQLColumns supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLSTATISTICS), "SQLStatistics supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLPRIMARYKEYS), "SQLPrimaryKeys supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLSPECIALCOLUMNS), "SQLSpecialColumns supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLFOREIGNKEYS), "SQLForeignKeys supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLGETTYPEINFO), "SQLGetTypeInfo supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLGETINFO), "SQLGetInfo supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLFETCH), "SQLFetch supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLGETDATA), "SQLGetData supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLBINDCOL), "SQLBindCol supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLEXECDIRECT), "SQLExecDirect supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLPREPARE), "SQLPrepare supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLEXECUTE), "SQLExecute supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLDESCRIBECOL), "SQLDescribeCol supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLNUMRESULTCOLS), "SQLNumResultCols supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLROWCOUNT), "SQLRowCount supported");

    // ODBC 2.x compatibility functions (Access uses these too)
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLEXTENDEDFETCH), "SQLExtendedFetch supported");
    AssertTrue(SQL_FUNC_EXISTS(supported, SQL_API_SQLCOLATTRIBUTES), "SQLColAttributes supported");

    FreeAccessHandles(env, dbc);
}

// 1-3: Key SQLGetInfo values used by Access for table listing
TEST(AccessWorkflow, CriticalGetInfoValues) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    // Catalog name separator (Access uses this to construct table names)
    char catalog_sep[16];
    SQLSMALLINT len;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_CATALOG_NAME_SEPARATOR, catalog_sep, sizeof(catalog_sep), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_CATALOG_NAME_SEPARATOR");

    // Identifier quote character
    char quote_char[16];
    ret = SQLGetInfo(dbc, SQL_IDENTIFIER_QUOTE_CHAR, quote_char, sizeof(quote_char), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_IDENTIFIER_QUOTE_CHAR");

    // SQL conformance level
    SQLUINTEGER conformance;
    ret = SQLGetInfo(dbc, SQL_SQL_CONFORMANCE, &conformance, sizeof(conformance), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_SQL_CONFORMANCE");

    // Transaction support (Access checks if auto-commit)
    SQLUSMALLINT txn_capable;
    ret = SQLGetInfo(dbc, SQL_TXN_CAPABLE, &txn_capable, sizeof(txn_capable), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_TXN_CAPABLE");

    // Catalog usage (Access checks if catalog names can be used in DML)
    SQLUINTEGER catalog_usage;
    ret = SQLGetInfo(dbc, SQL_CATALOG_USAGE, &catalog_usage, sizeof(catalog_usage), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_CATALOG_USAGE");

    // Table name terminology
    char table_term[64];
    ret = SQLGetInfo(dbc, SQL_TABLE_TERM, table_term, sizeof(table_term), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_TABLE_TERM");

    // Maximum table name length
    SQLUSMALLINT max_table_name_len;
    ret = SQLGetInfo(dbc, SQL_MAX_TABLE_NAME_LEN, &max_table_name_len, sizeof(max_table_name_len), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_MAX_TABLE_NAME_LEN");

    // Maximum column name length
    SQLUSMALLINT max_column_name_len;
    ret = SQLGetInfo(dbc, SQL_MAX_COLUMN_NAME_LEN, &max_column_name_len, sizeof(max_column_name_len), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQL_MAX_COLUMN_NAME_LEN");

    FreeAccessHandles(env, dbc);
}

// 1-4: Catalog function call sequence during table linking
// Access: SQLTables → SQLColumns → SQLStatistics → SQLSpecialColumns → SQLPrimaryKeys
TEST(AccessWorkflow, TableLinkCatalogSequence) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Step 1: SQLTables - Get table list
    // (In test environment, server is not connected so only result set column structure is verified)
    // Access requests both TABLE and VIEW
    SQLRETURN ret = SQLTables(stmt, nullptr, 0, nullptr, 0, nullptr, 0,
                               (SQLCHAR *)"TABLE", SQL_NTS);
    // Error due to no server connection, but statement is valid
    // Build result set directly to test the flow instead
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.Reset();
    // Simulate SQLTables result set schema
    PopulateMultiColumnResultSet(stmt, {
        {"TABLE_CAT", SQL_VARCHAR},
        {"TABLE_SCHEM", SQL_VARCHAR},
        {"TABLE_NAME", SQL_VARCHAR},
        {"TABLE_TYPE", SQL_VARCHAR},
        {"REMARKS", SQL_VARCHAR}
    }, {
        {std::string("default"), std::nullopt, std::string("users"), std::string("TABLE"), std::nullopt},
        {std::string("default"), std::nullopt, std::string("orders"), std::string("TABLE"), std::nullopt},
        {std::string("default"), std::nullopt, std::string("products"), std::string("TABLE"), std::nullopt}
    });

    // Access reads table names via SQLFetch + SQLGetData
    SQLCHAR table_name[256];
    SQLLEN ind;
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch first table");
    ret = SQLGetData(stmt, 3, SQL_C_CHAR, table_name, sizeof(table_name), &ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData TABLE_NAME");
    AssertEqual(std::string("users"), std::string((char *)table_name), "First table is users");

    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch second table");
    ret = SQLGetData(stmt, 3, SQL_C_CHAR, table_name, sizeof(table_name), &ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData TABLE_NAME 2");
    AssertEqual(std::string("orders"), std::string((char *)table_name), "Second table is orders");

    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch third table");
    ret = SQLGetData(stmt, 3, SQL_C_CHAR, table_name, sizeof(table_name), &ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData TABLE_NAME 3");
    AssertEqual(std::string("products"), std::string((char *)table_name), "Third table is products");

    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_NO_DATA, "No more tables");

    SQLFreeStmt(stmt, SQL_CLOSE);

    // Step 2: SQLColumns - Column info for selected table
    // (Simulated since server is not connected)
    PopulateMultiColumnResultSet(stmt, {
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
    }, {
        {std::string("default"), std::nullopt, std::string("users"), std::string("id"),
         std::to_string(SQL_INTEGER), std::string("INTEGER"), std::string("10"), std::string("4"),
         std::string("0"), std::string("10"), std::string("0"),
         std::nullopt, std::nullopt, std::to_string(SQL_INTEGER), std::nullopt, std::nullopt,
         std::string("1"), std::string("NO")},
        {std::string("default"), std::nullopt, std::string("users"), std::string("name"),
         std::to_string(SQL_VARCHAR), std::string("VARCHAR"), std::string("255"), std::string("255"),
         std::string("0"), std::nullopt, std::string("1"),
         std::nullopt, std::nullopt, std::to_string(SQL_VARCHAR), std::nullopt, std::string("255"),
         std::string("2"), std::string("YES")},
        {std::string("default"), std::nullopt, std::string("users"), std::string("email"),
         std::to_string(SQL_VARCHAR), std::string("VARCHAR"), std::string("255"), std::string("255"),
         std::string("0"), std::nullopt, std::string("1"),
         std::nullopt, std::nullopt, std::to_string(SQL_VARCHAR), std::nullopt, std::string("255"),
         std::string("3"), std::string("YES")},
        {std::string("default"), std::nullopt, std::string("users"), std::string("created_date"),
         std::to_string(SQL_TYPE_DATE), std::string("DATE"), std::string("10"), std::string("6"),
         std::string("0"), std::nullopt, std::string("1"),
         std::nullopt, std::nullopt, std::to_string(SQL_TYPE_DATE), std::to_string(SQL_CODE_DATE), std::nullopt,
         std::string("4"), std::string("YES")}
    });

    // Access reads column info in a SQLFetch loop
    int col_count = 0;
    while (SQLFetch(stmt) == SQL_SUCCESS) col_count++;
    AssertEqual(4, col_count, "users table has 4 columns");

    SQLFreeStmt(stmt, SQL_CLOSE);

    // Step 3: SQLStatistics - Index info (Access uses this to find primary key candidates)
    ret = SQLStatistics(stmt, nullptr, 0, nullptr, 0,
                         (SQLCHAR *)"users", SQL_NTS, SQL_INDEX_ALL, SQL_QUICK);
    AssertTrue(ret == SQL_SUCCESS, "SQLStatistics returns success");
    // ClickHouse has no indexes so empty result
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_NO_DATA, "No indexes for ClickHouse tables");
    SQLFreeStmt(stmt, SQL_CLOSE);

    // Step 4: SQLSpecialColumns - Columns that uniquely identify a row
    ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, nullptr, 0, nullptr, 0,
                             (SQLCHAR *)"users", SQL_NTS, SQL_SCOPE_SESSION, SQL_NULLABLE);
    AssertTrue(ret == SQL_SUCCESS, "SQLSpecialColumns returns success");
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_NO_DATA, "No special columns (ClickHouse)");
    SQLFreeStmt(stmt, SQL_CLOSE);

    // Step 5: SQLPrimaryKeys - Primary key info
    ret = SQLPrimaryKeys(stmt, nullptr, 0, nullptr, 0, (SQLCHAR *)"users", SQL_NTS);
    AssertTrue(ret == SQL_SUCCESS, "SQLPrimaryKeys returns success");
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_NO_DATA, "No primary keys (ClickHouse)");
    SQLFreeStmt(stmt, SQL_CLOSE);

    // Step 6: SQLForeignKeys - Foreign keys (Access explores relationships)
    ret = SQLForeignKeys(stmt, nullptr, 0, nullptr, 0, (SQLCHAR *)"users", SQL_NTS,
                          nullptr, 0, nullptr, 0, nullptr, 0);
    AssertTrue(ret == SQL_SUCCESS, "SQLForeignKeys returns success");
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_NO_DATA, "No foreign keys (ClickHouse)");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 1-5: SQLGetTypeInfo - Access enumerates supported data types
TEST(AccessWorkflow, TypeInfoEnumeration) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Access gets all types with SQL_ALL_TYPES
    SQLRETURN ret = SQLGetTypeInfo(stmt, SQL_ALL_TYPES);
    AssertTrue(ret == SQL_SUCCESS, "SQLGetTypeInfo SQL_ALL_TYPES");

    // Fetch type info and verify basic data types are included
    int type_count = 0;
    bool has_integer = false, has_varchar = false, has_date = false;
    bool has_double = false, has_bigint = false;

    while (SQLFetch(stmt) == SQL_SUCCESS) {
        type_count++;
        SQLCHAR type_name[128];
        SQLLEN ind;
        ret = SQLGetData(stmt, 1, SQL_C_CHAR, type_name, sizeof(type_name), &ind);
        if (ret == SQL_SUCCESS && ind != SQL_NULL_DATA) {
            std::string name((char *)type_name);
            if (name == "INTEGER") has_integer = true;
            if (name == "VARCHAR") has_varchar = true;
            if (name == "DATE") has_date = true;
            if (name == "DOUBLE") has_double = true;
            if (name == "BIGINT") has_bigint = true;
        }
    }

    AssertTrue(type_count >= 10, "At least 10 type entries");
    AssertTrue(has_integer, "INTEGER type present");
    AssertTrue(has_varchar, "VARCHAR type present");
    AssertTrue(has_date, "DATE type present");
    AssertTrue(has_double, "DOUBLE type present");
    AssertTrue(has_bigint, "BIGINT type present");

    // Verify filtering by individual type also works
    SQLFreeStmt(stmt, SQL_CLOSE);
    ret = SQLGetTypeInfo(stmt, SQL_INTEGER);
    AssertTrue(ret == SQL_SUCCESS, "SQLGetTypeInfo SQL_INTEGER");
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "INTEGER type found");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 2: Data reading workflow
// Data fetch sequence when opening a linked table in Access
// ============================================================================

// 2-1: Basic flow for opening a table in Access datasheet view
TEST(AccessWorkflow, OpenLinkedTableDatasheet) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);
    auto *s = static_cast<OdbcStatement *>(stmt);

    // Access reads SELECT * FROM table with bound columns
    // Simulated: 3 columns x 3 rows test result set
    PopulateMultiColumnResultSet(stmt, {
        {"id", SQL_INTEGER},
        {"name", SQL_VARCHAR},
        {"price", SQL_DOUBLE}
    }, {
        {std::string("1"), std::string("Widget A"), std::string("19.99")},
        {std::string("2"), std::string("Widget B"), std::string("29.99")},
        {std::string("3"), std::string("Widget C"), std::string("39.99")}
    });

    // Access checks column count with SQLNumResultCols
    SQLSMALLINT num_cols;
    SQLRETURN ret = SQLNumResultCols(stmt, &num_cols);
    AssertTrue(ret == SQL_SUCCESS, "NumResultCols");
    AssertEqual(3, (int)num_cols, "3 columns");

    // Access checks each column's type with SQLDescribeCol
    SQLCHAR col_name[128];
    SQLSMALLINT name_len, data_type, decimal_digits, nullable;
    SQLULEN col_size;

    ret = SQLDescribeCol(stmt, 1, col_name, sizeof(col_name), &name_len,
                          &data_type, &col_size, &decimal_digits, &nullable);
    AssertTrue(ret == SQL_SUCCESS, "DescribeCol col 1");
    AssertEqual(std::string("id"), std::string((char *)col_name), "Col 1 name");

    ret = SQLDescribeCol(stmt, 2, col_name, sizeof(col_name), &name_len,
                          &data_type, &col_size, &decimal_digits, &nullable);
    AssertTrue(ret == SQL_SUCCESS, "DescribeCol col 2");
    AssertEqual(std::string("name"), std::string((char *)col_name), "Col 2 name");

    // Access binds buffers with SQLBindCol then calls SQLFetch
    SQLINTEGER id_val;
    SQLCHAR name_buf[256];
    double price_val;
    SQLLEN id_ind, name_ind, price_ind;

    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &id_val, 0, &id_ind);
    AssertTrue(ret == SQL_SUCCESS, "BindCol id");
    ret = SQLBindCol(stmt, 2, SQL_C_CHAR, name_buf, sizeof(name_buf), &name_ind);
    AssertTrue(ret == SQL_SUCCESS, "BindCol name");
    ret = SQLBindCol(stmt, 3, SQL_C_DOUBLE, &price_val, 0, &price_ind);
    AssertTrue(ret == SQL_SUCCESS, "BindCol price");

    // Row 1
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch row 1");
    AssertEqual(1, (int)id_val, "Row 1 id");
    AssertEqual(std::string("Widget A"), std::string((char *)name_buf), "Row 1 name");

    // Row 2
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch row 2");
    AssertEqual(2, (int)id_val, "Row 2 id");
    AssertEqual(std::string("Widget B"), std::string((char *)name_buf), "Row 2 name");

    // Row 3
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch row 3");
    AssertEqual(3, (int)id_val, "Row 3 id");

    // No more data
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_NO_DATA, "No more rows");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 2-2: Access filtering (WHERE clause) - Parameterized queries
TEST(AccessWorkflow, FilteredQueryWithParameters) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Access uses parameterized queries
    // SELECT * FROM users WHERE name = ?
    SQLRETURN ret = SQLPrepare(stmt,
        (SQLCHAR *)"SELECT * FROM users WHERE name = ?", SQL_NTS);
    AssertTrue(ret == SQL_SUCCESS, "Prepare parameterized query");

    // Bind parameter
    SQLCHAR param_value[] = "Alice";
    SQLLEN param_ind = SQL_NTS;
    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR,
                            255, 0, param_value, sizeof(param_value), &param_ind);
    AssertTrue(ret == SQL_SUCCESS, "BindParameter name");

    // Verify parameter was bound
    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertTrue(s->bound_parameters.count(1) == 1, "Parameter 1 bound");
    AssertEqual(std::string("SELECT * FROM users WHERE name = ?"), s->query, "Query stored");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 2-3: NULL value handling (Access handles NULL specially)
TEST(AccessWorkflow, NullValueHandling) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Result set containing NULL values
    PopulateMultiColumnResultSet(stmt, {
        {"id", SQL_INTEGER},
        {"description", SQL_VARCHAR},
        {"amount", SQL_DOUBLE}
    }, {
        {std::string("1"), std::string("Active item"), std::string("100.50")},
        {std::string("2"), std::nullopt, std::string("200.00")},       // description = NULL
        {std::string("3"), std::string("Another item"), std::nullopt}  // amount = NULL
    });

    // Row 1: all non-NULL
    SQLRETURN ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch row 1");

    SQLCHAR desc_buf[256];
    SQLLEN desc_ind;
    ret = SQLGetData(stmt, 2, SQL_C_CHAR, desc_buf, sizeof(desc_buf), &desc_ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData desc row 1");
    AssertTrue(desc_ind != SQL_NULL_DATA, "Row 1 desc is not null");
    AssertEqual(std::string("Active item"), std::string((char *)desc_buf), "Row 1 desc value");

    // Row 2: description = NULL
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch row 2");
    ret = SQLGetData(stmt, 2, SQL_C_CHAR, desc_buf, sizeof(desc_buf), &desc_ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData desc row 2");
    AssertTrue(desc_ind == SQL_NULL_DATA, "Row 2 desc is null");

    // Row 3: amount = NULL
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch row 3");
    double amount;
    SQLLEN amount_ind;
    ret = SQLGetData(stmt, 3, SQL_C_DOUBLE, &amount, 0, &amount_ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData amount row 3");
    AssertTrue(amount_ind == SQL_NULL_DATA, "Row 3 amount is null");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 2-4: Access uses SQLExtendedFetch (ODBC 2.x compatible)
TEST(AccessWorkflow, ExtendedFetchLegacy) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"id", SQL_INTEGER},
        {"value", SQL_VARCHAR}
    }, {
        {std::string("1"), std::string("AAA")},
        {std::string("2"), std::string("BBB")},
        {std::string("3"), std::string("CCC")}
    });

    // Access sometimes uses SQLExtendedFetch(SQL_FETCH_NEXT)
    SQLULEN row_count;
    SQLUSMALLINT row_status[1];
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &row_count, row_status);
    AssertTrue(ret == SQL_SUCCESS, "ExtendedFetch NEXT");
    AssertTrue(row_count >= 1, "At least 1 row fetched");
    AssertTrue(row_status[0] == SQL_ROW_SUCCESS, "Row status is SUCCESS");

    // Row 2
    ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &row_count, row_status);
    AssertTrue(ret == SQL_SUCCESS, "ExtendedFetch NEXT 2");

    // Row 3
    ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &row_count, row_status);
    AssertTrue(ret == SQL_SUCCESS, "ExtendedFetch NEXT 3");

    // End of data
    ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &row_count, row_status);
    AssertTrue(ret == SQL_NO_DATA, "ExtendedFetch past end");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 3: ODBC escape sequences
// Access frequently uses escape sequences for date literals and functions
// ============================================================================

// 3-1: Escape sequences generated by Access for date filtering
TEST(AccessWorkflow, DateEscapeSequenceFilter) {
    // Access: WHERE order_date = {d '2024-01-15'}
    std::string input = "SELECT * FROM orders WHERE order_date = {d '2024-01-15'}";
    std::string result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT * FROM orders WHERE order_date = '2024-01-15'"), result,
                "Date escape in WHERE clause");
}

// 3-2: Timestamp escape generated by Access for datetime filtering
TEST(AccessWorkflow, TimestampEscapeFilter) {
    std::string input = "SELECT * FROM logs WHERE created_at >= {ts '2024-01-01 00:00:00'}";
    std::string result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT * FROM logs WHERE created_at >= '2024-01-01 00:00:00'"), result,
                "Timestamp escape in filter");
}

// 3-3: String function escapes used by Access
TEST(AccessWorkflow, StringFunctionEscapes) {
    // Access: {fn UCASE(name)}
    std::string input = "SELECT {fn UCASE(name)} FROM users";
    std::string result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT upper(name) FROM users"), result, "UCASE -> upper");

    // Access: {fn LCASE(name)}
    input = "SELECT {fn LCASE(name)} FROM users";
    result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT lower(name) FROM users"), result, "LCASE -> lower");

    // Access: {fn LENGTH(name)}
    input = "SELECT {fn LENGTH(name)} FROM users";
    result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT length(name) FROM users"), result, "LENGTH -> length");
}

// 3-4: Date functions used by Access
TEST(AccessWorkflow, DateFunctionEscapes) {
    // Access: {fn NOW()}
    std::string input = "SELECT {fn NOW()} as current_time";
    std::string result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT now() as current_time"), result, "NOW() -> now()");

    // Access: {fn CURDATE()}
    input = "SELECT {fn CURDATE()} as today";
    result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT today() as today"), result, "CURDATE -> today");

    // Access: {fn YEAR(order_date)}
    input = "SELECT {fn YEAR(order_date)} FROM orders";
    result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT toYear(order_date) FROM orders"), result, "YEAR -> toYear");

    // Access: {fn MONTH(order_date)}
    input = "SELECT {fn MONTH(order_date)} FROM orders";
    result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT toMonth(order_date) FROM orders"), result, "MONTH -> toMonth");
}

// 3-5: Access using CONVERT
TEST(AccessWorkflow, ConvertFunctionEscape) {
    // Access: {fn CONVERT(price, SQL_INTEGER)}
    std::string input = "SELECT {fn CONVERT(price, SQL_INTEGER)} FROM products";
    std::string result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT toInt32(price) FROM products"), result, "CONVERT SQL_INTEGER -> toInt32");

    // Access: {fn CONVERT(id, SQL_VARCHAR)}
    input = "SELECT {fn CONVERT(id, SQL_VARCHAR)} FROM products";
    result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT toString(id) FROM products"), result, "CONVERT SQL_VARCHAR -> toString");
}

// 3-6: Access compound query — combining multiple escapes
TEST(AccessWorkflow, ComplexAccessQuery) {
    // Typical query generated by Access in reports
    std::string input =
        "SELECT {fn UCASE(name)}, {fn YEAR(created_date)}, amount "
        "FROM orders "
        "WHERE created_date >= {d '2024-01-01'} "
        "AND {fn MONTH(created_date)} = 6";
    std::string result = ProcessOdbcEscapeSequences(input);
    std::string expected =
        "SELECT upper(name), toYear(created_date), amount "
        "FROM orders "
        "WHERE created_date >= '2024-01-01' "
        "AND toMonth(created_date) = 6";
    AssertEqual(expected, result, "Complex Access query with mixed escapes");
}

// 3-7: SQLNativeSql - Access converts ODBC escapes to native SQL
TEST(AccessWorkflow, NativeSqlTranslation) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    SQLCHAR input[] = "SELECT {fn NOW()}, {d '2024-06-15'} FROM dual";
    SQLCHAR output[512];
    SQLINTEGER out_len;

    SQLRETURN ret = SQLNativeSql(dbc, input, SQL_NTS, output, sizeof(output), &out_len);
    AssertTrue(ret == SQL_SUCCESS, "SQLNativeSql succeeds");
    AssertTrue(out_len > 0, "Output has content");
    std::string native_sql((char *)output, out_len);
    AssertEqual(std::string("SELECT now(), '2024-06-15' FROM dual"), native_sql,
                "NativeSql escape translation");

    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 4: Data type conversion
// Access calls SQLGetData with various C types
// ============================================================================

// 4-1: Integer type reading
TEST(AccessWorkflow, IntegerTypeConversion) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"count_val", SQL_INTEGER}
    }, {
        {std::string("42")},
        {std::string("0")},
        {std::string("-100")}
    });

    SQLRETURN ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch");

    SQLINTEGER val;
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_SLONG, &val, 0, &ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData as SLONG");
    AssertEqual(42, (int)val, "Value is 42");

    ret = SQLFetch(stmt);
    ret = SQLGetData(stmt, 1, SQL_C_SLONG, &val, 0, &ind);
    AssertEqual(0, (int)val, "Value is 0");

    ret = SQLFetch(stmt);
    ret = SQLGetData(stmt, 1, SQL_C_SLONG, &val, 0, &ind);
    AssertEqual(-100, (int)val, "Value is -100");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 4-2: String piecemeal retrieval (Memo field - Access long text)
TEST(AccessWorkflow, MemoFieldPiecemealRead) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Long string (equivalent to Access "Memo" type)
    std::string long_text(500, 'X');
    PopulateMultiColumnResultSet(stmt, {
        {"memo", SQL_LONGVARCHAR}
    }, {
        {long_text}
    });

    SQLRETURN ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch");

    // Chunked reading with small buffer (what Access does for Memo fields)
    SQLCHAR buf[100];
    SQLLEN ind;
    std::string accumulated;

    // 1st chunk
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertTrue(ret == SQL_SUCCESS_WITH_INFO || ret == SQL_SUCCESS, "First chunk");
    accumulated += (char *)buf;

    // Continue reading
    while (ret == SQL_SUCCESS_WITH_INFO) {
        ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
        if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
            accumulated += (char *)buf;
        }
    }

    AssertEqual(long_text, accumulated, "Full memo text reconstructed from pieces");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 4-3: Date type handling
TEST(AccessWorkflow, DateTypeHandling) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"event_date", SQL_TYPE_DATE}
    }, {
        {std::string("2024-06-15")}
    });

    SQLRETURN ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch");

    SQL_DATE_STRUCT date_val;
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_TYPE_DATE, &date_val, sizeof(date_val), &ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData as DATE");
    AssertEqual(2024, (int)date_val.year, "Year");
    AssertEqual(6, (int)date_val.month, "Month");
    AssertEqual(15, (int)date_val.day, "Day");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 4-4: Timestamp type (Access "Date/Time" type)
TEST(AccessWorkflow, TimestampTypeHandling) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"created_at", SQL_TYPE_TIMESTAMP}
    }, {
        {std::string("2024-06-15 14:30:45")}
    });

    SQLRETURN ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch");

    SQL_TIMESTAMP_STRUCT ts_val;
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_TYPE_TIMESTAMP, &ts_val, sizeof(ts_val), &ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData as TIMESTAMP");
    AssertEqual(2024, (int)ts_val.year, "Year");
    AssertEqual(6, (int)ts_val.month, "Month");
    AssertEqual(15, (int)ts_val.day, "Day");
    AssertEqual(14, (int)ts_val.hour, "Hour");
    AssertEqual(30, (int)ts_val.minute, "Minute");
    AssertEqual(45, (int)ts_val.second, "Second");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 4-5: Floating point / currency types
TEST(AccessWorkflow, FloatingPointAndCurrency) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"price", SQL_DOUBLE}
    }, {
        {std::string("19.99")},
        {std::string("0.01")},
        {std::string("99999.999")}
    });

    SQLRETURN ret = SQLFetch(stmt);
    double val;
    SQLLEN ind;
    ret = SQLGetData(stmt, 1, SQL_C_DOUBLE, &val, 0, &ind);
    AssertTrue(ret == SQL_SUCCESS, "GetData double");
    AssertTrue(val > 19.98 && val < 20.0, "Price ~19.99");

    ret = SQLFetch(stmt);
    ret = SQLGetData(stmt, 1, SQL_C_DOUBLE, &val, 0, &ind);
    AssertTrue(val > 0.009 && val < 0.011, "Price ~0.01");

    ret = SQLFetch(stmt);
    ret = SQLGetData(stmt, 1, SQL_C_DOUBLE, &val, 0, &ind);
    AssertTrue(val > 99999.0, "Price > 99999");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 5: Statement and Connection Attributes
// Statement/connection attributes set by Access
// ============================================================================

// 5-1: Basic connection attributes set by Access
TEST(AccessWorkflow, ConnectionAttributeSettings) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    // Access checks autocommit mode
    SQLUINTEGER autocommit;
    SQLINTEGER out_len;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_AUTOCOMMIT, &autocommit, sizeof(autocommit), &out_len);
    AssertTrue(ret == SQL_SUCCESS, "Get AUTOCOMMIT");
    AssertTrue(autocommit == SQL_AUTOCOMMIT_ON, "Autocommit is ON");

    // Access sets connection timeout
    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)30, 0);
    AssertTrue(ret == SQL_SUCCESS, "Set connection timeout");

    // Check access mode
    SQLUINTEGER access_mode;
    ret = SQLGetConnectAttr(dbc, SQL_ATTR_ACCESS_MODE, &access_mode, sizeof(access_mode), &out_len);
    AssertTrue(ret == SQL_SUCCESS, "Get ACCESS_MODE");

    // Current catalog
    SQLCHAR catalog[256];
    ret = SQLGetConnectAttr(dbc, SQL_ATTR_CURRENT_CATALOG, catalog, sizeof(catalog), &out_len);
    AssertTrue(ret == SQL_SUCCESS, "Get CURRENT_CATALOG");

    FreeAccessHandles(env, dbc);
}

// 5-2: Statement attributes set by Access
TEST(AccessWorkflow, StatementAttributeSettings) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Access sets rowset size via SQLSetStmtAttr
    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)1, 0);
    AssertTrue(ret == SQL_SUCCESS, "Set ROW_ARRAY_SIZE");

    // Cursor type
    ret = SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_TYPE,
                          (SQLPOINTER)(SQLULEN)SQL_CURSOR_FORWARD_ONLY, 0);
    AssertTrue(ret == SQL_SUCCESS, "Set CURSOR_TYPE");

    // Concurrency
    ret = SQLSetStmtAttr(stmt, SQL_ATTR_CONCURRENCY,
                          (SQLPOINTER)(SQLULEN)SQL_CONCUR_READ_ONLY, 0);
    AssertTrue(ret == SQL_SUCCESS, "Set CONCURRENCY");

    // Query timeout
    ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, (SQLPOINTER)30, 0);
    AssertTrue(ret == SQL_SUCCESS, "Set QUERY_TIMEOUT");

    // Verify values can be retrieved
    SQLULEN cursor_type;
    SQLINTEGER out_len;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_CURSOR_TYPE, &cursor_type, sizeof(cursor_type), &out_len);
    AssertTrue(ret == SQL_SUCCESS, "Get CURSOR_TYPE");
    AssertTrue(cursor_type == SQL_CURSOR_FORWARD_ONLY, "Cursor is forward only");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 5-3: Access transactions (ClickHouse does not support them, but returns no error)
TEST(AccessWorkflow, TransactionHandling) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    // Access may call SQLEndTran — ClickHouse always returns SQL_SUCCESS
    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_COMMIT);
    AssertTrue(ret == SQL_SUCCESS, "COMMIT succeeds (no-op)");

    ret = SQLEndTran(SQL_HANDLE_DBC, dbc, SQL_ROLLBACK);
    AssertTrue(ret == SQL_SUCCESS, "ROLLBACK succeeds (no-op)");

    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 6: Diagnostic Information
// Access retrieves details via SQLGetDiagRec when an error occurs
// ============================================================================

// 6-1: Retrieve diagnostic records after an error
TEST(AccessWorkflow, DiagnosticRecordRetrieval) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Simulate an error
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test error message", 42);

    SQLCHAR sqlstate[6];
    SQLINTEGER native_error;
    SQLCHAR msg[256];
    SQLSMALLINT msg_len;

    SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1,
                                   sqlstate, &native_error, msg, sizeof(msg), &msg_len);
    AssertTrue(ret == SQL_SUCCESS, "GetDiagRec");
    AssertEqual(std::string("HY000"), std::string((char *)sqlstate), "SQLSTATE");
    AssertEqual(42, (int)native_error, "Native error");
    AssertTrue(msg_len > 0, "Message has content");

    // Record 2 does not exist
    ret = SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 2,
                         sqlstate, &native_error, msg, sizeof(msg), &msg_len);
    AssertTrue(ret == SQL_NO_DATA, "No more diag records");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 6-2: SQLGetDiagField — Access also retrieves header fields
TEST(AccessWorkflow, DiagFieldRetrieval) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Get record count
    SQLINTEGER record_count = 0;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 0,
                                     SQL_DIAG_NUMBER, &record_count, sizeof(record_count), nullptr);
    AssertTrue(ret == SQL_SUCCESS, "GetDiagField NUMBER");
    AssertEqual(0, (int)record_count, "No diag records initially");

    // After adding an error
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("42S02", "Table not found");
    ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 0,
                           SQL_DIAG_NUMBER, &record_count, sizeof(record_count), nullptr);
    AssertTrue(ret == SQL_SUCCESS, "GetDiagField after error");
    AssertEqual(1, (int)record_count, "1 diag record after error");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 7: SQLColAttribute / SQLColAttributes (Access heavily uses column metadata)
// ============================================================================

// 7-1: Typical sequence where Access inspects column metadata
TEST(AccessWorkflow, ColumnMetadataInspection) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"id", SQL_INTEGER},
        {"name", SQL_VARCHAR},
        {"created_at", SQL_TYPE_TIMESTAMP}
    }, {
        {std::string("1"), std::string("test"), std::string("2024-01-01 00:00:00")}
    });

    // Get column name via SQLColAttribute
    SQLCHAR col_name[128];
    SQLSMALLINT name_len;
    SQLLEN numeric_attr;

    SQLRETURN ret = SQLColAttribute(stmt, 1, SQL_DESC_NAME, col_name, sizeof(col_name), &name_len, nullptr);
    AssertTrue(ret == SQL_SUCCESS, "ColAttribute NAME col 1");
    AssertEqual(std::string("id"), std::string((char *)col_name, name_len), "Col 1 name is 'id'");

    ret = SQLColAttribute(stmt, 2, SQL_DESC_NAME, col_name, sizeof(col_name), &name_len, nullptr);
    AssertTrue(ret == SQL_SUCCESS, "ColAttribute NAME col 2");
    AssertEqual(std::string("name"), std::string((char *)col_name, name_len), "Col 2 name is 'name'");

    // SQL_DESC_TYPE (type code)
    ret = SQLColAttribute(stmt, 1, SQL_DESC_TYPE, nullptr, 0, nullptr, &numeric_attr);
    AssertTrue(ret == SQL_SUCCESS, "ColAttribute TYPE col 1");
    AssertEqual((__int64)SQL_INTEGER, (__int64)numeric_attr, "Col 1 type is INTEGER");

    // SQL_DESC_NULLABLE
    ret = SQLColAttribute(stmt, 2, SQL_DESC_NULLABLE, nullptr, 0, nullptr, &numeric_attr);
    AssertTrue(ret == SQL_SUCCESS, "ColAttribute NULLABLE col 2");

    // SQL_DESC_DISPLAY_SIZE (Access uses this to determine display width)
    ret = SQLColAttribute(stmt, 2, SQL_DESC_DISPLAY_SIZE, nullptr, 0, nullptr, &numeric_attr);
    AssertTrue(ret == SQL_SUCCESS, "ColAttribute DISPLAY_SIZE");
    AssertTrue(numeric_attr > 0, "Display size is positive");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 7-2: ODBC 2.x SQLColAttributes (legacy API used by Access)
TEST(AccessWorkflow, LegacyColAttributes) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"id", SQL_INTEGER},
        {"description", SQL_VARCHAR}
    }, {
        {std::string("1"), std::string("test")}
    });

    // ODBC 2.x: SQL_COLUMN_NAME
    SQLCHAR name_buf[128];
    SQLSMALLINT name_len;
    SQLLEN numeric;
    SQLRETURN ret = SQLColAttributes(stmt, 1, SQL_COLUMN_NAME, name_buf, sizeof(name_buf), &name_len, &numeric);
    AssertTrue(ret == SQL_SUCCESS, "ColAttributes COLUMN_NAME");
    AssertEqual(std::string("id"), std::string((char *)name_buf, name_len), "Column name via 2.x API");

    // ODBC 2.x: SQL_COLUMN_TYPE
    ret = SQLColAttributes(stmt, 1, SQL_COLUMN_TYPE, nullptr, 0, nullptr, &numeric);
    AssertTrue(ret == SQL_SUCCESS, "ColAttributes COLUMN_TYPE");
    AssertEqual((__int64)SQL_INTEGER, (__int64)numeric, "Type via 2.x API");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 8: Multiple Statements
// Access uses multiple statements simultaneously
// ============================================================================

// 8-1: Use multiple statements on the same connection
TEST(AccessWorkflow, MultipleStatements) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt1 = AllocTestStmt(dbc);
    SQLHSTMT stmt2 = AllocTestStmt(dbc);
    SQLHSTMT stmt3 = AllocTestStmt(dbc);

    AssertTrue(stmt1 != SQL_NULL_HSTMT, "Stmt1 allocated");
    AssertTrue(stmt2 != SQL_NULL_HSTMT, "Stmt2 allocated");
    AssertTrue(stmt3 != SQL_NULL_HSTMT, "Stmt3 allocated");

    // Set independent result sets for each statement
    PopulateMultiColumnResultSet(stmt1, {{"a", SQL_INTEGER}}, {{std::string("1")}});
    PopulateMultiColumnResultSet(stmt2, {{"b", SQL_VARCHAR}}, {{std::string("hello")}});
    PopulateMultiColumnResultSet(stmt3, {{"c", SQL_DOUBLE}}, {{std::string("3.14")}});

    // Fetch data independently from each statement
    SQLRETURN ret;
    SQLINTEGER val_int;
    SQLCHAR val_str[64];
    double val_dbl;
    SQLLEN ind;

    ret = SQLFetch(stmt1);
    AssertTrue(ret == SQL_SUCCESS, "Fetch stmt1");
    ret = SQLGetData(stmt1, 1, SQL_C_SLONG, &val_int, 0, &ind);
    AssertEqual(1, (int)val_int, "Stmt1 value");

    ret = SQLFetch(stmt2);
    AssertTrue(ret == SQL_SUCCESS, "Fetch stmt2");
    ret = SQLGetData(stmt2, 1, SQL_C_CHAR, val_str, sizeof(val_str), &ind);
    AssertEqual(std::string("hello"), std::string((char *)val_str), "Stmt2 value");

    ret = SQLFetch(stmt3);
    AssertTrue(ret == SQL_SUCCESS, "Fetch stmt3");
    ret = SQLGetData(stmt3, 1, SQL_C_DOUBLE, &val_dbl, 0, &ind);
    AssertTrue(val_dbl > 3.13 && val_dbl < 3.15, "Stmt3 value ~3.14");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt1);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt3);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 9: Access Cursor Name Operations
// ============================================================================

// 9-1: Get/set cursor name (Access uses this for positioned update)
TEST(AccessWorkflow, CursorNameOperations) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Get default cursor name
    SQLCHAR cursor_name[128];
    SQLSMALLINT name_len;
    SQLRETURN ret = SQLGetCursorName(stmt, cursor_name, sizeof(cursor_name), &name_len);
    AssertTrue(ret == SQL_SUCCESS, "GetCursorName default");
    AssertTrue(name_len > 0, "Default cursor name exists");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 10: Access SQLSetPos (Cursor Positioning)
// ============================================================================

// 10-1: Positioning via SQLSetPos
TEST(AccessWorkflow, SetPosPositioning) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"id", SQL_INTEGER}
    }, {
        {std::string("1")},
        {std::string("2")},
        {std::string("3")}
    });

    // Fetch a row first
    SQLRETURN ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Fetch");

    // SQL_POSITION to position on the current row
    ret = SQLSetPos(stmt, 1, SQL_POSITION, SQL_LOCK_NO_CHANGE);
    AssertTrue(ret == SQL_SUCCESS, "SetPos POSITION row 1");

    // SQL_REFRESH is a no-op
    ret = SQLSetPos(stmt, 0, SQL_REFRESH, SQL_LOCK_NO_CHANGE);
    AssertTrue(ret == SQL_SUCCESS, "SetPos REFRESH");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 11: SQLFreeStmt Modes (Access calls this frequently)
// ============================================================================

// 11-1: SQLFreeStmt SQL_CLOSE / SQL_UNBIND / SQL_RESET_PARAMS
TEST(AccessWorkflow, FreeStmtModes) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Create a result set
    PopulateMultiColumnResultSet(stmt, {{"x", SQL_INTEGER}}, {{std::string("1")}});

    // Bind column
    SQLINTEGER val;
    SQLLEN ind;
    SQLBindCol(stmt, 1, SQL_C_SLONG, &val, 0, &ind);

    // Bind parameter
    SQLCHAR param[] = "test";
    SQLLEN param_ind = SQL_NTS;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, param, sizeof(param), &param_ind);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertTrue(s->bound_columns.size() > 0, "Has bound columns");
    AssertTrue(s->bound_parameters.size() > 0, "Has bound params");

    // SQL_CLOSE — Close cursor (bindings are preserved)
    SQLRETURN ret = SQLFreeStmt(stmt, SQL_CLOSE);
    AssertTrue(ret == SQL_SUCCESS, "FreeStmt CLOSE");

    // SQL_UNBIND — Unbind columns
    ret = SQLFreeStmt(stmt, SQL_UNBIND);
    AssertTrue(ret == SQL_SUCCESS, "FreeStmt UNBIND");
    AssertTrue(s->bound_columns.size() == 0, "Columns unbound");

    // SQL_RESET_PARAMS — Unbind parameters
    ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);
    AssertTrue(ret == SQL_SUCCESS, "FreeStmt RESET_PARAMS");
    AssertTrue(s->bound_parameters.size() == 0, "Params reset");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 12: Access-Specific GetInfo Values
// SQLGetInfo values that Access specifically checks
// ============================================================================

// 12-1: String/numeric/datetime function bitmasks checked by Access
TEST(AccessWorkflow, FunctionBitmaskValues) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    SQLUINTEGER bitmask;
    SQLSMALLINT len;

    // String functions
    SQLRETURN ret = SQLGetInfo(dbc, SQL_STRING_FUNCTIONS, &bitmask, sizeof(bitmask), &len);
    AssertTrue(ret == SQL_SUCCESS, "Get STRING_FUNCTIONS");
    AssertTrue((bitmask & SQL_FN_STR_UCASE) != 0, "UCASE supported");
    AssertTrue((bitmask & SQL_FN_STR_LCASE) != 0, "LCASE supported");
    AssertTrue((bitmask & SQL_FN_STR_LENGTH) != 0, "LENGTH supported");
    AssertTrue((bitmask & SQL_FN_STR_SUBSTRING) != 0, "SUBSTRING supported");
    AssertTrue((bitmask & SQL_FN_STR_CONCAT) != 0, "CONCAT supported");
    AssertTrue((bitmask & SQL_FN_STR_LTRIM) != 0, "LTRIM supported");
    AssertTrue((bitmask & SQL_FN_STR_RTRIM) != 0, "RTRIM supported");

    // Numeric functions
    ret = SQLGetInfo(dbc, SQL_NUMERIC_FUNCTIONS, &bitmask, sizeof(bitmask), &len);
    AssertTrue(ret == SQL_SUCCESS, "Get NUMERIC_FUNCTIONS");
    AssertTrue((bitmask & SQL_FN_NUM_ABS) != 0, "ABS supported");
    AssertTrue((bitmask & SQL_FN_NUM_CEILING) != 0, "CEILING supported");
    AssertTrue((bitmask & SQL_FN_NUM_FLOOR) != 0, "FLOOR supported");
    AssertTrue((bitmask & SQL_FN_NUM_ROUND) != 0, "ROUND supported");

    // Date/time functions
    ret = SQLGetInfo(dbc, SQL_TIMEDATE_FUNCTIONS, &bitmask, sizeof(bitmask), &len);
    AssertTrue(ret == SQL_SUCCESS, "Get TIMEDATE_FUNCTIONS");
    AssertTrue((bitmask & SQL_FN_TD_NOW) != 0, "NOW supported");
    AssertTrue((bitmask & SQL_FN_TD_CURDATE) != 0, "CURDATE supported");
    AssertTrue((bitmask & SQL_FN_TD_YEAR) != 0, "YEAR supported");
    AssertTrue((bitmask & SQL_FN_TD_MONTH) != 0, "MONTH supported");
    AssertTrue((bitmask & SQL_FN_TD_DAYOFMONTH) != 0, "DAYOFMONTH supported");

    // CONVERT functions
    ret = SQLGetInfo(dbc, SQL_CONVERT_FUNCTIONS, &bitmask, sizeof(bitmask), &len);
    AssertTrue(ret == SQL_SUCCESS, "Get CONVERT_FUNCTIONS");
    AssertTrue((bitmask & SQL_FN_CVT_CONVERT) != 0, "CONVERT supported");

    FreeAccessHandles(env, dbc);
}

// 12-2: SQL syntax support checked by Access
TEST(AccessWorkflow, SqlSyntaxSupport) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    SQLSMALLINT len;

    // ORDER BY support
    char order_by[4];
    SQLRETURN ret = SQLGetInfo(dbc, SQL_ORDER_BY_COLUMNS_IN_SELECT, order_by, sizeof(order_by), &len);
    AssertTrue(ret == SQL_SUCCESS, "ORDER_BY_COLUMNS_IN_SELECT");

    // GROUP BY support
    SQLUSMALLINT group_by;
    ret = SQLGetInfo(dbc, SQL_GROUP_BY, &group_by, sizeof(group_by), &len);
    AssertTrue(ret == SQL_SUCCESS, "GROUP_BY");

    // LIKE escape character
    char like_escape[4];
    ret = SQLGetInfo(dbc, SQL_LIKE_ESCAPE_CLAUSE, like_escape, sizeof(like_escape), &len);
    AssertTrue(ret == SQL_SUCCESS, "LIKE_ESCAPE_CLAUSE");

    // Subquery support
    SQLUINTEGER subqueries;
    ret = SQLGetInfo(dbc, SQL_SUBQUERIES, &subqueries, sizeof(subqueries), &len);
    AssertTrue(ret == SQL_SUCCESS, "SUBQUERIES");

    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 13: Complete Workflow — Table Link → Data Read → Filter
// Simulate the entire typical Access user operation flow
// ============================================================================

// 13-1: End-to-End Workflow (integration test)
TEST(AccessWorkflow, EndToEndTableLinkAndRead) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);

    // Phase A: Check driver information
    char driver_name[128];
    SQLSMALLINT len;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_DRIVER_NAME, driver_name, sizeof(driver_name), &len);
    AssertTrue(ret == SQL_SUCCESS, "Phase A: Get driver name");

    // Phase B: Discover table structure via catalog functions
    SQLHSTMT cat_stmt = AllocTestStmt(dbc);

    // SQLStatistics (check indexes)
    ret = SQLStatistics(cat_stmt, nullptr, 0, nullptr, 0,
                         (SQLCHAR *)"test_table", SQL_NTS, SQL_INDEX_ALL, SQL_QUICK);
    AssertTrue(ret == SQL_SUCCESS, "Phase B: Statistics");
    SQLFreeStmt(cat_stmt, SQL_CLOSE);

    // SQLSpecialColumns (check row ID)
    ret = SQLSpecialColumns(cat_stmt, SQL_BEST_ROWID, nullptr, 0, nullptr, 0,
                             (SQLCHAR *)"test_table", SQL_NTS, SQL_SCOPE_SESSION, SQL_NULLABLE);
    AssertTrue(ret == SQL_SUCCESS, "Phase B: SpecialColumns");
    SQLFreeStmt(cat_stmt, SQL_CLOSE);

    SQLFreeHandle(SQL_HANDLE_STMT, cat_stmt);

    // Phase C: Read data
    SQLHSTMT data_stmt = AllocTestStmt(dbc);
    PopulateMultiColumnResultSet(data_stmt, {
        {"id", SQL_INTEGER},
        {"name", SQL_VARCHAR},
        {"amount", SQL_DOUBLE},
        {"order_date", SQL_TYPE_DATE}
    }, {
        {std::string("1"), std::string("Alice"), std::string("100.50"), std::string("2024-01-15")},
        {std::string("2"), std::string("Bob"), std::string("200.75"), std::string("2024-02-20")},
        {std::string("3"), std::string("Charlie"), std::string("50.00"), std::string("2024-03-10")}
    });

    // Check column information
    SQLSMALLINT num_cols;
    ret = SQLNumResultCols(data_stmt, &num_cols);
    AssertEqual(4, (int)num_cols, "Phase C: 4 columns");

    // Fetch data using bound columns
    SQLINTEGER id;
    SQLCHAR name[128];
    double amount;
    SQL_DATE_STRUCT dt;
    SQLLEN id_ind, name_ind, amount_ind, dt_ind;

    SQLBindCol(data_stmt, 1, SQL_C_SLONG, &id, 0, &id_ind);
    SQLBindCol(data_stmt, 2, SQL_C_CHAR, name, sizeof(name), &name_ind);
    SQLBindCol(data_stmt, 3, SQL_C_DOUBLE, &amount, 0, &amount_ind);
    SQLBindCol(data_stmt, 4, SQL_C_TYPE_DATE, &dt, sizeof(dt), &dt_ind);

    // Row 1
    ret = SQLFetch(data_stmt);
    AssertTrue(ret == SQL_SUCCESS, "Phase C: Fetch row 1");
    AssertEqual(1, (int)id, "Row 1 id");
    AssertEqual(std::string("Alice"), std::string((char *)name), "Row 1 name");
    AssertTrue(amount > 100.0 && amount < 101.0, "Row 1 amount");
    AssertEqual(2024, (int)dt.year, "Row 1 date year");
    AssertEqual(1, (int)dt.month, "Row 1 date month");
    AssertEqual(15, (int)dt.day, "Row 1 date day");

    // Row 2
    ret = SQLFetch(data_stmt);
    AssertTrue(ret == SQL_SUCCESS, "Phase C: Fetch row 2");
    AssertEqual(2, (int)id, "Row 2 id");
    AssertEqual(std::string("Bob"), std::string((char *)name), "Row 2 name");

    // Row 3
    ret = SQLFetch(data_stmt);
    AssertTrue(ret == SQL_SUCCESS, "Phase C: Fetch row 3");
    AssertEqual(3, (int)id, "Row 3 id");

    // End of data
    ret = SQLFetch(data_stmt);
    AssertTrue(ret == SQL_NO_DATA, "Phase C: No more rows");

    SQLFreeHandle(SQL_HANDLE_STMT, data_stmt);

    // Phase D: Prepare a query with escape sequences
    SQLHSTMT esc_stmt = AllocTestStmt(dbc);
    ret = SQLPrepare(esc_stmt,
        (SQLCHAR *)"SELECT * FROM orders WHERE order_date >= {d '2024-01-01'} AND {fn YEAR(order_date)} = 2024",
        SQL_NTS);
    AssertTrue(ret == SQL_SUCCESS, "Phase D: Prepare with escapes");

    // Verify escapes have been processed
    auto *es = static_cast<OdbcStatement *>(esc_stmt);
    // Escapes already processed by SQLPrepare
    AssertTrue(es->query.find("{d") == std::string::npos, "Date escape processed");
    AssertTrue(es->query.find("{fn") == std::string::npos, "Function escape processed");
    AssertTrue(es->query.find("toYear") != std::string::npos, "YEAR -> toYear");

    SQLFreeHandle(SQL_HANDLE_STMT, esc_stmt);

    // Phase E: Cleanup
    FreeAccessHandles(env, dbc);
}

// 13-2: Access "Form" view sequence (display one record at a time)
TEST(AccessWorkflow, FormViewSingleRecordNavigation) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Set rowset_size = 1 (Access Form View fetches one row at a time)
    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)1, 0);
    AssertTrue(ret == SQL_SUCCESS, "Set rowset_size = 1");

    PopulateMultiColumnResultSet(stmt, {
        {"id", SQL_INTEGER},
        {"name", SQL_VARCHAR}
    }, {
        {std::string("101"), std::string("Record A")},
        {std::string("102"), std::string("Record B")},
        {std::string("103"), std::string("Record C")}
    });

    SQLCHAR name_buf[128];
    SQLLEN ind;

    // Forward navigation (Next record)
    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Navigate to record 1");
    SQLGetData(stmt, 2, SQL_C_CHAR, name_buf, sizeof(name_buf), &ind);
    AssertEqual(std::string("Record A"), std::string((char *)name_buf), "Record 1");

    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Navigate to record 2");
    SQLGetData(stmt, 2, SQL_C_CHAR, name_buf, sizeof(name_buf), &ind);
    AssertEqual(std::string("Record B"), std::string((char *)name_buf), "Record 2");

    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Navigate to record 3");
    SQLGetData(stmt, 2, SQL_C_CHAR, name_buf, sizeof(name_buf), &ind);
    AssertEqual(std::string("Record C"), std::string((char *)name_buf), "Record 3");

    ret = SQLFetch(stmt);
    AssertTrue(ret == SQL_NO_DATA, "Past last record");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 14: Escape sequences for aggregate queries used by Access Reports
// ============================================================================

// 14-1: Aggregate + date function combination generated by Access Reports
TEST(AccessWorkflow, ReportAggregateWithDateFunctions) {
    // Typical query generated by Access Reports
    std::string input =
        "SELECT {fn YEAR(order_date)} AS yr, "
        "{fn MONTH(order_date)} AS mo, "
        "COUNT(*), SUM(amount) "
        "FROM orders "
        "WHERE order_date BETWEEN {d '2024-01-01'} AND {d '2024-12-31'} "
        "GROUP BY {fn YEAR(order_date)}, {fn MONTH(order_date)}";
    std::string result = ProcessOdbcEscapeSequences(input);

    // Verify all escapes have been processed
    AssertTrue(result.find("{fn") == std::string::npos, "No fn escapes remain");
    AssertTrue(result.find("{d") == std::string::npos, "No date escapes remain");
    AssertTrue(result.find("toYear") != std::string::npos, "YEAR -> toYear");
    AssertTrue(result.find("toMonth") != std::string::npos, "MONTH -> toMonth");
    AssertTrue(result.find("'2024-01-01'") != std::string::npos, "Start date preserved");
    AssertTrue(result.find("'2024-12-31'") != std::string::npos, "End date preserved");
}

// 14-2: Access IFNULL conversion (used by Access Nz function)
TEST(AccessWorkflow, IfNullConversion) {
    // Access Nz() maps to ODBC IFNULL
    std::string input = "SELECT {fn IFNULL(description, 'N/A')} FROM products";
    std::string result = ProcessOdbcEscapeSequences(input);
    AssertEqual(std::string("SELECT ifNull(description, 'N/A') FROM products"), result,
                "IFNULL -> ifNull for Access Nz()");
}

// ============================================================================
// Scenario 15: ClickHouse Type Mapping Verification
// Verify that type conversions required by Access are performed correctly
// ============================================================================

// 15-1: ClickHouse type → ODBC SQL type mapping
TEST(AccessWorkflow, ClickHouseTypeMapping) {
    // Basic type mapping
    AssertEqual((int)SQL_INTEGER, (int)ClickHouseTypeToSqlType("Int32"), "Int32 -> SQL_INTEGER");
    AssertEqual((int)SQL_BIGINT, (int)ClickHouseTypeToSqlType("Int64"), "Int64 -> SQL_BIGINT");
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("String"), "String -> SQL_VARCHAR");
    AssertEqual((int)SQL_DOUBLE, (int)ClickHouseTypeToSqlType("Float64"), "Float64 -> SQL_DOUBLE");
    AssertEqual((int)SQL_TYPE_DATE, (int)ClickHouseTypeToSqlType("Date"), "Date -> SQL_TYPE_DATE");
    AssertEqual((int)SQL_TYPE_TIMESTAMP, (int)ClickHouseTypeToSqlType("DateTime"), "DateTime -> SQL_TYPE_TIMESTAMP");
    AssertEqual((int)SQL_TINYINT, (int)ClickHouseTypeToSqlType("UInt8"), "UInt8 -> SQL_TINYINT");
    AssertEqual((int)SQL_SMALLINT, (int)ClickHouseTypeToSqlType("Int16"), "Int16 -> SQL_SMALLINT");

    // Nullable wrapper
    AssertEqual((int)SQL_INTEGER, (int)ClickHouseTypeToSqlType("Nullable(Int32)"), "Nullable(Int32) -> SQL_INTEGER");
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Nullable(String)"), "Nullable(String) -> SQL_VARCHAR");

    // LowCardinality
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("LowCardinality(String)"), "LowCardinality(String) -> SQL_VARCHAR");
}

// 15-2: Column size calculation (Access uses this value to determine field width)
TEST(AccessWorkflow, ColumnSizeCalculation) {
    // FixedString
    SQLULEN size = GetColumnSizeForType("FixedString(100)", SQL_CHAR);
    AssertEqual((__int64)100, (__int64)size, "FixedString(100) size");

    // Decimal
    size = GetColumnSizeForType("Decimal(18,4)", SQL_DECIMAL);
    AssertEqual((__int64)18, (__int64)size, "Decimal(18,4) precision");

    // Basic types (driver returns default size)
    size = GetColumnSizeForType("Int32", SQL_INTEGER);
    AssertTrue(size > 0, "Int32 has column size");

    size = GetColumnSizeForType("String", SQL_VARCHAR);
    AssertTrue(size > 0, "String has column size");
}

// ============================================================================
// Scenario 16: Access Pass-Through Queries
// Access "pass-through queries" use SQLExecDirect + escape processing
// ============================================================================

// 16-1: Escape processing integration in Access pass-through queries
TEST(AccessWorkflow, PassthroughQueryEscapeIntegration) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    // Access pass-through queries apply escape processing via SQLPrepare
    SQLRETURN ret = SQLPrepare(stmt,
        (SQLCHAR *)"SELECT {fn UCASE(name)}, {fn NOW()} FROM users WHERE id > {fn ABS(-5)}",
        SQL_NTS);
    AssertTrue(ret == SQL_SUCCESS, "Prepare passthrough query");

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Verify the processed query
    AssertTrue(s->query.find("upper(name)") != std::string::npos, "UCASE converted");
    AssertTrue(s->query.find("now()") != std::string::npos, "NOW converted");
    AssertTrue(s->query.find("abs(-5)") != std::string::npos, "ABS converted");
    AssertTrue(s->query.find("{fn") == std::string::npos, "No escape sequences remain");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 16-2: LIKE escape (Access search feature)
TEST(AccessWorkflow, LikeEscapeSequence) {
    std::string input = "SELECT * FROM users WHERE name LIKE '%test%' {escape '\\'}";
    std::string result = ProcessOdbcEscapeSequences(input);
    // LIKE escape is passed through
    AssertTrue(result.find("LIKE '%test%'") != std::string::npos, "LIKE pattern preserved");
    AssertTrue(result.find("ESCAPE '\\'") != std::string::npos, "ESCAPE clause generated");
}

// ============================================================================
// Scenario 17: Outer Joins (used by Access for relationship display)
// ============================================================================

// 17-1: Access outer join escape
TEST(AccessWorkflow, OuterJoinEscape) {
    std::string input =
        "SELECT * FROM {oj users LEFT OUTER JOIN orders ON users.id = orders.user_id}";
    std::string result = ProcessOdbcEscapeSequences(input);
    AssertTrue(result.find("users LEFT OUTER JOIN orders") != std::string::npos,
               "Outer join preserved");
    AssertTrue(result.find("{oj") == std::string::npos, "OJ escape removed");
}

// ============================================================================
// Scenario 18: RowCount and SQLMoreResults
// ============================================================================

// 18-1: RowCount after SELECT (Access checks the count)
TEST(AccessWorkflow, RowCountAfterSelect) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    PopulateMultiColumnResultSet(stmt, {
        {"id", SQL_INTEGER}
    }, {
        {std::string("1")},
        {std::string("2")},
        {std::string("3")}
    });

    // Access calls SQLRowCount after fetching all rows
    while (SQLFetch(stmt) == SQL_SUCCESS) {}

    SQLLEN row_count;
    SQLRETURN ret = SQLRowCount(stmt, &row_count);
    AssertTrue(ret == SQL_SUCCESS, "RowCount after fetch");
    // For SELECT, return the row count
    AssertTrue(row_count >= 0, "RowCount is non-negative");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// 18-2: SQLCancel (Access cancels a query)
TEST(AccessWorkflow, CancelQuery) {
    SQLHENV env; SQLHDBC dbc;
    CreateAccessHandles(env, dbc);
    SQLHSTMT stmt = AllocTestStmt(dbc);

    SQLRETURN ret = SQLCancel(stmt);
    AssertTrue(ret == SQL_SUCCESS, "Cancel on idle statement");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeAccessHandles(env, dbc);
}

// ============================================================================
// Scenario 19: Proper Handle Cleanup
// Cleanup sequence when Access exits
// ============================================================================

// 19-1: Complete cleanup sequence
TEST(AccessWorkflow, CleanShutdownSequence) {
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLHSTMT stmt1 = SQL_NULL_HSTMT, stmt2 = SQL_NULL_HSTMT;

    // Startup
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt1);
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt2);

    // Usage
    PopulateMultiColumnResultSet(stmt1, {{"x", SQL_INTEGER}}, {{std::string("1")}});
    SQLFetch(stmt1);

    // Shutdown — Access closes in this order
    // 1. Close statements
    SQLFreeStmt(stmt1, SQL_CLOSE);
    SQLFreeStmt(stmt2, SQL_CLOSE);

    // 2. Free statements
    SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt1);
    AssertTrue(ret == SQL_SUCCESS, "Free stmt1");
    ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    AssertTrue(ret == SQL_SUCCESS, "Free stmt2");

    // 3. Disconnect
    conn->connected = false;

    // 4. Free connection handle
    ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    AssertTrue(ret == SQL_SUCCESS, "Free dbc");

    // 5. Free environment handle
    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    AssertTrue(ret == SQL_SUCCESS, "Free env");
}
