// Phase 15 Tests: Driver quality enhancement (additional bug fixes)
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

// Include driver headers
#include "../src/include/handle.h"
#include "../src/include/util.h"
#include "../src/include/type_mapping.h"

#include <httplib.h>

#ifdef UNICODE
#undef SQLColumns
#undef SQLTables
#undef SQLExecDirect
#undef SQLGetDiagRec
#undef SQLGetDiagField
#undef SQLError
#undef SQLDriverConnect
#undef SQLPrepare
#undef SQLDescribeCol
#endif

#pragma warning(disable: 4996)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLColumns(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                         SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLColumnsW(SQLHSTMT, SQLWCHAR *, SQLSMALLINT, SQLWCHAR *, SQLSMALLINT,
                                          SQLWCHAR *, SQLSMALLINT, SQLWCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLTables(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT,
                                        SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLTablesW(SQLHSTMT, SQLWCHAR *, SQLSMALLINT, SQLWCHAR *, SQLSMALLINT,
                                         SQLWCHAR *, SQLSMALLINT, SQLWCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLExecDirect(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLCHAR *, SQLINTEGER *,
                                            SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLWCHAR *, SQLINTEGER *,
                                             SQLWCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLSMALLINT,
                                              SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLSMALLINT,
                                               SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLError(SQLHENV, SQLHDBC, SQLHSTMT, SQLCHAR *, SQLINTEGER *,
                                       SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLErrorW(SQLHENV, SQLHDBC, SQLHSTMT, SQLWCHAR *, SQLINTEGER *,
                                        SQLWCHAR *, SQLSMALLINT, SQLSMALLINT *);

extern "C" SQLRETURN SQL_API SQLDriverConnect(SQLHDBC, SQLHWND, SQLCHAR *, SQLSMALLINT,
                                               SQLCHAR *, SQLSMALLINT, SQLSMALLINT *, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT, SQLUSMALLINT, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *,
                                             SQLSMALLINT *, SQLULEN *, SQLSMALLINT *, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// Helper: create env + conn + stmt
// ============================================================================
static void CreateEnvConn(SQLHENV &env, SQLHDBC &dbc) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
}

static void FreeEnvConn(SQLHENV env, SQLHDBC dbc) {
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

static SQLHSTMT CreateStmt(SQLHDBC dbc) {
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    return stmt;
}

// ============================================================================
// 1. SQLColumns SQL_DATA_TYPE fix for datetime types
// ============================================================================
TEST(Phase15_ColumnsDataType, DateTimeDataTypeIsSQL_DATETIME) {
    // Verify that BuildTypeInfoResultSet correctly uses SQL_DATETIME
    // for date/time/timestamp types in SQL_DATA_TYPE column
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_TYPE_DATE);

    // The SQL_DATA_TYPE column (index 15, 0-based) should be SQL_DATETIME(9), not SQL_TYPE_DATE(91)
    AssertTrue(!rs.rows.empty(), "Should have rows for SQL_TYPE_DATE");
    if (!rs.rows.empty()) {
        const auto &row = rs.rows[0];
        // Column index 15 = SQL_DATA_TYPE
        std::string sql_data_type = row[15].value_or("");
        AssertEqual(std::to_string(SQL_DATETIME), sql_data_type);
    }
}

TEST(Phase15_ColumnsDataType, TimestampDataTypeIsSQL_DATETIME) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_TYPE_TIMESTAMP);

    AssertTrue(!rs.rows.empty(), "Should have rows for SQL_TYPE_TIMESTAMP");
    if (!rs.rows.empty()) {
        const auto &row = rs.rows[0];
        std::string sql_data_type = row[15].value_or("");
        AssertEqual(std::to_string(SQL_DATETIME), sql_data_type);
    }
}

TEST(Phase15_ColumnsDataType, TimeDataTypeIsSQL_DATETIME) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_TYPE_TIME);

    AssertTrue(!rs.rows.empty(), "Should have rows for SQL_TYPE_TIME");
    if (!rs.rows.empty()) {
        const auto &row = rs.rows[0];
        std::string sql_data_type = row[15].value_or("");
        AssertEqual(std::to_string(SQL_DATETIME), sql_data_type);
    }
}

TEST(Phase15_ColumnsDataType, IntegerDataTypeIsSameAsConcise) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_INTEGER);

    AssertTrue(!rs.rows.empty(), "Should have rows for SQL_INTEGER");
    if (!rs.rows.empty()) {
        const auto &row = rs.rows[0];
        // For non-datetime types, SQL_DATA_TYPE should be same as DATA_TYPE
        std::string sql_data_type = row[15].value_or("");
        std::string data_type = row[1].value_or("");
        AssertEqual(data_type, sql_data_type);
    }
}

// ============================================================================
// 2. ExecuteQuery URL encoding (unit test for parameter safety)
// ============================================================================
TEST(Phase15_URLEncoding, SpecialCharsInParamsToQueryStr) {
    // Verify httplib's params_to_query_str handles special chars
    httplib::Params params;
    params.emplace("user", "testuser");
    params.emplace("password", "t@st&pass=123");
    params.emplace("database", "my db");

    std::string qs = httplib::detail::params_to_query_str(params);

    // Password should be URL-encoded (& and = are special)
    AssertTrue(qs.find("t@st&pass=123") == std::string::npos,
               "Special chars should be URL-encoded");
    // The encoded form should contain the encoded password
    AssertTrue(qs.find("password=") != std::string::npos,
               "Should contain password key");
    // Space in database name should be encoded
    AssertTrue(qs.find("database=my db") == std::string::npos,
               "Space should be URL-encoded");
}

TEST(Phase15_URLEncoding, NormalParamsUnchanged) {
    httplib::Params params;
    params.emplace("user", "default");
    params.emplace("database", "test");

    std::string qs = httplib::detail::params_to_query_str(params);

    AssertTrue(qs.find("user=default") != std::string::npos, "Normal params should be present");
    AssertTrue(qs.find("database=test") != std::string::npos, "Normal params should be present");
}

// ============================================================================
// 3. SQLGetDiagFieldW string field wide conversion
// ============================================================================
TEST(Phase15_DiagFieldW, DynamicFunctionReturnsWide) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    // SQL_DIAG_DYNAMIC_FUNCTION is a header string field (RecNumber=0)
    SQLWCHAR buf[128] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetDiagFieldW(SQL_HANDLE_STMT, stmt, 0, SQL_DIAG_DYNAMIC_FUNCTION,
                                      buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    // Should return empty string in wide format (not corrupted)
    // Length should be 0 (empty string)
    AssertEqual((int)0, (int)len);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

TEST(Phase15_DiagFieldW, ClassOriginReturnsWide) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    // Set an error so we have a diagnostic record
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test error for class origin");

    // SQL_DIAG_CLASS_ORIGIN is a record string field
    SQLWCHAR buf[128] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetDiagFieldW(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_CLASS_ORIGIN,
                                      buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    // Should be "ODBC 3.0" in wide chars
    // len is in bytes for SQLGetDiagFieldW
    AssertTrue(len > 0, "Class origin should have content");
    // Verify it's actually wide (not ANSI corruption)
    std::wstring result(buf);
    AssertTrue(result == L"ODBC 3.0", "Should be 'ODBC 3.0' in wide string");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

TEST(Phase15_DiagFieldW, ServerNameReturnsWide) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    conn->host = "clickhouse.example.com";

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test error");

    SQLWCHAR buf[256] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetDiagFieldW(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_SERVER_NAME,
                                      buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue(len > 0, "Server name should have content");
    std::wstring result(buf);
    AssertTrue(result == L"clickhouse.example.com", "Should match host in wide string");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

TEST(Phase15_DiagFieldW, SqlstateReturnsWide) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("42S02", "Table not found");

    SQLWCHAR buf[64] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetDiagFieldW(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_SQLSTATE,
                                      buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    std::wstring result(buf);
    AssertTrue(result == L"42S02", "SQLSTATE should be '42S02' in wide");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

// ============================================================================
// 4. SQLGetDiagRecW / SQLErrorW TextLength fix
// ============================================================================
TEST(Phase15_DiagRecW, TextLengthReturnsCharCount) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test message");

    // Call with null MessageText to get length
    SQLWCHAR state[6];
    SQLINTEGER native;
    SQLSMALLINT textLen = 0;
    SQLRETURN ret = SQLGetDiagRecW(SQL_HANDLE_STMT, stmt, 1, state, &native, nullptr, 0, &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    // "Test message" = 12 chars. textLen should be 12 (char count), not 24 (byte count)
    AssertEqual((int)12, (int)textLen);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

TEST(Phase15_DiagRecW, TextLengthWithBuffer) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Hello");

    SQLWCHAR state[6];
    SQLINTEGER native;
    SQLWCHAR msgBuf[64] = {};
    SQLSMALLINT textLen = 0;
    SQLRETURN ret = SQLGetDiagRecW(SQL_HANDLE_STMT, stmt, 1, state, &native, msgBuf, 64, &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    // CopyStringToBufferW sets textLen to char count (5)
    AssertEqual((int)5, (int)textLen);
    std::wstring msg(msgBuf);
    AssertTrue(msg == L"Hello", "Message should be 'Hello'");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

TEST(Phase15_ErrorW, TextLengthReturnsWideCharCount) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY001", "Memory allocation error");

    // Call SQLErrorW with null MessageText to get length
    SQLWCHAR state[6];
    SQLINTEGER native;
    SQLSMALLINT textLen = 0;
    SQLRETURN ret = SQLErrorW(nullptr, nullptr, stmt, state, &native, nullptr, 0, &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    // "Memory allocation error" = 23 chars
    AssertEqual((int)23, (int)textLen);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

TEST(Phase15_ErrorW, TextLengthWithBuffer) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Error");

    SQLWCHAR state[6];
    SQLINTEGER native;
    SQLWCHAR msgBuf[64] = {};
    SQLSMALLINT textLen = 0;
    SQLRETURN ret = SQLErrorW(nullptr, nullptr, stmt, state, &native, msgBuf, 64, &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)5, (int)textLen);
    std::wstring msg(msgBuf);
    AssertTrue(msg == L"Error", "Message should be 'Error'");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

// ============================================================================
// 5. LOCATE escape function argument order
// ============================================================================
TEST(Phase15_LocateArgOrder, BasicLocateArgSwap) {
    // ODBC: {fn LOCATE('abc', col1)} → ClickHouse: positionUTF8(col1, 'abc')
    std::string sql = "SELECT {fn LOCATE('test', name)} FROM t";
    std::string result = ProcessOdbcEscapeSequences(sql);
    AssertTrue(result.find("positionUTF8(name, 'test')") != std::string::npos,
               "LOCATE args should be swapped for positionUTF8");
}

TEST(Phase15_LocateArgOrder, LocateWithThreeArgs) {
    // ODBC: {fn LOCATE('x', col, 5)} → ClickHouse: positionUTF8(col, 'x', 5)
    std::string sql = "SELECT {fn LOCATE('x', mycolumn, 5)} FROM t";
    std::string result = ProcessOdbcEscapeSequences(sql);
    AssertTrue(result.find("positionUTF8(mycolumn, 'x', 5)") != std::string::npos,
               "LOCATE with 3 args should swap first two and preserve third");
}

TEST(Phase15_LocateArgOrder, LocateWithExpressionArgs) {
    // ODBC: {fn LOCATE(a, b)} → ClickHouse: positionUTF8(b, a)
    std::string sql = "SELECT {fn LOCATE(needle_col, haystack_col)} FROM t";
    std::string result = ProcessOdbcEscapeSequences(sql);
    AssertTrue(result.find("positionUTF8(haystack_col, needle_col)") != std::string::npos,
               "LOCATE with column args should swap");
}

// ============================================================================
// 6. Regression tests — existing escape functions still work
// ============================================================================
TEST(Phase15_Regression, UCASEStillWorks) {
    std::string sql = "SELECT {fn UCASE(name)} FROM t";
    std::string result = ProcessOdbcEscapeSequences(sql);
    AssertTrue(result.find("upper(name)") != std::string::npos, "UCASE should still map to upper");
}

TEST(Phase15_Regression, DateLiteralStillWorks) {
    std::string sql = "SELECT * FROM t WHERE d = {d '2025-01-15'}";
    std::string result = ProcessOdbcEscapeSequences(sql);
    AssertTrue(result.find("'2025-01-15'") != std::string::npos, "Date literal should be preserved");
}

TEST(Phase15_Regression, DiagRecANSIStillWorks) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test");

    SQLCHAR state[6];
    SQLINTEGER native;
    SQLCHAR msgBuf[64];
    SQLSMALLINT textLen = 0;
    SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, &native, msgBuf, 64, &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)4, (int)textLen);
    AssertEqual(std::string("Test"), std::string((char *)msgBuf, textLen));

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

TEST(Phase15_Regression, DiagFieldANSIStillWorks) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test");

    SQLINTEGER diagNum = 0;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 0, SQL_DIAG_NUMBER, &diagNum, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)1, (int)diagNum);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

// ============================================================================
// 7. DSN host read via DriverConnectImpl (regression: DSN host was ignored)
// ============================================================================
TEST(Phase15_Regression, DriverConnectDsnHostIsRead) {
    // Test 1: Explicit HOST= in connection string overrides default
    {
        SQLHENV env;
        SQLHDBC dbc;
        CreateEnvConn(env, dbc);
        auto *conn = static_cast<OdbcConnection *>(dbc);

        AssertEqual(std::string("localhost"), conn->host);

        std::string cs = "HOST=198.51.100.1;PORT=19999;DATABASE=testdb;UID=testuser;PWD=testpass";
        SQLRETURN ret = SQLDriverConnect(dbc, nullptr,
            (SQLCHAR *)cs.c_str(), (SQLSMALLINT)cs.size(),
            nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);

        AssertEqual(std::string("198.51.100.1"), conn->host);
        AssertEqual((int)19999, (int)conn->port);
        AssertEqual(std::string("testdb"), conn->database);
        AssertEqual(std::string("testuser"), conn->user);
        AssertEqual(std::string("testpass"), conn->password);

        FreeEnvConn(env, dbc);
    }

    // Test 2: Connection string with HOST= and DSN= — HOST= should win
    {
        SQLHENV env;
        SQLHDBC dbc;
        CreateEnvConn(env, dbc);
        auto *conn = static_cast<OdbcConnection *>(dbc);

        std::string cs = "DSN=NonExistentDSN;HOST=203.0.113.50;PORT=8123";
        SQLRETURN ret = SQLDriverConnect(dbc, nullptr,
            (SQLCHAR *)cs.c_str(), (SQLSMALLINT)cs.size(),
            nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);

        AssertEqual(std::string("203.0.113.50"), conn->host);
        AssertEqual((int)8123, (int)conn->port);

        FreeEnvConn(env, dbc);
    }

    // Test 3: Connection string with only HOST=, no DSN — host should be used
    {
        SQLHENV env;
        SQLHDBC dbc;
        CreateEnvConn(env, dbc);
        auto *conn = static_cast<OdbcConnection *>(dbc);

        std::string cs = "HOST=203.0.113.50";
        SQLRETURN ret = SQLDriverConnect(dbc, nullptr,
            (SQLCHAR *)cs.c_str(), (SQLSMALLINT)cs.size(),
            nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);

        AssertEqual(std::string("203.0.113.50"), conn->host);

        FreeEnvConn(env, dbc);
    }
}

// ============================================================================
// SQLPrepare → SQLNumResultCols / SQLDescribeCol metadata retrieval test
// MS Access calls SQLPrepare → SQLNumResultCols when creating linked tables
// Verify that SQLNumResultCols/SQLDescribeCol can return metadata after SQLPrepare
// ============================================================================
TEST(Phase15, PreparedStatementMetadata) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);
    SQLHSTMT stmt = CreateStmt(dbc);
    auto *s = static_cast<OdbcStatement *>(stmt);

    // Simulate: prepare a SELECT query (manually populate result columns
    // since we don't have a real server for FetchPreparedMetadata)
    SQLRETURN ret = SQLPrepare(stmt, (SQLCHAR *)"SELECT id, name FROM users", SQL_NTS);
    AssertTrue(ret == SQL_SUCCESS, "SQLPrepare succeeds");
    AssertTrue(s->prepared, "Statement is marked as prepared");

    // Without server, SQLNumResultCols returns 0 (FetchPreparedMetadata
    // will fail silently without a server). Verify no crash.
    SQLSMALLINT num_cols = -1;
    ret = SQLNumResultCols(stmt, &num_cols);
    AssertTrue(ret == SQL_SUCCESS, "SQLNumResultCols succeeds on prepared stmt");
    // num_cols is 0 here because no server; in real use FetchPreparedMetadata
    // would populate columns via LIMIT 0 query.

    // Manually populate columns to simulate what FetchPreparedMetadata does
    s->result_set.columns.clear();
    {
        ColumnInfo ci;
        ci.name = "id";
        ci.sql_type = SQL_INTEGER;
        ci.column_size = 10;
        ci.decimal_digits = 0;
        ci.nullable = SQL_NO_NULLS;
        s->result_set.columns.push_back(ci);
    }
    {
        ColumnInfo ci;
        ci.name = "name";
        ci.sql_type = SQL_VARCHAR;
        ci.column_size = 255;
        ci.decimal_digits = 0;
        ci.nullable = SQL_NULLABLE;
        s->result_set.columns.push_back(ci);
    }

    // Now SQLNumResultCols should return 2
    ret = SQLNumResultCols(stmt, &num_cols);
    AssertTrue(ret == SQL_SUCCESS, "SQLNumResultCols with columns");
    AssertEqual(2, (int)num_cols, "2 columns available");

    // SQLDescribeCol should return correct info
    SQLCHAR col_name[128];
    SQLSMALLINT name_len, data_type, dec_digits, nullable;
    SQLULEN col_size;

    ret = SQLDescribeCol(stmt, 1, col_name, sizeof(col_name), &name_len,
                         &data_type, &col_size, &dec_digits, &nullable);
    AssertTrue(ret == SQL_SUCCESS, "SQLDescribeCol col 1");
    AssertEqual(std::string("id"), std::string((char *)col_name), "Col 1 name = id");
    AssertEqual((int)SQL_INTEGER, (int)data_type, "Col 1 type = SQL_INTEGER");

    ret = SQLDescribeCol(stmt, 2, col_name, sizeof(col_name), &name_len,
                         &data_type, &col_size, &dec_digits, &nullable);
    AssertTrue(ret == SQL_SUCCESS, "SQLDescribeCol col 2");
    AssertEqual(std::string("name"), std::string((char *)col_name), "Col 2 name = name");
    AssertEqual((int)SQL_VARCHAR, (int)data_type, "Col 2 type = SQL_VARCHAR");

    // Invalid column should still return error
    ret = SQLDescribeCol(stmt, 3, col_name, sizeof(col_name), &name_len,
                         &data_type, &col_size, &dec_digits, &nullable);
    AssertTrue(ret == SQL_ERROR, "SQLDescribeCol col 3 (out of range) = error");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    FreeEnvConn(env, dbc);
}

// ============================================================================
// ODBC Search Pattern Escape: verify escape processing logic
// Access sends "M\_ITEM" (with backslash-escaped underscore) meaning literal "M_ITEM"
// ============================================================================

// Replicate the driver's static helper functions for direct unit testing
namespace search_pattern_test {

static bool HasUnescapedWildcards(const std::string &pattern) {
    for (size_t i = 0; i < pattern.size(); i++) {
        if (pattern[i] == '\\' && i + 1 < pattern.size()) {
            i++;
            continue;
        }
        if (pattern[i] == '%' || pattern[i] == '_') return true;
    }
    return false;
}

static std::string StripSearchPatternEscape(const std::string &pattern) {
    std::string result;
    result.reserve(pattern.size());
    for (size_t i = 0; i < pattern.size(); i++) {
        if (pattern[i] == '\\' && i + 1 < pattern.size() &&
            (pattern[i + 1] == '_' || pattern[i + 1] == '%')) {
            result += pattern[i + 1];
            i++;
        } else {
            result += pattern[i];
        }
    }
    return result;
}

static std::string BuildPatternCondition(const std::string &column, const std::string &pattern) {
    if (pattern.empty() || pattern == "%") {
        return "";
    }
    if (HasUnescapedWildcards(pattern)) {
        return " AND " + column + " LIKE '" + pattern + "'";
    }
    std::string literal = StripSearchPatternEscape(pattern);
    return " AND " + column + " = '" + literal + "'";
}

} // namespace search_pattern_test

TEST(Phase15_SearchPatternEscape, StripEscapedUnderscore) {
    // "M\_ITEM" -> "M_ITEM" (Access sends this for literal M_ITEM)
    auto result = search_pattern_test::StripSearchPatternEscape("M\\_ITEM");
    AssertEqual(std::string("M_ITEM"), result, "M\\_ITEM -> M_ITEM");
}

TEST(Phase15_SearchPatternEscape, StripEscapedPercent) {
    // "100\%" -> "100%"
    auto result = search_pattern_test::StripSearchPatternEscape("100\\%");
    AssertEqual(std::string("100%"), result, "100\\% -> 100%");
}

TEST(Phase15_SearchPatternEscape, NoEscapeNeeded) {
    // "SALES" -> "SALES" (no wildcards, no escapes)
    auto result = search_pattern_test::StripSearchPatternEscape("SALES");
    AssertEqual(std::string("SALES"), result, "SALES unchanged");
}

TEST(Phase15_SearchPatternEscape, MultipleEscapedUnderscores) {
    // "A\_B\_C" -> "A_B_C"
    auto result = search_pattern_test::StripSearchPatternEscape("A\\_B\\_C");
    AssertEqual(std::string("A_B_C"), result, "A\\_B\\_C -> A_B_C");
}

TEST(Phase15_SearchPatternEscape, HasUnescapedWildcards_None) {
    AssertTrue(!search_pattern_test::HasUnescapedWildcards("M\\_ITEM"),
               "M\\_ITEM has no unescaped wildcards");
}

TEST(Phase15_SearchPatternEscape, HasUnescapedWildcards_Percent) {
    AssertTrue(search_pattern_test::HasUnescapedWildcards("M%"),
               "M% has unescaped wildcard");
}

TEST(Phase15_SearchPatternEscape, HasUnescapedWildcards_Underscore) {
    AssertTrue(search_pattern_test::HasUnescapedWildcards("M_ITEM"),
               "M_ITEM has unescaped wildcard _");
}

TEST(Phase15_SearchPatternEscape, HasUnescapedWildcards_EscapedOnly) {
    AssertTrue(!search_pattern_test::HasUnescapedWildcards("100\\%"),
               "100\\% has only escaped wildcards");
}

TEST(Phase15_SearchPatternEscape, BuildCondition_EscapedUnderscore) {
    // "M\_ITEM" → exact match with literal "M_ITEM"
    auto cond = search_pattern_test::BuildPatternCondition("name", "M\\_ITEM");
    AssertEqual(std::string(" AND name = 'M_ITEM'"), cond,
                "Escaped underscore builds exact match");
}

TEST(Phase15_SearchPatternEscape, BuildCondition_RealWildcard) {
    // "M%" → LIKE pattern
    auto cond = search_pattern_test::BuildPatternCondition("name", "M%");
    AssertEqual(std::string(" AND name LIKE 'M%'"), cond,
                "Real wildcard builds LIKE");
}

TEST(Phase15_SearchPatternEscape, BuildCondition_PlainName) {
    // "SALES" → exact match
    auto cond = search_pattern_test::BuildPatternCondition("table", "SALES");
    AssertEqual(std::string(" AND table = 'SALES'"), cond,
                "Plain name builds exact match");
}

TEST(Phase15_SearchPatternEscape, BuildCondition_MatchAll) {
    // "%" → no filter
    auto cond = search_pattern_test::BuildPatternCondition("name", "%");
    AssertEqual(std::string(""), cond, "% returns empty condition");
}
