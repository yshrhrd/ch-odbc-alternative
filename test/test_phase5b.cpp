// Phase 5b Tests: SQLDescribeParam, cursor names, DSN registry read, SQLSetPos
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

#ifdef UNICODE
#undef SQLPrepare
#undef SQLExecDirect
#undef SQLDescribeCol
#undef SQLColAttribute
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLNativeSql
#undef SQLGetDiagRec
#undef SQLGetCursorName
#undef SQLSetCursorName
#undef SQLDescribeParam
#endif

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLNumParams(SQLHSTMT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT *, SQLULEN *, SQLSMALLINT *, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLBindParameter(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                               SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER,
                                               SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLGetCursorName(SQLHSTMT, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLSetCursorName(SQLHSTMT, SQLCHAR *, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLSetPos(SQLHSTMT, SQLSETPOSIROW, SQLUSMALLINT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLCHAR *, SQLINTEGER *, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// Helper: create env + dbc + stmt handles
static void CreateTestHandles(SQLHENV &env, SQLHDBC &dbc, SQLHSTMT &stmt) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    // Set connected flag to allow statement allocation (no real server needed for unit tests)
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
}

static void FreeTestHandles(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt) {
    if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (dbc) {
        auto *conn = static_cast<OdbcConnection *>(dbc);
        conn->connected = false;
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// SQLDescribeParam test
// ============================================================================

TEST(Phase5b_DescribeParam, DefaultTypeForUnboundParam) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Prepare query with 2 params
    const char *query = "SELECT * FROM t WHERE a = ? AND b = ?";
    auto ret = SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLPrepare should succeed");

    // Describe unbound param => defaults to SQL_VARCHAR/255
    SQLSMALLINT dataType = 0;
    SQLULEN paramSize = 0;
    SQLSMALLINT decDigits = 0;
    SQLSMALLINT nullable = 0;
    ret = SQLDescribeParam(stmt, 1, &dataType, &paramSize, &decDigits, &nullable);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLDescribeParam should succeed");
    AssertEqual((int)SQL_VARCHAR, (int)dataType, "Default type should be SQL_VARCHAR");
    AssertTrue(paramSize == 255, "Default size should be 255");
    AssertEqual((int)SQL_NULLABLE, (int)nullable, "Params should be nullable");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_DescribeParam, DescribeBoundParam) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?, ?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    // Bind param 1 as INTEGER
    SQLINTEGER val = 42;
    SQLLEN ind = sizeof(val);
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 10, 0, &val, sizeof(val), &ind);

    // Describe bound param
    SQLSMALLINT dataType = 0;
    SQLULEN paramSize = 0;
    SQLSMALLINT decDigits = 0;
    SQLSMALLINT nullable = 0;
    auto ret = SQLDescribeParam(stmt, 1, &dataType, &paramSize, &decDigits, &nullable);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLDescribeParam should succeed");
    AssertEqual((int)SQL_INTEGER, (int)dataType, "Bound type should be SQL_INTEGER");
    AssertTrue(paramSize == 10, "Column size should be 10");

    // Describe unbound param 2 => defaults
    ret = SQLDescribeParam(stmt, 2, &dataType, &paramSize, &decDigits, &nullable);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLDescribeParam param 2 should succeed");
    AssertEqual((int)SQL_VARCHAR, (int)dataType, "Unbound default should be SQL_VARCHAR");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_DescribeParam, InvalidParamIndex) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "SELECT * FROM t WHERE a = ?";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLSMALLINT dataType;
    // Param 0 is invalid
    auto ret = SQLDescribeParam(stmt, 0, &dataType, nullptr, nullptr, nullptr);
    AssertEqual((int)SQL_ERROR, (int)ret, "Param 0 should fail");

    // Param 2 exceeds count
    ret = SQLDescribeParam(stmt, 2, &dataType, nullptr, nullptr, nullptr);
    AssertEqual((int)SQL_ERROR, (int)ret, "Param 2 should fail (only 1 param)");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_DescribeParam, NullOutputPointers) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "SELECT ?";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    // All output pointers null => should still succeed
    auto ret = SQLDescribeParam(stmt, 1, nullptr, nullptr, nullptr, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLDescribeParam with all null outputs should succeed");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_DescribeParam, InvalidHandle) {
    auto ret = SQLDescribeParam(SQL_NULL_HSTMT, 1, nullptr, nullptr, nullptr, nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle should return SQL_INVALID_HANDLE");
}

// ============================================================================
// SQLNumParams improvement test (skip ? inside quotes)
// ============================================================================

TEST(Phase5b_NumParams, QuotedQuestionMark) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // ? inside string literal should not count
    const char *query = "SELECT * FROM t WHERE a = '?' AND b = ?";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLSMALLINT count = 0;
    auto ret = SQLNumParams(stmt, &count);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLNumParams should succeed");
    AssertEqual(1, (int)count, "Only 1 real param (? inside quotes should not count)");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_NumParams, EscapedQuoteInsideLiteral) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Escaped quote '' inside string literal
    const char *query = "SELECT * FROM t WHERE a = 'it''s ?' AND b = ?";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLSMALLINT count = 0;
    SQLNumParams(stmt, &count);
    AssertEqual(1, (int)count, "Only 1 real param (escaped quote handling)");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_NumParams, MultipleParamsWithQuotes) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "SELECT ?, '?', ?, 'test''?', ?";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLSMALLINT count = 0;
    SQLNumParams(stmt, &count);
    AssertEqual(3, (int)count, "3 real params outside quotes");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetCursorName / SQLSetCursorName test
// ============================================================================

TEST(Phase5b_CursorName, DefaultCursorName) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLCHAR name[128] = {};
    SQLSMALLINT nameLen = 0;
    auto ret = SQLGetCursorName(stmt, name, sizeof(name), &nameLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLGetCursorName should succeed");
    AssertTrue(nameLen > 0, "Cursor name should have length > 0");

    // Should start with "SQL_CUR"
    std::string nameStr(reinterpret_cast<const char *>(name), nameLen);
    AssertTrue(nameStr.substr(0, 7) == "SQL_CUR", "Default cursor name should start with SQL_CUR");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_CursorName, SetAndGetCursorName) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Set cursor name
    const char *cursorName = "MY_CURSOR";
    auto ret = SQLSetCursorName(stmt, (SQLCHAR *)cursorName, SQL_NTS);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLSetCursorName should succeed");

    // Get cursor name back
    SQLCHAR name[128] = {};
    SQLSMALLINT nameLen = 0;
    ret = SQLGetCursorName(stmt, name, sizeof(name), &nameLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLGetCursorName should succeed");
    AssertEqual(std::string("MY_CURSOR"), std::string(reinterpret_cast<const char *>(name), nameLen), "Cursor name should match");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_CursorName, CursorNameTruncation) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetCursorName(stmt, (SQLCHAR *)"LONG_CURSOR_NAME", SQL_NTS);

    // Buffer too small
    SQLCHAR name[5] = {};
    SQLSMALLINT nameLen = 0;
    auto ret = SQLGetCursorName(stmt, name, 5, &nameLen);
    AssertEqual((int)SQL_SUCCESS_WITH_INFO, (int)ret, "Should return SQL_SUCCESS_WITH_INFO for truncation");
    AssertEqual((int)16, (int)nameLen, "Full length should be reported");
    AssertEqual(std::string("LONG"), std::string(reinterpret_cast<const char *>(name)), "Truncated name");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_CursorName, SetNullCursorName) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLSetCursorName(stmt, nullptr, SQL_NTS);
    AssertEqual((int)SQL_ERROR, (int)ret, "Null cursor name should fail");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_CursorName, SetCursorNameWithLength) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Use explicit length (take only first 3 chars)
    auto ret = SQLSetCursorName(stmt, (SQLCHAR *)"ABCDEF", 3);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLSetCursorName with explicit length");

    SQLCHAR name[128] = {};
    SQLSMALLINT nameLen = 0;
    SQLGetCursorName(stmt, name, sizeof(name), &nameLen);
    AssertEqual(std::string("ABC"), std::string(reinterpret_cast<const char *>(name), nameLen), "Should get first 3 chars");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_CursorName, InvalidHandle) {
    auto ret = SQLGetCursorName(SQL_NULL_HSTMT, nullptr, 0, nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle");

    ret = SQLSetCursorName(SQL_NULL_HSTMT, (SQLCHAR *)"X", SQL_NTS);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle");
}

TEST(Phase5b_CursorName, UniqueCursorNamesPerStatement) {
    SQLHENV env = SQL_NULL_HENV; SQLHDBC dbc = SQL_NULL_HDBC;
    SQLHSTMT stmt1 = SQL_NULL_HSTMT, stmt2 = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt1);
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt2);

    SQLCHAR name1[128] = {}, name2[128] = {};
    SQLSMALLINT len1 = 0, len2 = 0;
    SQLGetCursorName(stmt1, name1, sizeof(name1), &len1);
    SQLGetCursorName(stmt2, name2, sizeof(name2), &len2);

    std::string s1(reinterpret_cast<const char *>(name1), len1);
    std::string s2(reinterpret_cast<const char *>(name2), len2);
    AssertTrue(s1 != s2, "Each statement should have a unique cursor name");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt1);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt2);
    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// SQLSetPos test
// ============================================================================

TEST(Phase5b_SetPos, PositionCurrentRow) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Populate result set with a row so SQLSetPos can position on it
    auto *s = static_cast<OdbcStatement *>(stmt);
    ColumnInfo ci;
    ci.name = "col1";
    ci.sql_type = SQL_VARCHAR;
    ci.column_size = 255;
    s->result_set.columns.push_back(ci);
    s->result_set.rows.push_back({std::optional<std::string>("value1")});
    s->result_set.current_row = 0;

    // Position on current row (row 0 = current, row 1 = first)
    auto ret = SQLSetPos(stmt, 0, SQL_POSITION, SQL_LOCK_NO_CHANGE);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_POSITION row 0 should succeed");

    ret = SQLSetPos(stmt, 1, SQL_POSITION, SQL_LOCK_NO_CHANGE);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_POSITION row 1 should succeed");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_SetPos, PositionInvalidRow) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Position beyond row 1 => error for forward-only cursor
    auto ret = SQLSetPos(stmt, 5, SQL_POSITION, SQL_LOCK_NO_CHANGE);
    AssertEqual((int)SQL_ERROR, (int)ret, "SQL_POSITION row 5 should fail");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_SetPos, RefreshNoOp) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLSetPos(stmt, 0, SQL_REFRESH, SQL_LOCK_NO_CHANGE);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_REFRESH should succeed (no-op)");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_SetPos, UpdateNotSupported) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLSetPos(stmt, 1, SQL_UPDATE, SQL_LOCK_NO_CHANGE);
    AssertEqual((int)SQL_ERROR, (int)ret, "SQL_UPDATE should fail (not supported)");

    ret = SQLSetPos(stmt, 1, SQL_DELETE, SQL_LOCK_NO_CHANGE);
    AssertEqual((int)SQL_ERROR, (int)ret, "SQL_DELETE should fail (not supported)");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_SetPos, InvalidHandle) {
    auto ret = SQLSetPos(SQL_NULL_HSTMT, 0, SQL_POSITION, SQL_LOCK_NO_CHANGE);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle");
}

// ============================================================================
// SQLFreeStmt test (SQL_CLOSE preserves prepared query)
// ============================================================================

TEST(Phase5b_FreeStmt, ClosePreservesQuery) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "SELECT 1";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);
    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertTrue(s->prepared, "Should be prepared");
    AssertEqual(std::string("SELECT 1"), s->query, "Query should be set");

    // SQL_CLOSE: reset result_set but keep query and prepared flag
    auto ret = SQLFreeStmt(stmt, SQL_CLOSE);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_CLOSE should succeed");
    AssertTrue(s->prepared, "Should still be prepared after SQL_CLOSE");
    AssertEqual(std::string("SELECT 1"), s->query, "Query should be preserved after SQL_CLOSE");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_FreeStmt, UnbindClearsColumns) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Manually add a bound column
    BoundColumn col = {};
    s->bound_columns[1] = col;
    AssertTrue(!s->bound_columns.empty(), "Should have bound column");

    auto ret = SQLFreeStmt(stmt, SQL_UNBIND);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_UNBIND should succeed");
    AssertTrue(s->bound_columns.empty(), "Bound columns should be cleared");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase5b_FreeStmt, ResetParamsClearsParams) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    BoundParameter param = {};
    s->bound_parameters[1] = param;
    AssertTrue(!s->bound_parameters.empty(), "Should have bound param");

    auto ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_RESET_PARAMS should succeed");
    AssertTrue(s->bound_parameters.empty(), "Bound params should be cleared");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetFunctions test (verify new functions)
// ============================================================================

TEST(Phase5b_GetFunctions, DescribeParamSupported) {
    SQLHENV env = SQL_NULL_HENV; SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    auto ret = SQLGetFunctions(dbc, SQL_API_SQLDESCRIBEPARAM, &supported);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLGetFunctions should succeed");
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLDescribeParam should be supported");

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase5b_GetFunctions, CursorNameFunctionsSupported) {
    SQLHENV env = SQL_NULL_HENV; SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLGETCURSORNAME, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetCursorName should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLSETCURSORNAME, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetCursorName should be supported");

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase5b_GetFunctions, SetPosSupported) {
    SQLHENV env = SQL_NULL_HENV; SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLSETPOS, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetPos should be supported");

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase5b_GetFunctions, BitmapIncludesNewFunctions) {
    SQLHENV env = SQL_NULL_HENV; SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    SQLUSMALLINT bitmap[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE] = {};
    auto ret = SQLGetFunctions(dbc, SQL_API_ODBC3_ALL_FUNCTIONS, bitmap);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Bitmap query should succeed");

    // Check individual bits
    auto checkBit = [&](SQLUSMALLINT id) -> bool {
        return (bitmap[id >> 4] & (1 << (id & 0x000F))) != 0;
    };

    AssertTrue(checkBit(SQL_API_SQLDESCRIBEPARAM), "SQLDescribeParam in bitmap");
    AssertTrue(checkBit(SQL_API_SQLSETPOS), "SQLSetPos in bitmap");
    AssertTrue(checkBit(SQL_API_SQLGETCURSORNAME), "SQLGetCursorName in bitmap");
    AssertTrue(checkBit(SQL_API_SQLSETCURSORNAME), "SQLSetCursorName in bitmap");

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}
