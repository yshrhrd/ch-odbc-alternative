// Phase 9 Tests: SQLBrowseConnect, Row Array Binding, new statement attributes
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
#undef SQLBrowseConnect
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLExecDirect
#undef SQLDescribeCol
#undef SQLColAttribute
#undef SQLDriverConnect
#undef SQLGetDiagRec
#endif

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC, SQLCHAR *, SQLSMALLINT, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLBrowseConnectW(SQLHDBC, SQLWCHAR *, SQLSMALLINT, SQLWCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLBindCol(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLDisconnect(SQLHDBC);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);

using namespace test_framework;
using namespace clickhouse_odbc;

// Helper: create env + dbc + stmt handles
static void CreateTestHandles(SQLHENV &env, SQLHDBC &dbc, SQLHSTMT &stmt) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
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

static void CreateConnHandles(SQLHENV &env, SQLHDBC &dbc) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
}

static void FreeConnHandles(SQLHENV env, SQLHDBC dbc) {
    if (dbc) SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// SQLBrowseConnect test
// ============================================================================

TEST(Phase9_BrowseConnect, NullHandleReturnsInvalidHandle) {
    SQLCHAR out[256];
    SQLSMALLINT outLen;
    SQLRETURN ret = SQLBrowseConnect(nullptr, (SQLCHAR *)"HOST=localhost", SQL_NTS, out, sizeof(out), &outLen);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle should return SQL_INVALID_HANDLE");
}

TEST(Phase9_BrowseConnect, EmptyConnectionStringNeedData) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLCHAR out[512] = {};
    SQLSMALLINT outLen = 0;
    SQLRETURN ret = SQLBrowseConnect(dbc, (SQLCHAR *)"", SQL_NTS, out, sizeof(out), &outLen);
    // With no HOST or UID, should return SQL_NEED_DATA
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "Empty connection string should require more data");
    // Output should contain HOST and UID
    std::string outStr(reinterpret_cast<const char *>(out));
    AssertTrue(outStr.find("HOST") != std::string::npos, "Output should mention HOST");
    AssertTrue(outStr.find("UID") != std::string::npos, "Output should mention UID");

    FreeConnHandles(env, dbc);
}

TEST(Phase9_BrowseConnect, PartialConnectionStringMissingUID) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLCHAR out[512] = {};
    SQLSMALLINT outLen = 0;
    SQLRETURN ret = SQLBrowseConnect(dbc, (SQLCHAR *)"HOST=localhost", SQL_NTS, out, sizeof(out), &outLen);
    // HOST is provided but UID is missing
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "Missing UID should return SQL_NEED_DATA");
    std::string outStr(reinterpret_cast<const char *>(out));
    AssertTrue(outStr.find("UID") != std::string::npos, "Output should mention missing UID");

    FreeConnHandles(env, dbc);
}

TEST(Phase9_BrowseConnect, AlreadyConnectedError) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLCHAR out[256] = {};
    SQLSMALLINT outLen = 0;
    SQLRETURN ret = SQLBrowseConnect(dbc, (SQLCHAR *)"HOST=localhost;UID=default", SQL_NTS, out, sizeof(out), &outLen);
    AssertEqual((int)SQL_ERROR, (int)ret, "Already connected should return SQL_ERROR");

    conn->connected = false;
    FreeConnHandles(env, dbc);
}

TEST(Phase9_BrowseConnect, ConnectionFailReturnsError) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLCHAR out[512] = {};
    SQLSMALLINT outLen = 0;
    // Use invalid host to trigger connection failure
    SQLRETURN ret = SQLBrowseConnect(dbc, (SQLCHAR *)"HOST=invalid_host_xyz;PORT=19999;UID=test", SQL_NTS,
                                      out, sizeof(out), &outLen);
    AssertEqual((int)SQL_ERROR, (int)ret, "Invalid host should return SQL_ERROR");

    FreeConnHandles(env, dbc);
}

TEST(Phase9_BrowseConnect, InvalidPortError) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLCHAR out[256] = {};
    SQLSMALLINT outLen = 0;
    SQLRETURN ret = SQLBrowseConnect(dbc, (SQLCHAR *)"HOST=localhost;PORT=99999;UID=test", SQL_NTS,
                                      out, sizeof(out), &outLen);
    AssertEqual((int)SQL_ERROR, (int)ret, "Invalid port should return SQL_ERROR");

    FreeConnHandles(env, dbc);
}

TEST(Phase9_BrowseConnect, InvalidPortNotNumber) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLCHAR out[256] = {};
    SQLSMALLINT outLen = 0;
    SQLRETURN ret = SQLBrowseConnect(dbc, (SQLCHAR *)"HOST=localhost;PORT=abc;UID=test", SQL_NTS,
                                      out, sizeof(out), &outLen);
    AssertEqual((int)SQL_ERROR, (int)ret, "Non-numeric port should return SQL_ERROR");

    FreeConnHandles(env, dbc);
}

// ============================================================================
// SQLBrowseConnectW test
// ============================================================================

TEST(Phase9_BrowseConnectW, NullHandleReturnsInvalidHandle) {
    SQLWCHAR out[256];
    SQLSMALLINT outLen;
    SQLRETURN ret = SQLBrowseConnectW(nullptr, (SQLWCHAR *)L"HOST=localhost", SQL_NTS, out, 256, &outLen);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle should return SQL_INVALID_HANDLE");
}

TEST(Phase9_BrowseConnectW, EmptyStringNeedData) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLWCHAR out[512] = {};
    SQLSMALLINT outLen = 0;
    SQLRETURN ret = SQLBrowseConnectW(dbc, (SQLWCHAR *)L"", SQL_NTS, out, 512, &outLen);
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "Empty string should return SQL_NEED_DATA");

    FreeConnHandles(env, dbc);
}

TEST(Phase9_BrowseConnectW, PartialStringMissingUID) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLWCHAR out[512] = {};
    SQLSMALLINT outLen = 0;
    SQLRETURN ret = SQLBrowseConnectW(dbc, (SQLWCHAR *)L"HOST=localhost", SQL_NTS, out, 512, &outLen);
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "Missing UID should return SQL_NEED_DATA");

    FreeConnHandles(env, dbc);
}

// ============================================================================
// SQLGetFunctions: SQLBrowseConnect support test
// ============================================================================

TEST(Phase9_GetFunctions, BrowseConnectInBitmap) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT bitmap[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE] = {};
    SQLRETURN ret = SQLGetFunctions(dbc, SQL_API_ODBC3_ALL_FUNCTIONS, bitmap);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLGetFunctions should succeed");

    // Check SQL_API_SQLBROWSECONNECT bit
    bool supported = (bitmap[SQL_API_SQLBROWSECONNECT >> 4] & (1 << (SQL_API_SQLBROWSECONNECT & 0x000F))) != 0;
    AssertTrue(supported, "SQLBrowseConnect should be in bitmap");

    FreeConnHandles(env, dbc);
}

TEST(Phase9_GetFunctions, BrowseConnectIndividual) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLRETURN ret = SQLGetFunctions(dbc, SQL_API_SQLBROWSECONNECT, &supported);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Individual check should succeed");
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLBrowseConnect should be supported");

    FreeConnHandles(env, dbc);
}

// ============================================================================
// SQL_ATTR_ROW_BIND_TYPE test
// ============================================================================

TEST(Phase9_RowBindType, DefaultIsBindByColumn) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN bindType = 0;
    SQLRETURN ret = SQLGetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, &bindType, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get row bind type should succeed");
    AssertEqual((__int64)SQL_BIND_BY_COLUMN, (__int64)bindType, "Default should be SQL_BIND_BY_COLUMN");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_RowBindType, SetRowWiseBinding) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Set row-wise binding with struct size = 128
    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)128, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set row bind type should succeed");

    SQLULEN bindType = 0;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, &bindType, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get row bind type should succeed");
    AssertEqual((__int64)128, (__int64)bindType, "Should be 128");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_RowBindType, ResetToColumnWise) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)256, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)SQL_BIND_BY_COLUMN, 0);

    SQLULEN bindType = 999;
    SQLGetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, &bindType, 0, nullptr);
    AssertEqual((__int64)SQL_BIND_BY_COLUMN, (__int64)bindType, "Should be SQL_BIND_BY_COLUMN after reset");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQL_ATTR_PARAMSET_SIZE test
// ============================================================================

TEST(Phase9_ParamsetSize, DefaultIsOne) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN paramsetSize = 0;
    SQLRETURN ret = SQLGetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, &paramsetSize, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get paramset size should succeed");
    AssertEqual((__int64)1, (__int64)paramsetSize, "Default should be 1");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_ParamsetSize, SetParamsetSize) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)10, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set paramset size should succeed");

    SQLULEN paramsetSize = 0;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, &paramsetSize, 0, nullptr);
    AssertEqual((__int64)10, (__int64)paramsetSize, "Should be 10");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_ParamsetSize, ZeroClampedToOne) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, (SQLPOINTER)0, 0);

    SQLULEN paramsetSize = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE, &paramsetSize, 0, nullptr);
    AssertEqual((__int64)1, (__int64)paramsetSize, "Zero should be clamped to 1");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// Multi-row fetch test (column-wise binding)
// ============================================================================

TEST(Phase9_MultiRowFetch, SingleRowFetchStillWorks) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Populate result set
    s->result_set.columns = {{"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"1"}, {"2"}, {"3"}};
    s->result_set.current_row = -1;

    char buf[64] = {};
    SQLLEN ind = 0;
    SQLBindCol(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);

    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Fetch should succeed");
    AssertEqual(std::string("1"), std::string(buf), "Should fetch first row");

    ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Fetch should succeed");
    AssertEqual(std::string("2"), std::string(buf), "Should fetch second row");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_MultiRowFetch, ColumnWiseMultiRow) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"val", "String", SQL_VARCHAR, 50, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"aaa"}, {"bbb"}, {"ccc"}, {"ddd"}};
    s->result_set.current_row = -1;

    // Set rowset size to 3
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)3, 0);

    SQLULEN rowsFetched = 0;
    SQLUSMALLINT rowStatus[3] = {};
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, rowStatus, 0);

    // Column-wise binding: 3 buffers in array
    char buffers[3][64] = {};
    SQLLEN inds[3] = {};
    SQLBindCol(stmt, 1, SQL_C_CHAR, buffers[0], 64, &inds[0]);

    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Multi-row fetch should succeed");
    AssertEqual((__int64)3, (__int64)rowsFetched, "Should fetch 3 rows");
    AssertEqual(std::string("aaa"), std::string(buffers[0]), "Row 1 value");
    AssertEqual(std::string("bbb"), std::string(buffers[1]), "Row 2 value");
    AssertEqual(std::string("ccc"), std::string(buffers[2]), "Row 3 value");

    // Row status should all be SQL_ROW_SUCCESS
    AssertEqual((int)SQL_ROW_SUCCESS, (int)rowStatus[0], "Row 1 status");
    AssertEqual((int)SQL_ROW_SUCCESS, (int)rowStatus[1], "Row 2 status");
    AssertEqual((int)SQL_ROW_SUCCESS, (int)rowStatus[2], "Row 3 status");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_MultiRowFetch, PartialRowset) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"val", "String", SQL_VARCHAR, 50, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"x"}, {"y"}};
    s->result_set.current_row = -1;

    // Rowset size = 5, but only 2 rows available
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)5, 0);

    SQLULEN rowsFetched = 0;
    SQLUSMALLINT rowStatus[5] = {};
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, rowStatus, 0);

    char buffers[5][32] = {};
    SQLLEN inds[5] = {};
    SQLBindCol(stmt, 1, SQL_C_CHAR, buffers[0], 32, &inds[0]);

    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Fetch should succeed even if partial");
    AssertEqual((__int64)2, (__int64)rowsFetched, "Should fetch only 2 rows");
    AssertEqual(std::string("x"), std::string(buffers[0]), "Row 1");
    AssertEqual(std::string("y"), std::string(buffers[1]), "Row 2");

    // Remaining statuses should be SQL_ROW_NOROW
    AssertEqual((int)SQL_ROW_NOROW, (int)rowStatus[2], "Row 3 should be NOROW");
    AssertEqual((int)SQL_ROW_NOROW, (int)rowStatus[3], "Row 4 should be NOROW");
    AssertEqual((int)SQL_ROW_NOROW, (int)rowStatus[4], "Row 5 should be NOROW");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_MultiRowFetch, NoDataWhenEmpty) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"val", "String", SQL_VARCHAR, 50, 0, SQL_NO_NULLS}};
    s->result_set.rows = {}; // empty
    s->result_set.current_row = -1;

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)3, 0);

    SQLULEN rowsFetched = 99;
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0);

    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "Empty result should return SQL_NO_DATA");
    AssertEqual((__int64)0, (__int64)rowsFetched, "rowsFetched should be 0");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_MultiRowFetch, SecondFetchGetsRemainingRows) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"n", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"10"}, {"20"}, {"30"}, {"40"}, {"50"}};
    s->result_set.current_row = -1;

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)3, 0);
    SQLULEN rowsFetched = 0;
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0);

    char buffers[3][32] = {};
    SQLLEN inds[3] = {};
    SQLBindCol(stmt, 1, SQL_C_CHAR, buffers[0], 32, &inds[0]);

    // First fetch: rows 1-3
    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "First fetch should succeed");
    AssertEqual((__int64)3, (__int64)rowsFetched, "First fetch: 3 rows");
    AssertEqual(std::string("10"), std::string(buffers[0]), "Row 1");
    AssertEqual(std::string("20"), std::string(buffers[1]), "Row 2");
    AssertEqual(std::string("30"), std::string(buffers[2]), "Row 3");

    // Second fetch: rows 4-5 (partial)
    memset(buffers, 0, sizeof(buffers));
    ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Second fetch should succeed");
    AssertEqual((__int64)2, (__int64)rowsFetched, "Second fetch: 2 rows");
    AssertEqual(std::string("40"), std::string(buffers[0]), "Row 4");
    AssertEqual(std::string("50"), std::string(buffers[1]), "Row 5");

    // Third fetch: no more data
    ret = SQLFetch(stmt);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "Third fetch should return SQL_NO_DATA");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_MultiRowFetch, RowWiseBinding) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"name", "String", SQL_VARCHAR, 50, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"Alice"}, {"Bob"}, {"Carol"}};
    s->result_set.current_row = -1;

    // Row-wise struct
    struct RowStruct {
        char name[64];
        SQLLEN name_ind;
    };
    RowStruct rows[3] = {};

    // Set row-wise binding
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(RowStruct), 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)3, 0);

    SQLULEN rowsFetched = 0;
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0);

    // Bind to first element
    SQLBindCol(stmt, 1, SQL_C_CHAR, rows[0].name, sizeof(rows[0].name), &rows[0].name_ind);

    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Row-wise fetch should succeed");
    AssertEqual((__int64)3, (__int64)rowsFetched, "Should fetch 3 rows");
    AssertEqual(std::string("Alice"), std::string(rows[0].name), "Row 1 name");
    AssertEqual(std::string("Bob"), std::string(rows[1].name), "Row 2 name");
    AssertEqual(std::string("Carol"), std::string(rows[2].name), "Row 3 name");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_MultiRowFetch, RowsFetchedPtrWithoutRowStatus) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"v", "String", SQL_VARCHAR, 10, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"a"}, {"b"}};
    s->result_set.current_row = -1;

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)2, 0);

    SQLULEN rowsFetched = 0;
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0);
    // No row_status_ptr

    char bufs[2][16] = {};
    SQLLEN inds[2] = {};
    SQLBindCol(stmt, 1, SQL_C_CHAR, bufs[0], 16, &inds[0]);

    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Fetch should succeed without row_status_ptr");
    AssertEqual((__int64)2, (__int64)rowsFetched, "Should fetch 2 rows");
    AssertEqual(std::string("a"), std::string(bufs[0]), "Row 1");
    AssertEqual(std::string("b"), std::string(bufs[1]), "Row 2");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// OdbcStatement field default values test
// ============================================================================

TEST(Phase9_StmtDefaults, RowBindTypeDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)SQL_BIND_BY_COLUMN, (__int64)s->row_bind_type, "Default row_bind_type");
    AssertEqual((__int64)1, (__int64)s->paramset_size, "Default paramset_size");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase9_StmtDefaults, RowsetSizeDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)1, (__int64)s->rowset_size, "Default rowset_size should be 1");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLFetch with no result set test
// ============================================================================

TEST(Phase9_MultiRowFetch, FetchWithoutResultSetError) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_ERROR, (int)ret, "Fetch without result set should return SQL_ERROR");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// Multi-row fetch with multiple columns
// ============================================================================

TEST(Phase9_MultiRowFetch, MultipleColumnsColumnWise) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {
        {"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS},
        {"name", "String", SQL_VARCHAR, 50, 0, SQL_NO_NULLS}
    };
    s->result_set.rows = {
        {std::optional<std::string>("1"), std::optional<std::string>("Alice")},
        {std::optional<std::string>("2"), std::optional<std::string>("Bob")}
    };
    s->result_set.current_row = -1;

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)2, 0);

    SQLULEN rowsFetched = 0;
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rowsFetched, 0);

    char ids[2][16] = {};
    SQLLEN id_inds[2] = {};
    char names[2][64] = {};
    SQLLEN name_inds[2] = {};

    SQLBindCol(stmt, 1, SQL_C_CHAR, ids[0], 16, &id_inds[0]);
    SQLBindCol(stmt, 2, SQL_C_CHAR, names[0], 64, &name_inds[0]);

    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Fetch should succeed");
    AssertEqual((__int64)2, (__int64)rowsFetched, "Should fetch 2 rows");
    AssertEqual(std::string("1"), std::string(ids[0]), "Row 1 id");
    AssertEqual(std::string("2"), std::string(ids[1]), "Row 2 id");
    AssertEqual(std::string("Alice"), std::string(names[0]), "Row 1 name");
    AssertEqual(std::string("Bob"), std::string(names[1]), "Row 2 name");

    FreeTestHandles(env, dbc, stmt);
}
