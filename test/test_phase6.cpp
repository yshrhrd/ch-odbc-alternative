// Phase 6 Tests: ODBC 2.x compatibility / full MS Access support
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
#undef SQLColAttributes
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLGetInfo
#undef SQLGetDiagRec
#undef SQLGetCursorName
#undef SQLSetCursorName
#endif

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLExecDirect(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT, SQLSMALLINT, SQLLEN);
extern "C" SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT, SQLUSMALLINT, SQLLEN, SQLULEN *, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLColAttributes(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER,
                                               SQLSMALLINT, SQLSMALLINT *, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLColAttribute(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT, SQLPOINTER,
                                              SQLSMALLINT, SQLSMALLINT *, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLSetScrollOptions(SQLHSTMT, SQLUSMALLINT, SQLLEN, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetInfo(SQLHDBC, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT, SQLUSMALLINT, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *,
                                             SQLSMALLINT *, SQLULEN *, SQLSMALLINT *, SQLSMALLINT *);

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

// Helper: create env + dbc only (no stmt)
static void CreateConnHandles(SQLHENV &env, SQLHDBC &dbc) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
}

static void FreeConnHandles(SQLHENV env, SQLHDBC dbc) {
    if (dbc) {
        auto *conn = static_cast<OdbcConnection *>(dbc);
        conn->connected = false;
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// Helper: populate a result set directly for testing
static void PopulateTestResultSet(SQLHSTMT stmt, int numRows) {
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns.clear();
    s->result_set.rows.clear();
    s->result_set.current_row = -1;

    ColumnInfo col1;
    col1.name = "id";
    col1.clickhouse_type = "Int32";
    col1.sql_type = SQL_INTEGER;
    col1.column_size = 10;
    col1.decimal_digits = 0;
    col1.nullable = SQL_NO_NULLS;
    s->result_set.columns.push_back(col1);

    ColumnInfo col2;
    col2.name = "name";
    col2.clickhouse_type = "String";
    col2.sql_type = SQL_VARCHAR;
    col2.column_size = 255;
    col2.decimal_digits = 0;
    col2.nullable = SQL_NULLABLE;
    s->result_set.columns.push_back(col2);

    for (int i = 0; i < numRows; i++) {
        std::vector<std::optional<std::string>> row;
        row.push_back(std::to_string(i + 1));
        row.push_back("row_" + std::to_string(i + 1));
        s->result_set.rows.push_back(row);
    }
}

// ============================================================================
// SQLExtendedFetch test
// ============================================================================

TEST(Phase6_ExtendedFetch, FetchNextBasic) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 3);

    SQLULEN rowCount = 0;
    SQLUSMALLINT rowStatus = 0;

    auto ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, &rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "ExtendedFetch NEXT should succeed");
    AssertEqual((__int64)1, (__int64)rowCount, "RowCount should be 1");
    AssertEqual((int)SQL_ROW_SUCCESS, (int)rowStatus, "RowStatus should be SQL_ROW_SUCCESS");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ExtendedFetch, FetchFirstAndLast) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 5);

    SQLULEN rowCount = 0;
    SQLUSMALLINT rowStatus = 0;

    // Fetch first
    auto ret = SQLExtendedFetch(stmt, SQL_FETCH_FIRST, 0, &rowCount, &rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "ExtendedFetch FIRST should succeed");
    AssertEqual((__int64)1, (__int64)rowCount, "RowCount should be 1");

    // Verify current_row is 0 (first row)
    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)0, (__int64)s->result_set.current_row, "current_row should be 0 after FIRST");

    // Fetch last
    ret = SQLExtendedFetch(stmt, SQL_FETCH_LAST, 0, &rowCount, &rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "ExtendedFetch LAST should succeed");
    AssertEqual((__int64)4, (__int64)s->result_set.current_row, "current_row should be 4 after LAST");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ExtendedFetch, FetchAbsolute) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 5);

    SQLULEN rowCount = 0;
    SQLUSMALLINT rowStatus = 0;

    // Fetch absolute row 3
    auto ret = SQLExtendedFetch(stmt, SQL_FETCH_ABSOLUTE, 3, &rowCount, &rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "ExtendedFetch ABSOLUTE(3) should succeed");

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)2, (__int64)s->result_set.current_row, "current_row should be 2 (0-based)");

    // Fetch beyond end
    ret = SQLExtendedFetch(stmt, SQL_FETCH_ABSOLUTE, 10, &rowCount, &rowStatus);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "ExtendedFetch beyond range should return NO_DATA");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ExtendedFetch, FetchOnEmptyResultSet) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 0);

    SQLULEN rowCount = 0;
    SQLUSMALLINT rowStatus = 0;

    auto ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, &rowStatus);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "ExtendedFetch on empty set should return NO_DATA");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ExtendedFetch, NullOutputPointers) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 3);

    // Should work with NULL output pointers
    auto ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, nullptr, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "ExtendedFetch with NULL pointers should succeed");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ExtendedFetch, InvalidHandle) {
    auto ret = SQLExtendedFetch(SQL_NULL_HSTMT, SQL_FETCH_NEXT, 0, nullptr, nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "ExtendedFetch with NULL handle should return INVALID_HANDLE");
}

// ============================================================================
// SQLColAttributes test (ODBC 2.x)
// ============================================================================

TEST(Phase6_ColAttributes, MapToColAttribute) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 1);

    // SQL_COLUMN_NAME → SQL_DESC_NAME
    char nameBuffer[128] = {};
    SQLSMALLINT nameLen = 0;
    SQLLEN numAttr = 0;
    auto ret = SQLColAttributes(stmt, 1, SQL_COLUMN_NAME, nameBuffer, sizeof(nameBuffer), &nameLen, &numAttr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLColAttributes COLUMN_NAME should succeed");
    AssertEqual(std::string("id"), std::string(nameBuffer), "Column name should be 'id'");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ColAttributes, ColumnType) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 1);

    SQLLEN numAttr = 0;
    auto ret = SQLColAttributes(stmt, 1, SQL_COLUMN_TYPE, nullptr, 0, nullptr, &numAttr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLColAttributes COLUMN_TYPE should succeed");
    AssertEqual((__int64)SQL_INTEGER, (__int64)numAttr, "Column type should be SQL_INTEGER");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ColAttributes, ColumnNullable) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 1);

    // Column 2 (name) is nullable
    SQLLEN numAttr = 0;
    auto ret = SQLColAttributes(stmt, 2, SQL_COLUMN_NULLABLE, nullptr, 0, nullptr, &numAttr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLColAttributes COLUMN_NULLABLE should succeed");
    AssertEqual((__int64)SQL_NULLABLE, (__int64)numAttr, "name column should be nullable");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ColAttributes, ColumnDisplaySize) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 1);

    SQLLEN numAttr = 0;
    auto ret = SQLColAttributes(stmt, 2, SQL_COLUMN_DISPLAY_SIZE, nullptr, 0, nullptr, &numAttr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLColAttributes DISPLAY_SIZE should succeed");
    AssertTrue(numAttr > 0, "Display size should be positive");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ColAttributes, InvalidHandle) {
    SQLLEN numAttr = 0;
    auto ret = SQLColAttributes(SQL_NULL_HSTMT, 1, SQL_COLUMN_TYPE, nullptr, 0, nullptr, &numAttr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "SQLColAttributes with NULL handle should return INVALID_HANDLE");
}

// ============================================================================
// SQLSetScrollOptions test
// ============================================================================

TEST(Phase6_ScrollOptions, SetForwardOnly) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLSetScrollOptions(stmt, SQL_CONCUR_READ_ONLY, SQL_SCROLL_FORWARD_ONLY, 1);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetScrollOptions FORWARD_ONLY should succeed");

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)SQL_CURSOR_FORWARD_ONLY, (__int64)s->cursor_type, "Cursor type should be FORWARD_ONLY");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ScrollOptions, SetStatic) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLSetScrollOptions(stmt, SQL_CONCUR_READ_ONLY, SQL_SCROLL_STATIC, 10);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetScrollOptions STATIC should succeed");

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)SQL_CURSOR_STATIC, (__int64)s->cursor_type, "Cursor type should be STATIC");
    AssertEqual((__int64)10, (__int64)s->rowset_size, "Rowset size should be 10");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ScrollOptions, InvalidHandle) {
    auto ret = SQLSetScrollOptions(SQL_NULL_HSTMT, SQL_CONCUR_READ_ONLY, SQL_SCROLL_STATIC, 1);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "SetScrollOptions with NULL handle should return INVALID_HANDLE");
}

// ============================================================================
// SQL_ROWSET_SIZE / SQL_ATTR_ROW_STATUS_PTR / SQL_ATTR_ROWS_FETCHED_PTR test
// ============================================================================

TEST(Phase6_StmtAttr, RowsetSize) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Set rowset size
    auto ret = SQLSetStmtAttr(stmt, SQL_ROWSET_SIZE, (SQLPOINTER)25, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetStmtAttr SQL_ROWSET_SIZE should succeed");

    // Get rowset size
    SQLULEN val = 0;
    ret = SQLGetStmtAttr(stmt, SQL_ROWSET_SIZE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetStmtAttr SQL_ROWSET_SIZE should succeed");
    AssertEqual((__int64)25, (__int64)val, "Rowset size should be 25");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_StmtAttr, RowArraySize) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)50, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetStmtAttr ROW_ARRAY_SIZE should succeed");

    SQLULEN val = 0;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetStmtAttr ROW_ARRAY_SIZE should succeed");
    AssertEqual((__int64)50, (__int64)val, "Row array size should be 50");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_StmtAttr, RowStatusPtr) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLUSMALLINT statusArray[10] = {};
    auto ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, statusArray, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetStmtAttr ROW_STATUS_PTR should succeed");

    SQLUSMALLINT *ptr = nullptr;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, &ptr, sizeof(ptr), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetStmtAttr ROW_STATUS_PTR should succeed");
    AssertTrue(ptr == statusArray, "Row status ptr should match");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_StmtAttr, RowsFetchedPtr) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN fetchedCount = 0;
    auto ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &fetchedCount, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetStmtAttr ROWS_FETCHED_PTR should succeed");

    SQLULEN *ptr = nullptr;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &ptr, sizeof(ptr), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetStmtAttr ROWS_FETCHED_PTR should succeed");
    AssertTrue(ptr == &fetchedCount, "Rows fetched ptr should match");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetFunctions test (ODBC 2.x function verification)
// ============================================================================

TEST(Phase6_GetFunctions, ExtendedFetchSupported) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    auto ret = SQLGetFunctions(dbc, SQL_API_SQLEXTENDEDFETCH, &supported);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetFunctions should succeed");
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLExtendedFetch should be supported");

    FreeConnHandles(env, dbc);
}

TEST(Phase6_GetFunctions, ColAttributesSupported) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    auto ret = SQLGetFunctions(dbc, SQL_API_SQLCOLATTRIBUTES, &supported);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetFunctions should succeed");
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLColAttributes should be supported");

    FreeConnHandles(env, dbc);
}

TEST(Phase6_GetFunctions, SetScrollOptionsSupported) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    auto ret = SQLGetFunctions(dbc, SQL_API_SQLSETSCROLLOPTIONS, &supported);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetFunctions should succeed");
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetScrollOptions should be supported");

    FreeConnHandles(env, dbc);
}

TEST(Phase6_GetFunctions, BitmapIncludesOdbc2Functions) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT bitmap[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE] = {};
    auto ret = SQLGetFunctions(dbc, SQL_API_ODBC3_ALL_FUNCTIONS, bitmap);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetFunctions ALL should succeed");

    // Check ODBC 2.x functions in bitmap
    auto checkFunc = [&](SQLUSMALLINT id, const char *name) {
        bool set = (bitmap[id >> 4] & (1 << (id & 0x000F))) != 0;
        AssertTrue(set, std::string(name) + " should be in bitmap");
    };

    checkFunc(SQL_API_SQLEXTENDEDFETCH, "SQLExtendedFetch");
    checkFunc(SQL_API_SQLCOLATTRIBUTES, "SQLColAttributes");
    checkFunc(SQL_API_SQLSETSCROLLOPTIONS, "SQLSetScrollOptions");

    FreeConnHandles(env, dbc);
}

// ============================================================================
// SQLGetInfo test (SQL_DESCRIBE_PARAMETER = "Y")
// ============================================================================

TEST(Phase6_GetInfo, DescribeParameterIsY) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    char buf[32] = {};
    SQLSMALLINT len = 0;
    auto ret = SQLGetInfo(dbc, SQL_DESCRIBE_PARAMETER, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetInfo SQL_DESCRIBE_PARAMETER should succeed");
    AssertEqual(std::string("Y"), std::string(buf), "SQL_DESCRIBE_PARAMETER should be Y");

    FreeConnHandles(env, dbc);
}

// ============================================================================
// ExtendedFetch and FetchScroll consistency test
// ============================================================================

TEST(Phase6_ExtendedFetch, ConsistentWithFetchScroll) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 5);

    // Use ExtendedFetch to navigate
    SQLULEN rowCount = 0;
    SQLUSMALLINT rowStatus = 0;

    // Fetch row 1
    auto ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, &rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "First NEXT");
    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)0, (__int64)s->result_set.current_row, "After first NEXT, row=0");

    // Fetch row 2
    ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, &rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Second NEXT");
    AssertEqual((__int64)1, (__int64)s->result_set.current_row, "After second NEXT, row=1");

    // Fetch relative -1 (back to row 1)
    ret = SQLExtendedFetch(stmt, SQL_FETCH_RELATIVE, -1, &rowCount, &rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "RELATIVE(-1)");
    AssertEqual((__int64)0, (__int64)s->result_set.current_row, "After RELATIVE(-1), row=0");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase6_ExtendedFetch, FetchAllRows) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateTestResultSet(stmt, 3);

    SQLULEN rowCount = 0;
    SQLUSMALLINT rowStatus = 0;
    int fetchedRows = 0;

    while (SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, &rowStatus) == SQL_SUCCESS) {
        fetchedRows++;
    }
    AssertEqual(3, fetchedRows, "Should fetch all 3 rows");

    FreeTestHandles(env, dbc, stmt);
}
