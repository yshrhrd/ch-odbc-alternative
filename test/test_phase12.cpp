// Phase 12 Tests: SQLGetData piecemeal retrieval, RowCount improvement, SQLCancel, new attributes
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
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetDiagField
#undef SQLGetDiagRec
#endif

#pragma warning(disable: 4996)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLRowCount(SQLHSTMT, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLCancel(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// Helper: create env + dbc + stmt
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

// Helper: populate a result set with test data directly
static void PopulateResultSet(SQLHSTMT hstmt, const std::string &col_name,
                               SQLSMALLINT sql_type, const std::string &value) {
    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    stmt->result_set.Reset();
    ColumnInfo ci;
    ci.name = col_name;
    ci.sql_type = sql_type;
    ci.column_size = 1000;
    stmt->result_set.columns.push_back(ci);
    ResultRow row;
    row.push_back(value);
    stmt->result_set.rows.push_back(row);
    stmt->result_set.current_row = -1;
}

// ============================================================================
// Piecemeal SQLGetData tests (SQL_C_CHAR)
// ============================================================================
TEST(Phase12_PiecemealGetData, CharSmallBuffer) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateResultSet(stmt, "col1", SQL_VARCHAR, "ABCDEFGHIJ");
    SQLFetch(stmt);

    // First call: buffer of 5 bytes (4 chars + null)
    char buf[5] = {};
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_SUCCESS_WITH_INFO, (int)ret);
    AssertEqual(std::string("ABCD"), std::string(buf));
    AssertEqual((__int64)10, (__int64)ind);  // Total length

    // Second call: get next chunk
    char buf2[5] = {};
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf2, sizeof(buf2), &ind);
    AssertEqual((int)SQL_SUCCESS_WITH_INFO, (int)ret);
    AssertEqual(std::string("EFGH"), std::string(buf2));
    AssertEqual((__int64)6, (__int64)ind);  // Remaining length

    // Third call: get last 2 chars
    char buf3[5] = {};
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf3, sizeof(buf3), &ind);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("IJ"), std::string(buf3));

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_PiecemealGetData, CharFullBuffer) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateResultSet(stmt, "col1", SQL_VARCHAR, "Hello");
    SQLFetch(stmt);

    // Buffer large enough for full string
    char buf[20] = {};
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("Hello"), std::string(buf));
    AssertEqual((__int64)5, (__int64)ind);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_PiecemealGetData, CharExhausted) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateResultSet(stmt, "col1", SQL_VARCHAR, "ABC");
    SQLFetch(stmt);

    // Read all in first call with small buffer
    char buf[3] = {};
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_SUCCESS_WITH_INFO, (int)ret);
    AssertEqual(std::string("AB"), std::string(buf));

    // Read remaining
    char buf2[3] = {};
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf2, sizeof(buf2), &ind);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("C"), std::string(buf2));

    // No more data
    char buf3[3] = {};
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf3, sizeof(buf3), &ind);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_PiecemealGetData, BinarySmallBuffer) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);
    PopulateResultSet(stmt, "bin1", SQL_VARBINARY, "ABCDEF");
    SQLFetch(stmt);

    char buf[3] = {};
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData(stmt, 1, SQL_C_BINARY, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_SUCCESS_WITH_INFO, (int)ret);
    AssertEqual((__int64)6, (__int64)ind);
    AssertTrue(buf[0] == 'A' && buf[1] == 'B' && buf[2] == 'C');

    char buf2[5] = {};
    ret = SQLGetData(stmt, 1, SQL_C_BINARY, buf2, sizeof(buf2), &ind);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)3, (__int64)ind);
    AssertTrue(buf2[0] == 'D' && buf2[1] == 'E' && buf2[2] == 'F');

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_PiecemealGetData, ColumnSwitch) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Two-column result set
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.Reset();
    ColumnInfo c1; c1.name = "col1"; c1.sql_type = SQL_VARCHAR; c1.column_size = 100;
    ColumnInfo c2; c2.name = "col2"; c2.sql_type = SQL_VARCHAR; c2.column_size = 100;
    s->result_set.columns.push_back(c1);
    s->result_set.columns.push_back(c2);
    ResultRow row;
    row.push_back(std::string("ABCDE"));
    row.push_back(std::string("FGHIJ"));
    s->result_set.rows.push_back(row);
    s->result_set.current_row = -1;
    SQLFetch(stmt);

    // Partial read of col1
    char buf[3] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("AB"), std::string(buf));

    // Switch to col2 - should reset offset and read col2 from start
    char buf2[10] = {};
    SQLRETURN ret = SQLGetData(stmt, 2, SQL_C_CHAR, buf2, sizeof(buf2), &ind);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("FGHIJ"), std::string(buf2));

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_PiecemealGetData, FetchResetsOffset) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.Reset();
    ColumnInfo c1; c1.name = "col1"; c1.sql_type = SQL_VARCHAR; c1.column_size = 100;
    s->result_set.columns.push_back(c1);
    ResultRow r1; r1.push_back(std::string("AAABBB"));
    ResultRow r2; r2.push_back(std::string("CCCDDD"));
    s->result_set.rows.push_back(r1);
    s->result_set.rows.push_back(r2);
    s->result_set.current_row = -1;

    // Fetch row 1, partial read
    SQLFetch(stmt);
    char buf[4] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("AAA"), std::string(buf));

    // Fetch row 2 - offset should reset
    SQLFetch(stmt);
    char buf2[10] = {};
    SQLRETURN ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf2, sizeof(buf2), &ind);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("CCCDDD"), std::string(buf2));

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_PiecemealGetData, NullValue) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.Reset();
    ColumnInfo c1; c1.name = "col1"; c1.sql_type = SQL_VARCHAR; c1.column_size = 100;
    s->result_set.columns.push_back(c1);
    ResultRow row;
    row.push_back(std::nullopt);
    s->result_set.rows.push_back(row);
    s->result_set.current_row = -1;
    SQLFetch(stmt);

    char buf[10] = {};
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)SQL_NULL_DATA, (__int64)ind);

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLRowCount tests
// ============================================================================
TEST(Phase12_RowCount, SelectReturnsRowCount) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Populate with 3 rows
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.Reset();
    ColumnInfo c1; c1.name = "x"; c1.sql_type = SQL_INTEGER; c1.column_size = 10;
    s->result_set.columns.push_back(c1);
    for (int i = 0; i < 3; i++) {
        ResultRow row; row.push_back(std::to_string(i));
        s->result_set.rows.push_back(row);
    }

    SQLLEN cnt = -1;
    SQLRETURN ret = SQLRowCount(stmt, &cnt);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)3, (__int64)cnt);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_RowCount, AffectedRowsForNonSelect) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Simulate non-SELECT result
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->affected_rows = 0;

    SQLLEN cnt = -1;
    SQLRETURN ret = SQLRowCount(stmt, &cnt);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)0, (__int64)cnt);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_RowCount, DefaultMinusOne) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Default affected_rows is -1, so it should return rows.size() = 0
    SQLLEN cnt = 99;
    SQLRowCount(stmt, &cnt);
    AssertEqual((__int64)0, (__int64)cnt);

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLCancel tests
// ============================================================================
TEST(Phase12_Cancel, InvalidHandle) {
    SQLRETURN ret = SQLCancel(nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret);
}

TEST(Phase12_Cancel, ResetsDAE) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->need_data = true;
    s->pending_dae_params.push_back(1);
    s->current_dae_index = 0;
    s->dae_buffers[1] = "partial";

    SQLRETURN ret = SQLCancel(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertFalse(s->need_data);
    AssertTrue(s->pending_dae_params.empty());
    AssertEqual(-1, s->current_dae_index);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_Cancel, ResetsGetDataState) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->getdata_col = 3;
    s->getdata_offset = 100;

    SQLCancel(stmt);
    AssertEqual((int)0, (int)s->getdata_col);
    AssertEqual((__int64)0, (__int64)s->getdata_offset);

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// Cursor attribute tests
// ============================================================================
TEST(Phase12_CursorAttrs, ScrollableDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN val = 99;
    SQLGetStmtAttr(stmt, SQL_ATTR_CURSOR_SCROLLABLE, &val, sizeof(val), nullptr);
    AssertEqual((__int64)SQL_NONSCROLLABLE, (__int64)val);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_CursorAttrs, SensitivityDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN val = 99;
    SQLGetStmtAttr(stmt, SQL_ATTR_CURSOR_SENSITIVITY, &val, sizeof(val), nullptr);
    AssertEqual((__int64)SQL_INSENSITIVE, (__int64)val);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_CursorAttrs, ScrollableRoundtrip) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_SCROLLABLE, (SQLPOINTER)(uintptr_t)SQL_SCROLLABLE, 0);
    SQLULEN val = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_CURSOR_SCROLLABLE, &val, sizeof(val), nullptr);
    AssertEqual((__int64)SQL_SCROLLABLE, (__int64)val);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_CursorAttrs, SensitivityRoundtrip) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_CURSOR_SENSITIVITY, (SQLPOINTER)(uintptr_t)SQL_SENSITIVE, 0);
    SQLULEN val = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_CURSOR_SENSITIVITY, &val, sizeof(val), nullptr);
    AssertEqual((__int64)SQL_SENSITIVE, (__int64)val);

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetDiagField header field tests
// ============================================================================
TEST(Phase12_DiagField, CursorRowCount) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Populate result set with 5 rows
    auto *s = static_cast<OdbcStatement *>(stmt);
    ColumnInfo c1; c1.name = "x"; c1.sql_type = SQL_INTEGER; c1.column_size = 10;
    s->result_set.columns.push_back(c1);
    for (int i = 0; i < 5; i++) {
        ResultRow row; row.push_back(std::to_string(i));
        s->result_set.rows.push_back(row);
    }

    SQLLEN cursor_count = 0;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 0, SQL_DIAG_CURSOR_ROW_COUNT,
                                     &cursor_count, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)5, (__int64)cursor_count);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_DiagField, DynamicFunction) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    char buf[50] = "xxx";
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 0, SQL_DIAG_DYNAMIC_FUNCTION,
                                     buf, sizeof(buf), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string(""), std::string(buf));

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_DiagField, DynamicFunctionCode) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLINTEGER code = -1;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 0, SQL_DIAG_DYNAMIC_FUNCTION_CODE,
                                     &code, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_DIAG_UNKNOWN_STATEMENT, (int)code);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_DiagField, RowCountFromStmt) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->affected_rows = 42;

    SQLLEN row_count = 0;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 0, SQL_DIAG_ROW_COUNT,
                                     &row_count, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)42, (__int64)row_count);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase12_DiagField, DiagNumber) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "test error");

    SQLINTEGER num = 0;
    SQLGetDiagField(SQL_HANDLE_STMT, stmt, 0, SQL_DIAG_NUMBER, &num, 0, nullptr);
    AssertEqual(1, (int)num);

    FreeTestHandles(env, dbc, stmt);
}
