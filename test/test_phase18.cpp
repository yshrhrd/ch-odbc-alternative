// Phase 18 Tests: Access background loading support
// SQLFetchScroll / SQLExtendedFetch ABSOLUTE/RELATIVE boundary checks,
// SQL_ATTR_RETRIEVE_DATA, cursor attribute improvements
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

#include "../src/include/handle.h"
#include "../src/include/util.h"

#ifdef UNICODE
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetInfo
#endif

#pragma warning(disable: 4996)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT, SQLSMALLINT, SQLLEN);
extern "C" SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT, SQLUSMALLINT, SQLLEN, SQLULEN *, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLBindCol(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLGetInfo(SQLHDBC, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// Helper: create env + dbc + stmt
static void CreateHandles(SQLHENV &env, SQLHDBC &dbc, SQLHSTMT &stmt) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
}

static void FreeHandles(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt) {
    if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (dbc) {
        auto *conn = static_cast<OdbcConnection *>(dbc);
        conn->connected = false;
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// Helper: populate result set with N rows (col: "id" INTEGER, values "0".."N-1")
static void PopulateRows(SQLHSTMT hstmt, int n) {
    auto *s = static_cast<OdbcStatement *>(hstmt);
    s->result_set.Reset();
    ColumnInfo ci;
    ci.name = "id";
    ci.sql_type = SQL_INTEGER;
    ci.column_size = 10;
    s->result_set.columns.push_back(ci);
    for (int i = 0; i < n; i++) {
        ResultRow row;
        row.push_back(std::to_string(i));
        s->result_set.rows.push_back(row);
    }
    s->result_set.current_row = -1;
}

// ============================================================================
// SQLFetchScroll ABSOLUTE test
// ============================================================================
TEST(Phase18_FetchScrollAbsolute, PositiveOffset) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    // FetchScroll ABSOLUTE 3 → row index 2 (value "2")
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, 3);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("2"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchScrollAbsolute, NegativeOffset_LastRow) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    // ABSOLUTE -1 → last row (index 4, value "4")
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, -1);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("4"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchScrollAbsolute, NegativeOffset_SecondFromEnd) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    // ABSOLUTE -2 → second-to-last (index 3, value "3")
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, -2);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("3"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchScrollAbsolute, NegativeOffset_BeyondStart) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    // ABSOLUTE -10 → beyond start → SQL_NO_DATA
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, -10);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchScrollAbsolute, ZeroOffset) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    // ABSOLUTE 0 → SQL_NO_DATA
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, 0);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchScrollAbsolute, BeyondEnd) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    // ABSOLUTE 100 → beyond end → SQL_NO_DATA
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, 100);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    FreeHandles(env, dbc, stmt);
}

// ============================================================================
// SQLFetchScroll RELATIVE test
// ============================================================================
TEST(Phase18_FetchScrollRelative, ForwardFromCurrent) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    // Fetch first row
    SQLFetch(stmt);

    // RELATIVE +2 → skip 1, land on row index 2 (value "2")
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_RELATIVE, 2);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("2"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchScrollRelative, BackwardFromCurrent) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    // Position to row 3 (index 2)
    SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, 3);

    // RELATIVE -2 → go back 2, land on row index 0 (value "0")
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_RELATIVE, -2);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("0"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchScrollRelative, BeyondEndReturnsNoData) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    SQLFetch(stmt); // row 0

    // RELATIVE +100 → beyond end
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_RELATIVE, 100);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchScrollRelative, BeforeStartReturnsNoData) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    SQLFetch(stmt); // row 0

    // RELATIVE -10 → before start
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_RELATIVE, -10);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    FreeHandles(env, dbc, stmt);
}

// ============================================================================
// SQLExtendedFetch ABSOLUTE (negative offset) test
// ============================================================================
TEST(Phase18_ExtFetchAbsolute, NegativeOffset) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    SQLULEN fetched = 0;
    SQLUSMALLINT status[1] = {};
    // ABSOLUTE -1 → last row
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_ABSOLUTE, -1, &fetched, status);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)1, (__int64)fetched);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("4"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_ExtFetchAbsolute, ZeroReturnsNoData) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    SQLULEN fetched = 0;
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_ABSOLUTE, 0, &fetched, nullptr);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    FreeHandles(env, dbc, stmt);
}

// ============================================================================
// SQLExtendedFetch RELATIVE (bounds) test
// ============================================================================
TEST(Phase18_ExtFetchRelative, BoundsCheck) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    SQLFetch(stmt); // row 0

    SQLULEN fetched = 0;
    // RELATIVE +100 → beyond end
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_RELATIVE, 100, &fetched, nullptr);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    // RELATIVE -10 → before start
    ret = SQLExtendedFetch(stmt, SQL_FETCH_RELATIVE, -10, &fetched, nullptr);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    FreeHandles(env, dbc, stmt);
}

// ============================================================================
// SQL_ATTR_RETRIEVE_DATA test
// ============================================================================
TEST(Phase18_RetrieveData, DefaultIsOn) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);

    SQLULEN val = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, &val, sizeof(val), nullptr);
    AssertEqual((__int64)SQL_RD_ON, (__int64)val);

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_RetrieveData, OffSkipsDataCopy) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    // Bind column
    char buf[20] = "UNCHANGED";
    SQLLEN ind = 0;
    SQLBindCol(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);

    // Set RETRIEVE_DATA to OFF
    SQLSetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, (SQLPOINTER)(uintptr_t)SQL_RD_OFF, 0);

    // Fetch → cursor should advance but buffer should NOT be updated
    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("UNCHANGED"), std::string(buf));

    // Verify cursor advanced (current_row should be 0)
    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)0, (__int64)s->result_set.current_row);

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_RetrieveData, OnCopiesData) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 3);

    char buf[20] = "UNCHANGED";
    SQLLEN ind = 0;
    SQLBindCol(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);

    // Default: RETRIEVE_DATA = ON → data should be copied
    SQLRETURN ret = SQLFetch(stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("0"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_RetrieveData, ToggleOnOffDuringFetch) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLBindCol(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);

    // Fetch row 0 with ON
    SQLFetch(stmt);
    AssertEqual(std::string("0"), std::string(buf));

    // Turn OFF, skip rows 1 and 2
    SQLSetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, (SQLPOINTER)(uintptr_t)SQL_RD_OFF, 0);
    SQLFetch(stmt); // row 1, no copy
    SQLFetch(stmt); // row 2, no copy

    // Turn ON, fetch row 3
    SQLSetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, (SQLPOINTER)(uintptr_t)SQL_RD_ON, 0);
    SQLFetch(stmt);
    AssertEqual(std::string("3"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

// ============================================================================
// Access background loading simulation
// ============================================================================
TEST(Phase18_AccessBackground, SequentialThenJump) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 100);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->rowset_size = 10;
    SQLULEN fetched = 0;
    s->rows_fetched_ptr = &fetched;
    SQLUSMALLINT status[10] = {};
    s->row_status_ptr = status;

    // Simulate Access initial load: fetch first 10 rows
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &fetched, status);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)10, (__int64)fetched);

    // Background: fetch next rowset
    ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &fetched, status);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)10, (__int64)fetched);

    // User scrolls to last row: ABSOLUTE -1 (single row fetch for precise positioning)
    s->rowset_size = 1;
    ret = SQLExtendedFetch(stmt, SQL_FETCH_ABSOLUTE, -1, &fetched, status);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    // Verify last row
    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("99"), std::string(buf));

    // Navigate back: RELATIVE -5
    ret = SQLExtendedFetch(stmt, SQL_FETCH_RELATIVE, -5, &fetched, status);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    char buf2[20] = {};
    SQLGetData(stmt, 1, SQL_C_CHAR, buf2, sizeof(buf2), &ind);
    AssertEqual(std::string("94"), std::string(buf2));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_AccessBackground, RowsetWithRetrieveDataOff) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 50);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->rowset_size = 5;

    // Column-wise binding: buffers must be sized for the full rowset
    char buf[5][20] = {};
    SQLLEN ind[5] = {};
    strcpy(buf[0], "INITIAL");
    SQLBindCol(stmt, 1, SQL_C_CHAR, buf[0], 20, &ind[0]);

    // Fetch first rowset (data ON) — rows 0-4
    SQLFetch(stmt);
    AssertEqual(std::string("0"), std::string(buf[0]));

    // Skip ahead with RETRIEVE_DATA OFF
    SQLSetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, (SQLPOINTER)(uintptr_t)SQL_RD_OFF, 0);
    SQLFetch(stmt); // rows 5-9
    SQLFetch(stmt); // rows 10-14

    // Resume with data ON at row 15
    SQLSetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, (SQLPOINTER)(uintptr_t)SQL_RD_ON, 0);
    SQLFetch(stmt); // rows 15-19
    AssertEqual(std::string("15"), std::string(buf[0]));

    FreeHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetInfo cursor attribute test
// ============================================================================
TEST(Phase18_CursorInfo, StaticCursorAttrs1) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);

    SQLUINTEGER val = 0;
    SQLGetInfo(dbc, SQL_STATIC_CURSOR_ATTRIBUTES1, &val, sizeof(val), nullptr);

    // Should include NEXT, ABSOLUTE, RELATIVE, POSITIONED_UPDATE/DELETE
    AssertTrue((val & SQL_CA1_NEXT) != 0, "CA1_NEXT");
    AssertTrue((val & SQL_CA1_ABSOLUTE) != 0, "CA1_ABSOLUTE");
    AssertTrue((val & SQL_CA1_RELATIVE) != 0, "CA1_RELATIVE");

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_CursorInfo, StaticCursorAttrs2_ExactRowCount) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);

    SQLUINTEGER val = 0;
    SQLGetInfo(dbc, SQL_STATIC_CURSOR_ATTRIBUTES2, &val, sizeof(val), nullptr);

    AssertTrue((val & SQL_CA2_READ_ONLY_CONCURRENCY) != 0, "READ_ONLY_CONCURRENCY");
    AssertTrue((val & SQL_CA2_CRC_EXACT) != 0, "CRC_EXACT");

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_CursorInfo, ScrollOptions) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);

    SQLUINTEGER val = 0;
    SQLGetInfo(dbc, SQL_SCROLL_OPTIONS, &val, sizeof(val), nullptr);

    AssertTrue((val & SQL_SO_FORWARD_ONLY) != 0, "FORWARD_ONLY");
    AssertTrue((val & SQL_SO_STATIC) != 0, "STATIC");

    FreeHandles(env, dbc, stmt);
}

// ============================================================================
// FETCH_FIRST / FETCH_LAST after navigation
// ============================================================================
TEST(Phase18_FetchNavigation, FirstAfterAbsolute) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    // Go to row 3
    SQLFetchScroll(stmt, SQL_FETCH_ABSOLUTE, 3);

    // FIRST → row 0
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_FIRST, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("0"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}

TEST(Phase18_FetchNavigation, LastAfterFirst) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateHandles(env, dbc, stmt);
    PopulateRows(stmt, 5);

    SQLFetchScroll(stmt, SQL_FETCH_FIRST, 0);

    // LAST → last row (index 4, value "4")
    SQLRETURN ret = SQLFetchScroll(stmt, SQL_FETCH_LAST, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    char buf[20] = {};
    SQLLEN ind = 0;
    SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual(std::string("4"), std::string(buf));

    FreeHandles(env, dbc, stmt);
}
