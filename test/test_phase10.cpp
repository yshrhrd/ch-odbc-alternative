// Phase 10 Tests: Parameter array execution, SQLExtendedFetch multi-row, compatibility attributes
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
#undef SQLExecDirect
#undef SQLPrepare
#undef SQLDescribeCol
#undef SQLColAttribute
#undef SQLGetDiagRec
#endif

#pragma warning(disable: 4996) // ODBC 2.x deprecated functions

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLExecute(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLBindParameter(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                               SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER,
                                               SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLBindCol(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT, SQLUSMALLINT, SQLLEN, SQLULEN *, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT *);

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
// SQL_ATTR_PARAM_BIND_TYPE test
// ============================================================================

TEST(Phase10_ParamBindType, DefaultIsBindByColumn) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN bindType = 999;
    SQLRETURN ret = SQLGetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, &bindType, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get param bind type should succeed");
    AssertEqual((__int64)SQL_PARAM_BIND_BY_COLUMN, (__int64)bindType, "Default should be SQL_PARAM_BIND_BY_COLUMN");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_ParamBindType, SetRowWise) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, (SQLPOINTER)64, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set param bind type should succeed");

    SQLULEN bindType = 0;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_PARAM_BIND_TYPE, &bindType, 0, nullptr);
    AssertEqual((__int64)64, (__int64)bindType, "Should be 64");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQL_ATTR_PARAM_STATUS_PTR / SQL_ATTR_PARAMS_PROCESSED_PTR test
// ============================================================================

TEST(Phase10_ParamStatus, SetParamStatusPtr) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLUSMALLINT status[5] = {};
    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, status, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set param status ptr should succeed");

    SQLUSMALLINT *got = nullptr;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, &got, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get should succeed");
    AssertTrue(got == status, "Should return same pointer");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_ParamStatus, SetParamsProcessedPtr) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN processed = 0;
    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &processed, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set params processed ptr should succeed");

    SQLULEN *got = nullptr;
    ret = SQLGetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &got, 0, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get should succeed");
    AssertTrue(got == &processed, "Should return same pointer");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQL_ATTR_NOSCAN / SQL_ATTR_CONCURRENCY / SQL_ATTR_MAX_LENGTH test
// ============================================================================

TEST(Phase10_CompatAttrs, NoscanDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN val = 999;
    SQLGetStmtAttr(stmt, SQL_ATTR_NOSCAN, &val, 0, nullptr);
    AssertEqual((__int64)SQL_NOSCAN_OFF, (__int64)val, "Default noscan should be OFF");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_CompatAttrs, NoscanSetOn) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_NOSCAN, (SQLPOINTER)SQL_NOSCAN_ON, 0);
    SQLULEN val = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_NOSCAN, &val, 0, nullptr);
    AssertEqual((__int64)SQL_NOSCAN_ON, (__int64)val, "Noscan should be ON");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_CompatAttrs, ConcurrencyDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN val = 999;
    SQLGetStmtAttr(stmt, SQL_ATTR_CONCURRENCY, &val, 0, nullptr);
    AssertEqual((__int64)SQL_CONCUR_READ_ONLY, (__int64)val, "Default concurrency");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_CompatAttrs, MaxLengthDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN val = 999;
    SQLGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, &val, 0, nullptr);
    AssertEqual((__int64)0, (__int64)val, "Default max_length should be 0 (unlimited)");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_CompatAttrs, RetrieveDataDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN val = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, &val, 0, nullptr);
    AssertEqual((__int64)SQL_RD_ON, (__int64)val, "Default retrieve_data should be ON");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_CompatAttrs, UseBookmarksDefault) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLULEN val = 999;
    SQLGetStmtAttr(stmt, SQL_ATTR_USE_BOOKMARKS, &val, 0, nullptr);
    AssertEqual((__int64)SQL_UB_OFF, (__int64)val, "Default use_bookmarks should be OFF");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_CompatAttrs, UseBookmarksSet) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_USE_BOOKMARKS, (SQLPOINTER)SQL_UB_VARIABLE, 0);
    SQLULEN val = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_USE_BOOKMARKS, &val, 0, nullptr);
    AssertEqual((__int64)SQL_UB_VARIABLE, (__int64)val, "Should be SQL_UB_VARIABLE");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLExtendedFetch multi-row test
// ============================================================================

TEST(Phase10_ExtendedFetch, MultiRowNext) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"val", "String", SQL_VARCHAR, 50, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"aaa"}, {"bbb"}, {"ccc"}};
    s->result_set.current_row = -1;

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)3, 0);

    char buffers[3][32] = {};
    SQLLEN inds[3] = {};
    SQLBindCol(stmt, 1, SQL_C_CHAR, buffers[0], 32, &inds[0]);

    SQLULEN rowCount = 0;
    SQLUSMALLINT rowStatus[3] = {};
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "ExtendedFetch should succeed");
    AssertEqual((__int64)3, (__int64)rowCount, "Should fetch 3 rows");
    AssertEqual(std::string("aaa"), std::string(buffers[0]), "Row 1");
    AssertEqual(std::string("bbb"), std::string(buffers[1]), "Row 2");
    AssertEqual(std::string("ccc"), std::string(buffers[2]), "Row 3");
    AssertEqual((int)SQL_ROW_SUCCESS, (int)rowStatus[0], "Row 1 status");
    AssertEqual((int)SQL_ROW_SUCCESS, (int)rowStatus[1], "Row 2 status");
    AssertEqual((int)SQL_ROW_SUCCESS, (int)rowStatus[2], "Row 3 status");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_ExtendedFetch, PartialRowset) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"n", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"10"}, {"20"}};
    s->result_set.current_row = -1;

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)5, 0);

    char buffers[5][16] = {};
    SQLLEN inds[5] = {};
    SQLBindCol(stmt, 1, SQL_C_CHAR, buffers[0], 16, &inds[0]);

    SQLULEN rowCount = 0;
    SQLUSMALLINT rowStatus[5] = {};
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, rowStatus);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "ExtendedFetch should succeed with partial");
    AssertEqual((__int64)2, (__int64)rowCount, "Should fetch only 2 rows");
    AssertEqual(std::string("10"), std::string(buffers[0]), "Row 1");
    AssertEqual(std::string("20"), std::string(buffers[1]), "Row 2");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_ExtendedFetch, FirstOrientation) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"v", "String", SQL_VARCHAR, 10, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"a"}, {"b"}, {"c"}, {"d"}};
    s->result_set.current_row = 2; // Already at row 3

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)2, 0);

    char buffers[2][16] = {};
    SQLLEN inds[2] = {};
    SQLBindCol(stmt, 1, SQL_C_CHAR, buffers[0], 16, &inds[0]);

    SQLULEN rowCount = 0;
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_FIRST, 0, &rowCount, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "ExtendedFetch FIRST should succeed");
    AssertEqual((__int64)2, (__int64)rowCount, "Should fetch 2 rows from beginning");
    AssertEqual(std::string("a"), std::string(buffers[0]), "Row 1");
    AssertEqual(std::string("b"), std::string(buffers[1]), "Row 2");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_ExtendedFetch, NoDataOnEmpty) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"v", "String", SQL_VARCHAR, 10, 0, SQL_NO_NULLS}};
    s->result_set.rows = {};
    s->result_set.current_row = -1;

    SQLULEN rowCount = 99;
    SQLRETURN ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, nullptr);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "Should return SQL_NO_DATA for empty");
    AssertEqual((__int64)0, (__int64)rowCount, "rowCount should be 0");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_ExtendedFetch, PreservesStmtPointers) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->result_set.columns = {{"v", "String", SQL_VARCHAR, 10, 0, SQL_NO_NULLS}};
    s->result_set.rows = {{"x"}};
    s->result_set.current_row = -1;

    // Set stmt-level pointers
    SQLULEN stmtFetched = 0;
    SQLUSMALLINT stmtStatus[1] = {};
    SQLSetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &stmtFetched, 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_STATUS_PTR, stmtStatus, 0);

    char buf[16] = {};
    SQLLEN ind = 0;
    SQLBindCol(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);

    SQLULEN extCount = 0;
    SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &extCount, nullptr);

    // Verify stmt-level pointers are restored (not overwritten)
    SQLULEN *gotFetched = nullptr;
    SQLGetStmtAttr(stmt, SQL_ATTR_ROWS_FETCHED_PTR, &gotFetched, 0, nullptr);
    AssertTrue(gotFetched == &stmtFetched, "rows_fetched_ptr should be preserved after ExtendedFetch");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// Parameter array execution test (paramset_size)
// ============================================================================

TEST(Phase10_ArrayExec, ParamsProcessedPtrSingleExecution) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);

    // Prepare a query (won't actually execute - no server)
    SQLPrepare(stmt, (SQLCHAR *)"SELECT 1", SQL_NTS);

    SQLULEN processed = 0;
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &processed, 0);

    SQLUSMALLINT status[1] = {99};
    SQLSetStmtAttr(stmt, SQL_ATTR_PARAM_STATUS_PTR, status, 0);

    // Execute (will fail because no server connection, but params_processed should be set)
    SQLExecute(stmt);

    // Even on failure, params_processed should be set to 1 for single execution
    AssertEqual((__int64)1, (__int64)processed, "params_processed should be 1 for single execution");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_ArrayExec, ParamsetSizeField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)1, (__int64)s->paramset_size, "Default paramset_size");
    AssertEqual((__int64)SQL_PARAM_BIND_BY_COLUMN, (__int64)s->param_bind_type, "Default param_bind_type");
    AssertNull(s->param_status_ptr, "Default param_status_ptr should be null");
    AssertNull(s->params_processed_ptr, "Default params_processed_ptr should be null");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// OdbcStatement default values test
// ============================================================================

TEST(Phase10_Defaults, ConcurrencyField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)SQL_CONCUR_READ_ONLY, (__int64)s->concurrency, "Default concurrency");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_Defaults, NoscanField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)SQL_NOSCAN_OFF, (__int64)s->noscan, "Default noscan");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_Defaults, UseBookmarksField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)SQL_UB_OFF, (__int64)s->use_bookmarks, "Default use_bookmarks");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_Defaults, RetrieveDataField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)SQL_RD_ON, (__int64)s->retrieve_data, "Default retrieve_data");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_Defaults, MaxLengthField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual((__int64)0, (__int64)s->max_length, "Default max_length");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// Compatibility attribute Get/Set roundtrip
// ============================================================================

TEST(Phase10_AttrRoundtrip, ConcurrencySet) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_CONCURRENCY, (SQLPOINTER)SQL_CONCUR_VALUES, 0);
    SQLULEN val = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_CONCURRENCY, &val, 0, nullptr);
    AssertEqual((__int64)SQL_CONCUR_VALUES, (__int64)val, "Concurrency roundtrip");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_AttrRoundtrip, MaxLengthSet) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, (SQLPOINTER)4096, 0);
    SQLULEN val = 0;
    SQLGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, &val, 0, nullptr);
    AssertEqual((__int64)4096, (__int64)val, "MaxLength roundtrip");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase10_AttrRoundtrip, RetrieveDataOff) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, (SQLPOINTER)SQL_RD_OFF, 0);
    SQLULEN val = 99;
    SQLGetStmtAttr(stmt, SQL_ATTR_RETRIEVE_DATA, &val, 0, nullptr);
    AssertEqual((__int64)SQL_RD_OFF, (__int64)val, "RetrieveData off roundtrip");

    FreeTestHandles(env, dbc, stmt);
}
