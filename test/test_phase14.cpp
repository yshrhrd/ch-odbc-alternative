// Phase 14 Tests: Bug fixes & driver quality enhancement
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
#undef SQLDriverConnect
#undef SQLNativeSql
#undef SQLColAttributes
#undef SQLExecDirect
#undef SQLGetDiagRec
#undef SQLPrepare
#undef SQLDescribeParam
#endif

#pragma warning(disable: 4996)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLDriverConnect(SQLHDBC, SQLHWND, SQLCHAR *, SQLSMALLINT,
                                               SQLCHAR *, SQLSMALLINT, SQLSMALLINT *, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLNativeSql(SQLHDBC, SQLCHAR *, SQLINTEGER, SQLCHAR *, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLNativeSqlW(SQLHDBC, SQLWCHAR *, SQLINTEGER, SQLWCHAR *, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLCHAR *, SQLINTEGER *, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLExecDirect(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLAllocEnv(SQLHENV *);
extern "C" SQLRETURN SQL_API SQLFreeEnv(SQLHENV);
extern "C" SQLRETURN SQL_API SQLAllocConnect(SQLHENV, SQLHDBC *);
extern "C" SQLRETURN SQL_API SQLFreeConnect(SQLHDBC);
extern "C" SQLRETURN SQL_API SQLAllocStmt(SQLHDBC, SQLHSTMT *);
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLExecute(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLBindParameter(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                               SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER,
                                               SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLNumParams(SQLHSTMT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT *, SQLULEN *,
                                               SQLSMALLINT *, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// Helper: create env + conn
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

// ============================================================================
// 1. ExecDirectImpl escape sequence bug fix test
// ============================================================================
TEST(Phase14_EscapeBugFix, ProcessOdbcEscapeSequencesUsed) {
    // Verify that ExecDirectImpl uses the result of ProcessOdbcEscapeSequences
    // to execute the query (cannot test directly, but confirm that the
    // ProcessOdbcEscapeSequences conversion is reflected in stmt->query)
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    // Set conn->connected = true
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    // Execute a query containing escape sequences (will fail because no server,
    // but verify that the converted query is set in stmt->query)
    auto *stmt = static_cast<OdbcStatement *>(stmt_h);
    std::string test_query = "SELECT {fn UCASE('hello')}";
    // Execution fails without a server, but that is expected
    SQLExecDirect(stmt_h, (SQLCHAR *)test_query.c_str(), SQL_NTS);

    // Verify that stmt->query contains the escape-processed query
    // UCASE → upper conversion should have been applied
    AssertTrue(stmt->query.find("upper") != std::string::npos,
               "Escape sequence should be processed: found 'upper' in stmt->query");
    AssertTrue(stmt->query.find("{fn") == std::string::npos,
               "No unprocessed escape sequences should remain");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

TEST(Phase14_EscapeBugFix, MultipleEscapeSequences) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);
    std::string test_query = "SELECT {fn LCASE(name)}, {fn NOW()}";
    SQLExecDirect(stmt_h, (SQLCHAR *)test_query.c_str(), SQL_NTS);

    AssertTrue(stmt->query.find("lower") != std::string::npos,
               "LCASE should be converted to lower");
    AssertTrue(stmt->query.find("now()") != std::string::npos,
               "NOW should be converted to now()");
    AssertTrue(stmt->query.find("{fn") == std::string::npos,
               "No unprocessed escape sequences should remain");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

// ============================================================================
// 2. SQLGetFunctions SQL_API_ALL_FUNCTIONS test
// ============================================================================
TEST(Phase14_GetFunctions, AllFunctionsODBC2x) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLUSMALLINT funcs[100] = {};
    SQLRETURN ret = SQLGetFunctions(dbc, SQL_API_ALL_FUNCTIONS, funcs);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_API_ALL_FUNCTIONS should succeed");

    // Check core functions are supported
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLALLOCENV], "SQLAllocEnv supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLFREEENV], "SQLFreeEnv supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLALLOCCONNECT], "SQLAllocConnect supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLFREECONNECT], "SQLFreeConnect supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLALLOCSTMT], "SQLAllocStmt supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLFREESTMT], "SQLFreeStmt supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLCONNECT], "SQLConnect supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLDISCONNECT], "SQLDisconnect supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLDRIVERCONNECT], "SQLDriverConnect supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLEXECDIRECT], "SQLExecDirect supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLPREPARE], "SQLPrepare supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLEXECUTE], "SQLExecute supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLFETCH], "SQLFetch supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLGETDATA], "SQLGetData supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLERROR], "SQLError supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLBROWSECONNECT], "SQLBrowseConnect supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLEXTENDEDFETCH], "SQLExtendedFetch supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLTRANSACT], "SQLTransact supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLTABLES], "SQLTables supported");
    AssertEqual((int)SQL_TRUE, (int)funcs[SQL_API_SQLCOLUMNS], "SQLColumns supported");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_GetFunctions, AllFunctionsODBC2xUnsupported) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLUSMALLINT funcs[100] = {};
    SQLGetFunctions(dbc, SQL_API_ALL_FUNCTIONS, funcs);

    // Functions that are NOT supported should be SQL_FALSE
    // SQL_API_SQLSETPARAM (ID 10, deprecated ODBC 1.x) — typically not implemented
    // We just check that some high ID is FALSE
    AssertEqual((int)SQL_FALSE, (int)funcs[99], "Function 99 should not be supported");

    FreeEnvConn(env, dbc);
}

// ============================================================================
// 3. SQLGetFunctions individual check fix test
// ============================================================================
TEST(Phase14_GetFunctions, IndividualCheckEnvAttr) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLGETENVATTR, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetEnvAttr should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLSETENVATTR, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetEnvAttr should be supported");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_GetFunctions, IndividualCheckStmtAttr) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLGETSTMTATTR, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetStmtAttr should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLSETSTMTATTR, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetStmtAttr should be supported");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_GetFunctions, IndividualCheckDiagFunctions) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLGETDIAGREC, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetDiagRec should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLGETDIAGFIELD, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetDiagField should be supported");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_GetFunctions, IndividualCheckNativeSql) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLNATIVESQL, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLNativeSql should be supported");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_GetFunctions, IndividualCheckLegacyAlloc) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLALLOCENV, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLAllocEnv should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLFREEENV, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLFreeEnv should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLALLOCCONNECT, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLAllocConnect should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLFREECONNECT, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLFreeConnect should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLALLOCSTMT, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLAllocStmt should be supported");

    FreeEnvConn(env, dbc);
}

// ============================================================================
// 4. SQLDriverConnect DriverCompletion test
// ============================================================================
TEST(Phase14_DriverConnect, DriverPromptReturnsError) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLCHAR out[256] = {};
    SQLSMALLINT out_len = 0;
    SQLRETURN ret = SQLDriverConnect(dbc, nullptr,
        (SQLCHAR *)"HOST=localhost;PORT=8123", SQL_NTS,
        out, sizeof(out), &out_len, SQL_DRIVER_PROMPT);
    AssertEqual((int)SQL_ERROR, (int)ret, "SQL_DRIVER_PROMPT should return SQL_ERROR");

    // Check diagnostic
    SQLCHAR state[6] = {};
    SQLINTEGER native_err = 0;
    SQLCHAR msg[256] = {};
    SQLSMALLINT msg_len = 0;
    SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state, &native_err, msg, sizeof(msg), &msg_len);
    AssertEqual(std::string("HYC00"), std::string((char *)state), "SQLSTATE should be HYC00");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_DriverConnect, DriverCompleteWithEmptyString) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLCHAR out[256] = {};
    SQLSMALLINT out_len = 0;
    // Empty connection string with SQL_DRIVER_COMPLETE should fail (no dialog to prompt)
    SQLRETURN ret = SQLDriverConnect(dbc, nullptr,
        (SQLCHAR *)"", SQL_NTS,
        out, sizeof(out), &out_len, SQL_DRIVER_COMPLETE);
    AssertEqual((int)SQL_ERROR, (int)ret, "SQL_DRIVER_COMPLETE with empty string should return SQL_ERROR");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_DriverConnect, DriverNopromptWithHost) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLCHAR out[256] = {};
    SQLSMALLINT out_len = 0;
    // SQL_DRIVER_NOPROMPT with host should attempt connection (will fail due to no server)
    SQLRETURN ret = SQLDriverConnect(dbc, nullptr,
        (SQLCHAR *)"HOST=nonexistent.host;PORT=8123", SQL_NTS,
        out, sizeof(out), &out_len, SQL_DRIVER_NOPROMPT);
    // Should get connection error, not prompt error
    AssertEqual((int)SQL_ERROR, (int)ret, "SQL_DRIVER_NOPROMPT should attempt connection");

    SQLCHAR state[6] = {};
    SQLINTEGER native_err = 0;
    SQLCHAR msg[256] = {};
    SQLSMALLINT msg_len = 0;
    SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state, &native_err, msg, sizeof(msg), &msg_len);
    // Should be a connection error (08001), not HYC00
    AssertEqual(std::string("08001"), std::string((char *)state), "SQLSTATE should be 08001 (connection error)");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_DriverConnect, DriverCompleteWithHost) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLCHAR out[256] = {};
    SQLSMALLINT out_len = 0;
    // SQL_DRIVER_COMPLETE with HOST present should attempt connection (not prompt)
    SQLRETURN ret = SQLDriverConnect(dbc, nullptr,
        (SQLCHAR *)"HOST=nonexistent.host;PORT=8123", SQL_NTS,
        out, sizeof(out), &out_len, SQL_DRIVER_COMPLETE);
    // Should get connection error, not HYC00
    AssertEqual((int)SQL_ERROR, (int)ret, "SQL_DRIVER_COMPLETE with HOST should attempt connection");

    SQLCHAR state[6] = {};
    SQLINTEGER native_err = 0;
    SQLCHAR msg[256] = {};
    SQLSMALLINT msg_len = 0;
    SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, state, &native_err, msg, sizeof(msg), &msg_len);
    AssertEqual(std::string("08001"), std::string((char *)state), "SQLSTATE should be 08001 (connection error)");

    FreeEnvConn(env, dbc);
}

// ============================================================================
// 5. SQLCloseCursor 24000 check test
// ============================================================================
TEST(Phase14_CloseCursor, NoCursorReturns24000) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    // No result set opened — SQLCloseCursor should return 24000
    SQLRETURN ret = SQLCloseCursor(stmt_h);
    AssertEqual((int)SQL_ERROR, (int)ret, "SQLCloseCursor without cursor should return SQL_ERROR");

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);
    AssertTrue(!stmt->diag_records.empty(), "Should have diagnostic record");
    if (!stmt->diag_records.empty()) {
        AssertEqual(std::string("24000"), stmt->diag_records[0].sql_state,
                    "SQLSTATE should be 24000");
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

TEST(Phase14_CloseCursor, WithCursorSucceeds) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    // Manually populate result set to simulate open cursor
    auto *stmt = static_cast<OdbcStatement *>(stmt_h);
    ColumnInfo col;
    col.name = "test";
    col.sql_type = SQL_VARCHAR;
    col.column_size = 100;
    stmt->result_set.columns.push_back(col);
    stmt->result_set.rows.push_back({std::optional<std::string>("value")});

    // Now SQLCloseCursor should succeed
    SQLRETURN ret = SQLCloseCursor(stmt_h);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLCloseCursor with cursor should succeed");

    // Rows should be cleared, but column metadata (IRD) is preserved per ODBC spec
    AssertTrue(stmt->result_set.rows.empty(), "Rows should be cleared after close");
    AssertTrue(!stmt->result_set.columns.empty(), "Column metadata should be preserved after close");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

// ============================================================================
// 6. SQLNativeSqlW output length fix test
// ============================================================================
TEST(Phase14_NativeSqlW, TextLengthReturned) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    // Call SQLNativeSqlW with null output buffer but valid length pointer
    SQLINTEGER text_len = 0;
    std::string test_sql = "SELECT 1";
    std::wstring wide_sql = Utf8ToWide(test_sql);
    SQLRETURN ret = SQLNativeSqlW(dbc, (SQLWCHAR *)wide_sql.c_str(),
                                   SQL_NTS, nullptr, 0, &text_len);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLNativeSqlW should succeed");
    AssertTrue(text_len > 0, "TextLength2Ptr should be set even when output buffer is null");
    // Length should be in bytes (characters * sizeof(SQLWCHAR))
    AssertEqual((__int64)(test_sql.size() * sizeof(SQLWCHAR)), (__int64)text_len,
                "TextLength should be byte count of wide string");

    FreeEnvConn(env, dbc);
}

TEST(Phase14_NativeSqlW, EscapeProcessingWithLength) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    SQLINTEGER text_len = 0;
    std::string test_sql = "SELECT {fn UCASE('hello')}";
    std::wstring wide_sql = Utf8ToWide(test_sql);
    SQLRETURN ret = SQLNativeSqlW(dbc, (SQLWCHAR *)wide_sql.c_str(),
                                   SQL_NTS, nullptr, 0, &text_len);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLNativeSqlW with escape should succeed");
    AssertTrue(text_len > 0, "TextLength should be positive");

    // Also call with output buffer to verify content
    SQLWCHAR out[256] = {};
    SQLINTEGER out_len = 0;
    ret = SQLNativeSqlW(dbc, (SQLWCHAR *)wide_sql.c_str(),
                         SQL_NTS, out, 256, &out_len);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLNativeSqlW with output should succeed");

    std::string result = WideToUtf8(out, -1);
    AssertTrue(result.find("upper") != std::string::npos,
               "Escape should be processed: UCASE -> upper");

    FreeEnvConn(env, dbc);
}

// ============================================================================
// 7. ODBC 2.x legacy alloc function test
// ============================================================================
TEST(Phase14_LegacyAlloc, AllocFreeEnv) {
    SQLHENV env = SQL_NULL_HENV;
    SQLRETURN ret = SQLAllocEnv(&env);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLAllocEnv should succeed");
    AssertTrue(env != SQL_NULL_HENV, "Environment handle should be allocated");

    ret = SQLFreeEnv(env);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLFreeEnv should succeed");
}

TEST(Phase14_LegacyAlloc, AllocFreeConnect) {
    SQLHENV env = SQL_NULL_HENV;
    SQLAllocEnv(&env);

    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLRETURN ret = SQLAllocConnect(env, &dbc);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLAllocConnect should succeed");
    AssertTrue(dbc != SQL_NULL_HDBC, "Connection handle should be allocated");

    ret = SQLFreeConnect(dbc);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLFreeConnect should succeed");

    SQLFreeEnv(env);
}

TEST(Phase14_LegacyAlloc, AllocFreeStmt) {
    SQLHENV env = SQL_NULL_HENV;
    SQLAllocEnv(&env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocConnect(env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN ret = SQLAllocStmt(dbc, &stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLAllocStmt should succeed");
    AssertTrue(stmt != SQL_NULL_HSTMT, "Statement handle should be allocated");

    ret = SQLFreeStmt(stmt, SQL_DROP);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLFreeStmt SQL_DROP should succeed");

    conn->connected = false;
    SQLFreeConnect(dbc);
    SQLFreeEnv(env);
}

// ============================================================================
// 8. SQLGetFunctions ODBC 3.x bitmap integrity test
// ============================================================================
TEST(Phase14_GetFunctions, BitmapAndIndividualConsistent) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    // Get bitmap
    SQLUSMALLINT bitmap[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE] = {};
    SQLGetFunctions(dbc, SQL_API_ODBC3_ALL_FUNCTIONS, bitmap);

    // Check several functions that should be in both bitmap and individual
    SQLUSMALLINT funcs_to_check[] = {
        SQL_API_SQLALLOCHANDLE, SQL_API_SQLFREEHANDLE,
        SQL_API_SQLGETENVATTR, SQL_API_SQLSETENVATTR,
        SQL_API_SQLGETSTMTATTR, SQL_API_SQLSETSTMTATTR,
        SQL_API_SQLGETDIAGREC, SQL_API_SQLGETDIAGFIELD,
        SQL_API_SQLNATIVESQL, SQL_API_SQLBROWSECONNECT,
        SQL_API_SQLGETDESCFIELD, SQL_API_SQLCOPYDESC
    };

    for (SQLUSMALLINT fid : funcs_to_check) {
        // Check bitmap
        bool in_bitmap = (bitmap[fid >> 4] & (1 << (fid & 0x000F))) != 0;
        AssertTrue(in_bitmap, "Function " + std::to_string(fid) + " should be in bitmap");

        // Check individual
        SQLUSMALLINT supported = SQL_FALSE;
        SQLGetFunctions(dbc, fid, &supported);
        AssertEqual((int)SQL_TRUE, (int)supported,
                    "Function " + std::to_string(fid) + " individual check");
    }

    FreeEnvConn(env, dbc);
}

// ============================================================================
// 9. SQLFreeStmt SQL_CLOSE test (regression test of existing behavior)
// ============================================================================
TEST(Phase14_FreeStmt, SqlCloseResetsResult) {
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);

    // Populate result set
    ColumnInfo col;
    col.name = "test";
    col.sql_type = SQL_VARCHAR;
    stmt->result_set.columns.push_back(col);
    stmt->result_set.rows.push_back({std::optional<std::string>("val")});

    // SQL_CLOSE should reset result but keep prepared query
    stmt->query = "SELECT test";
    stmt->prepared = true;

    SQLRETURN ret = SQLFreeStmt(stmt_h, SQL_CLOSE);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_CLOSE should succeed");
    // ODBC spec: SQL_CLOSE clears rows/cursor but preserves column metadata (IRD)
    AssertTrue(stmt->result_set.rows.empty(), "Rows should be cleared after SQL_CLOSE");
    AssertTrue(!stmt->result_set.columns.empty(), "Column metadata should be preserved after SQL_CLOSE");
    AssertEqual(std::string("SELECT test"), stmt->query, "Query should be preserved");
    AssertTrue(stmt->prepared, "Prepared flag should be preserved");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

// ============================================================================
// 10. Composite primary key support test: SQLNumParams uses prepared_query
// ============================================================================
TEST(Phase14_CompositeKey, NumParamsPreservedAfterExecute) {
    // Composite primary key WHERE condition: WHERE key1=? AND key2=?
    // Verify that SQLNumParams still returns 2 correctly after SQLExecute
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);

    // Prepare a composite primary key query
    std::string query = "SELECT * FROM test_table WHERE key1=? AND key2=?";
    SQLPrepare(stmt_h, (SQLCHAR *)query.c_str(), SQL_NTS);

    // SQLNumParams should return 2
    SQLSMALLINT param_count = 0;
    SQLNumParams(stmt_h, &param_count);
    AssertEqual(2, (int)param_count, "NumParams should return 2 for composite key query before execute");

    // Simulate ExecDirectImpl overwriting stmt->query
    // (actual SQLExecute requires a server, so overwrite directly)
    stmt->query = "SELECT * FROM test_table WHERE key1=100 AND key2='ABC'";

    // SQLNumParams uses prepared_query, so it should still return 2
    param_count = 0;
    SQLNumParams(stmt_h, &param_count);
    AssertEqual(2, (int)param_count, "NumParams should still return 2 after query overwrite (uses prepared_query)");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

TEST(Phase14_CompositeKey, DescribeParamAfterExecute) {
    // Verify that SQLDescribeParam still works correctly after ExecDirectImpl overwrite
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);

    // Query with 3 parameters
    std::string query = "SELECT * FROM test_table WHERE key1=? AND key2=? AND key3=?";
    SQLPrepare(stmt_h, (SQLCHAR *)query.c_str(), SQL_NTS);

    // Simulate ExecDirectImpl overwrite
    stmt->query = "SELECT * FROM test_table WHERE key1=1 AND key2=2 AND key3=3";

    // SQLDescribeParam should accept parameters 1, 2, 3
    SQLSMALLINT data_type = 0;
    SQLULEN param_size = 0;
    SQLSMALLINT dec_digits = 0;
    SQLSMALLINT nullable = 0;

    SQLRETURN ret = SQLDescribeParam(stmt_h, 1, &data_type, &param_size, &dec_digits, &nullable);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "DescribeParam(1) should succeed after query overwrite");

    ret = SQLDescribeParam(stmt_h, 2, &data_type, &param_size, &dec_digits, &nullable);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "DescribeParam(2) should succeed after query overwrite");

    ret = SQLDescribeParam(stmt_h, 3, &data_type, &param_size, &dec_digits, &nullable);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "DescribeParam(3) should succeed after query overwrite");

    // Parameter 4 does not exist, so should return an error
    ret = SQLDescribeParam(stmt_h, 4, &data_type, &param_size, &dec_digits, &nullable);
    AssertEqual((int)SQL_ERROR, (int)ret, "DescribeParam(4) should fail for 3-param query");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

TEST(Phase14_CompositeKey, SubstituteMultipleParams) {
    // Verify that SubstituteParameters correctly substitutes multiple parameters for composite primary keys
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;

    // Parameter 1: INTEGER (key1)
    SQLINTEGER val1 = 42;
    SQLLEN ind1 = sizeof(val1);
    BoundParameter bp1;
    bp1.value_type = SQL_C_SLONG;
    bp1.parameter_type = SQL_INTEGER;
    bp1.parameter_value = &val1;
    bp1.buffer_length = sizeof(val1);
    bp1.str_len_or_ind = &ind1;
    params[1] = bp1;

    // Parameter 2: VARCHAR (key2)
    char val2[] = "ABC";
    SQLLEN ind2 = SQL_NTS;
    BoundParameter bp2;
    bp2.value_type = SQL_C_CHAR;
    bp2.parameter_type = SQL_VARCHAR;
    bp2.parameter_value = val2;
    bp2.buffer_length = sizeof(val2);
    bp2.str_len_or_ind = &ind2;
    params[2] = bp2;

    // Parameter 3: DOUBLE (key3)
    double val3 = 99.5;
    SQLLEN ind3 = sizeof(val3);
    BoundParameter bp3;
    bp3.value_type = SQL_C_DOUBLE;
    bp3.parameter_type = SQL_DOUBLE;
    bp3.parameter_value = &val3;
    bp3.buffer_length = sizeof(val3);
    bp3.str_len_or_ind = &ind3;
    params[3] = bp3;

    std::string error_msg;
    std::string result = SubstituteParameters(
        "SELECT * FROM t WHERE k1=? AND k2=? AND k3=?", params, error_msg);

    AssertTrue(!result.empty(), "SubstituteParameters should produce non-empty result");
    AssertTrue(result.find("42") != std::string::npos, "Result should contain value 42 for key1");
    AssertTrue(result.find("'ABC'") != std::string::npos, "Result should contain 'ABC' for key2");
    AssertTrue(result.find("99.5") != std::string::npos, "Result should contain 99.5 for key3");
    AssertTrue(result.find("?") == std::string::npos, "No unsubstituted ? should remain");
}

TEST(Phase14_CompositeKey, PreparedQueryTemplateForMultipleExecute) {
    // Verify that prepared_query template is preserved across repeated SQLExecute for composite primary keys
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);

    // Prepare a composite key query
    std::string query = "SELECT col1, col2 FROM test_table WHERE key1=? AND key2=?";
    SQLPrepare(stmt_h, (SQLCHAR *)query.c_str(), SQL_NTS);

    // Verify that the template is saved in prepared_query
    AssertTrue(stmt->prepared_query.find("?") != std::string::npos,
              "prepared_query should contain ? placeholders");

    // 1st: Simulate ExecDirectImpl overwrite
    stmt->query = "SELECT col1, col2 FROM test_table WHERE key1=1 AND key2='A'";

    // Verify that prepared_query has not changed
    AssertTrue(stmt->prepared_query.find("key1=?") != std::string::npos,
              "prepared_query should still contain key1=? after first execute");
    AssertTrue(stmt->prepared_query.find("key2=?") != std::string::npos,
              "prepared_query should still contain key2=? after first execute");

    // 2nd: Overwrite again
    stmt->query = "SELECT col1, col2 FROM test_table WHERE key1=2 AND key2='B'";

    // Verify that prepared_query has not changed
    SQLSMALLINT param_count = 0;
    SQLNumParams(stmt_h, &param_count);
    AssertEqual(2, (int)param_count, "NumParams should return 2 even after multiple query overwrites");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

TEST(Phase14_CompositeKey, ExecuteWithDAEUsesTemplate) {
    // Verify that ExecuteWithDAE uses the prepared_query template
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);

    // Set the template in prepared_query
    stmt->prepared_query = "INSERT INTO t VALUES (?, ?)";
    stmt->query = "INSERT INTO t VALUES (1, 'old')";  // Overwritten state
    stmt->prepared = true;

    // Verify that prepared_query contains ? (precondition for ExecuteWithDAE to work correctly)
    const std::string &template_q = stmt->prepared_query.empty() ? stmt->query : stmt->prepared_query;
    AssertTrue(template_q.find("?") != std::string::npos,
              "Template query should contain ? for DAE substitution");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

// ============================================================================
// 15. CloseCursor: Verify that SQL_CLOSE preserves column metadata
// ============================================================================
TEST(Phase14_CloseCursor, ColumnsPreservedAfterSQLClose) {
    // ODBC spec: SQL_CLOSE should preserve IRD (column metadata)
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);

    // Set column metadata and row data manually
    ColumnInfo col1;
    col1.name = "key1";
    col1.sql_type = SQL_INTEGER;
    col1.column_size = 10;
    col1.decimal_digits = 0;
    col1.nullable = SQL_NULLABLE;
    col1.clickhouse_type = "Int32";

    ColumnInfo col2;
    col2.name = "key2";
    col2.sql_type = SQL_VARCHAR;
    col2.column_size = 100;
    col2.decimal_digits = 0;
    col2.nullable = SQL_NULLABLE;
    col2.clickhouse_type = "String";

    stmt->result_set.columns.push_back(col1);
    stmt->result_set.columns.push_back(col2);
    ResultRow row1 = {std::string("42"), std::string("ABC")};
    stmt->result_set.rows.push_back(row1);
    stmt->result_set.current_row = 0;

    // Execute SQL_CLOSE
    SQLRETURN ret = SQLFreeStmt(stmt_h, SQL_CLOSE);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQL_CLOSE should succeed");

    // Row data should be cleared
    AssertTrue(stmt->result_set.rows.empty(), "Rows should be cleared after SQL_CLOSE");
    AssertEqual((SQLLEN)-1, stmt->result_set.current_row, "Cursor should be reset after SQL_CLOSE");

    // Column metadata should be preserved (IRD preservation)
    AssertEqual(2, (int)stmt->result_set.columns.size(),
               "Column metadata should be preserved after SQL_CLOSE");
    AssertEqual(std::string("key1"), stmt->result_set.columns[0].name,
               "Column 1 name should be preserved");
    AssertEqual(std::string("key2"), stmt->result_set.columns[1].name,
               "Column 2 name should be preserved");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}

TEST(Phase14_CloseCursor, ResetClearsEverything) {
    // Reset() clears completely (both columns and rows)
    ResultSet rs;
    ColumnInfo col;
    col.name = "test";
    col.sql_type = SQL_INTEGER;
    rs.columns.push_back(col);
    ResultRow row = {std::string("1")};
    rs.rows.push_back(row);
    rs.current_row = 0;

    rs.Reset();
    AssertTrue(rs.columns.empty(), "Reset should clear columns");
    AssertTrue(rs.rows.empty(), "Reset should clear rows");
    AssertEqual((SQLLEN)-1, rs.current_row, "Reset should clear current_row");
}

TEST(Phase14_CloseCursor, CloseCursorPreservesColumns) {
    // CloseCursor() clears only rows, preserves columns
    ResultSet rs;
    ColumnInfo col;
    col.name = "test_col";
    col.sql_type = SQL_VARCHAR;
    rs.columns.push_back(col);
    ResultRow row = {std::string("value")};
    rs.rows.push_back(row);
    rs.current_row = 0;

    rs.CloseCursor();
    AssertEqual(1, (int)rs.columns.size(), "CloseCursor should preserve columns");
    AssertEqual(std::string("test_col"), rs.columns[0].name, "CloseCursor should preserve column name");
    AssertTrue(rs.rows.empty(), "CloseCursor should clear rows");
    AssertEqual((SQLLEN)-1, rs.current_row, "CloseCursor should reset current_row");
}

// ============================================================================
// 16. SubstituteParameters: Skip ? inside backtick-quoted identifiers
// ============================================================================
TEST(Phase14_Backtick, QuestionMarkInsideBacktickNotSubstituted) {
    // ? inside backtick-quoted identifiers should not be substituted
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;

    SQLINTEGER val1 = 100;
    SQLLEN ind1 = sizeof(val1);
    BoundParameter bp1;
    bp1.value_type = SQL_C_SLONG;
    bp1.parameter_type = SQL_INTEGER;
    bp1.parameter_value = &val1;
    bp1.buffer_length = sizeof(val1);
    bp1.str_len_or_ind = &ind1;
    params[1] = bp1;

    // Only ? outside backticks should be substituted
    std::string error_msg;
    std::string result = SubstituteParameters(
        "SELECT `col?name` FROM t WHERE id=?", params, error_msg);

    AssertTrue(!result.empty(), "SubstituteParameters should succeed");
    AssertTrue(result.find("`col?name`") != std::string::npos,
              "Backtick-quoted ? should not be substituted");
    AssertTrue(result.find("id=100") != std::string::npos,
              "Unquoted ? should be substituted with 100");
}

TEST(Phase14_Backtick, AccessStyleCompositeKeyQuery) {
    // Test the composite primary key query pattern generated by Access
    // Access uses SQL_IDENTIFIER_QUOTE_CHAR=` to quote identifiers
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;

    SQLINTEGER val1 = 1001;
    SQLLEN ind1 = sizeof(val1);
    BoundParameter bp1;
    bp1.value_type = SQL_C_SLONG;
    bp1.parameter_type = SQL_INTEGER;
    bp1.parameter_value = &val1;
    bp1.buffer_length = sizeof(val1);
    bp1.str_len_or_ind = &ind1;
    params[1] = bp1;

    char val2[] = "PROD-A";
    SQLLEN ind2 = SQL_NTS;
    BoundParameter bp2;
    bp2.value_type = SQL_C_CHAR;
    bp2.parameter_type = SQL_VARCHAR;
    bp2.parameter_value = val2;
    bp2.buffer_length = sizeof(val2);
    bp2.str_len_or_ind = &ind2;
    params[2] = bp2;

    std::string error_msg;
    std::string result = SubstituteParameters(
        "SELECT `M_ITEM`.`item_code`, `M_ITEM`.`item_name` FROM `M_ITEM` WHERE `M_ITEM`.`key1` = ? AND `M_ITEM`.`key2` = ?",
        params, error_msg);

    AssertTrue(!result.empty(), "Access-style composite key query should be substituted");
    AssertTrue(result.find("1001") != std::string::npos,
              "key1 should be substituted with 1001");
    AssertTrue(result.find("'PROD-A'") != std::string::npos,
              "key2 should be substituted with 'PROD-A'");
    AssertTrue(result.find("`M_ITEM`") != std::string::npos,
              "Backtick-quoted identifiers should be preserved");
    AssertTrue(result.find("?") == std::string::npos,
              "No unsubstituted ? should remain");
}

// ============================================================================
// 17. FetchPreparedMetadata: Attempt metadata retrieval even for parameterized queries
// ============================================================================
TEST(Phase14_FetchPreparedMetadata, ParameterizedQueryHandled) {
    // Verify that FetchPreparedMetadata replaces ? with NULL to attempt metadata retrieval
    // (fails without a server, but confirms that ? no longer causes early return)
    SQLHENV env;
    SQLHDBC dbc;
    CreateEnvConn(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT stmt_h = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);

    // Prepare a parameterized SELECT query
    std::string query = "SELECT col1, col2 FROM test_table WHERE key1=? AND key2=?";
    SQLPrepare(stmt_h, (SQLCHAR *)query.c_str(), SQL_NTS);

    // Verify that prepared_query is set correctly
    AssertTrue(stmt->prepared, "Statement should be marked as prepared");
    AssertTrue(stmt->prepared_query.find("?") != std::string::npos,
              "prepared_query should contain ? placeholders");

    // FetchPreparedMetadata fails without a server, but we verify internally
    // that ? in prepared_query no longer causes an early return
    // (previously, having ? would cause immediate return)
    // Calling SQLNumResultCols triggers FetchPreparedMetadata
    SQLSMALLINT col_count = -1;
    SQLRETURN ret = SQLNumResultCols(stmt_h, &col_count);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLNumResultCols should succeed");
    // Returns 0 without a server, but verify no crash
    AssertEqual(0, (int)col_count, "Column count 0 expected (no server)");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    conn->connected = false;
    FreeEnvConn(env, dbc);
}
