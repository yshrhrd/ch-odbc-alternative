// Phase 7 Tests: ODBC 2.x legacy functions & Data-at-Execution
#include "test_framework.h"

// Suppress deprecation warnings for ODBC 2.x functions we're intentionally testing
#pragma warning(disable: 4996)

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
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLGetDiagRec
#undef SQLError
#endif

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLExecute(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLBindParameter(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                               SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER,
                                               SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLParamData(SQLHSTMT, SQLPOINTER *);
extern "C" SQLRETURN SQL_API SQLPutData(SQLHSTMT, SQLPOINTER, SQLLEN);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLNumParams(SQLHSTMT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLError(SQLHENV, SQLHDBC, SQLHSTMT, SQLCHAR *, SQLINTEGER *, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLErrorW(SQLHENV, SQLHDBC, SQLHSTMT, SQLWCHAR *, SQLINTEGER *, SQLWCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC, SQLUSMALLINT, SQLPOINTER);
extern "C" SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC, SQLUSMALLINT, SQLULEN);
extern "C" SQLRETURN SQL_API SQLGetStmtOption(SQLHSTMT, SQLUSMALLINT, SQLPOINTER);
extern "C" SQLRETURN SQL_API SQLSetStmtOption(SQLHSTMT, SQLUSMALLINT, SQLULEN);
extern "C" SQLRETURN SQL_API SQLTransact(SQLHENV, SQLHDBC, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
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
// SQLParamData / SQLPutData test (Data-at-Execution)
// ============================================================================

TEST(Phase7_DAE, ExecuteWithNoDAEParams) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Prepare and bind a normal (non-DAE) parameter
    const char *query = "SELECT ?";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    char buf[32] = "hello";
    SQLLEN ind = SQL_NTS;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, buf, sizeof(buf), &ind);

    // SQLExecute should NOT return SQL_NEED_DATA since ind is SQL_NTS (not DAE)
    auto ret = SQLExecute(stmt);
    // Without server connection, it will fail with connection error, but NOT SQL_NEED_DATA
    AssertTrue(ret != SQL_NEED_DATA, "Non-DAE params should not trigger NEED_DATA");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, ExecuteDetectsDAEParam) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?, ?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    // Param 1: normal
    SQLINTEGER val = 42;
    SQLLEN ind1 = sizeof(val);
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 10, 0, &val, sizeof(val), &ind1);

    // Param 2: data-at-execution
    SQLLEN ind2 = SQL_DATA_AT_EXEC;
    SQLPOINTER token = (SQLPOINTER)(intptr_t)2; // token to identify param
    SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, token, 0, &ind2);

    auto ret = SQLExecute(stmt);
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "Should return SQL_NEED_DATA for DAE param");

    // Verify statement is in need_data state
    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertTrue(s->need_data, "need_data should be true");
    AssertEqual(1, (int)s->pending_dae_params.size(), "Should have 1 DAE param pending");
    AssertEqual(2, (int)s->pending_dae_params[0], "DAE param should be param 2");

    // Clean up DAE state
    s->ResetDAE();
    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, ExecuteDetectsLenDataAtExec) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    // SQL_LEN_DATA_AT_EXEC macro: indicator = SQL_LEN_DATA_AT_EXEC_OFFSET - length
    SQLLEN ind = SQL_LEN_DATA_AT_EXEC(100);
    SQLPOINTER token = (SQLPOINTER)(intptr_t)1;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, token, 0, &ind);

    auto ret = SQLExecute(stmt);
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "SQL_LEN_DATA_AT_EXEC should trigger NEED_DATA");

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->ResetDAE();
    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, ParamDataReturnsTokens) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?, ?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    // Both params are DAE
    SQLLEN ind1 = SQL_DATA_AT_EXEC;
    SQLLEN ind2 = SQL_DATA_AT_EXEC;
    SQLPOINTER token1 = (SQLPOINTER)(intptr_t)101;
    SQLPOINTER token2 = (SQLPOINTER)(intptr_t)102;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, token1, 0, &ind1);
    SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, token2, 0, &ind2);

    auto ret = SQLExecute(stmt);
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "Should need data");

    // First SQLParamData should return token for param 1
    SQLPOINTER value = nullptr;
    ret = SQLParamData(stmt, &value);
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "First ParamData should return NEED_DATA");
    AssertTrue(value == token1, "First token should be param 1's token");

    // Put data for param 1
    ret = SQLPutData(stmt, (SQLPOINTER)"hello", 5);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "PutData should succeed");

    // Second SQLParamData should return token for param 2
    ret = SQLParamData(stmt, &value);
    AssertEqual((int)SQL_NEED_DATA, (int)ret, "Second ParamData should return NEED_DATA");
    AssertTrue(value == token2, "Second token should be param 2's token");

    // Put data for param 2
    ret = SQLPutData(stmt, (SQLPOINTER)"world", 5);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "PutData should succeed");

    // Final SQLParamData should attempt execution (will fail without server, but not NEED_DATA)
    ret = SQLParamData(stmt, &value);
    // Without server connection, it should fail with connection error
    AssertTrue(ret != SQL_NEED_DATA, "All params provided, should not need more data");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, PutDataAccumulates) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLLEN ind = SQL_DATA_AT_EXEC;
    SQLPOINTER token = (SQLPOINTER)(intptr_t)1;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 1000, 0, token, 0, &ind);

    SQLExecute(stmt);

    // Get first param
    SQLPOINTER value = nullptr;
    SQLParamData(stmt, &value);

    // Send data in multiple chunks
    SQLPutData(stmt, (SQLPOINTER)"Hello", 5);
    SQLPutData(stmt, (SQLPOINTER)" ", 1);
    SQLPutData(stmt, (SQLPOINTER)"World", 5);

    // Check accumulated buffer
    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual(std::string("Hello World"), s->dae_buffers[1], "Buffer should accumulate all chunks");

    s->ResetDAE();
    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, PutDataNullData) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLLEN ind = SQL_DATA_AT_EXEC;
    SQLPOINTER token = (SQLPOINTER)(intptr_t)1;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, token, 0, &ind);

    SQLExecute(stmt);

    SQLPOINTER value = nullptr;
    SQLParamData(stmt, &value);

    // Send NULL
    auto ret = SQLPutData(stmt, nullptr, SQL_NULL_DATA);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "PutData with SQL_NULL_DATA should succeed");

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertTrue(s->dae_buffers.count(1) > 0, "DAE buffer entry should exist for null");

    s->ResetDAE();
    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, PutDataWithSQLNTS) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLLEN ind = SQL_DATA_AT_EXEC;
    SQLPOINTER token = (SQLPOINTER)(intptr_t)1;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, token, 0, &ind);

    SQLExecute(stmt);

    SQLPOINTER value = nullptr;
    SQLParamData(stmt, &value);

    auto ret = SQLPutData(stmt, (SQLPOINTER)"test_string", SQL_NTS);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "PutData with SQL_NTS should succeed");

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual(std::string("test_string"), s->dae_buffers[1], "Buffer should contain full NTS string");

    s->ResetDAE();
    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, ParamDataWithoutNeedData) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // No DAE in progress
    SQLPOINTER value = nullptr;
    auto ret = SQLParamData(stmt, &value);
    AssertEqual((int)SQL_ERROR, (int)ret, "ParamData without NEED_DATA should fail");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, PutDataWithoutNeedData) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLPutData(stmt, (SQLPOINTER)"data", 4);
    AssertEqual((int)SQL_ERROR, (int)ret, "PutData without NEED_DATA should fail");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, PutDataNullPointerError) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLLEN ind = SQL_DATA_AT_EXEC;
    SQLPOINTER token = (SQLPOINTER)(intptr_t)1;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, token, 0, &ind);

    SQLExecute(stmt);

    SQLPOINTER value = nullptr;
    SQLParamData(stmt, &value);

    // Null pointer with non-NULL_DATA indicator should fail
    auto ret = SQLPutData(stmt, nullptr, 10);
    AssertEqual((int)SQL_ERROR, (int)ret, "PutData with null pointer and positive length should fail");

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->ResetDAE();
    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_DAE, InvalidHandles) {
    SQLPOINTER value = nullptr;
    auto ret = SQLParamData(SQL_NULL_HSTMT, &value);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "ParamData null handle");

    ret = SQLPutData(SQL_NULL_HSTMT, (SQLPOINTER)"x", 1);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "PutData null handle");
}

// ============================================================================
// SQLError / SQLErrorW test (ODBC 2.x)
// ============================================================================

TEST(Phase7_Error, BasicErrorRetrieval) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test error message");

    SQLCHAR sqlstate[6] = {};
    SQLINTEGER nativeError = 0;
    SQLCHAR message[256] = {};
    SQLSMALLINT textLen = 0;

    auto ret = SQLError(SQL_NULL_HENV, SQL_NULL_HDBC, stmt, sqlstate, &nativeError, message, sizeof(message), &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SQLError should succeed");
    AssertEqual(std::string("HY000"), std::string(reinterpret_cast<const char *>(sqlstate)), "SQLSTATE should match");
    AssertTrue(textLen > 0, "Message length should be > 0");

    // Second call should return NO_DATA (record was consumed)
    ret = SQLError(SQL_NULL_HENV, SQL_NULL_HDBC, stmt, sqlstate, &nativeError, message, sizeof(message), &textLen);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "Second call should return NO_DATA");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_Error, MultipleDiagRecords) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->ClearDiagRecords();
    s->AddDiagRecord("01004", 0, "String truncated");
    s->AddDiagRecord("HY000", 1, "General error");

    SQLCHAR sqlstate[6] = {};
    SQLINTEGER nativeError = 0;
    SQLCHAR message[256] = {};
    SQLSMALLINT textLen = 0;

    // First record
    auto ret = SQLError(SQL_NULL_HENV, SQL_NULL_HDBC, stmt, sqlstate, &nativeError, message, sizeof(message), &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "First record");
    AssertEqual(std::string("01004"), std::string(reinterpret_cast<const char *>(sqlstate)), "First SQLSTATE");

    // Second record
    ret = SQLError(SQL_NULL_HENV, SQL_NULL_HDBC, stmt, sqlstate, &nativeError, message, sizeof(message), &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Second record");
    AssertEqual(std::string("HY000"), std::string(reinterpret_cast<const char *>(sqlstate)), "Second SQLSTATE");
    AssertEqual(1, (int)nativeError, "Native error for second record");

    // No more records
    ret = SQLError(SQL_NULL_HENV, SQL_NULL_HDBC, stmt, sqlstate, &nativeError, message, sizeof(message), &textLen);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "No more records");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_Error, ErrorFromConnection) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->SetError("08003", "Connection not open");

    SQLCHAR sqlstate[6] = {};
    SQLINTEGER nativeError = 0;
    SQLCHAR message[256] = {};
    SQLSMALLINT textLen = 0;

    auto ret = SQLError(SQL_NULL_HENV, dbc, SQL_NULL_HSTMT, sqlstate, &nativeError, message, sizeof(message), &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Error from connection handle");
    AssertEqual(std::string("08003"), std::string(reinterpret_cast<const char *>(sqlstate)), "SQLSTATE from connection");

    FreeConnHandles(env, dbc);
}

TEST(Phase7_Error, ErrorFromEnvironment) {
    SQLHENV env = SQL_NULL_HENV;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    auto *e = static_cast<OdbcEnvironment *>(env);
    e->SetError("HY010", "Function sequence error");

    SQLCHAR sqlstate[6] = {};
    SQLINTEGER nativeError = 0;
    SQLCHAR message[256] = {};
    SQLSMALLINT textLen = 0;

    auto ret = SQLError(env, SQL_NULL_HDBC, SQL_NULL_HSTMT, sqlstate, &nativeError, message, sizeof(message), &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Error from environment handle");
    AssertEqual(std::string("HY010"), std::string(reinterpret_cast<const char *>(sqlstate)), "SQLSTATE from environment");

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase7_Error, ErrorHandlePriority) {
    // stmt > dbc > env: when stmt is provided, use stmt handle
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("42000", "Stmt error");

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->SetError("08003", "Conn error");

    SQLCHAR sqlstate[6] = {};
    SQLINTEGER nativeError = 0;
    SQLCHAR message[256] = {};
    SQLSMALLINT textLen = 0;

    // stmt is most specific, should return stmt's error
    auto ret = SQLError(env, dbc, stmt, sqlstate, &nativeError, message, sizeof(message), &textLen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Should use stmt handle");
    AssertEqual(std::string("42000"), std::string(reinterpret_cast<const char *>(sqlstate)), "Should be stmt's SQLSTATE");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_Error, AllNullHandles) {
    SQLCHAR sqlstate[6] = {};
    auto ret = SQLError(SQL_NULL_HENV, SQL_NULL_HDBC, SQL_NULL_HSTMT, sqlstate, nullptr, nullptr, 0, nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "All null handles should return INVALID_HANDLE");
}

TEST(Phase7_Error, NoError) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // No error set
    SQLCHAR sqlstate[6] = {};
    auto ret = SQLError(SQL_NULL_HENV, SQL_NULL_HDBC, stmt, sqlstate, nullptr, nullptr, 0, nullptr);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "No error records should return NO_DATA");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetConnectOption / SQLSetConnectOption test (ODBC 2.x)
// ============================================================================

TEST(Phase7_ConnOption, SetAndGetAutocommit) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    auto ret = SQLSetConnectOption(dbc, SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_OFF);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetConnectOption AUTOCOMMIT");

    SQLUINTEGER value = 0;
    ret = SQLGetConnectOption(dbc, SQL_AUTOCOMMIT, &value);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetConnectOption AUTOCOMMIT");
    AssertEqual((int)SQL_AUTOCOMMIT_OFF, (int)value, "Autocommit should be off");

    FreeConnHandles(env, dbc);
}

TEST(Phase7_ConnOption, SetAndGetAccessMode) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    auto ret = SQLSetConnectOption(dbc, SQL_ACCESS_MODE, SQL_MODE_READ_ONLY);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetConnectOption ACCESS_MODE");

    SQLUINTEGER value = 0;
    ret = SQLGetConnectOption(dbc, SQL_ACCESS_MODE, &value);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetConnectOption ACCESS_MODE");
    AssertEqual((int)SQL_MODE_READ_ONLY, (int)value, "Access mode should be read-only");

    FreeConnHandles(env, dbc);
}

TEST(Phase7_ConnOption, SetLoginTimeout) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    auto ret = SQLSetConnectOption(dbc, SQL_LOGIN_TIMEOUT, 30);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetConnectOption LOGIN_TIMEOUT");

    SQLUINTEGER value = 0;
    ret = SQLGetConnectOption(dbc, SQL_LOGIN_TIMEOUT, &value);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetConnectOption LOGIN_TIMEOUT");
    AssertEqual(30, (int)value, "Login timeout should be 30");

    FreeConnHandles(env, dbc);
}

TEST(Phase7_ConnOption, InvalidHandle) {
    SQLUINTEGER value = 0;
    auto ret = SQLGetConnectOption(SQL_NULL_HDBC, SQL_AUTOCOMMIT, &value);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle");

    ret = SQLSetConnectOption(SQL_NULL_HDBC, SQL_AUTOCOMMIT, SQL_AUTOCOMMIT_ON);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle");
}

// ============================================================================
// SQLGetStmtOption / SQLSetStmtOption test (ODBC 2.x)
// ============================================================================

TEST(Phase7_StmtOption, SetAndGetQueryTimeout) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLSetStmtOption(stmt, SQL_QUERY_TIMEOUT, 60);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetStmtOption QUERY_TIMEOUT");

    SQLULEN value = 0;
    ret = SQLGetStmtOption(stmt, SQL_QUERY_TIMEOUT, &value);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetStmtOption QUERY_TIMEOUT");
    AssertTrue(value == 60, "Query timeout should be 60");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_StmtOption, SetAndGetMaxRows) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto ret = SQLSetStmtOption(stmt, SQL_MAX_ROWS, 100);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetStmtOption MAX_ROWS");

    SQLULEN value = 0;
    ret = SQLGetStmtOption(stmt, SQL_MAX_ROWS, &value);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetStmtOption MAX_ROWS");
    AssertTrue(value == 100, "Max rows should be 100");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase7_StmtOption, InvalidHandle) {
    SQLULEN value = 0;
    auto ret = SQLGetStmtOption(SQL_NULL_HSTMT, SQL_QUERY_TIMEOUT, &value);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle");

    ret = SQLSetStmtOption(SQL_NULL_HSTMT, SQL_QUERY_TIMEOUT, 10);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Null handle");
}

// ============================================================================
// SQLTransact test (ODBC 2.x)
// ============================================================================

TEST(Phase7_Transact, CommitOnConnection) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    auto ret = SQLTransact(SQL_NULL_HENV, dbc, SQL_COMMIT);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Commit on connection should succeed");

    FreeConnHandles(env, dbc);
}

TEST(Phase7_Transact, RollbackOnConnection) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    auto ret = SQLTransact(SQL_NULL_HENV, dbc, SQL_ROLLBACK);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Rollback on connection should succeed");

    FreeConnHandles(env, dbc);
}

TEST(Phase7_Transact, TransactOnEnvironment) {
    SQLHENV env = SQL_NULL_HENV;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    auto ret = SQLTransact(env, SQL_NULL_HDBC, SQL_COMMIT);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Commit on environment should succeed");

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase7_Transact, BothNullHandles) {
    auto ret = SQLTransact(SQL_NULL_HENV, SQL_NULL_HDBC, SQL_COMMIT);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Both null should return INVALID_HANDLE");
}

// ============================================================================
// SQLGetFunctions test (verify new functions)
// ============================================================================

TEST(Phase7_GetFunctions, LegacyFunctionsSupported) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;

    SQLGetFunctions(dbc, SQL_API_SQLERROR, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLError should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLGETCONNECTOPTION, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetConnectOption should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLSETCONNECTOPTION, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetConnectOption should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLGETSTMTOPTION, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetStmtOption should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLSETSTMTOPTION, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetStmtOption should be supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLTRANSACT, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLTransact should be supported");

    FreeConnHandles(env, dbc);
}

TEST(Phase7_GetFunctions, BitmapIncludesLegacyFunctions) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT bitmap[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE] = {};
    auto ret = SQLGetFunctions(dbc, SQL_API_ODBC3_ALL_FUNCTIONS, bitmap);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Bitmap query should succeed");

    auto checkBit = [&](SQLUSMALLINT id) -> bool {
        return (bitmap[id >> 4] & (1 << (id & 0x000F))) != 0;
    };

    AssertTrue(checkBit(SQL_API_SQLERROR), "SQLError in bitmap");
    AssertTrue(checkBit(SQL_API_SQLGETCONNECTOPTION), "SQLGetConnectOption in bitmap");
    AssertTrue(checkBit(SQL_API_SQLSETCONNECTOPTION), "SQLSetConnectOption in bitmap");
    AssertTrue(checkBit(SQL_API_SQLGETSTMTOPTION), "SQLGetStmtOption in bitmap");
    AssertTrue(checkBit(SQL_API_SQLSETSTMTOPTION), "SQLSetStmtOption in bitmap");
    AssertTrue(checkBit(SQL_API_SQLTRANSACT), "SQLTransact in bitmap");
    AssertTrue(checkBit(SQL_API_SQLPARAMDATA), "SQLParamData in bitmap");
    AssertTrue(checkBit(SQL_API_SQLPUTDATA), "SQLPutData in bitmap");

    FreeConnHandles(env, dbc);
}

// ============================================================================
// DAE and SQLFreeStmt interaction test
// ============================================================================

TEST(Phase7_DAE, ResetParamsClearsDAE) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    const char *query = "INSERT INTO t VALUES (?)";
    SQLPrepare(stmt, (SQLCHAR *)query, SQL_NTS);

    SQLLEN ind = SQL_DATA_AT_EXEC;
    SQLPOINTER token = (SQLPOINTER)(intptr_t)1;
    SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, token, 0, &ind);

    SQLExecute(stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertTrue(s->need_data, "Should be in DAE state");

    // SQL_RESET_PARAMS should clear DAE state too
    SQLFreeStmt(stmt, SQL_RESET_PARAMS);
    AssertTrue(s->bound_parameters.empty(), "Params should be cleared");

    FreeTestHandles(env, dbc, stmt);
}
