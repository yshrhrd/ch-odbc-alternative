#include "test_framework.h"
#include "../src/include/handle.h"
#include "../src/include/util.h"

#ifdef UNICODE
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#endif

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// ODBC Handle Allocation/Deallocation (SQLAllocHandle / SQLFreeHandle)
// ============================================================================

// These are extern "C" functions, so we test by calling them directly
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);

TEST(ODBC_Handle, AllocEnvironment) {
    SQLHENV env = nullptr;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertNotNull(env, "Environment handle should be allocated");

    ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
}

TEST(ODBC_Handle, AllocConnection) {
    SQLHENV env = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);

    SQLHDBC conn = nullptr;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertNotNull(conn, "Connection handle should be allocated");

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Handle, AllocStatement_NotConnected) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLHSTMT stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, (SQLHANDLE *)&stmt);
    AssertEqual((int)SQL_ERROR, (int)ret);

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Handle, AllocStatement_Connected) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    // Simulate connected state
    auto *c = static_cast<OdbcConnection *>((SQLHANDLE)conn);
    c->connected = true;

    SQLHSTMT stmt = nullptr;
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, (SQLHANDLE *)&stmt);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertNotNull(stmt, "Statement handle should be allocated");

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    c->connected = false;
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Handle, AllocHandle_InvalidType) {
    SQLHANDLE handle = nullptr;
    SQLRETURN ret = SQLAllocHandle((SQLSMALLINT)999, SQL_NULL_HANDLE, &handle);
    AssertEqual((int)SQL_ERROR, (int)ret);
}

TEST(ODBC_Handle, AllocHandle_NullOutput) {
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, nullptr);
    AssertEqual((int)SQL_ERROR, (int)ret);
}

TEST(ODBC_Handle, FreeHandle_Null) {
    SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_ENV, nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret);
}

// ============================================================================
// Environment Attributes

extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);

TEST(ODBC_Handle, SetGetEnvAttr_ODBCVersion) {
    SQLHENV env = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);

    SQLRETURN ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    SQLINTEGER version = 0;
    ret = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, sizeof(version), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_OV_ODBC3, (int)version);

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// Connection Attributes

extern "C" SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);

TEST(ODBC_Handle, SetGetConnectAttr_AutoCommit) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLRETURN ret = SQLSetConnectAttr(conn, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    SQLUINTEGER value = 0;
    ret = SQLGetConnectAttr(conn, SQL_ATTR_AUTOCOMMIT, &value, sizeof(value), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_AUTOCOMMIT_OFF, (int)value);

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// Statement Attributes

extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);

TEST(ODBC_Handle, FreeStmt_Close) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLHSTMT stmt = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    // Simulate connected state to allow statement allocation
    auto *c = static_cast<OdbcConnection *>((SQLHANDLE)conn);
    c->connected = true;

    SQLAllocHandle(SQL_HANDLE_STMT, conn, (SQLHANDLE *)&stmt);
    AssertNotNull(stmt, "Statement should be allocated");

    SQLRETURN ret = SQLFreeStmt(stmt, SQL_CLOSE);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    c->connected = false;
    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// SQLEndTran (always succeeds)

extern "C" SQLRETURN SQL_API SQLEndTran(SQLSMALLINT, SQLHANDLE, SQLSMALLINT);

TEST(ODBC_Handle, EndTran_AlwaysSuccess) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, conn, SQL_COMMIT);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    ret = SQLEndTran(SQL_HANDLE_DBC, conn, SQL_ROLLBACK);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// SQLGetFunctions
// ============================================================================

extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT *);

TEST(ODBC_Handle, GetFunctions_Individual) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLUSMALLINT supported = SQL_FALSE;
    SQLRETURN ret = SQLGetFunctions(conn, SQL_API_SQLCONNECT, &supported);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_TRUE, (int)supported);

    supported = SQL_FALSE;
    ret = SQLGetFunctions(conn, SQL_API_SQLTABLES, &supported);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_TRUE, (int)supported);

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}
