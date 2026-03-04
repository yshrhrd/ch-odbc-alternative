#include "test_framework.h"
#include "../src/include/handle.h"
#include "../src/include/util.h"

#ifdef UNICODE
#undef SQLGetDiagRec
#undef SQLGetInfo
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#endif

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// SQLGetDiagRec / SQLGetDiagRecW
// ============================================================================

extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT,
                                            SQLCHAR *, SQLINTEGER *, SQLCHAR *,
                                            SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT, SQLHANDLE, SQLSMALLINT,
                                             SQLWCHAR *, SQLINTEGER *, SQLWCHAR *,
                                             SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetInfo(SQLHDBC, SQLUSMALLINT, SQLPOINTER,
                                         SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetInfoW(SQLHDBC, SQLUSMALLINT, SQLPOINTER,
                                          SQLSMALLINT, SQLSMALLINT *);

// ============================================================================
// Diagnostic Record Tests

TEST(ODBC_Diag, GetDiagRec_NoRecords) {
    SQLHENV env = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);

    SQLCHAR sqlstate[6];
    SQLINTEGER native_error;
    SQLCHAR msg[256];
    SQLSMALLINT msg_len;

    SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_ENV, env, 1, sqlstate, &native_error, msg, sizeof(msg), &msg_len);
    AssertEqual((int)SQL_NO_DATA, (int)ret);

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Diag, GetDiagRec_WithError) {
    SQLHENV env = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);

    // Manually set an error on the handle
    auto *handle = static_cast<OdbcHandle *>(env);
    handle->SetError("HY000", "Test error message", 42);

    SQLCHAR sqlstate[6] = {0};
    SQLINTEGER native_error = 0;
    SQLCHAR msg[256] = {0};
    SQLSMALLINT msg_len = 0;

    SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_ENV, env, 1, sqlstate, &native_error, msg, sizeof(msg), &msg_len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual("HY000", std::string((char *)sqlstate));
    AssertEqual(42, (int)native_error);
    AssertTrue(msg_len > 0, "Message length should be > 0");

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Diag, GetDiagRec_InvalidHandle) {
    SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_ENV, nullptr, 1, nullptr, nullptr, nullptr, 0, nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret);
}

TEST(ODBC_Diag, GetDiagRecW_WithError) {
    SQLHDBC conn = nullptr;
    SQLHENV env = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    auto *handle = static_cast<OdbcHandle *>(conn);
    handle->SetError("01004", "Data truncated");

    SQLWCHAR sqlstate[6] = {0};
    SQLINTEGER native_error = 0;
    SQLWCHAR msg[256] = {0};
    SQLSMALLINT msg_len = 0;

    SQLRETURN ret = SQLGetDiagRecW(SQL_HANDLE_DBC, conn, 1, sqlstate, &native_error, msg, 256, &msg_len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue(msg_len > 0, "W version message length should be > 0");

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// ============================================================================
// SQLGetInfo Tests
TEST(ODBC_Info, GetInfo_DriverName) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLCHAR buffer[256] = {0};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfo(conn, SQL_DRIVER_NAME, buffer, sizeof(buffer), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue(len > 0, "Driver name length should be > 0");
    AssertTrue(std::string((char *)buffer).find("ch-odbc") != std::string::npos,
               "Driver name should contain 'ch-odbc'");

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Info, GetInfo_DBMSName) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLCHAR buffer[256] = {0};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfo(conn, SQL_DBMS_NAME, buffer, sizeof(buffer), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual("ClickHouse", std::string((char *)buffer));

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Info, GetInfo_ODBCVersion) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLCHAR buffer[32] = {0};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfo(conn, SQL_DRIVER_ODBC_VER, buffer, sizeof(buffer), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual("03.80", std::string((char *)buffer));

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Info, GetInfoW_DBMSName) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLWCHAR buffer[256] = {0};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfoW(conn, SQL_DBMS_NAME, buffer, 256, &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue(len > 0, "W version should return non-zero length");

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(ODBC_Info, GetInfo_InvalidHandle) {
    SQLRETURN ret = SQLGetInfo(nullptr, SQL_DBMS_NAME, nullptr, 0, nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret);
}

TEST(ODBC_Info, GetInfo_MaxTableNameLen) {
    SQLHENV env = nullptr;
    SQLHDBC conn = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)&env);
    SQLAllocHandle(SQL_HANDLE_DBC, env, (SQLHANDLE *)&conn);

    SQLUSMALLINT value = 0;
    SQLRETURN ret = SQLGetInfo(conn, SQL_MAX_TABLE_NAME_LEN, &value, sizeof(value), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue(value > 0, "Max table name len should be > 0");

    SQLFreeHandle(SQL_HANDLE_DBC, conn);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}
