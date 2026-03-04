#include "test_framework.h"
#include "../src/include/handle.h"
#include "../src/include/clickhouse_client.h"
#include "../src/include/type_mapping.h"
#include "../src/include/util.h"

#include <sql.h>
#include <sqlext.h>
#include <cstring>
#include <thread>
#include <atomic>
#include <vector>

#ifdef UNICODE
#undef SQLColAttribute
#undef SQLDescribeCol
#undef SQLGetInfo
#undef SQLGetData
#undef SQLGetTypeInfo
#undef SQLDriverConnect
#undef SQLConnect
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLExecDirect
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetDiagRec
#undef SQLFreeStmt
#endif

using namespace test_framework;
using namespace clickhouse_odbc;

// Forward declarations for ODBC API functions used in tests
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLCHAR *, SQLINTEGER *,
                                            SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLDriverConnect(SQLHDBC, SQLHWND, SQLCHAR *, SQLSMALLINT,
                                               SQLCHAR *, SQLSMALLINT, SQLSMALLINT *, SQLUSMALLINT);

// ============================================================================
// Thread safety test
// ============================================================================

TEST(Phase4_ThreadSafety, Handle_mutex_exists) {
    // Verify that OdbcHandle has a mutex by creating handles and accessing them
    OdbcEnvironment env;
    HandleLock lock(&env);
    // If we get here without deadlock, the mutex works
    AssertTrue(true);
}

TEST(Phase4_ThreadSafety, DiagRecord_thread_safe) {
    OdbcStatement stmt;
    std::atomic<int> errors{0};

    // Multiple threads writing diagnostic records concurrently
    auto writer = [&stmt, &errors](int id) {
        for (int i = 0; i < 100; i++) {
            try {
                stmt.SetError("HY000", "Thread " + std::to_string(id) + " error " + std::to_string(i));
                stmt.ClearDiagRecords();
                stmt.AddDiagRecord("01000", 0, "Warning from thread " + std::to_string(id));
            } catch (...) {
                errors++;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; i++) {
        threads.emplace_back(writer, i);
    }
    for (auto &t : threads) {
        t.join();
    }

    // No crashes or exceptions = success
    AssertEqual(0, (int)errors.load());
}

TEST(Phase4_ThreadSafety, HandleLock_RAII) {
    OdbcConnection conn;
    {
        HandleLock lock1(&conn);
        // Nested lock should work with recursive_mutex
        HandleLock lock2(&conn);
        conn.host = "test_host";
    }
    // After lock released, state should be intact
    AssertEqual(std::string("test_host"), conn.host);
}

// ============================================================================
// Timeout settings test
// ============================================================================

TEST(Phase4_Timeout, Client_default_timeouts) {
    ClickHouseClient client;
    AssertEqual(30, (int)client.GetConnectionTimeout());
    AssertEqual(300, (int)client.GetQueryTimeout());
}

TEST(Phase4_Timeout, Client_set_connection_timeout) {
    ClickHouseClient client;
    client.SetConnectionTimeout(10);
    AssertEqual(10, (int)client.GetConnectionTimeout());
}

TEST(Phase4_Timeout, Client_set_query_timeout) {
    ClickHouseClient client;
    client.SetQueryTimeout(60);
    AssertEqual(60, (int)client.GetQueryTimeout());
}

TEST(Phase4_Timeout, Connection_attr_login_timeout) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    // Set login timeout
    SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER)15, 0);

    // Verify it was set
    SQLUINTEGER timeout = 0;
    SQLGetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, &timeout, sizeof(timeout), nullptr);
    AssertEqual(15, (int)timeout);

    // Set connection timeout
    SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER)20, 0);
    SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, sizeof(timeout), nullptr);
    AssertEqual(20, (int)timeout);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// Retry settings test
// ============================================================================

TEST(Phase4_Retry, Default_retry_count_zero) {
    ClickHouseClient client;
    AssertEqual(0, (int)client.GetRetryCount());
}

TEST(Phase4_Retry, Set_retry_count) {
    ClickHouseClient client;
    client.SetRetryCount(3);
    AssertEqual(3, (int)client.GetRetryCount());
}

TEST(Phase4_Retry, Set_retry_delay) {
    ClickHouseClient client;
    client.SetRetryDelayMs(500);
    // No getter for delay but should not crash
    AssertTrue(true);
}

// ============================================================================
// Error handling robustness test
// ============================================================================

TEST(Phase4_ErrorHandling, Fetch_without_result_set) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    // Mark as connected to allow statement allocation
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT hstmt = nullptr;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);

    // Fetch on empty statement should return SQL_ERROR with 24000
    SQLRETURN ret = SQLFetch(hstmt);
    AssertEqual((int)SQL_ERROR, (int)ret);

    // Check diagnostic record
    SQLCHAR sqlstate[6] = {};
    SQLINTEGER native = 0;
    SQLCHAR msg[256] = {};
    SQLSMALLINT msg_len = 0;
    SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlstate, &native, msg, sizeof(msg), &msg_len);
    AssertEqual(std::string("24000"), std::string(reinterpret_cast<char *>(sqlstate)));

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase4_ErrorHandling, GetData_invalid_column) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT hstmt = nullptr;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);

    // Set up a minimal result set for testing
    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    ColumnInfo ci;
    ci.name = "test";
    ci.sql_type = SQL_VARCHAR;
    ci.column_size = 100;
    stmt->result_set.columns.push_back(ci);
    stmt->result_set.rows.push_back({std::string("hello")});
    stmt->result_set.current_row = 0;

    // GetData with invalid column (0 or too large)
    char buf[64];
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData(hstmt, 0, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_ERROR, (int)ret);

    ret = SQLGetData(hstmt, 99, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_ERROR, (int)ret);

    // Valid column should succeed
    ret = SQLGetData(hstmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("hello"), std::string(buf));

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase4_ErrorHandling, Truncation_sets_01004) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT hstmt = nullptr;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    ColumnInfo ci;
    ci.name = "long_str";
    ci.sql_type = SQL_VARCHAR;
    ci.column_size = 1000;
    stmt->result_set.columns.push_back(ci);
    stmt->result_set.rows.push_back({std::string("This is a long string that will be truncated")});
    stmt->result_set.current_row = 0;

    // Get data with small buffer -> truncation
    char buf[10];
    SQLLEN ind = 0;
    SQLRETURN ret = SQLGetData(hstmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_SUCCESS_WITH_INFO, (int)ret);

    // Check for 01004 diagnostic
    SQLCHAR sqlstate[6] = {};
    SQLINTEGER native = 0;
    SQLCHAR msg[256] = {};
    SQLSMALLINT msg_len = 0;
    SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, sqlstate, &native, msg, sizeof(msg), &msg_len);
    AssertEqual(std::string("01004"), std::string(reinterpret_cast<char *>(sqlstate)));

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase4_ErrorHandling, Invalid_handle_returns_SQL_INVALID_HANDLE) {
    SQLRETURN ret = SQLFetch(nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret);

    char buf[64];
    SQLLEN ind;
    ret = SQLGetData(nullptr, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret);
}

// ============================================================================
// Memory leak prevention / safe cleanup test
// ============================================================================

TEST(Phase4_Cleanup, FreeStmt_clears_bindings) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT hstmt = nullptr;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    // Add some bindings
    stmt->bound_columns[1] = {SQL_C_CHAR, nullptr, 100, nullptr};
    stmt->bound_columns[2] = {SQL_C_SLONG, nullptr, 4, nullptr};
    AssertEqual(2, (int)stmt->bound_columns.size());

    // SQL_UNBIND should clear
    SQLFreeStmt(hstmt, SQL_UNBIND);
    AssertEqual(0, (int)stmt->bound_columns.size());

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase4_Cleanup, FreeHandle_DBC_auto_disconnects) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    // Allocate a statement
    SQLHSTMT hstmt = nullptr;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);

    // Free DBC while still connected - should auto-cleanup
    SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    // hstmt is now freed by DBC cleanup, don't use it

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase4_Cleanup, FreeHandle_ENV_cascades) {
    SQLHENV env = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    // Free ENV should cascade and free the DBC too
    SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
}

TEST(Phase4_Cleanup, FreeStmt_RESET_PARAMS) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLHSTMT hstmt = nullptr;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    stmt->bound_parameters[1] = {};
    AssertEqual(1, (int)stmt->bound_parameters.size());

    SQLFreeStmt(hstmt, SQL_RESET_PARAMS);
    AssertEqual(0, (int)stmt->bound_parameters.size());

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// Connection pooling awareness test
// ============================================================================

TEST(Phase4_Pooling, CONNECTION_DEAD_not_connected) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    // Not connected -> should report dead
    SQLUINTEGER dead = 0;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead, sizeof(dead), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_CD_TRUE, (int)dead);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// ClickHouseClient unit test
// ============================================================================

TEST(Phase4_Client, Connect_to_invalid_host_fails) {
    ClickHouseClient client;
    client.SetConnectionTimeout(1); // Fast timeout for test
    bool result = client.Connect("invalid_host_that_does_not_exist", 9999, "default", "default", "");
    AssertTrue(!result);
    AssertTrue(!client.IsConnected());
}

TEST(Phase4_Client, Connect_with_retry_still_fails_on_invalid) {
    ClickHouseClient client;
    client.SetConnectionTimeout(1);
    client.SetRetryCount(1);
    client.SetRetryDelayMs(100);
    bool result = client.Connect("invalid_host", 9999, "default", "default", "");
    AssertTrue(!result);
}

TEST(Phase4_Client, Disconnect_sets_not_connected) {
    ClickHouseClient client;
    client.Disconnect();
    AssertTrue(!client.IsConnected());
}

TEST(Phase4_Client, ExecuteQuery_not_connected_fails) {
    ClickHouseClient client;
    client.SetConnectionTimeout(1);
    ResultSet rs;
    std::string err;
    bool result = client.ExecuteQuery("SELECT 1", rs, err);
    AssertTrue(!result);
    AssertTrue(!err.empty());
}

// ============================================================================
// Connection refusal test (double connect)
// ============================================================================

TEST(Phase4_Connection, Double_connect_prevented) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    // Simulate connected state
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    // Try to connect again via SQLDriverConnect should fail
    std::string cs = "HOST=localhost;PORT=8123";
    SQLRETURN ret = SQLDriverConnect(dbc, nullptr, (SQLCHAR *)cs.c_str(), SQL_NTS,
                                     nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    AssertEqual((int)SQL_ERROR, (int)ret);

    // Check SQLSTATE 08002
    SQLCHAR sqlstate[6] = {};
    SQLINTEGER native = 0;
    SQLCHAR msg[256] = {};
    SQLSMALLINT msg_len = 0;
    SQLGetDiagRec(SQL_HANDLE_DBC, dbc, 1, sqlstate, &native, msg, sizeof(msg), &msg_len);
    AssertEqual(std::string("08002"), std::string(reinterpret_cast<char *>(sqlstate)));

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}
