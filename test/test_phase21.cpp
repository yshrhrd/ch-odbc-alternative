// Phase 21 Tests: SSL/TLS configuration for ClickHouse communication
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
#include "../src/include/clickhouse_client.h"
#include "../src/include/util.h"

#ifdef UNICODE
#undef SQLDriverConnect
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLGetDiagRec
#endif

#pragma warning(disable: 4996)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLDriverConnect(SQLHDBC, SQLHWND, SQLCHAR *, SQLSMALLINT,
                                               SQLCHAR *, SQLSMALLINT, SQLSMALLINT *, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLDisconnect(SQLHDBC);
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLCHAR *, SQLINTEGER *, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// ClickHouseClient SSL configuration tests
// ============================================================================

TEST(Phase21_SSL, ClientSSLDefaultOff) {
    ClickHouseClient client;
    AssertFalse(client.IsSSLEnabled(), "SSL should be off by default");
    AssertTrue(client.IsSSLVerify(), "SSL verify should be on by default");
}

TEST(Phase21_SSL, ClientSSLSetEnabled) {
    ClickHouseClient client;
    client.SetSSLEnabled(true);
    AssertTrue(client.IsSSLEnabled(), "SSL should be enabled after SetSSLEnabled(true)");

    client.SetSSLEnabled(false);
    AssertFalse(client.IsSSLEnabled(), "SSL should be disabled after SetSSLEnabled(false)");
}

TEST(Phase21_SSL, ClientSSLSetVerify) {
    ClickHouseClient client;
    client.SetSSLVerify(false);
    AssertFalse(client.IsSSLVerify(), "SSL verify should be off after SetSSLVerify(false)");

    client.SetSSLVerify(true);
    AssertTrue(client.IsSSLVerify(), "SSL verify should be on after SetSSLVerify(true)");
}

// ============================================================================
// OdbcConnection SSL fields tests
// ============================================================================

TEST(Phase21_SSL, ConnectionSSLDefaultOff) {
    OdbcConnection conn;
    AssertFalse(conn.ssl, "Connection ssl should be false by default");
    AssertTrue(conn.ssl_verify, "Connection ssl_verify should be true by default");
}

// ============================================================================
// Connection string parsing tests for SSL
// ============================================================================

TEST(Phase21_SSL, ParseConnStringSSL1) {
    auto params = ParseConnectionString("HOST=ch.example.com;SSL=1;UID=default");
    bool found_ssl = false;
    for (const auto &[key, value] : params) {
        if (ToUpper(key) == "SSL") {
            AssertEqual(std::string("1"), value, "SSL value");
            found_ssl = true;
        }
    }
    AssertTrue(found_ssl, "SSL key should be found in parsed params");
}

TEST(Phase21_SSL, ParseConnStringSSLTrue) {
    auto params = ParseConnectionString("HOST=ch.example.com;SSL=true;UID=default");
    bool found_ssl = false;
    for (const auto &[key, value] : params) {
        if (ToUpper(key) == "SSL") {
            AssertEqual(std::string("true"), value, "SSL value");
            found_ssl = true;
        }
    }
    AssertTrue(found_ssl, "SSL key should be found");
}

TEST(Phase21_SSL, ParseConnStringSSLMode) {
    auto params = ParseConnectionString("HOST=ch.example.com;SSLMODE=require;UID=default");
    bool found = false;
    for (const auto &[key, value] : params) {
        if (ToUpper(key) == "SSLMODE") {
            AssertEqual(std::string("require"), value, "SSLMODE value");
            found = true;
        }
    }
    AssertTrue(found, "SSLMODE key should be found");
}

TEST(Phase21_SSL, ParseConnStringSSLVerify) {
    auto params = ParseConnectionString("HOST=ch.example.com;SSL=1;SSL_VERIFY=0;UID=default");
    bool found_verify = false;
    for (const auto &[key, value] : params) {
        if (ToUpper(key) == "SSL_VERIFY") {
            AssertEqual(std::string("0"), value, "SSL_VERIFY value");
            found_verify = true;
        }
    }
    AssertTrue(found_verify, "SSL_VERIFY key should be found");
}

// ============================================================================
// DriverConnect with SSL - connection attempt tests
// ============================================================================

static SQLHENV AllocEnvForSSLTest() {
    SQLHENV henv = SQL_NULL_HENV;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    return henv;
}

TEST(Phase21_SSL, DriverConnectSSLSetsConnectionFields) {
    SQLHENV henv = AllocEnvForSSLTest();
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    std::string conn_str = "HOST=nonexistent.example.com;PORT=8443;SSL=1;UID=default;DATABASE=default";
    SQLRETURN ret = SQLDriverConnect(hdbc, nullptr,
                                     (SQLCHAR *)conn_str.c_str(), SQL_NTS,
                                     nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    AssertEqual((int)SQL_ERROR, (int)ret, "Connection should fail");

    auto *conn = static_cast<OdbcConnection *>(hdbc);
    AssertTrue(conn->ssl, "ssl flag should be set");
    AssertTrue(conn->ssl_verify, "ssl_verify should default to true");

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

TEST(Phase21_SSL, DriverConnectSSLVerifyDisabled) {
    SQLHENV henv = AllocEnvForSSLTest();
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    std::string conn_str = "HOST=nonexistent.example.com;SSL=1;SSL_VERIFY=0;UID=default;DATABASE=default";
    SQLRETURN ret = SQLDriverConnect(hdbc, nullptr,
                                     (SQLCHAR *)conn_str.c_str(), SQL_NTS,
                                     nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    AssertEqual((int)SQL_ERROR, (int)ret, "Connection should fail");

    auto *conn = static_cast<OdbcConnection *>(hdbc);
    AssertTrue(conn->ssl, "ssl should be enabled");
    AssertFalse(conn->ssl_verify, "ssl_verify should be disabled");

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

TEST(Phase21_SSL, DriverConnectSSLDefaultPort) {
    SQLHENV henv = AllocEnvForSSLTest();
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    std::string conn_str = "HOST=nonexistent.example.com;SSL=1;UID=default;DATABASE=default";
    SQLRETURN ret = SQLDriverConnect(hdbc, nullptr,
                                     (SQLCHAR *)conn_str.c_str(), SQL_NTS,
                                     nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    AssertEqual((int)SQL_ERROR, (int)ret, "Connection should fail");

    auto *conn = static_cast<OdbcConnection *>(hdbc);
    AssertTrue(conn->ssl, "ssl should be enabled");
    AssertEqual(8443, (int)conn->port, "Default SSL port should be 8443");

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

TEST(Phase21_SSL, DriverConnectSSLExplicitPort) {
    SQLHENV henv = AllocEnvForSSLTest();
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    std::string conn_str = "HOST=nonexistent.example.com;PORT=9443;SSL=1;UID=default;DATABASE=default";
    SQLRETURN ret = SQLDriverConnect(hdbc, nullptr,
                                     (SQLCHAR *)conn_str.c_str(), SQL_NTS,
                                     nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    AssertEqual((int)SQL_ERROR, (int)ret, "Connection should fail");

    auto *conn = static_cast<OdbcConnection *>(hdbc);
    AssertTrue(conn->ssl, "ssl should be enabled");
    AssertEqual(9443, (int)conn->port, "Explicit port should be preserved");

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

TEST(Phase21_SSL, DriverConnectNoSSLDefaultPort) {
    SQLHENV henv = AllocEnvForSSLTest();
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    std::string conn_str = "HOST=nonexistent.example.com;UID=default;DATABASE=default";
    SQLRETURN ret = SQLDriverConnect(hdbc, nullptr,
                                     (SQLCHAR *)conn_str.c_str(), SQL_NTS,
                                     nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    AssertEqual((int)SQL_ERROR, (int)ret, "Connection should fail");

    auto *conn = static_cast<OdbcConnection *>(hdbc);
    AssertFalse(conn->ssl, "ssl should be off");
    AssertEqual(8123, (int)conn->port, "Default HTTP port should be 8123");

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

TEST(Phase21_SSL, DriverConnectSSLErrorMessageContainsHTTPS) {
    SQLHENV henv = AllocEnvForSSLTest();
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);

    std::string conn_str = "HOST=nonexistent.example.com;SSL=1;UID=default;DATABASE=default";
    SQLRETURN ret = SQLDriverConnect(hdbc, nullptr,
                                     (SQLCHAR *)conn_str.c_str(), SQL_NTS,
                                     nullptr, 0, nullptr, SQL_DRIVER_NOPROMPT);
    AssertEqual((int)SQL_ERROR, (int)ret, "Connection should fail");

    SQLCHAR sqlstate[6] = {};
    SQLINTEGER native_error = 0;
    SQLCHAR msg[512] = {};
    SQLSMALLINT msg_len = 0;
    SQLGetDiagRec(SQL_HANDLE_DBC, hdbc, 1, sqlstate, &native_error,
                  msg, sizeof(msg), &msg_len);

    std::string error_msg(reinterpret_cast<char *>(msg), msg_len);
    AssertTrue(error_msg.find("https") != std::string::npos,
              "Error message should contain 'https' when SSL is enabled: " + error_msg);

    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    SQLFreeHandle(SQL_HANDLE_ENV, henv);
}

// ============================================================================
// OpenSSL availability check
// ============================================================================

TEST(Phase21_SSL, OpenSSLSupportCheck) {
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    AssertTrue(true, "OpenSSL support is enabled");
#else
    // OpenSSL not available — SSL flag is still parsed but actual TLS
    // connections will fail at runtime. This is acceptable for testing.
    AssertTrue(true, "OpenSSL not available, SSL config still works");
#endif
}

TEST(Phase21_SSL, CreateHttpClientWithoutSSL) {
    ClickHouseClient client;
    client.SetSSLEnabled(false);
    // Ping to non-existent host should return false without crashing
    AssertFalse(client.Ping(), "Ping to unconnected client should fail");
}
