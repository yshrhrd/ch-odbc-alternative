#include "include/handle.h"
#include "include/clickhouse_client.h"
#include "include/util.h"
#include "include/trace.h"

#include <sql.h>
#include <sqlext.h>
#include <odbcinst.h>
#include <map>
#include <memory>
#include <mutex>

#ifdef UNICODE
#undef SQLDriverConnect
#undef SQLConnect
#undef SQLBrowseConnect
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLGetPrivateProfileString
#endif

namespace clickhouse_odbc {

// Global map: connection handle -> ClickHouseClient (protected by mutex)
static std::mutex g_clients_mutex;
static std::map<OdbcConnection *, std::unique_ptr<ClickHouseClient>> g_clients;

ClickHouseClient *GetClient(OdbcConnection *conn) {
    std::lock_guard<std::mutex> lock(g_clients_mutex);
    auto it = g_clients.find(conn);
    if (it != g_clients.end()) {
        return it->second.get();
    }
    return nullptr;
}

} // namespace clickhouse_odbc

using namespace clickhouse_odbc;

// ============================================================================
// SQLAllocHandle
// ============================================================================
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle,
                                            SQLHANDLE *OutputHandle) {
    if (!OutputHandle) {
        return SQL_ERROR;
    }

    switch (HandleType) {
    case SQL_HANDLE_ENV: {
        auto *env = new OdbcEnvironment();
        *OutputHandle = static_cast<SQLHANDLE>(env);
        return SQL_SUCCESS;
    }
    case SQL_HANDLE_DBC: {
        if (!IsValidEnvHandle(InputHandle)) {
            return SQL_INVALID_HANDLE;
        }
        auto *env = static_cast<OdbcEnvironment *>(InputHandle);
        HandleLock lock(env);
        auto *conn = new OdbcConnection();
        conn->env = env;
        env->connections.push_back(conn);
        *OutputHandle = static_cast<SQLHANDLE>(conn);
        return SQL_SUCCESS;
    }
    case SQL_HANDLE_STMT: {
        if (!IsValidDbcHandle(InputHandle)) {
            return SQL_INVALID_HANDLE;
        }
        auto *conn = static_cast<OdbcConnection *>(InputHandle);
        HandleLock lock(conn);
        if (!conn->connected) {
            conn->SetError("08003", "Connection not open");
            return SQL_ERROR;
        }
        auto *stmt = new OdbcStatement();
        stmt->conn = conn;
        // Apply connection-level defaults to new statement
        if (conn->default_max_rows > 0) {
            stmt->max_rows = conn->default_max_rows;
        }
        conn->statements.push_back(stmt);
        *OutputHandle = static_cast<SQLHANDLE>(stmt);
        return SQL_SUCCESS;
    }
    case SQL_HANDLE_DESC: {
        if (!IsValidDbcHandle(InputHandle)) {
            return SQL_INVALID_HANDLE;
        }
        auto *conn = static_cast<OdbcConnection *>(InputHandle);
        HandleLock lock(conn);
        auto *desc = new OdbcDescriptor();
        desc->conn = conn;
        *OutputHandle = static_cast<SQLHANDLE>(desc);
        return SQL_SUCCESS;
    }
    default:
        return SQL_ERROR;
    }
}

// ============================================================================
// SQLFreeHandle
// ============================================================================
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle) {
    if (!Handle) {
        return SQL_INVALID_HANDLE;
    }

    switch (HandleType) {
    case SQL_HANDLE_ENV: {
        auto *env = static_cast<OdbcEnvironment *>(Handle);
        // Defensive: free any remaining connections
        while (!env->connections.empty()) {
            auto *conn = env->connections.back();
            SQLFreeHandle(SQL_HANDLE_DBC, conn);
        }
        delete env;
        return SQL_SUCCESS;
    }
    case SQL_HANDLE_DBC: {
        auto *conn = static_cast<OdbcConnection *>(Handle);
        // Auto-disconnect and cleanup if still connected (defensive cleanup)
        if (conn->connected) {
            // Free all statements first
            for (auto *stmt : conn->statements) {
                stmt->result_set.Reset();
                stmt->bound_columns.clear();
                stmt->bound_parameters.clear();
                delete stmt;
            }
            conn->statements.clear();

            // Disconnect client
            {
                std::lock_guard<std::mutex> lock(g_clients_mutex);
                auto it = g_clients.find(conn);
                if (it != g_clients.end()) {
                    it->second->Disconnect();
                    g_clients.erase(it);
                }
            }
            conn->connected = false;
        }
        // Remove from environment
        if (conn->env) {
            auto &conns = conn->env->connections;
            conns.erase(std::remove(conns.begin(), conns.end(), conn), conns.end());
        }
        {
            std::lock_guard<std::mutex> lock(g_clients_mutex);
            g_clients.erase(conn);
        }
        delete conn;
        return SQL_SUCCESS;
    }
    case SQL_HANDLE_STMT: {
        auto *stmt = static_cast<OdbcStatement *>(Handle);
        // Clean up resources before freeing
        stmt->result_set.Reset();
        stmt->bound_columns.clear();
        stmt->bound_parameters.clear();
        // Remove from connection
        if (stmt->conn) {
            auto &stmts = stmt->conn->statements;
            stmts.erase(std::remove(stmts.begin(), stmts.end(), stmt), stmts.end());
        }
        delete stmt;
        return SQL_SUCCESS;
    }
    case SQL_HANDLE_DESC: {
        auto *desc = static_cast<OdbcDescriptor *>(Handle);
        delete desc;
        return SQL_SUCCESS;
    }
    default:
        return SQL_ERROR;
    }
}

// ============================================================================
// Legacy allocation functions
// ============================================================================
extern "C" SQLRETURN SQL_API SQLAllocEnv(SQLHENV *EnvironmentHandle) {
    return SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, (SQLHANDLE *)EnvironmentHandle);
}

extern "C" SQLRETURN SQL_API SQLFreeEnv(SQLHENV EnvironmentHandle) {
    return SQLFreeHandle(SQL_HANDLE_ENV, EnvironmentHandle);
}

extern "C" SQLRETURN SQL_API SQLAllocConnect(SQLHENV EnvironmentHandle, SQLHDBC *ConnectionHandle) {
    return SQLAllocHandle(SQL_HANDLE_DBC, EnvironmentHandle, (SQLHANDLE *)ConnectionHandle);
}

extern "C" SQLRETURN SQL_API SQLFreeConnect(SQLHDBC ConnectionHandle) {
    return SQLFreeHandle(SQL_HANDLE_DBC, ConnectionHandle);
}

extern "C" SQLRETURN SQL_API SQLAllocStmt(SQLHDBC ConnectionHandle, SQLHSTMT *StatementHandle) {
    return SQLAllocHandle(SQL_HANDLE_STMT, ConnectionHandle, (SQLHANDLE *)StatementHandle);
}

extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option) {
    if (!StatementHandle) {
        return SQL_INVALID_HANDLE;
    }
    if (Option == SQL_DROP) {
        return SQLFreeHandle(SQL_HANDLE_STMT, StatementHandle);
    }
    auto *stmt = static_cast<OdbcStatement *>(StatementHandle);
    HandleLock lock(stmt);

    switch (Option) {
    case SQL_CLOSE:
        stmt->result_set.CloseCursor();
        return SQL_SUCCESS;
    case SQL_UNBIND:
        stmt->bound_columns.clear();
        return SQL_SUCCESS;
    case SQL_RESET_PARAMS:
        stmt->bound_parameters.clear();
        return SQL_SUCCESS;
    default:
        stmt->SetError("HY092", "Invalid attribute/option identifier");
        return SQL_ERROR;
    }
}

// Forward declaration for ReadDsnSetting (defined in SQLConnect section)
static std::string ReadDsnSetting(const std::string &dsn, const std::string &key, const std::string &default_val);

// ============================================================================
// SQLDriverConnect / SQLDriverConnectW
// ============================================================================
static SQLRETURN DriverConnectImpl(OdbcConnection *conn, const std::string &conn_str) {
    TRACE_ENTRY("DriverConnectImpl", "host=" + conn->host + " port=" + std::to_string(conn->port));
    if (conn->connected) {
        conn->SetError("08002", "Connection already in use");
        return SQL_ERROR;
    }

    auto params = ParseConnectionString(conn_str);

    // If DSN is specified, read settings from registry as defaults
    for (const auto &[key, value] : params) {
        if (ToUpper(key) == "DSN" && !value.empty()) {
            std::string dsn_host = ReadDsnSetting(value, "Host", "");
            std::string dsn_port = ReadDsnSetting(value, "Port", "");
            std::string dsn_db = ReadDsnSetting(value, "Database", "");
            std::string dsn_user = ReadDsnSetting(value, "UID", "");
            std::string dsn_pwd = ReadDsnSetting(value, "PWD", "");
            std::string dsn_compress = ReadDsnSetting(value, "Compression", "");
            std::string dsn_maxrows = ReadDsnSetting(value, "DefaultMaxRows", "");
            std::string dsn_pagesize = ReadDsnSetting(value, "PageSize", "");
            std::string dsn_lazypaging = ReadDsnSetting(value, "LazyPaging", "");
            std::string dsn_maxlazyrows = ReadDsnSetting(value, "MaxLazyRows", "");
            std::string dsn_ssl = ReadDsnSetting(value, "SSL", "");
            std::string dsn_ssl_verify = ReadDsnSetting(value, "SSL_Verify", "");
            if (!dsn_host.empty()) conn->host = dsn_host;
            if (!dsn_port.empty()) {
                try { conn->port = static_cast<uint16_t>(std::stoi(dsn_port)); } catch (...) {}
            }
            if (!dsn_db.empty()) conn->database = dsn_db;
            if (!dsn_user.empty()) conn->user = dsn_user;
            if (!dsn_pwd.empty()) conn->password = dsn_pwd;
            if (!dsn_compress.empty()) {
                std::string uc = ToUpper(dsn_compress);
                conn->compression = (uc != "0" && uc != "NO" && uc != "OFF" && uc != "FALSE");
            }
            if (!dsn_maxrows.empty()) {
                try { conn->default_max_rows = static_cast<SQLULEN>(std::stoull(dsn_maxrows)); } catch (...) {}
            }
            if (!dsn_pagesize.empty()) {
                try { conn->page_size = static_cast<size_t>(std::stoull(dsn_pagesize)); } catch (...) {}
            }
            if (!dsn_lazypaging.empty()) {
                std::string uc = ToUpper(dsn_lazypaging);
                conn->lazy_paging = (uc != "0" && uc != "NO" && uc != "OFF" && uc != "FALSE");
            }
            if (!dsn_maxlazyrows.empty()) {
                try { conn->max_lazy_rows = static_cast<size_t>(std::stoull(dsn_maxlazyrows)); } catch (...) {}
            }
            if (!dsn_ssl.empty()) {
                std::string uc = ToUpper(dsn_ssl);
                conn->ssl = (uc == "1" || uc == "YES" || uc == "ON" || uc == "TRUE" || uc == "REQUIRE");
            }
            if (!dsn_ssl_verify.empty()) {
                std::string uc = ToUpper(dsn_ssl_verify);
                conn->ssl_verify = (uc != "0" && uc != "NO" && uc != "OFF" && uc != "FALSE");
            }
            TRACE_LOG(TraceLevel::Info, "DriverConnectImpl",
                      "DSN='" + value + "' host=" + conn->host + " port=" + std::to_string(conn->port));
            break;
        }
    }

    // Extract connection parameters (explicit params override DSN settings)
    for (const auto &[key, value] : params) {
        std::string ukey = ToUpper(key);
        if (ukey == "HOST" || ukey == "SERVER") {
            conn->host = value;
        } else if (ukey == "PORT") {
            try {
                int p = std::stoi(value);
                if (p < 1 || p > 65535) {
                    conn->SetError("HY024", "Invalid port number: " + value);
                    return SQL_ERROR;
                }
                conn->port = static_cast<uint16_t>(p);
            } catch (...) {
                conn->SetError("HY024", "Invalid port number: " + value);
                return SQL_ERROR;
            }
        } else if (ukey == "DATABASE" || ukey == "DB") {
            conn->database = value;
        } else if (ukey == "UID" || ukey == "USER") {
            conn->user = value;
        } else if (ukey == "PWD" || ukey == "PASSWORD") {
            conn->password = value;
        } else if (ukey == "COMPRESSION" || ukey == "COMPRESS") {
            std::string uc = ToUpper(value);
            conn->compression = (uc != "0" && uc != "NO" && uc != "OFF" && uc != "FALSE");
        } else if (ukey == "DEFAULTMAXROWS" || ukey == "DEFAULT_MAX_ROWS" || ukey == "MAXROWS") {
            try { conn->default_max_rows = static_cast<SQLULEN>(std::stoull(value)); } catch (...) {}
        } else if (ukey == "PAGESIZE" || ukey == "PAGE_SIZE") {
            try { conn->page_size = static_cast<size_t>(std::stoull(value)); } catch (...) {}
        } else if (ukey == "LAZYPAGING" || ukey == "LAZY_PAGING") {
            std::string uc = ToUpper(value);
            conn->lazy_paging = (uc != "0" && uc != "NO" && uc != "OFF" && uc != "FALSE");
        } else if (ukey == "MAXLAZYROWS" || ukey == "MAX_LAZY_ROWS") {
            try { conn->max_lazy_rows = static_cast<size_t>(std::stoull(value)); } catch (...) {}
        } else if (ukey == "SSL" || ukey == "SSLMODE") {
            std::string uc = ToUpper(value);
            conn->ssl = (uc == "1" || uc == "YES" || uc == "ON" || uc == "TRUE" || uc == "REQUIRE");
        } else if (ukey == "SSL_VERIFY" || ukey == "SSLVERIFY") {
            std::string uc = ToUpper(value);
            conn->ssl_verify = (uc != "0" && uc != "NO" && uc != "OFF" && uc != "FALSE");
        }
    }

    // If SSL is enabled and port was not explicitly specified, use default HTTPS port
    bool port_explicitly_set = false;
    for (const auto &[key, value] : params) {
        std::string ukey = ToUpper(key);
        if (ukey == "PORT") { port_explicitly_set = true; break; }
    }
    if (conn->ssl && !port_explicitly_set && conn->port == 8123) {
        conn->port = 8443;  // ClickHouse default HTTPS port
    }

    // Create and connect client
    auto client = std::make_unique<ClickHouseClient>();

    // Apply timeout settings from connection attributes
    if (conn->login_timeout > 0) {
        client->SetConnectionTimeout(static_cast<unsigned int>(conn->login_timeout));
    }
    if (conn->connection_timeout > 0) {
        client->SetConnectionTimeout(static_cast<unsigned int>(conn->connection_timeout));
    }

    // Apply HTTP compression setting to client
    client->SetCompressionEnabled(conn->compression);

    // Apply SSL settings to client
    client->SetSSLEnabled(conn->ssl);
    client->SetSSLVerify(conn->ssl_verify);

    TRACE_LOG(TraceLevel::Info, "DriverConnectImpl",
              "SSL=" + std::string(conn->ssl ? "ON" : "OFF") +
              " SSL_Verify=" + std::string(conn->ssl_verify ? "ON" : "OFF") +
              " Compression=" + std::string(conn->compression ? "ON" : "OFF") +
              " DefaultMaxRows=" + std::to_string(conn->default_max_rows) +
              " PageSize=" + std::to_string(conn->page_size) +
              " LazyPaging=" + std::string(conn->lazy_paging ? "ON" : "OFF") +
              " MaxLazyRows=" + std::to_string(conn->max_lazy_rows));

    if (!client->Connect(conn->host, conn->port, conn->database, conn->user, conn->password)) {
        std::string scheme = conn->ssl ? "https" : "http";
        conn->SetError("08001", "Unable to connect to ClickHouse server at " + scheme + "://" + conn->host + ":" + std::to_string(conn->port));
        return SQL_ERROR;
    }

    conn->connected = true;
    conn->current_catalog = conn->database;
    {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        g_clients[conn] = std::move(client);
    }

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLDriverConnect(SQLHDBC ConnectionHandle, SQLHWND WindowHandle,
                                              SQLCHAR *InConnectionString, SQLSMALLINT StringLength1,
                                              SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength,
                                              SQLSMALLINT *StringLength2Ptr, SQLUSMALLINT DriverCompletion) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    std::string conn_str;
    if (InConnectionString) {
        if (StringLength1 == SQL_NTS) {
            conn_str = reinterpret_cast<const char *>(InConnectionString);
        } else {
            conn_str = std::string(reinterpret_cast<const char *>(InConnectionString), StringLength1);
        }
    }

    // Handle DriverCompletion modes
    if (DriverCompletion == SQL_DRIVER_PROMPT) {
        // No dialog support — return error
        conn->SetError("HYC00", "Driver does not support dialog prompt");
        return SQL_ERROR;
    }

    if (DriverCompletion == SQL_DRIVER_COMPLETE || DriverCompletion == SQL_DRIVER_COMPLETE_REQUIRED) {
        // Check if required parameters are present; if missing, return error (no dialog to prompt)
        auto params = ParseConnectionString(conn_str);
        bool has_host = false;
        for (const auto &[key, value] : params) {
            std::string ukey = ToUpper(key);
            if (ukey == "HOST" || ukey == "SERVER" || ukey == "DSN") has_host = true;
        }
        if (!has_host && conn_str.empty()) {
            conn->SetError("HYC00", "Connection dialog not supported; required parameters missing");
            return SQL_ERROR;
        }
    }

    SQLRETURN ret = DriverConnectImpl(conn, conn_str);

    // Copy connection string to output
    if (ret == SQL_SUCCESS && OutConnectionString && BufferLength > 0) {
        CopyStringToBuffer(conn_str, OutConnectionString, BufferLength, StringLength2Ptr);
    }

    return ret;
}

extern "C" SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC ConnectionHandle, SQLHWND WindowHandle,
                                                SQLWCHAR *InConnectionString, SQLSMALLINT StringLength1,
                                                SQLWCHAR *OutConnectionString, SQLSMALLINT BufferLength,
                                                SQLSMALLINT *StringLength2Ptr, SQLUSMALLINT DriverCompletion) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    std::string conn_str;
    if (InConnectionString) {
        conn_str = WideToUtf8(InConnectionString, StringLength1);
    }

    // Handle DriverCompletion modes
    if (DriverCompletion == SQL_DRIVER_PROMPT) {
        conn->SetError("HYC00", "Driver does not support dialog prompt");
        return SQL_ERROR;
    }

    if (DriverCompletion == SQL_DRIVER_COMPLETE || DriverCompletion == SQL_DRIVER_COMPLETE_REQUIRED) {
        auto params = ParseConnectionString(conn_str);
        bool has_host = false;
        for (const auto &[key, value] : params) {
            std::string ukey = ToUpper(key);
            if (ukey == "HOST" || ukey == "SERVER" || ukey == "DSN") has_host = true;
        }
        if (!has_host && conn_str.empty()) {
            conn->SetError("HYC00", "Connection dialog not supported; required parameters missing");
            return SQL_ERROR;
        }
    }

    SQLRETURN ret = DriverConnectImpl(conn, conn_str);

    if (ret == SQL_SUCCESS && OutConnectionString && BufferLength > 0) {
        CopyStringToBufferW(conn_str, OutConnectionString, BufferLength, StringLength2Ptr);
    }

    return ret;
}

// ============================================================================
// SQLConnect / SQLConnectW
// ============================================================================

// Helper: Read DSN settings from ODBC registry
static std::string ReadDsnSetting(const std::string &dsn, const std::string &key, const std::string &default_val) {
    char buf[256] = {};
    int len = SQLGetPrivateProfileString(
        dsn.c_str(), key.c_str(), default_val.c_str(),
        buf, sizeof(buf), "ODBC.INI");
    if (len > 0) {
        return std::string(buf, len);
    }
    return default_val;
}

static std::string BuildConnStrFromDsn(const std::string &dsn) {
    std::string host = ReadDsnSetting(dsn, "Host", "localhost");
    std::string port = ReadDsnSetting(dsn, "Port", "8123");
    std::string db = ReadDsnSetting(dsn, "Database", "default");
    std::string user = ReadDsnSetting(dsn, "UID", "default");
    std::string pwd = ReadDsnSetting(dsn, "PWD", "");
    std::string compress = ReadDsnSetting(dsn, "Compression", "");
    std::string ssl = ReadDsnSetting(dsn, "SSL", "");
    std::string ssl_verify = ReadDsnSetting(dsn, "SSL_Verify", "");

    std::string result = "HOST=" + host + ";PORT=" + port + ";DATABASE=" + db +
                         ";UID=" + user + ";PWD=" + pwd;
    if (!compress.empty()) {
        result += ";COMPRESSION=" + compress;
    }
    if (!ssl.empty()) {
        result += ";SSL=" + ssl;
    }
    if (!ssl_verify.empty()) {
        result += ";SSL_VERIFY=" + ssl_verify;
    }
    return result;
}

extern "C" SQLRETURN SQL_API SQLConnect(SQLHDBC ConnectionHandle, SQLCHAR *ServerName, SQLSMALLINT NameLength1,
                                         SQLCHAR *UserName, SQLSMALLINT NameLength2,
                                         SQLCHAR *Authentication, SQLSMALLINT NameLength3) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    std::string dsn;
    if (ServerName) {
        dsn = std::string(reinterpret_cast<const char *>(ServerName),
                          NameLength1 == SQL_NTS ? strlen(reinterpret_cast<const char *>(ServerName)) : NameLength1);
    }

    // Read connection parameters from DSN registry
    std::string conn_str = dsn.empty() ? "HOST=localhost;PORT=8123;DATABASE=default" : BuildConnStrFromDsn(dsn);

    // Override with explicit parameters from SQLConnect args
    if (UserName) {
        std::string user(reinterpret_cast<const char *>(UserName),
                         NameLength2 == SQL_NTS ? strlen(reinterpret_cast<const char *>(UserName)) : NameLength2);
        conn_str += ";UID=" + user;
    }
    if (Authentication) {
        std::string pwd(reinterpret_cast<const char *>(Authentication),
                        NameLength3 == SQL_NTS ? strlen(reinterpret_cast<const char *>(Authentication)) : NameLength3);
        conn_str += ";PWD=" + pwd;
    }

    TRACE_LOG(TraceLevel::Info, "SQLConnect", "DSN='" + dsn + "'");
    return DriverConnectImpl(conn, conn_str);
}

extern "C" SQLRETURN SQL_API SQLConnectW(SQLHDBC ConnectionHandle, SQLWCHAR *ServerName, SQLSMALLINT NameLength1,
                                          SQLWCHAR *UserName, SQLSMALLINT NameLength2,
                                          SQLWCHAR *Authentication, SQLSMALLINT NameLength3) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    std::string dsn = ServerName ? WideToUtf8(ServerName, NameLength1) : "";
    std::string conn_str = dsn.empty() ? "HOST=localhost;PORT=8123;DATABASE=default" : BuildConnStrFromDsn(dsn);

    // Override with explicit parameters
    if (UserName) {
        conn_str += ";UID=" + WideToUtf8(UserName, NameLength2);
    }
    if (Authentication) {
        conn_str += ";PWD=" + WideToUtf8(Authentication, NameLength3);
    }

    TRACE_LOG(TraceLevel::Info, "SQLConnectW", "DSN='" + dsn + "'");
    return DriverConnectImpl(conn, conn_str);
}

// ============================================================================
// SQLDisconnect
// ============================================================================
extern "C" SQLRETURN SQL_API SQLDisconnect(SQLHDBC ConnectionHandle) {
    TRACE_ENTRY("SQLDisconnect", "");
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    // Free all statements
    for (auto *stmt : conn->statements) {
        delete stmt;
    }
    conn->statements.clear();

    // Disconnect client
    {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        auto it = g_clients.find(conn);
        if (it != g_clients.end()) {
            it->second->Disconnect();
            g_clients.erase(it);
        }
    }

    conn->connected = false;
    return SQL_SUCCESS;
}

// ============================================================================
// SQLGetConnectAttr / SQLSetConnectAttr
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute,
                                                SQLPOINTER Value, SQLINTEGER BufferLength,
                                                SQLINTEGER *StringLength) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    switch (Attribute) {
    case SQL_ATTR_ACCESS_MODE:
        *(SQLUINTEGER *)Value = conn->access_mode;
        return SQL_SUCCESS;
    case SQL_ATTR_AUTOCOMMIT:
        *(SQLUINTEGER *)Value = conn->autocommit;
        return SQL_SUCCESS;
    case SQL_ATTR_LOGIN_TIMEOUT:
        *(SQLUINTEGER *)Value = conn->login_timeout;
        return SQL_SUCCESS;
    case SQL_ATTR_CONNECTION_TIMEOUT:
        *(SQLUINTEGER *)Value = conn->connection_timeout;
        return SQL_SUCCESS;
    case SQL_ATTR_CONNECTION_DEAD: {
        // Connection pooling support: check if the connection is still alive
        auto *client = GetClient(conn);
        if (!client || !conn->connected) {
            *(SQLUINTEGER *)Value = SQL_CD_TRUE;
        } else {
            *(SQLUINTEGER *)Value = client->Ping() ? SQL_CD_FALSE : SQL_CD_TRUE;
        }
        return SQL_SUCCESS;
    }
    case SQL_ATTR_CURRENT_CATALOG:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(conn->current_catalog, (SQLCHAR *)Value, (SQLSMALLINT)BufferLength, (SQLSMALLINT *)StringLength);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_TRACE:
        *(SQLUINTEGER *)Value = conn->trace_enabled ? SQL_OPT_TRACE_ON : SQL_OPT_TRACE_OFF;
        return SQL_SUCCESS;
    case SQL_ATTR_TRACEFILE:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(conn->trace_file, (SQLCHAR *)Value, (SQLSMALLINT)BufferLength, (SQLSMALLINT *)StringLength);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_METADATA_ID:
        *(SQLUINTEGER *)Value = conn->metadata_id;
        return SQL_SUCCESS;
    case SQL_ATTR_TXN_ISOLATION:
        *(SQLUINTEGER *)Value = conn->txn_isolation;
        return SQL_SUCCESS;
    case SQL_ATTR_PACKET_SIZE:
        *(SQLUINTEGER *)Value = conn->packet_size;
        return SQL_SUCCESS;
    case SQL_ATTR_ASYNC_ENABLE:
        *(SQLULEN *)Value = conn->async_enable;
        return SQL_SUCCESS;
    case SQL_ATTR_QUIET_MODE:
        *(SQLHWND *)Value = conn->quiet_mode;
        return SQL_SUCCESS;
    case SQL_ATTR_ODBC_CURSORS:
        *(SQLULEN *)Value = conn->odbc_cursors;
        return SQL_SUCCESS;
    case SQL_ATTR_TRANSLATE_LIB:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(conn->translate_lib, (SQLCHAR *)Value, (SQLSMALLINT)BufferLength, (SQLSMALLINT *)StringLength);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_TRANSLATE_OPTION:
        *(SQLUINTEGER *)Value = conn->translate_option;
        return SQL_SUCCESS;
    default:
        // Return 0 for unknown integer attributes (compatibility)
        if (Value) *(SQLUINTEGER *)Value = 0;
        return SQL_SUCCESS;
    }
}

extern "C" SQLRETURN SQL_API SQLGetConnectAttrW(SQLHDBC ConnectionHandle, SQLINTEGER Attribute,
                                                 SQLPOINTER Value, SQLINTEGER BufferLength,
                                                 SQLINTEGER *StringLength) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    switch (Attribute) {
    case SQL_ATTR_CURRENT_CATALOG:
        if (Value && BufferLength > 0) {
            CopyStringToBufferW(conn->current_catalog, (SQLWCHAR *)Value, (SQLSMALLINT)(BufferLength / sizeof(SQLWCHAR)),
                                (SQLSMALLINT *)StringLength);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_TRACEFILE:
        if (Value && BufferLength > 0) {
            CopyStringToBufferW(conn->trace_file, (SQLWCHAR *)Value, (SQLSMALLINT)(BufferLength / sizeof(SQLWCHAR)),
                                (SQLSMALLINT *)StringLength);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_TRANSLATE_LIB:
        if (Value && BufferLength > 0) {
            CopyStringToBufferW(conn->translate_lib, (SQLWCHAR *)Value, (SQLSMALLINT)(BufferLength / sizeof(SQLWCHAR)),
                                (SQLSMALLINT *)StringLength);
        }
        return SQL_SUCCESS;
    default:
        return SQLGetConnectAttr(ConnectionHandle, Attribute, Value, BufferLength, StringLength);
    }
}

extern "C" SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute,
                                                SQLPOINTER Value, SQLINTEGER StringLength) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    switch (Attribute) {
    case SQL_ATTR_ACCESS_MODE:
        conn->access_mode = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_AUTOCOMMIT:
        conn->autocommit = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_LOGIN_TIMEOUT:
        conn->login_timeout = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_CONNECTION_TIMEOUT:
        conn->connection_timeout = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_CURRENT_CATALOG:
        if (Value) {
            conn->current_catalog = std::string(reinterpret_cast<const char *>(Value),
                                                StringLength == SQL_NTS ? strlen(reinterpret_cast<const char *>(Value)) : StringLength);
            conn->database = conn->current_catalog;
        }
        return SQL_SUCCESS;
    case SQL_ATTR_TRACE: {
        SQLUINTEGER trace_val = (SQLUINTEGER)(uintptr_t)Value;
        conn->trace_enabled = (trace_val == SQL_OPT_TRACE_ON);
        TraceLog::Instance().SetEnabled(conn->trace_enabled);
        return SQL_SUCCESS;
    }
    case SQL_ATTR_TRACEFILE:
        if (Value) {
            conn->trace_file = std::string(reinterpret_cast<const char *>(Value),
                                           StringLength == SQL_NTS ? strlen(reinterpret_cast<const char *>(Value)) : StringLength);
            TraceLog::Instance().SetTraceFile(conn->trace_file);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_METADATA_ID:
        conn->metadata_id = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_TXN_ISOLATION:
        conn->txn_isolation = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_PACKET_SIZE:
        conn->packet_size = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_ASYNC_ENABLE:
        conn->async_enable = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_QUIET_MODE:
        conn->quiet_mode = (SQLHWND)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_ODBC_CURSORS:
        conn->odbc_cursors = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_TRANSLATE_LIB:
        if (Value) {
            conn->translate_lib = std::string(reinterpret_cast<const char *>(Value),
                                              StringLength == SQL_NTS ? strlen(reinterpret_cast<const char *>(Value)) : StringLength);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_TRANSLATE_OPTION:
        conn->translate_option = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    default:
        // Silently ignore unsupported attributes for compatibility
        return SQL_SUCCESS;
    }
}

extern "C" SQLRETURN SQL_API SQLSetConnectAttrW(SQLHDBC ConnectionHandle, SQLINTEGER Attribute,
                                                 SQLPOINTER Value, SQLINTEGER StringLength) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);

    switch (Attribute) {
    case SQL_ATTR_CURRENT_CATALOG:
        if (Value) {
            conn->current_catalog = WideToUtf8((SQLWCHAR *)Value, (SQLSMALLINT)(StringLength / sizeof(SQLWCHAR)));
            conn->database = conn->current_catalog;
        }
        return SQL_SUCCESS;
    case SQL_ATTR_TRACEFILE:
        if (Value) {
            conn->trace_file = WideToUtf8((SQLWCHAR *)Value, (SQLSMALLINT)(StringLength / sizeof(SQLWCHAR)));
            TraceLog::Instance().SetTraceFile(conn->trace_file);
        }
        return SQL_SUCCESS;
    case SQL_ATTR_TRANSLATE_LIB:
        if (Value) {
            conn->translate_lib = WideToUtf8((SQLWCHAR *)Value, (SQLSMALLINT)(StringLength / sizeof(SQLWCHAR)));
        }
        return SQL_SUCCESS;
    default:
        return SQLSetConnectAttr(ConnectionHandle, Attribute, Value, StringLength);
    }
}

// ============================================================================
// Environment attributes
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute,
                                            SQLPOINTER Value, SQLINTEGER BufferLength,
                                            SQLINTEGER *StringLength) {
    if (!IsValidEnvHandle(EnvironmentHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *env = static_cast<OdbcEnvironment *>(EnvironmentHandle);
    HandleLock lock(env);

    switch (Attribute) {
    case SQL_ATTR_ODBC_VERSION:
        *(SQLINTEGER *)Value = env->odbc_version;
        return SQL_SUCCESS;
    case SQL_ATTR_CONNECTION_POOLING:
        *(SQLUINTEGER *)Value = env->connection_pooling;
        return SQL_SUCCESS;
    case SQL_ATTR_CP_MATCH:
        *(SQLUINTEGER *)Value = env->cp_match;
        return SQL_SUCCESS;
    case SQL_ATTR_OUTPUT_NTS:
        *(SQLINTEGER *)Value = SQL_TRUE;
        return SQL_SUCCESS;
    default:
        env->SetError("HYC00", "Optional feature not implemented");
        return SQL_ERROR;
    }
}

extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute,
                                            SQLPOINTER Value, SQLINTEGER StringLength) {
    if (!IsValidEnvHandle(EnvironmentHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *env = static_cast<OdbcEnvironment *>(EnvironmentHandle);
    HandleLock lock(env);

    switch (Attribute) {
    case SQL_ATTR_ODBC_VERSION:
        env->odbc_version = (SQLINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_CONNECTION_POOLING:
        env->connection_pooling = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_CP_MATCH:
        env->cp_match = (SQLUINTEGER)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_ATTR_OUTPUT_NTS:
        return SQL_SUCCESS;
    default:
        env->SetError("HYC00", "Optional feature not implemented");
        return SQL_ERROR;
    }
}

// ============================================================================
// SQLEndTran
// ============================================================================
extern "C" SQLRETURN SQL_API SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                         SQLSMALLINT CompletionType) {
    // ClickHouse doesn't support transactions in the traditional sense
    // Always succeed for compatibility
    return SQL_SUCCESS;
}

// ============================================================================
// SQLTransact (ODBC 2.x) — delegates to SQLEndTran
// ============================================================================
extern "C" SQLRETURN SQL_API SQLTransact(SQLHENV EnvironmentHandle, SQLHDBC ConnectionHandle,
                                          SQLUSMALLINT CompletionType) {
    if (ConnectionHandle) {
        return SQLEndTran(SQL_HANDLE_DBC, ConnectionHandle, static_cast<SQLSMALLINT>(CompletionType));
    }
    if (EnvironmentHandle) {
        return SQLEndTran(SQL_HANDLE_ENV, EnvironmentHandle, static_cast<SQLSMALLINT>(CompletionType));
    }
    return SQL_INVALID_HANDLE;
}

// ============================================================================
// SQLGetConnectOption / SQLSetConnectOption (ODBC 2.x)
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetConnectOption(SQLHDBC ConnectionHandle,
                                                  SQLUSMALLINT Option, SQLPOINTER Value) {
    // ODBC 2.x option IDs are the same as ODBC 3.x attribute IDs for common options
    return SQLGetConnectAttr(ConnectionHandle, static_cast<SQLINTEGER>(Option),
                             Value, SQL_MAX_OPTION_STRING_LENGTH, nullptr);
}

extern "C" SQLRETURN SQL_API SQLSetConnectOption(SQLHDBC ConnectionHandle,
                                                  SQLUSMALLINT Option, SQLULEN Value) {
    return SQLSetConnectAttr(ConnectionHandle, static_cast<SQLINTEGER>(Option),
                             reinterpret_cast<SQLPOINTER>(Value), SQL_IS_UINTEGER);
}

// ============================================================================
// SQLBrowseConnect / SQLBrowseConnectW
// ============================================================================
static SQLRETURN BrowseConnectImpl(OdbcConnection *conn, const std::string &in_conn_str,
                                    std::string &out_conn_str, bool &need_more) {
    TRACE_ENTRY("BrowseConnectImpl", "in=" + in_conn_str.substr(0, 200));

    if (conn->connected) {
        conn->SetError("08002", "Connection already in use");
        return SQL_ERROR;
    }

    auto params = ParseConnectionString(in_conn_str);

    // Extract provided parameters
    std::string host, port_str, database, uid, pwd;
    for (const auto &[key, value] : params) {
        std::string ukey = ToUpper(key);
        if (ukey == "HOST" || ukey == "SERVER") host = value;
        else if (ukey == "PORT") port_str = value;
        else if (ukey == "DATABASE" || ukey == "DB") database = value;
        else if (ukey == "UID" || ukey == "USER") uid = value;
        else if (ukey == "PWD" || ukey == "PASSWORD") pwd = value;
        else if (ukey == "DSN") {
            // Read defaults from DSN
            if (host.empty()) host = ReadDsnSetting(value, "Host", "");
            if (port_str.empty()) port_str = ReadDsnSetting(value, "Port", "");
            if (database.empty()) database = ReadDsnSetting(value, "Database", "");
            if (uid.empty()) uid = ReadDsnSetting(value, "UID", "");
            if (pwd.empty()) pwd = ReadDsnSetting(value, "PWD", "");
        }
    }

    // Check for required attributes and build browse result for missing ones
    std::string missing;
    if (host.empty()) {
        if (!missing.empty()) missing += ";";
        missing += "*HOST:Server={?}";
    }
    if (uid.empty()) {
        if (!missing.empty()) missing += ";";
        missing += "*UID:Login ID={?}";
    }

    if (!missing.empty()) {
        // Return browse result with required attributes
        out_conn_str = missing;
        if (!port_str.empty()) out_conn_str += ";PORT:Port Number={" + port_str + "}";
        if (!database.empty()) out_conn_str += ";DATABASE:Database={" + database + "}";
        need_more = true;
        return SQL_NEED_DATA;
    }

    // All required attributes present — attempt connection
    if (host.empty()) host = "localhost";
    if (port_str.empty()) port_str = "8123";
    if (database.empty()) database = "default";
    if (uid.empty()) uid = "default";

    conn->host = host;
    try {
        int p = std::stoi(port_str);
        if (p < 1 || p > 65535) {
            conn->SetError("HY024", "Invalid port number: " + port_str);
            return SQL_ERROR;
        }
        conn->port = static_cast<uint16_t>(p);
    } catch (...) {
        conn->SetError("HY024", "Invalid port number: " + port_str);
        return SQL_ERROR;
    }
    conn->database = database;
    conn->user = uid;
    conn->password = pwd;

    auto client = std::make_unique<ClickHouseClient>();

    if (conn->login_timeout > 0)
        client->SetConnectionTimeout(static_cast<unsigned int>(conn->login_timeout));
    if (conn->connection_timeout > 0)
        client->SetConnectionTimeout(static_cast<unsigned int>(conn->connection_timeout));

    if (!client->Connect(conn->host, conn->port, conn->database, conn->user, conn->password)) {
        conn->SetError("08001", "Unable to connect to ClickHouse server at " +
                        conn->host + ":" + std::to_string(conn->port));
        return SQL_ERROR;
    }

    conn->connected = true;
    conn->current_catalog = conn->database;
    {
        std::lock_guard<std::mutex> lock(g_clients_mutex);
        g_clients[conn] = std::move(client);
    }

    // Build output connection string
    out_conn_str = "HOST=" + conn->host + ";PORT=" + std::to_string(conn->port) +
                   ";DATABASE=" + conn->database + ";UID=" + conn->user + ";PWD=" + conn->password;
    need_more = false;
    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLBrowseConnect(SQLHDBC ConnectionHandle,
                                               SQLCHAR *InConnectionString, SQLSMALLINT StringLength1,
                                               SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength,
                                               SQLSMALLINT *StringLength2Ptr) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    std::string in_str;
    if (InConnectionString) {
        if (StringLength1 == SQL_NTS)
            in_str = reinterpret_cast<const char *>(InConnectionString);
        else
            in_str = std::string(reinterpret_cast<const char *>(InConnectionString), StringLength1);
    }

    std::string out_str;
    bool need_more = false;
    SQLRETURN ret = BrowseConnectImpl(conn, in_str, out_str, need_more);

    if (OutConnectionString && BufferLength > 0) {
        CopyStringToBuffer(out_str, OutConnectionString, BufferLength, StringLength2Ptr);
    } else if (StringLength2Ptr) {
        *StringLength2Ptr = (SQLSMALLINT)out_str.size();
    }

    return ret;
}

extern "C" SQLRETURN SQL_API SQLBrowseConnectW(SQLHDBC ConnectionHandle,
                                                SQLWCHAR *InConnectionString, SQLSMALLINT StringLength1,
                                                SQLWCHAR *OutConnectionString, SQLSMALLINT BufferLength,
                                                SQLSMALLINT *StringLength2Ptr) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    conn->ClearDiagRecords();

    std::string in_str;
    if (InConnectionString) {
        in_str = WideToUtf8(InConnectionString, StringLength1);
    }

    std::string out_str;
    bool need_more = false;
    SQLRETURN ret = BrowseConnectImpl(conn, in_str, out_str, need_more);

    if (OutConnectionString && BufferLength > 0) {
        CopyStringToBufferW(out_str, OutConnectionString, BufferLength, StringLength2Ptr);
    } else if (StringLength2Ptr) {
        std::wstring wide = Utf8ToWide(out_str);
        *StringLength2Ptr = (SQLSMALLINT)(wide.size() * sizeof(SQLWCHAR));
    }

    return ret;
}
