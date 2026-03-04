# clickhouse-odbc-alternative

A Windows ODBC driver for ClickHouse (v1.1.0), designed with a focus on compatibility with MS Access and other Windows applications.

## Features

- **ODBC 3.x compliant + ODBC 2.x backward compatible** — Exports 127 ODBC API functions
- **ANSI / Unicode dual support** — Exports both A and W variants
- **HTTP interface** — Uses ClickHouse port 8123 (no native protocol required)
- **SSL/TLS support** — Optional HTTPS connection (requires OpenSSL)
- **Lightweight** — Header-only third-party libraries (cpp-httplib, nlohmann/json)
- **MS Access optimized** — Linked tables, datasheet view, form view, and report support
- **Parameterized queries** — Client-side parameter substitution via `SQLBindParameter`
- **ODBC escape sequences** — Native translation of 60+ functions (`{fn}`, `{d}`, `{ts}`, `{oj}`, etc.)
- **Descriptor handles** — Full APD/IPD/ARD/IRD support
- **Data-at-Execution** — Streaming parameters via `SQLParamData`/`SQLPutData`
- **Connection reliability** — Timeout, retry, thread-safety, and connection pooling awareness
- **784 unit tests** — All tests pass in both Debug and Release configurations

## Build Requirements

- Visual Studio 2022 or later (Desktop development with C++ workload)
- Windows SDK 10.0
- Platform: x64

## Build Instructions

### Visual Studio (Recommended)

1. Open `clickhouse_odbc.sln` in Visual Studio
2. Set configuration to `Debug|x64` or `Release|x64`
3. Build (Ctrl+Shift+B)

Output:
- DLL: `out\<Configuration>\ch-odbc-alternative.dll`
- Tests: `out\<Configuration>\test\ch-odbc-alternative-test.exe`
- Installer: `out\<Configuration>\ch-odbc-alternative-installer.exe`

### CMake

```bash
mkdir build && cd build
cmake .. -G "Visual Studio 17 2022" -A x64
cmake --build . --config Release
```

## Installation

### 1. Place the Driver

Copy the built `ch-odbc-alternative.dll` to a directory of your choice.

Alternatively, use the GUI installer (`ch-odbc-alternative-installer.exe`) to copy files, register the driver, and create a DSN all at once.

### 2. Register the Driver

Run from an elevated (Administrator) command prompt:

```bat
setup\register_driver.bat
```

> **Note:** To change the DLL path, edit `DRIVER_PATH` in the batch file or pass it as an argument: `register_driver.bat <full path to DLL>`.

### 3. Create a DSN (Optional)

Create via ODBC Data Source Administrator (`odbcad32.exe`), or register directly in the registry:

```bat
REM Using create_dsn.bat (hostname is required):
setup\create_dsn.bat MyClickHouse your-server.example.com 8123 default default

REM Or register manually in the registry:
reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "Driver" /t REG_SZ /d "C:\path\to\ch-odbc-alternative.dll" /f
reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "Host" /t REG_SZ /d "your-server.example.com" /f
reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "Port" /t REG_SZ /d "8123" /f
reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "Database" /t REG_SZ /d "default" /f
reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "UID" /t REG_SZ /d "default" /f
reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" /v "MyClickHouse" /t REG_SZ /d "CH ODBC Alternative" /f
```

## Connection String

```
Driver={CH ODBC Alternative};Host=localhost;Port=8123;Database=default;UID=default;PWD=;
```

| Parameter | Description | Default |
|-----------|-------------|---------|
| Host | ClickHouse server hostname/IP | localhost |
| Port | HTTP port (8123) or HTTPS port (8443) | 8123 |
| Database | Default database | default |
| UID | Username | default |
| PWD | Password | (empty) |
| SSL | Enable SSL/TLS (`1`, `true`, `yes`) | 0 |
| SSL_VERIFY | Verify server certificate (`0` to disable) | 1 |
| Compression | HTTP gzip compression (`0` to disable) | 1 |
| DefaultMaxRows | Default `SQL_ATTR_MAX_ROWS` for new statements | 0 (unlimited) |
| PageSize | Rows per lazy-paging page | 10000 |
| LazyPaging | Enable lazy paging (`0` to disable) | 1 |
| MaxLazyRows | Max rows exposed in lazy paging mode (`0` = unlimited) | 0 (unlimited) |

## Supported ODBC Functions

| Category | Functions |
|----------|-----------|
| Handle Management | SQLAllocHandle, SQLFreeHandle, SQLAllocEnv/Connect/Stmt, SQLFreeEnv/Connect/Stmt |
| Connection | SQLConnect(W), SQLDriverConnect(W), SQLBrowseConnect(W), SQLDisconnect |
| Statement | SQLExecDirect(W), SQLPrepare(W), SQLExecute, SQLCancel, SQLNativeSql(W) |
| Parameters | SQLBindParameter, SQLDescribeParam, SQLNumParams, SQLParamData, SQLPutData |
| Result Retrieval | SQLFetch, SQLFetchScroll, SQLExtendedFetch, SQLGetData, SQLRowCount, SQLCloseCursor, SQLMoreResults |
| Metadata | SQLNumResultCols, SQLDescribeCol(W), SQLColAttribute(W), SQLColAttributes |
| Catalog | SQLTables(W), SQLColumns(W), SQLPrimaryKeys(W), SQLStatistics(W), SQLSpecialColumns(W), SQLForeignKeys(W), SQLGetTypeInfo(W), SQLProcedures(W), SQLProcedureColumns(W), SQLTablePrivileges(W), SQLColumnPrivileges(W) |
| Descriptors | SQLGetDescField(W), SQLSetDescField(W), SQLGetDescRec(W), SQLSetDescRec, SQLCopyDesc |
| Diagnostics | SQLGetDiagRec(W), SQLGetDiagField(W), SQLError(W) |
| Attributes | SQLGetConnectAttr(W), SQLSetConnectAttr(W), SQLGetStmtAttr(W), SQLSetStmtAttr(W), SQLGetEnvAttr, SQLSetEnvAttr |
| Cursor | SQLGetCursorName(W), SQLSetCursorName(W), SQLSetPos, SQLBulkOperations, SQLSetScrollOptions |
| Other | SQLEndTran, SQLGetFunctions, SQLGetInfo(W), SQLFreeStmt, ConfigDSN(W) |
| ODBC 2.x Compat | SQLTransact, SQLGetConnectOption, SQLSetConnectOption, SQLGetStmtOption, SQLSetStmtOption |

## Testing

```bat
REM Run tests after Debug build
build\Debug\test\ch-odbc-alternative-test.exe

REM Run tests after Release build
build\Release\test\ch-odbc-alternative-test.exe
```

784 unit tests (no ClickHouse server required):
- Utilities, type mapping, handle management
- ODBC API (connection, statement, result retrieval, catalog, diagnostics)
- Parameterized queries, Data-at-Execution
- ODBC escape sequences (60+ function translations)
- Descriptor handle operations
- SSL/TLS configuration, lazy paging, HTTP compression
- ODBC 2.x backward compatibility
- MS Access workflow simulation (19 scenarios)

## Documentation

- [Architecture](docs/ARCHITECTURE.md) — Project structure and component design
- [Roadmap](docs/ROADMAP.md) — Development plan and milestones
- [Contributing](docs/CONTRIBUTING.md) — Build instructions and coding conventions
- [Changelog](CHANGELOG.md) — Changes per version

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | C++17 |
| Build | Visual Studio / CMake |
| HTTP | cpp-httplib v0.18.3 (header-only) |
| JSON | nlohmann/json v3.11.3 (header-only) |
| Protocol | ClickHouse HTTP Interface (port 8123) |

## Known Limitations

- ClickHouse does not support transactions, so `SQLEndTran` always returns success
- SSL/TLS requires OpenSSL (`CPPHTTPLIB_OPENSSL_SUPPORT` build flag)
- x64 only (no 32-bit build)
- Bookmarks are not implemented
- MS Access has a 2GB temporary database limit; navigating to the last row of very large tables may trigger an error. Set `MaxLazyRows` (e.g., `500000`) in the connection string as a workaround

## License

Apache License 2.0 — See [LICENSE](LICENSE) for details.
