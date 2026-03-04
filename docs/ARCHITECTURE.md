# Architecture

## Overview

clickhouse-odbc-alternative is a Windows DLL project that provides an ODBC 3.x-compliant driver using the ClickHouse HTTP interface (default port: 8123). It is designed with compatibility with MS Access and other Windows applications as the top priority.

## Layer Structure

```
+-------------------------------------+
|        ODBC Application             |  (MS Access, Excel, etc.)
+-------------------------------------+
|           ODBC Driver Manager       |  (odbc32.dll)
+-------------------------------------+
|    ch-odbc-alternative.dll          |
|  +-------------------------------+  |
|  |  ODBC API Entry Points        |  |  clickhouse_odbc.cpp, connection.cpp, ...
|  |  (ANSI + Unicode dual)        |  |
|  +-------------------------------+  |
|  |  Handle Management            |  |  handle.h, handle.cpp
|  |  (Env/Dbc/Stmt)               |  |
|  +-------------------------------+  |
|  |  Type Mapping                 |  |  type_mapping.h, type_mapping.cpp
|  +-------------------------------+  |
|  |  ClickHouse HTTP Client       |  |  clickhouse_client.h, clickhouse_client.cpp
|  +-------------------------------+  |
+-------------------------------------+
|  cpp-httplib (HTTP) | nlohmann/json |  Third-party (header-only)
+-------------------------------------+
|        ClickHouse Server            |  HTTP port 8123
+-------------------------------------+
```

## Directory Structure

```
clickhouse-odbc-alternative/
├── clickhouse_odbc.sln          # Visual Studio solution
├── clickhouse_odbc.vcxproj      # Visual Studio project (x64, C++17)
├── CMakeLists.txt               # CMake build (alternative)
├── README.md                    # Project overview
├── docs/                        # Documentation
│   ├── ARCHITECTURE.md          # This file
│   ├── ROADMAP.md               # Development roadmap
│   ├── CONTRIBUTING.md          # Contributing guide
│   └── MSACCESS_COMPATIBILITY.md # MS Access compatibility guide (debug findings)
├── setup/                       # Installation and registration
│   ├── register_driver.bat      # ODBC driver registration script
│   └── setup.inf                # Driver information
├── src/                         # Source code
│   ├── include/                 # Header files
│   │   ├── handle.h             # ODBC handle structures
│   │   ├── clickhouse_client.h  # HTTP client class
│   │   ├── type_mapping.h       # Type conversion utilities
│   │   └── util.h               # String conversion and parsing
│   ├── dllmain.cpp              # DLL entry point
│   ├── clickhouse_odbc.cpp      # Diagnostic functions (SQLGetDiagRec/Field)
│   ├── handle.cpp               # Handle operations (ResultSet)
│   ├── connection.cpp           # Connection management (SQLConnect, SQLDriverConnect, ...)
│   ├── statement.cpp            # Statement (SQLExecDirect, SQLPrepare, ...)
│   ├── result.cpp               # Result retrieval (SQLFetch, SQLGetData, ...)
│   ├── catalog.cpp              # Catalog functions (SQLTables, SQLColumns, ...)
│   ├── info.cpp                 # Driver information (SQLGetInfo, ...)
│   ├── clickhouse_client.cpp    # HTTP client implementation
│   ├── type_mapping.cpp         # ClickHouse <-> ODBC type mapping
│   ├── util.cpp                 # Utility function implementation
│   ├── clickhouse_odbc.def      # DLL export definitions
│   └── CMakeLists.txt           # CMake configuration for src
└── third_party/                 # Third-party libraries
    ├── cpp-httplib/httplib.h     # HTTP client (v0.18.3)
    └── nlohmann/nlohmann/json.hpp # JSON parser (v3.11.3)
```

## Key Components

### 1. ODBC Handle Management (`handle.h`)

Four handle types defined as structs:
- **OdbcEnvironment** — Environment handle (`SQL_HANDLE_ENV`)
- **OdbcConnection** — Connection handle (`SQL_HANDLE_DBC`), holds connection parameters
- **OdbcStatement** — Statement handle (`SQL_HANDLE_STMT`), holds query, result set, and binding info
- **OdbcDescriptor** — Descriptor handle (`SQL_HANDLE_DESC`)

All handles inherit from the `OdbcHandle` base class, sharing diagnostic record (`DiagRecord`) management.

### 2. ClickHouse HTTP Client (`clickhouse_client.h/cpp`)

- Connects to the ClickHouse HTTP interface using cpp-httplib
- Sends queries via POST requests (authentication via query parameters)
- Appends `FORMAT JSONCompact` to SELECT queries and parses JSON responses
- Stores column metadata and row data in the `ResultSet` struct

### 3. Type Mapping (`type_mapping.h/cpp`)

- Converts ClickHouse type names to ODBC `SQL_xxx` types
- Calculates display size, octet length, and default C type
- Builds type information result sets for `SQLGetTypeInfo`

### 4. ODBC API Implementation

Each ODBC function exports both ANSI (`SQLCHAR*`) and Unicode (`SQLWCHAR*`) variants:
- **connection.cpp** — `SQLConnect`, `SQLDriverConnect`, `SQLGetConnectAttr`, `SQLSetConnectAttr`, ...
- **statement.cpp** — `SQLExecDirect`, `SQLPrepare`, `SQLExecute`, `SQLColAttribute`, ...
- **result.cpp** — `SQLFetch`, `SQLFetchScroll`, `SQLGetData`, `SQLCloseCursor`, ...
- **catalog.cpp** — `SQLTables`, `SQLColumns`, `SQLPrimaryKeys`, `SQLGetTypeInfo`, ...
- **info.cpp** — `SQLGetInfo`, `SQLGetDiagField`, ...
- **clickhouse_odbc.cpp** — `SQLGetDiagRec`, `SQLGetDiagField`

### 5. Unicode Macro Handling

The project is built with `CharacterSet=Unicode`, causing ODBC header UNICODE macros (`SQLTables` -> `SQLTablesW`, etc.) to create conflicts. Each source file uses `#undef` to explicitly export both ANSI and Wide function names.

### 6. Parameter Binding and Query Processing (`util.h/cpp`)

- **Parameter substitution** (`SubstituteParameters`): Replaces `?` placeholders with bound parameter values
- **Parameter value extraction** (`ExtractParameterValue`): Generates SQL string representation from `BoundParameter` structs
  - `SQL_C_DEFAULT` type resolution (`ResolveCDefaultType`): Infers default C type from SQL type
  - Binary numeric to string conversion (IEEE 754 double -> `"9.5"`, etc.)
  - UTF-16 to UTF-8 conversion (`SQL_C_WCHAR` parameters)
- **ODBC escape sequence processing** (`ProcessOdbcEscapeSequences`): Translates `{fn UCASE(x)}` -> `upper(x)`, etc. (60+ functions supported)
- **String conversion**: `WideToUtf8` / `Utf8ToWide` for UTF-16 <-> UTF-8 interconversion

### 7. Trace Logging (`trace.h/cpp`)

- `TraceLog` singleton class (5 levels: Off/Error/Warning/Info/Debug/Verbose)
- Auto-enabled at DLL load via the `CLICKHOUSE_ODBC_TRACE` environment variable
- Conditional log output via `TRACE_LOG` / `TRACE_ENTRY` / `TRACE_EXIT` macros
- Supports both `OutputDebugString` and file output

### 8. Search Pattern Processing (`catalog.cpp`)

- ODBC search pattern escape analysis (`HasUnescapedWildcards`, `StripSearchPatternEscape`)
- Generates `LIKE` / `=` queries based on wildcard presence (`BuildPatternCondition`)
- Correct handling of `M\_ITEM` (escaped underscores) sent by MS Access

> **Details**: See [`docs/MSACCESS_COMPATIBILITY.md`](MSACCESS_COMPATIBILITY.md) for MS Access-specific behavior

## Build System

| Method | Files | Notes |
|--------|-------|-------|
| Visual Studio | `clickhouse_odbc.sln` / `.vcxproj` | Recommended. x64, C++17, PlatformToolset v145 |
| CMake | `CMakeLists.txt` | FetchContent for third-party retrieval |

## Communication Protocol

Uses the ClickHouse **HTTP interface** (default port 8123):
- Authentication: URL parameters (`?user=...&password=...&database=...`)
- Query submission: POST body (`Content-Type: text/plain`)
- Response: JSON via `FORMAT JSONCompact`
- Native protocol (port 9000) is not used
