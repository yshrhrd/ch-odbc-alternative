# Contributing Guide

## Prerequisites

- **Visual Studio 2022 or later** (Desktop development with C++ workload)
- **Windows SDK 10.0**
- **ClickHouse server** (for integration testing; Docker recommended)

## Build Instructions

### Visual Studio (Recommended)

1. Open `clickhouse_odbc.sln` in Visual Studio
2. Set configuration to `Debug|x64` or `Release|x64`
3. **Build** > **Build Solution** (Ctrl+Shift+B)

Output: `out\<Configuration>\ch-odbc-alternative.dll`

### CMake (Alternative)

```bash
mkdir build && cd build
cmake .. -G "Visual Studio 17 2022" -A x64
cmake --build . --config Release
```

## Project Configuration

| Setting | Value |
|---------|-------|
| C++ Standard | C++17 (`/std:c++17`) |
| Character Set | Unicode |
| Platform | x64 |
| Output Type | DLL (DynamicLibrary) |
| Preprocessor | `_CRT_SECURE_NO_WARNINGS`, `CLICKHOUSE_ODBC_EXPORTS` |

## Driver Registration (for Testing)

Register the driver after building:

```bat
REM Run as Administrator
setup\register_driver.bat
```

Registry key: `HKLM\SOFTWARE\ODBC\ODBCINST.INI\CH ODBC Alternative`

## ClickHouse Server for Testing

```bash
docker run -d --name clickhouse \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server
```

## Coding Conventions

### General

- Use C++17
- Indentation: 4 spaces
- Namespace: `clickhouse_odbc`
- Header guard: `#pragma once`

### ODBC API Function Implementation Pattern

```cpp
// ANSI version
extern "C" SQLRETURN SQL_API SQLXxx(SQLHSTMT StatementHandle, SQLCHAR *Param, ...) {
    // Implementation
}

// Unicode version
extern "C" SQLRETURN SQL_API SQLXxxW(SQLHSTMT StatementHandle, SQLWCHAR *Param, ...) {
    // Convert SQLWCHAR to UTF-8 and call ANSI implementation
}
```

### UNICODE Macro Handling

This project is built with `CharacterSet=Unicode`, causing ODBC header UNICODE macros to rewrite ANSI function names to their W variants. In `.cpp` files that define both ANSI and Wide functions, `#undef` the relevant macros after the includes:

```cpp
#include <sql.h>
#include <sqlext.h>

#ifdef UNICODE
#undef SQLXxx    // Functions defined in this file
#undef SQLYyy
#endif
```

### Windows Header Include Order

Include `windows.h` before ODBC headers and define `WIN32_LEAN_AND_MEAN` to prevent winsock conflicts:

```cpp
#pragma once
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>

#include <sql.h>
#include <sqlext.h>
```

This is handled centrally in `handle.h`.

### Error Handling

- Return appropriate `SQLRETURN` values (`SQL_SUCCESS`, `SQL_ERROR`, `SQL_NO_DATA`, etc.)
- Set diagnostic records via `OdbcHandle::SetError()` on errors
- SQLSTATE codes follow the ODBC specification

### Adding New ODBC Functions

1. Add the export name to `src/clickhouse_odbc.def`
2. Implement both ANSI and Unicode versions in the appropriate `.cpp` file
3. Add `#undef` macros as needed
4. Build and verify

## Third-Party Libraries

| Library | Version | Purpose | Management |
|---------|---------|---------|------------|
| cpp-httplib | v0.18.3 | HTTP client | `third_party/` (VS) / FetchContent (CMake) |
| nlohmann/json | v3.11.3 | JSON parser | `third_party/` (VS) / FetchContent (CMake) |

- OpenSSL is **not required** (HTTP plaintext communication)
- Do not define `CPPHTTPLIB_OPENSSL_SUPPORT` (it activates via `#ifdef`, so even a value of 0 enables it)

## Troubleshooting

### MSB8020: Build tools not found
-> Change `<PlatformToolset>` in `clickhouse_odbc.vcxproj` to match your environment

### Type errors in sqltypes.h
-> Verify that `<windows.h>` is included before ODBC headers

### winsock2.h redefinition error
-> Verify that `WIN32_LEAN_AND_MEAN` is defined

### openssl/err.h not found
-> Verify that `CPPHTTPLIB_OPENSSL_SUPPORT` is not defined (check both preprocessor definitions and source code)
