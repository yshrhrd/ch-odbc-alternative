# MS Access Compatibility Guide — ODBC Driver Development Findings

This document summarizes issues discovered and solutions implemented when using the ClickHouse ODBC driver from MS Access.
It records MS Access-specific behavior patterns relevant to ODBC driver development.

---

## Table of Contents

1. [MS Access ODBC Call Patterns](#1-ms-access-odbc-call-patterns)
2. [Catalog Name Issue](#2-catalog-name-issue)
3. [Search Pattern Escape Issue](#3-search-pattern-escape-issue)
4. [Parameter Binding Issue (SQL_C_DEFAULT)](#4-parameter-binding-issue-sql_c_default)
5. [Wide String (UTF-16) and UTF-8 Conversion](#5-wide-string-utf-16-and-utf-8-conversion)
6. [ODBC Escape Sequences](#6-odbc-escape-sequences)
7. [Metadata Retrieval Pattern (SQLPrepare -> SQLNumResultCols)](#7-metadata-retrieval-pattern-sqlprepare---sqlnumresultcols)
8. [DSN Registry Reading](#8-dsn-registry-reading)
9. [System Table Classification](#9-system-table-classification)
10. [Debugging with Traces](#10-debugging-with-traces)
11. [ODBC 2.x Compatibility](#11-odbc-2x-compatibility)
12. [Large Table Navigation (2GB Limit)](#12-large-table-navigation-2gb-limit)
13. [Known Limitations and Workarounds](#13-known-limitations-and-workarounds)

---

## 1. MS Access ODBC Call Patterns

### Linked Table Creation Sequence

ODBC API call sequence when MS Access creates a linked table:

```
1. SQLAllocHandle(SQL_HANDLE_ENV)
2. SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3)
3. SQLAllocHandle(SQL_HANDLE_DBC)
4. SQLDriverConnectW(...)          <- Connect
5. SQLGetInfo(many info types)     <- Check driver capabilities
6. SQLGetFunctions(...)             <- Check supported functions
7. SQLTablesW(...)                  <- Get table list
8. SQLColumnsW(...)                 <- Get column info
9. SQLStatisticsW(...)              <- Index info
10. SQLSpecialColumnsW(...)         <- Row identifiers
11. SQLPrimaryKeysW(...)            <- Primary keys
12. SQLForeignKeysW(...)            <- Foreign keys
```

### Data Reading Sequence

```
1. SQLPrepareW("SELECT * FROM table WHERE ...")
2. SQLNumResultCols(...)            <- Pre-check column count
3. SQLDescribeColW(...)             <- Metadata for each column
4. SQLBindCol(...)                  <- Bind result buffers
5. SQLExecute(...)
6. SQLFetch(...) / SQLExtendedFetch(...)  <- Fetch data
```

### Key Characteristics

- **Always uses W (Wide) versions**: Access is a Unicode application and only calls Wide variants such as `SQLTablesW`, `SQLColumnsW`, `SQLDriverConnectW`
- **Also uses ODBC 2.x functions**: May preferentially use legacy functions like `SQLExtendedFetch`, `SQLColAttributes`, `SQLError`
- **Calls SQLGetInfo extensively**: Queries 50+ info types immediately after connection to check driver capabilities in detail

---

## 2. Catalog Name Issue

### Symptom

When Access executes queries, it generates SQL with catalog-qualified table names:

```sql
SELECT `default`.`M_ITEM`.`id` FROM `default`.`M_ITEM`
```

ClickHouse does not accept such catalog qualification, resulting in an error.

### Cause

When `SQLGetInfo(SQL_CATALOG_USAGE)` returns non-zero, Access prepends the catalog name to table names.

### Solution

Set the following `SQLGetInfo` values to disable catalog usage:

| Info Type | Value | Description |
|---|---|---|
| `SQL_CATALOG_USAGE` | `0` | Do not use catalog names |
| `SQL_CATALOG_NAME` | `"N"` | No catalog name support |
| `SQL_CATALOG_NAME_SEPARATOR` | `""` | No separator |
| `SQL_CATALOG_TERM` | `""` | No catalog term |
| `SQL_CATALOG_LOCATION` | `0` | No catalog location |

**File**: `src/info.cpp`

---

## 3. Search Pattern Escape Issue

### Symptom

When linking table `M_ITEM` in Access, the error "Object 'M_ITEM' not found" occurs.

### Root Cause (ODBC Trace)

The ODBC trace shows the following sequence:

```
SQLGetInfoW(SQL_SEARCH_PATTERN_ESCAPE) -> "\"
SQLColumnsW(TableName="M\_ITEM")
```

Access uses the escape character (`\`) from `SQLGetInfo(SQL_SEARCH_PATTERN_ESCAPE)` to escape underscores `_` in table names, sending `M\_ITEM`. Since ODBC's `_` is a wildcard (any single character), Access escapes literal `_`.

If the driver sends `M\_ITEM` directly to ClickHouse, the table is not found.

### Solution

When processing pattern arguments in catalog functions (`SQLTables`, `SQLColumns`):

1. **`HasUnescapedWildcards(pattern)`**: Determine if unescaped wildcards (`%`, `_`) exist
2. **`StripSearchPatternEscape(pattern)`**: Remove escape sequences (`\_` -> `_`, `\%` -> `%`)
3. **`BuildPatternCondition(column, pattern)`**: Use `LIKE` if wildcards present, otherwise exact match (`=`)

```cpp
// Pattern processing logic
static std::string BuildPatternCondition(const std::string &column, const std::string &pattern) {
    if (pattern.empty() || pattern == "%") return "";  // No filter
    if (HasUnescapedWildcards(pattern)) {
        return " AND " + column + " LIKE '" + pattern + "'";  // LIKE query
    }
    std::string literal = StripSearchPatternEscape(pattern);
    return " AND " + column + " = '" + literal + "'";  // Exact match
}
```

**File**: `src/catalog.cpp`

### Typical Pattern Conversion Examples

| Access Sends | Determination | Generated SQL |
|---|---|---|
| `M\_ITEM` | Escaped `_` -> exact match | `name = 'M_ITEM'` |
| `M%` | Wildcard `%` | `name LIKE 'M%'` |
| `SALES` | No wildcards | `name = 'SALES'` |
| `%` | Match all | (no filter) |
| `100\%` | Escaped `%` -> exact match | `name = '100%'` |

---

## 4. Parameter Binding Issue (SQL_C_DEFAULT)

### Symptom

When executing parameterized queries from Access, ClickHouse returns:

```
Cannot convert string '\0\0\0\0?S"A' to Float64 (TYPE_MISMATCH)
```

### Root Cause (ODBC Trace)

```
SQLBindParameter:
  ParameterNumber=1
  ValueType=SQL_C_DEFAULT (99)
  ParameterType=SQL_DOUBLE (8)
  ColumnSize=15
  BufferLength=8
  StrLen_or_IndPtr=0x... -> 8

SQLExecute -> SQL_ERROR
```

Access uses `SQL_C_DEFAULT` (value 99) as `ValueType`, meaning "use the default C type for this SQL type." If the driver doesn't recognize this, it falls through to the `default:` case and sends 8 bytes of IEEE 754 binary data as a string.

### Solution

1. Add **`ResolveCDefaultType(sql_type)`** helper function
2. Resolve `SQL_C_DEFAULT` to the actual C type at the start of `ExtractParameterValue()`

```cpp
SQLSMALLINT ResolveCDefaultType(SQLSMALLINT sql_type) {
    switch (sql_type) {
    case SQL_DOUBLE:        return SQL_C_DOUBLE;
    case SQL_FLOAT:         return SQL_C_DOUBLE;  // ODBC SQL_FLOAT is double precision
    case SQL_REAL:          return SQL_C_FLOAT;
    case SQL_INTEGER:       return SQL_C_SLONG;
    case SQL_SMALLINT:      return SQL_C_SSHORT;
    case SQL_BIGINT:        return SQL_C_SBIGINT;
    case SQL_VARCHAR:       return SQL_C_CHAR;
    case SQL_WVARCHAR:      return SQL_C_WCHAR;
    case SQL_TYPE_DATE:     return SQL_C_TYPE_DATE;
    case SQL_TYPE_TIMESTAMP:return SQL_C_TYPE_TIMESTAMP;
    // ... other types
    default:                return SQL_C_CHAR;
    }
}
```

### Important Notes

- **`SQL_FLOAT` vs `SQL_REAL`**: In the ODBC spec, `SQL_FLOAT` is double precision (8-byte `double`), while `SQL_REAL` is single precision (4-byte `float`). This is opposite to C language `float`/`double` naming.
- **Reading binary data**: Read native values with `*(double*)param.parameter_value` and convert to string with `std::to_string()`.

**File**: `src/util.cpp`, `src/include/util.h`

---

## 5. Wide String (UTF-16) and UTF-8 Conversion

### Overview

MS Access is a Unicode application that sends and receives all string data as UTF-16 (Wide). The ClickHouse HTTP interface uses UTF-8. The driver must correctly convert between both.

### Where Conversion is Needed

| Direction | Context | Function |
|---|---|---|
| UTF-16 -> UTF-8 | Parameter values (`SQL_C_WCHAR`) | `ExtractParameterValue()` |
| UTF-16 -> UTF-8 | Connection string (`SQLDriverConnectW`) | `DriverConnectImpl()` |
| UTF-16 -> UTF-8 | Query string (`SQLPrepareW`, `SQLExecDirectW`) | Wide version of each function |
| UTF-8 -> UTF-16 | Result set data (`SQLGetData` with `SQL_C_WCHAR`) | `SQLGetData()` |
| UTF-8 -> UTF-16 | Diagnostic messages (`SQLGetDiagRecW`) | `SQLGetDiagRecW()` |
| UTF-8 -> UTF-16 | Column names (`SQLDescribeColW`) | `SQLDescribeColW()` |
| UTF-8 -> UTF-16 | Diagnostic fields (`SQLGetDiagFieldW`) | `SQLGetDiagFieldW()` |

### Conversion Functions

```cpp
// UTF-16 -> UTF-8
std::string WideToUtf8(const SQLWCHAR *wide, SQLSMALLINT len);

// UTF-8 -> UTF-16
std::wstring Utf8ToWide(const std::string &utf8);
```

Internally uses Windows API `WideCharToMultiByte` / `MultiByteToWideChar` (code page `CP_UTF8`).

### Wide Version Function Pattern

```cpp
// Wide version converts UTF-16 to UTF-8 and calls ANSI implementation
extern "C" SQLRETURN SQL_API SQLXxxW(SQLWCHAR *wide_param, ...) {
    std::string utf8_param = WideToUtf8(wide_param, len);
    // Call ANSI logic
    return SQLXxx((SQLCHAR *)utf8_param.c_str(), ...);
}
```

### TextLength (Character Count vs Byte Count) Caveat

ODBC spec requires Wide version functions to return `TextLength` as **character count** (number of SQLWCHARs):

- Wrong: `textLen = wide_msg.size() * sizeof(SQLWCHAR)` (byte count)
- Correct: `textLen = wide_msg.size()` (character count)

This applies to `SQLGetDiagRecW`, `SQLErrorW`, `SQLGetDiagFieldW`, etc.

---

## 6. ODBC Escape Sequences

### Overview

Access includes ODBC escape sequences in SQL queries:

```sql
SELECT {fn UCASE(name)}, {fn YEAR(date_col)} FROM t WHERE d > {d '2024-01-01'}
```

The driver must convert these to ClickHouse native SQL:

```sql
SELECT upper(name), toYear(date_col) FROM t WHERE d > '2024-01-01'
```

### Supported Escape Types

| Escape | Example | Converted Result |
|---|---|---|
| Date literal | `{d '2024-01-15'}` | `'2024-01-15'` |
| Time literal | `{t '13:45:00'}` | `'13:45:00'` |
| Timestamp | `{ts '2024-01-15 13:45:00'}` | `'2024-01-15 13:45:00'` |
| Scalar function | `{fn UCASE(x)}` | `upper(x)` |
| Outer join | `{oj t1 LEFT OUTER JOIN t2 ON ...}` | `t1 LEFT OUTER JOIN t2 ON ...` |
| LIKE escape | `{escape '\'}` | `ESCAPE '\'` |
| CONVERT | `{fn CONVERT(col, SQL_INTEGER)}` | `toInt32(col)` |

### LOCATE Function Argument Order

ODBC and ClickHouse have different argument orders:

- **ODBC**: `{fn LOCATE(needle, haystack[, start])}`
- **ClickHouse**: `positionUTF8(haystack, needle[, start])`

The driver swaps the first and second arguments.

### Processing Timing

Escape sequence processing is executed at three points:

1. `ExecDirectImpl()` — Before query execution
2. `SQLPrepare()`/`SQLPrepareW()` — At prepare time
3. `SQLNativeSql()`/`SQLNativeSqlW()` — Native SQL translation

**File**: `src/util.cpp` (`ProcessOdbcEscapeSequences()`)

---

## 7. Metadata Retrieval Pattern (SQLPrepare -> SQLNumResultCols)

### Symptom

Access uses the following pattern when creating linked tables:

```
SQLPrepareW("SELECT * FROM table")
SQLNumResultCols(...)           <- Column count needed here
SQLDescribeColW(1, ...)         <- Column info needed here
```

`SQLPrepare` normally only "prepares" the query without executing it. However, Access immediately requests metadata via `SQLNumResultCols`.

### Solution: FetchPreparedMetadata

When `SQLNumResultCols` / `SQLDescribeCol` is called and metadata is empty, execute a `LIMIT 0` query to fetch column information only:

```cpp
static void FetchPreparedMetadata(OdbcStatement *stmt) {
    if (!stmt->result_set.columns.empty()) return;  // Already fetched
    if (stmt->query.empty()) return;

    auto *conn = static_cast<OdbcConnection *>(stmt->parent_connection);
    auto client = GetClient(conn);
    if (!client) return;

    std::string meta_query = stmt->query + " LIMIT 0";
    ResultSet rs;
    std::string error;
    if (client->ExecuteQuery(meta_query, rs, error)) {
        stmt->result_set.columns = rs.columns;
    }
}
```

**File**: `src/statement.cpp`

---

## 8. DSN Registry Reading

### Symptom

When connecting via DSN, the hostname remains `localhost`.

### Cause

`DriverConnectImpl` was not reading connection parameters from the registry for the DSN name.

### Solution

Use `ReadDsnSetting()` helper with `SQLGetPrivateProfileString` to read from the registry:

```cpp
static std::string ReadDsnSetting(const std::string &dsn, const std::string &key,
                                   const std::string &default_value) {
    char buf[256] = {};
    SQLGetPrivateProfileString(dsn.c_str(), key.c_str(), default_value.c_str(),
                                buf, sizeof(buf), "ODBC.INI");
    return std::string(buf);
}
```

### Connection Parameter Priority

1. Explicit keys in the connection string (`HOST=...`, `PORT=...`) take highest priority
2. If a DSN is specified, fill unspecified parameters from the registry
3. Default values (host=localhost, port=8123, user=default)

**File**: `src/connection.cpp`

---

## 9. System Table Classification

### Problem

When `SQLTables` receives a `TABLE_TYPE` filter of `TABLE` only, ClickHouse system tables (`system.*`) are included in the list.

### Solution

Apply the following rules in `TablesImpl()`:

- Tables in the `system` database -> `SYSTEM TABLE`
- Tables in the `information_schema` database -> `SYSTEM TABLE`
- All others -> `TABLE`

When the filter is `TABLE`, system tables are excluded.

**File**: `src/catalog.cpp`

---

## 10. Debugging with Traces

### ODBC Driver Manager Trace

The most important debugging tool. Records all ODBC API calls between Access and the driver.

**How to enable**:
1. Open `odbcad32.exe` (ODBC Data Source Administrator)
2. "Tracing" tab -> "Start Tracing Now"
3. Create/open linked tables in Access
4. Stop tracing and examine the file

### Built-in Driver Trace

Enable driver-internal tracing via the `CLICKHOUSE_ODBC_TRACE` environment variable:

```powershell
# Enable
$env:CLICKHOUSE_ODBC_TRACE = "1"

# File output (OutputDebugString + file)
$env:CLICKHOUSE_ODBC_TRACE = "C:\temp\clickhouse_odbc.log"

# Disable
Remove-Item Env:CLICKHOUSE_ODBC_TRACE
```

Traced content:
- HTTP requests/responses (`ExecuteQuery`)
- Catalog function call parameters (`TablesImpl`, `ColumnsImpl`)
- SQLPrepare query strings
- Parameter binding type resolution (`SQL_C_DEFAULT` -> actual C type)

**File**: `src/dllmain.cpp` (initialization), `src/include/trace.h` (TraceLog class)

### Debugging Best Practices

1. First use ODBC Driver Manager trace to get the overall picture
2. Identify the problematic API call
3. Use built-in driver trace for details as needed
4. Write a test case to reproduce the issue
5. Fix and verify all tests pass

---

## 11. ODBC 2.x Compatibility

### ODBC 2.x Functions Used by Access

Access calls some ODBC 2.x functions even with ODBC 3.x-compliant drivers:

| ODBC 2.x Function | Corresponding 3.x Function | Notes |
|---|---|---|
| `SQLExtendedFetch` | `SQLFetchScroll` | Preferentially used by Access |
| `SQLColAttributes` | `SQLColAttribute` | Field ID mapping required |
| `SQLError` | `SQLGetDiagRec` | Iterative record consumption |
| `SQLGetConnectOption` | `SQLGetConnectAttr` | Implemented as wrapper |
| `SQLSetConnectOption` | `SQLSetConnectAttr` | Implemented as wrapper |
| `SQLGetStmtOption` | `SQLGetStmtAttr` | Implemented as wrapper |
| `SQLSetStmtOption` | `SQLSetStmtAttr` | Implemented as wrapper |
| `SQLTransact` | `SQLEndTran` | Implemented as wrapper |

### SQLGetFunctions Completeness

Access checks individual function support via `SQLGetFunctions`. The following must all be supported:

- `SQL_API_ODBC3_ALL_FUNCTIONS` (bitmap — 4000-element array)
- `SQL_API_ALL_FUNCTIONS` (ODBC 2.x — 100-element array)
- Individual function ID checks (switch-case)

---

## 12. Large Table Navigation (2GB Limit)

### Problem

When navigating to the last row of a large table in MS Access datasheet view (Ctrl+End or navigation bar),
Access shows the following error:

> クエリを完了できません。クエリ結果のサイズがデータベースの最大サイズ（2 GB）より大きいか、クエリ結果を一時的に保存するディスクの空き容量が不足しています。

("Cannot complete the query. The query result size exceeds the maximum database size (2 GB), or there is not enough disk space to temporarily store the query results.")

### Root Cause

MS Access caches all fetched rows in a temporary MDB/ACCDB database file. When a user navigates to the last row,
Access attempts to fetch **all rows** from the first to the last to populate its local cache.
The temporary database has a 2GB size limit, so tables with large total data volume exceed this limit.

The driver's lazy paging feature reduces memory usage on the driver side by fetching pages on demand,
but Access still receives and caches all rows locally.

### Solution: `MaxLazyRows` Parameter

The `MaxLazyRows` connection parameter caps the number of rows the driver exposes in lazy paging mode.
This prevents Access from attempting to cache more data than its 2GB limit allows.

| Parameter | Description | Default |
|---|---|---|
| `MaxLazyRows` | Maximum rows exposed in lazy paging mode | 0 (unlimited) |

By default, the driver exposes all rows (no cap). If the 2GB error occurs, set `MaxLazyRows` to an
appropriate value based on the table's average row size.

**Configuration examples:**

```
REM Connection string (set when 2GB error occurs)
Driver={CH ODBC Alternative};Host=localhost;Port=8123;Database=default;UID=default;MaxLazyRows=500000;

REM DSN registry
reg add "HKCU\SOFTWARE\ODBC\ODBC.INI\MyClickHouse" /v "MaxLazyRows" /t REG_SZ /d "500000" /f
```

**Behavior:**

- `MaxLazyRows=0` (default): No cap — all rows are accessible. The 2GB error may occur on very large tables
- `MaxLazyRows=N`: The driver reports at most N rows to Access, preventing the 2GB overflow
- For tables with wide rows (many columns or large text fields), a smaller value (e.g., 100000) is recommended

### Performance Optimizations

The driver includes two optimizations to reduce the time required for large table navigation:

1. **SQL_RD_OFF fast-path**: When Access positions the cursor without reading data (`SQL_ATTR_RETRIEVE_DATA=SQL_RD_OFF`),
   the driver skips HTTP page fetches entirely in lazy mode. This makes cursor scanning near-instant.

2. **Batch prefetch**: The driver fetches 5 pages per HTTP request (50,000 rows at default `PageSize=10000`),
   reducing HTTP round-trips by 80%.

### Choosing an Appropriate MaxLazyRows Value

When the 2GB error occurs, set `MaxLazyRows` based on the average row size:

| Average Row Size | Recommended `MaxLazyRows` | Estimated Cache Size |
|---|---|---|
| ~200 bytes | 500000 | ~100 MB |
| ~1 KB | 500000 | ~500 MB |
| ~2 KB | 500000 | ~1 GB |
| ~4 KB | 250000 | ~1 GB |
| ~10 KB | 100000 | ~1 GB |

> **Note:** If `DefaultMaxRows` is set (non-zero), lazy paging is not used and `MaxLazyRows` has no effect.
> In that case, `DefaultMaxRows` directly limits the total row count via a server-side `LIMIT` clause.

---

## 13. Known Limitations and Workarounds

### ClickHouse-Specific Limitations

| Limitation | Impact | Workaround |
|---|---|---|
| No transaction support | `SQLEndTran` is a no-op | Always return `SQL_SUCCESS` |
| UPDATE/DELETE restrictions | Depends on table engine | MergeTree supports `ALTER TABLE ... UPDATE/DELETE` only |
| No connection dialog | `SQL_DRIVER_PROMPT` not supported | Connect with `SQL_DRIVER_NOPROMPT` |
| HTTP only | Native protocol not used | Use HTTP port 8123 |

### Access-Specific Notes

| Note | Description |
|---|---|
| Catalog name must be disabled | Set `SQL_CATALOG_USAGE=0` or Access generates catalog-qualified SQL |
| `_` escape handling is required | Access escapes `_` in table names using `SQL_SEARCH_PATTERN_ESCAPE` |
| `SQL_C_DEFAULT` resolution is required | Access uses `SQL_C_DEFAULT` for parameter binding |
| Only Wide API versions are used | ANSI versions are not called by Access, but Wide versions internally calling ANSI versions is an effective pattern |
| Immediate metadata request after SQLPrepare | Lazy metadata fetch with `LIMIT 0` is required |
| URL encoding | Safe encoding needed when passwords contain special characters (`&`, `=`, `%`) |
| Large table 2GB limit | Access caches all rows locally; set `MaxLazyRows` if the 2GB error occurs |

---

## Testing

### Test File Reference

| Test File | Test Count | Content |
|---|---|---|
| `test_phase15.cpp` | 12 | Search pattern escape, PreparedStatementMetadata |
| `test_phase16.cpp` | 41 | SQL_C_DEFAULT resolution, parameter type conversion, UTF-16 conversion |
| `test_access_workflow.cpp` | 46 | MS Access workflow simulation |

### Running Tests

```powershell
# Debug build & test
msbuild clickhouse_odbc.sln /p:Configuration=Debug /p:Platform=x64 /m
.\build\Debug\test\ch-odbc-alternative-test.exe

# Release build & test
msbuild clickhouse_odbc.sln /p:Configuration=Release /p:Platform=x64 /m
.\build\Release\test\ch-odbc-alternative-test.exe
```

---

## Reference: Reading ODBC Trace Logs

### Typical Trace Log Format

```
<application_name>  <thread_id>-<sequence>  <direction>  <function_name>
    <parameter_name>=<value>

<application_name>  <thread_id>-<sequence>  <direction>  SQLRetcode=<return_code>
```

### Key Points for Issue Identification

1. **Search for `SQL_ERROR`**: Identify the API call where the error occurred
2. **Check `SQLGetDiagRecW` immediately after**: Verify error message and SQLSTATE
3. **Check `SQLGetInfoW` return values**: Verify capability information returned by the driver
4. **Check `SQLColumnsW` / `SQLTablesW` parameters**: Verify table names/patterns sent by Access
5. **Check `SQLBindParameter` parameters**: Verify ValueType, ParameterType, BufferLength
