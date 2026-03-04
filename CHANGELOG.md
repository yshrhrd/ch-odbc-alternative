# Changelog

All notable changes to this project are documented in this file.

## [1.1.1] - 2026-03-03

### Fixed
- **MS Access 2GB limit on large table navigation** (`src/statement.cpp`, `src/include/handle.h`):
  - When navigating to the last row (Ctrl+End) of a large table in Access, all rows were fetched and cached in Access's temporary database, exceeding the 2GB limit
  - Added `MaxLazyRows` connection parameter (default: 0 = unlimited) to optionally cap the number of rows exposed in lazy paging mode
  - Users who encounter the 2GB error can set `MaxLazyRows` (e.g., `500000`) as a workaround
- **Large table navigation performance** (`src/result.cpp`, `src/handle.cpp`):
  - SQL_RD_OFF fast-path: skip HTTP page fetches when Access is only positioning the cursor (e.g., Ctrl+End background scan)
  - Batch prefetch: fetch 5 pages per HTTP request, reducing round-trips by 80%

### Added
- **`MaxLazyRows` connection/DSN parameter** (`src/connection.cpp`, `src/config_dsn.cpp`):
  - Configurable via connection string (`MaxLazyRows=500000`) or DSN registry
  - Default `0` (unlimited) — all rows accessible; set only when 2GB error occurs
  - Persisted through DSN configuration dialog save/load

### Documentation
- `README.md`: Added `MaxLazyRows` to connection string parameter table, updated Known Limitations
- `docs/MSACCESS_COMPATIBILITY.md`: Added section 12 "Large Table Navigation (2GB Limit)" with root cause analysis, configuration examples, and sizing guidelines

## [1.1.0] - 2026-03-01

### Fixes Based on MS Access Real-Environment Testing

Fixed issues discovered during actual table linking and query execution tests from MS Access.
Root causes identified through ODBC trace log analysis.

#### Fixed
- **`SQL_CATALOG_USAGE` disabled** (`src/info.cpp`):
  - Addressed issue where Access generates catalog-qualified SQL (`default.M_ITEM.id`)
  - Set `SQL_CATALOG_USAGE=0`, `SQL_CATALOG_NAME="N"`, `SQL_CATALOG_NAME_SEPARATOR=""`, `SQL_CATALOG_TERM=""`, `SQL_CATALOG_LOCATION=0`
- **ODBC search pattern escape handling** (`src/catalog.cpp`):
  - Addressed issue where Access sends `M\_ITEM` (escaped underscore)
  - Added `HasUnescapedWildcards()`, `StripSearchPatternEscape()`, `BuildPatternCondition()` helpers
  - Correctly process pattern arguments in `TablesImpl`, `ColumnsImpl`
- **`SQL_C_DEFAULT` parameter type resolution** (`src/util.cpp`):
  - Addressed issue where Access binds binary parameters with `SQL_C_DEFAULT` (99) + `SQL_DOUBLE` (8)
  - `ResolveCDefaultType()` helper resolves default C type from SQL type
  - `ExtractParameterValue()` converts `SQL_C_DEFAULT` to actual C type before extracting value
  - Correct mapping of `SQL_FLOAT` → `SQL_C_DOUBLE` (ODBC spec: SQL_FLOAT is double precision)

#### Added
- **Trace log infrastructure** (`src/dllmain.cpp`, `src/include/trace.h`, `src/trace.cpp`):
  - Automatic trace activation via `CLICKHOUSE_ODBC_TRACE` environment variable
  - Tracing for HTTP request/response, catalog functions, SQLPrepare, parameter resolution
- **MS Access compatibility guide** (`docs/MSACCESS_COMPATIBILITY.md`):
  - Comprehensive 12-section document
  - Access ODBC call patterns, debugging techniques, known limitations

#### Tests
- `test/test_phase15.cpp`: 12 search pattern escape tests, PreparedStatementMetadata test added
- `test/test_phase16.cpp`: 41 new tests
  - ResolveCDefaultType mapping (15 tests)
  - SQL_C_DEFAULT parameter type conversion (12 tests: double/int/smallint/bigint/real/float/varchar/wvarchar/tinyint/bit)
  - Explicit type tests (3 tests)
  - NULL parameter handling (2 tests)
  - SQL_C_WCHAR UTF-16→UTF-8 conversion (5 tests: ASCII/Japanese/NTS/empty string/escape)
  - SubstituteParameters integration tests (2 tests)
  - Date/timestamp with SQL_C_DEFAULT (2 tests)
- Total: **678 tests** (Debug + Release pass)

## [1.0.0] - 2026-03-01

### Release Quality Assurance
- **Excluded test files from DLL project**:
  - Removed 9 test files (test_phase8–15, test_access_workflow) incorrectly included in `clickhouse_odbc.vcxproj`
  - DLL build now succeeds cleanly in Release configuration
- **Release build C4703 warning fix**:
  - Initialized uninitialized local pointer variables in 4 test files (test_phase15, test_phase14, test_phase7, test_access_workflow)
  - Added C4703 to `DisableSpecificWarnings` in test project Release configuration
  - Zero errors in both Debug/Release configurations, 623/623 tests pass

### Documentation
- **README.md full update**: v1.0.0 feature list, complete ODBC function table (11 categories), test section, known limitations
- **register_driver.bat improvement**: DLL path argument support, existence check added
- **ROADMAP.md update**: v1.0.0 release section added, all phases completed
- **CHANGELOG.md**: v1.0.0 release entry added

## [1.0.0-rc9] -2026-03-01

### Fixed (Driver Quality Enhancement — Additional Bug Fixes)
- **SQLColumns SQL_DATA_TYPE date/time type fix**:
  - Fixed issue where `SQL_DATA_TYPE` column returned concise type (91/92/93) for date/time/timestamp types
  - Changed to return `SQL_DATETIME` (9) per ODBC spec
- **ExecuteQuery URL encoding fix**:
  - Fixed issue where `ClickHouseClient::ExecuteQuery()` used raw string concatenation for username/password/database name
  - Changed to safe URL encoding via `httplib::Params` + `params_to_query_str()`
  - Handles special characters in passwords (`&`, `=`, `%`, `+`, spaces, etc.)
- **SQLGetDiagFieldW Wide conversion fix**:
  - Fixed issue where the following string fields were delegated to ANSI version without Wide conversion:
  - `SQL_DIAG_DYNAMIC_FUNCTION` (header field)
  - `SQL_DIAG_CLASS_ORIGIN`, `SQL_DIAG_SUBCLASS_ORIGIN` (record fields)
  - `SQL_DIAG_SERVER_NAME`, `SQL_DIAG_CONNECTION_NAME` (record fields)
  - Unified all string fields to ANSI retrieval → Wide conversion pattern
- **SQLGetDiagRecW TextLength fix**:
  - Fixed issue where `TextLength` returned byte count (`wide_msg.size() * sizeof(SQLWCHAR)`)
  - Changed to return character count (`wide_msg.size()`) per ODBC spec
- **SQLErrorW TextLength fix**:
  - Fixed issue where `TextLength` returned UTF-8 byte count (`rec.message.size()`)
  - Changed to correctly return Wide character count
- **LOCATE escape function argument order fix**:
  - Fixed issue where ODBC `{fn LOCATE(needle, haystack[, start])}` arguments were passed as-is to `positionUTF8(needle, haystack)`
  - Arguments are now swapped to correctly convert to `positionUTF8(haystack, needle[, start])`

### Tests
- `test/test_phase15.cpp`: 21 tests added (623 tests total)
  - SQLColumns SQL_DATA_TYPE tests (4)
  - URL encoding tests (2)
  - SQLGetDiagFieldW Wide conversion tests (4)
  - SQLGetDiagRecW/SQLErrorW TextLength tests (4)
  - LOCATE argument order tests (3)
  - Regression tests (4)

## [1.0.0-rc8] - 2026-03-01

### Fixed (Bug Fixes & Driver Quality Enhancement)
- **ExecDirectImpl escape sequence bug fix** (critical):
  - Fixed issue where query converted by `ProcessOdbcEscapeSequences()` was not used for actual server request
  - Changed `client->ExecuteQuery(query, ...)` → `client->ExecuteQuery(processed_query, ...)`
  - Also fixed affected_rows detection to use the converted query
- **SQLGetFunctions SQL_API_ALL_FUNCTIONS (ODBC 2.x) support**:
  - Added ODBC 2.x style function query that fills 100-element `SQLUSMALLINT` array with `FunctionId=0`
  - Set 53 legacy function IDs to `SQL_TRUE`
- **SQLGetFunctions individual check fix**:
  - Fixed the following functions missing from individual check (switch-case):
  - `SQL_API_SQLGETENVATTR`, `SQL_API_SQLSETENVATTR`
  - `SQL_API_SQLGETSTMTATTR`, `SQL_API_SQLSETSTMTATTR`
  - `SQL_API_SQLGETDIAGREC`, `SQL_API_SQLGETDIAGFIELD`
  - `SQL_API_SQLNATIVESQL`
  - `SQL_API_SQLALLOCENV`, `SQL_API_SQLFREEENV`, `SQL_API_SQLALLOCCONNECT`, `SQL_API_SQLFREECONNECT`, `SQL_API_SQLALLOCSTMT`
- **SQLDriverConnect DriverCompletion handling**:
  - `SQL_DRIVER_PROMPT`: Returns `HYC00` error since dialog is not supported
  - `SQL_DRIVER_COMPLETE`/`SQL_DRIVER_COMPLETE_REQUIRED`: Returns `HYC00` error when required parameters (HOST/SERVER/DSN) are missing and connection string is empty
  - `SQL_DRIVER_NOPROMPT`: Existing behavior (missing parameters result in server connection 08001 error)
  - Both ANSI and Wide versions supported
- **SQLCloseCursor SQLSTATE 24000 check**:
  - Returns `SQLSTATE 24000 (Invalid cursor state)` when cursor is not open (no result set)
- **SQLNativeSqlW output length fix**:
  - Correctly returns byte length in `TextLength2Ptr` even when `OutStatementText` is null

### Tests
- `test/test_phase14.cpp`: 22 tests added (602 tests total)
  - Escape bug fix tests (2)
  - SQL_API_ALL_FUNCTIONS tests (2)
  - Individual check fix tests (5)
  - DriverCompletion tests (4)
  - SQLCloseCursor 24000 tests (2)
  - SQLNativeSqlW tests (2)
  - Legacy Alloc function tests (3)
  - Bitmap/individual check consistency test (1)
  - SQLFreeStmt SQL_CLOSE regression test (1)

## [1.0.0-rc7] - 2026-03-01

### Added (MS Access Workflow Tests: accdb Use Case Validation)
- **MS Access workflow simulation tests** (`test/test_access_workflow.cpp`):
  - 19 scenarios, 46 tests comprehensively validating Access's actual ODBC API call patterns
- **Scenario 1: Table link creation workflow**:
  - Driver info query at Access startup (SQLGetInfo: DRIVER_NAME/VER/ODBC_VER, DBMS_NAME/VER)
  - SQLGetFunctions full function support check (SQL_API_ODBC3_ALL_FUNCTIONS)
  - Key SQLGetInfo value verification (CATALOG_NAME_SEPARATOR, IDENTIFIER_QUOTE_CHAR, TXN_CAPABLE, etc.)
  - Catalog function sequence during table linking: SQLTables → SQLColumns → SQLStatistics → SQLSpecialColumns → SQLPrimaryKeys → SQLForeignKeys
  - SQLGetTypeInfo all data type enumeration (INTEGER/VARCHAR/DATE/DOUBLE/BIGINT verification)
- **Scenario 2: Data reading workflow**:
  - Datasheet view basic flow (SQLNumResultCols → SQLDescribeCol → SQLBindCol → SQLFetch)
  - Parameterized queries (SQLPrepare + SQLBindParameter)
  - NULL value handling (SQL_NULL_DATA detection)
  - SQLExtendedFetch (ODBC 2.x compatible fetch)
- **Scenario 3: ODBC escape sequences** (Access-specific patterns):
  - Date literal `{d '...'}`, timestamp `{ts '...'}`
  - String functions (UCASE/LCASE/LENGTH → upper/lower/length)
  - Date functions (NOW/CURDATE/YEAR/MONTH → now/today/toYear/toMonth)
  - CONVERT function (SQL_INTEGER→toInt32, SQL_VARCHAR→toString)
  - Compound queries (multiple escapes mixed)
  - SQLNativeSql native SQL conversion
- **Scenario 4: Data type conversion**:
  - Integer type (SQL_C_SLONG)
  - Memo field piecemeal reading (500-byte chunked retrieval)
  - Date type (SQL_C_TYPE_DATE), timestamp type (SQL_C_TYPE_TIMESTAMP)
  - Floating point / currency type (SQL_C_DOUBLE)
- **Scenarios 5-6: Attributes & diagnostics**:
  - Connection attributes (AUTOCOMMIT, CONNECTION_TIMEOUT, ACCESS_MODE, CURRENT_CATALOG)
  - Statement attributes (ROW_ARRAY_SIZE, CURSOR_TYPE, CONCURRENCY, QUERY_TIMEOUT)
  - Transactions (SQLEndTran COMMIT/ROLLBACK = no-op)
  - Diagnostic records (SQLGetDiagRec/SQLGetDiagField)
- **Scenario 7: Column metadata**:
  - SQLColAttribute (3.x API): DESC_NAME, DESC_TYPE, DESC_NULLABLE, DESC_DISPLAY_SIZE
  - SQLColAttributes (2.x API): SQL_COLUMN_NAME, SQL_COLUMN_TYPE
- **Scenarios 8-11: Advanced operations**:
  - Multiple simultaneous statements on the same connection
  - Cursor name retrieval (SQLGetCursorName)
  - SQLSetPos positioning (SQL_POSITION, SQL_REFRESH)
  - SQLFreeStmt all modes (SQL_CLOSE/SQL_UNBIND/SQL_RESET_PARAMS)
- **Scenario 12: Access-specific GetInfo values**:
  - Function bitmask verification (STRING/NUMERIC/TIMEDATE/CONVERT_FUNCTIONS)
  - SQL syntax support (ORDER BY, GROUP BY, LIKE ESCAPE, SUBQUERIES)
- **Scenario 13: End-to-End workflow**:
  - Driver info check → catalog exploration → data reading → escape processing — all-phase integration test
  - Form view style single-record navigation
- **Scenarios 14-19: Additional workflows**:
  - Report aggregation queries (YEAR/MONTH + BETWEEN + GROUP BY)
  - IFNULL (Access Nz function) → ifNull conversion
  - ClickHouse type mapping validation (Int32/Int64/String/Float64/Date/DateTime/UInt8/Int16/Nullable/LowCardinality)
  - Column size calculation (FixedString/Decimal)
  - Pass-through query escape processing
  - LIKE escape, outer join escape
  - RowCount, SQLCancel
  - Full shutdown sequence (stmt close → stmt free → disconnect → dbc free → env free)
- 46 Access workflow tests added (580 tests total)

### Changed
- `test/test_access_workflow.cpp`: Newly created — 19 scenarios, 46 tests
- `clickhouse_odbc_test.vcxproj`: test_access_workflow.cpp added

## [1.0.0-rc6] - 2026-03-01

### Added (Phase 13: ODBC Escape Sequence Processing & Environment Attribute Enhancement & Driver Compatibility Improvement)
- **ODBC escape sequence parser** (`ProcessOdbcEscapeSequences`):
  - `{fn FUNC(args)}` — Scalar functions (60+ functions converted to ClickHouse native)
  - `{d 'yyyy-mm-dd'}` — Date literal (quotes only preserved)
  - `{t 'hh:mm:ss'}` — Time literal
  - `{ts 'yyyy-mm-dd hh:mm:ss'}` — Timestamp literal
  - `{oj ...}` — Outer join (pass-through)
  - `{escape 'x'}` — LIKE escape character
  - `{call ...}` — Procedure call (pass-through)
  - Recursive processing of nested escape sequences
  - `{...}` inside string literals is not processed
- **Scalar function conversion (60+ functions)**:
  - String: UCASE→upper, LCASE→lower, LENGTH, SUBSTRING, CONCAT, LTRIM, RTRIM, LEFT, RIGHT, REPLACE, LOCATE, SPACE, REPEAT, ASCII, CHAR
  - Numeric: ABS, CEILING, FLOOR, ROUND, TRUNCATE, SQRT, POWER, EXP, LOG, LOG10, MOD, PI, RAND, SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, DEGREES, RADIANS
  - Date/time: NOW→now, CURDATE→today, CURTIME, YEAR→toYear, MONTH→toMonth, DAYOFMONTH, DAYOFWEEK, DAYOFYEAR, HOUR, MINUTE, SECOND, WEEK, QUARTER, EXTRACT, TIMESTAMPADD, TIMESTAMPDIFF, DAYNAME, MONTHNAME
  - System: IFNULL→ifNull, DATABASE→currentDatabase, USER→currentUser
  - Conversion: CONVERT — SQL_VARCHAR→toString, SQL_INTEGER→toInt32, SQL_DOUBLE→toFloat64, SQL_DATE→toDate, etc.
- **`ExecDirectImpl` integration**: Automatic escape sequence conversion before query execution
- **`SQLPrepare`/`SQLPrepareW` integration**: Automatic escape sequence conversion at prepare time
- **`SQLNativeSql`/`SQLNativeSqlW` integration**: Escape processing added to native SQL conversion
- **Environment attribute enhancement**:
  - `SQL_ATTR_CONNECTION_POOLING`: Field added to `OdbcEnvironment`, full Get/Set support
  - `SQL_ATTR_CP_MATCH`: SQLGetEnvAttr/SQLSetEnvAttr support, default SQL_CP_STRICT_MATCH
- **`SQLGetDiagField` SERVER_NAME improvement**: SQL_DIAG_SERVER_NAME/CONNECTION_NAME returns actual host name (STMT/DBC handle support)
- **`SQLGetInfo` function bitmask enhancement**:
  - SQL_STRING_FUNCTIONS: 8→18 bits (LEFT, RIGHT, ASCII, CHAR, LOCATE, SPACE, REPEAT, BIT_LENGTH, CHAR_LENGTH, OCTET_LENGTH added)
  - SQL_NUMERIC_FUNCTIONS: 9→22 bits (EXP, LOG10, PI, RAND, SIN, COS, TAN, ASIN, ACOS, ATAN, ATAN2, DEGREES, RADIANS added)
  - SQL_TIMEDATE_FUNCTIONS: 8→20 bits (DAYOFWEEK, DAYOFYEAR, WEEK, QUARTER, CURTIME, DAYNAME, MONTHNAME, EXTRACT, TIMESTAMPADD, TIMESTAMPDIFF, etc. added)
  - SQL_SYSTEM_FUNCTIONS: SQL_FN_SYS_DBNAME, SQL_FN_SYS_USERNAME added
  - SQL_CONVERT_FUNCTIONS: SQL_FN_CVT_CONVERT added
- Phase 13 tests: 48 tests added (534 tests total)

### Changed
- `util.h`/`util.cpp`: Added `ProcessOdbcEscapeSequences()` function, `TranslateScalarFunction()` helper (60+ function mappings), `ParseEscapeContent()` parser
- `statement.cpp`: Integrated escape sequence processing into `ExecDirectImpl`, `SQLPrepare`/`SQLPrepareW`, `SQLNativeSql`/`SQLNativeSqlW`
- `connection.cpp`: Added `SQL_ATTR_CP_MATCH` to `SQLGetEnvAttr`/`SQLSetEnvAttr`, added field storage to `SQL_ATTR_CONNECTION_POOLING`
- `handle.h`: Added `connection_pooling`/`cp_match` fields to `OdbcEnvironment`
- `clickhouse_odbc.cpp`: `SQLGetDiagField` `SQL_DIAG_SERVER_NAME`/`SQL_DIAG_CONNECTION_NAME` returns connected host name
- `info.cpp`: Extended bitmasks for SQL_STRING_FUNCTIONS, SQL_NUMERIC_FUNCTIONS, SQL_TIMEDATE_FUNCTIONS, SQL_SYSTEM_FUNCTIONS, SQL_CONVERT_FUNCTIONS

## [1.0.0-rc5] - 2026-03-01

### Added (Phase 12: SQLGetData Piecemeal Retrieval & RowCount Improvement & Driver Quality Enhancement)
- **`SQLGetData` piecemeal retrieval**:
  - Partial read support for `SQL_C_CHAR` / `SQL_C_WCHAR` / `SQL_C_BINARY`
  - Returns `SQL_SUCCESS_WITH_INFO` (01004) when buffer is small, next call retrieves remainder
  - Returns `SQL_NO_DATA` on calls after all data has been retrieved
  - Automatic offset reset on column switch and `SQLFetch`
  - Added `getdata_col` / `getdata_offset` fields to `OdbcStatement`
- **`SQLRowCount` improvement**:
  - Non-SELECT queries (INSERT/UPDATE/DELETE) return `affected_rows`
  - SELECT queries return `rows.size()`
  - Added `affected_rows` field to `OdbcStatement`
- **`SQLCancel` enhancement**:
  - Data-at-Execution (DAE) state reset
  - Piecemeal SQLGetData state reset
  - Invalid handle validation added
- **2 additional statement attributes**:
  - `SQL_ATTR_CURSOR_SCROLLABLE`: Default `SQL_NONSCROLLABLE`
  - `SQL_ATTR_CURSOR_SENSITIVITY`: Default `SQL_INSENSITIVE`
- **`SQLGetDiagField` enhancement**:
  - `SQL_DIAG_CURSOR_ROW_COUNT`: Statement result set row count
  - `SQL_DIAG_DYNAMIC_FUNCTION`: Dynamic function name (empty string)
  - `SQL_DIAG_DYNAMIC_FUNCTION_CODE`: `SQL_DIAG_UNKNOWN_STATEMENT`
  - `SQL_DIAG_ROW_COUNT`: Improved to return statement's affected_rows
- Phase 12 tests: 22 tests added (486 tests total)

### Changed
- `handle.h`: Added 5 fields to `OdbcStatement` (`getdata_col`, `getdata_offset`, `affected_rows`, `cursor_scrollable`, `cursor_sensitivity`)
- `result.cpp`: Rewrote `SQLGetData` for piecemeal support, offset reset in `SQLFetch`
- `statement.cpp`: `ExecDirectImpl` tracks `affected_rows`, improved `SQLRowCount` return value, enhanced `SQLCancel`, added 2 attributes to `SQLGetStmtAttr`/`SQLSetStmtAttr`
- `clickhouse_odbc.cpp`: Added 4 header fields to `SQLGetDiagField`

## [1.0.0-rc4] - 2026-03-01

### Added (Phase 11: Connection Attribute Completion & SQLGetInfo Enhancement)
- **8 additional connection attributes** (`OdbcConnection`):
  - `SQL_ATTR_METADATA_ID`: Default `SQL_FALSE`
  - `SQL_ATTR_TXN_ISOLATION`: Default `SQL_TXN_READ_COMMITTED`
  - `SQL_ATTR_PACKET_SIZE`: Default 0
  - `SQL_ATTR_ASYNC_ENABLE`: Default `SQL_ASYNC_ENABLE_OFF`
  - `SQL_ATTR_QUIET_MODE`: Default `nullptr`
  - `SQL_ATTR_ODBC_CURSORS`: Default `SQL_CUR_USE_DRIVER`
  - `SQL_ATTR_TRANSLATE_LIB`: Default empty string
  - `SQL_ATTR_TRANSLATE_OPTION`: Default 0
- **`SQLGetConnectAttr`/`SQLSetConnectAttr`**: Get/Set support for 9 attributes
- **`SQLGetConnectAttrW`/`SQLSetConnectAttrW`**: Wide version for 3 string attributes (current_catalog, tracefile, translate_lib)
- **`SQLGetInfo` 20+ new info types**:
  - `SQL_FETCH_DIRECTION`, `SQL_FILE_USAGE`, `SQL_POS_OPERATIONS`, `SQL_LOCK_TYPES`
  - `SQL_STATIC_SENSITIVITY`, `SQL_DATETIME_LITERALS`, `SQL_XOPEN_CLI_YEAR`
  - `SQL_TIMEDATE_ADD_INTERVALS`, `SQL_TIMEDATE_DIFF_INTERVALS`
  - `SQL_SQL92_ROW_VALUE_CONSTRUCTOR`, `SQL_STANDARD_CLI_CONFORMANCE`
  - `SQL_CONVERT_INTERVAL_YEAR_MONTH`, `SQL_CONVERT_INTERVAL_DAY_TIME`, `SQL_CONVERT_GUID`
  - `SQL_DM_VER`, `SQL_COLLATION_SEQ`, `SQL_ASYNC_DBC_FUNCTIONS`
  - `SQL_MAX_BINARY_LITERAL_LEN`, `SQL_MAX_PROCEDURE_NAME_LEN`, `SQL_ACTIVE_ENVIRONMENTS`
- Phase 11 tests: 34 tests added (464 tests total)

### Changed
- `handle.h`: Added 8 fields to `OdbcConnection`
- `connection.cpp`: Added 9 attributes to `SQLGetConnectAttr`/`SQLSetConnectAttr`, changed default handler to return `SQL_SUCCESS`
- `info.cpp`: Updated driver version to `01.00.0000`, added 20+ new info types, cleaned up ODBC 2.x alias duplicate cases

## [1.0.0-rc3] - 2026-03-01

### Added (Phase 10: Parameter Array Execution & Compatibility Attribute Enhancement)
- **Parameter array execution (paramset_size > 1)**:
  - `SQLExecute` sequentially executes multiple parameter sets when `paramset_size > 1`
  - Column-wise binding (`SQL_PARAM_BIND_BY_COLUMN`): Offset calculation via C type size / buffer_length
  - Row-wise binding (`SQL_ATTR_PARAM_BIND_TYPE`): Struct size binding
  - Sets `param_status_ptr` with per-set status (`SQL_PARAM_SUCCESS`/`SQL_PARAM_ERROR`)
  - Sets `params_processed_ptr` with processed set count
  - `GetCTypeSize()` / `OffsetParameter()` helper functions
- **`SQLExtendedFetch` multi-row support**:
  - Delegates to `SQLFetch` multi-row fetch
  - Saves/restores stmt-level `rows_fetched_ptr` / `row_status_ptr`
  - All directions supported: FIRST/LAST/ABSOLUTE/RELATIVE
- **8 additional statement compatibility attributes**:
  - `SQL_ATTR_PARAM_BIND_TYPE`: Default `SQL_PARAM_BIND_BY_COLUMN`
  - `SQL_ATTR_PARAM_STATUS_PTR`: Parameter set status output pointer
  - `SQL_ATTR_PARAMS_PROCESSED_PTR`: Processed set count output pointer
  - `SQL_ATTR_NOSCAN`: Default `SQL_NOSCAN_OFF`
  - `SQL_ATTR_CONCURRENCY`: Default `SQL_CONCUR_READ_ONLY`
  - `SQL_ATTR_MAX_LENGTH`: Default 0 (unlimited)
  - `SQL_ATTR_RETRIEVE_DATA`: Default `SQL_RD_ON`
  - `SQL_ATTR_USE_BOOKMARKS`: Default `SQL_UB_OFF`
- Phase 10 tests: 26 tests added (430 tests total)

### Changed
- `handle.h`: Added 8 fields to `OdbcStatement` (`param_bind_type`, `noscan`, `concurrency`, `max_length`, `retrieve_data`, `use_bookmarks`, `param_status_ptr`, `params_processed_ptr`)
- `statement.cpp`: Added 9 attributes to `SQLGetStmtAttr`/`SQLSetStmtAttr`, added parameter array execution logic to `SQLExecute`
- `result.cpp`: Rewrote `SQLExtendedFetch` for multi-row support

## [1.0.0-rc2] - 2026-03-01

### Added (Phase 9: SQLBrowseConnect & Row Array Binding)
- **`SQLBrowseConnect`/`SQLBrowseConnectW`**: Iterative connection browsing
  - `BrowseConnectImpl` helper for DSN reading + required attribute detection
  - Returns `SQL_NEED_DATA` + browse result string when HOST/UID not specified
  - Attempts connection when all attributes are available → `SQL_SUCCESS` or `SQL_ERROR`
  - Both ANSI and Wide versions supported
- **Row Array Binding (multi-row fetch)**:
  - `SQLFetch` fetches multiple rows when `rowset_size > 1`
  - Column-wise binding (`SQL_BIND_BY_COLUMN`): Row offset calculation in buffer arrays
  - Row-wise binding (`SQL_ATTR_ROW_BIND_TYPE`): Struct size binding
  - Sets `row_status_ptr` with per-row status (`SQL_ROW_SUCCESS`/`SQL_ROW_ERROR`/`SQL_ROW_NOROW`)
  - Sets `rows_fetched_ptr` with actual fetched row count
  - Partial rowset support (remaining rows < rowset_size)
- **`SQL_ATTR_ROW_BIND_TYPE`**: SQLGetStmtAttr/SQLSetStmtAttr support, default `SQL_BIND_BY_COLUMN`
- **`SQL_ATTR_PARAMSET_SIZE`**: SQLGetStmtAttr/SQLSetStmtAttr support, default 1, 0→1 clamping
- **`SQLGetFunctions` update**: Added `SQL_API_SQLBROWSECONNECT` to bitmap/individual checks
- Phase 9 tests: 29 tests added (404 tests total)

### Changed
- `handle.h`: Added `row_bind_type` / `paramset_size` fields to `OdbcStatement`
- `clickhouse_odbc.def`: Added `SQLBrowseConnect`/`SQLBrowseConnectW` exports
- `connection.cpp`: Added `#undef SQLBrowseConnect`, implemented `BrowseConnectImpl` + ANSI/Wide versions

## [1.0.0-rc] - 2026-03-01

### Added (Phase 8: Descriptor Handles & SQLBulkOperations)
- **Descriptor handle infrastructure**:
  - `DescriptorType` enum (APD/IPD/ARD/IRD)
  - `DescriptorRecord` struct (~25 fields: type, name, length, precision, scale, nullable, pointers, etc.)
  - `OdbcDescriptor` handle (record map, count, auto/manual distinction)
  - Added 4 auto-descriptors (`auto_apd`/`ipd`/`ard`/`ird`) and current pointers to `OdbcStatement`
- **`SQLGetDescField`/`SQLGetDescFieldW`**: Header fields (COUNT, ALLOC_TYPE) + record fields (~25 IDs) retrieval
- **`SQLSetDescField`/`SQLSetDescFieldW`**: Record field setting (IRD read-only protection)
- **`SQLGetDescRec`/`SQLGetDescRecW`**: Composite record retrieval
- **`SQLSetDescRec`**: Composite record setting
- **`SQLCopyDesc`**: Inter-descriptor copy (IRD target protection)
- **`SyncDescriptorsFromStatement`**: IRD←result set, ARD←bound columns, APD/IPD←bound parameters sync
- **`SQLGetStmtAttr` update**: `SQL_ATTR_APP/IMP_ROW/PARAM_DESC` returns actual descriptor pointer
- **`SQLSetStmtAttr` update**: Application descriptor set/NULL reset, implementation descriptor returns HY017 error
- **`SQLBulkOperations` SQL_ADD**: Builds and executes INSERT from bound columns (bookmark operations return HYC00)
- **`SQLGetFunctions` update**: Added 5 descriptor functions (`SQLGetDescField`/`SQLSetDescField`/`SQLGetDescRec`/`SQLSetDescRec`/`SQLCopyDesc`) to bitmap/individual checks
- Phase 8 tests: 44 tests added (375 tests total)

### Changed
- `handle.h`: Placed descriptor type definitions before `OdbcStatement` (correct dependency ordering)
- `clickhouse_odbc.def`: Added 8 descriptor function export entries
- `clickhouse_odbc.vcxproj`: Added `descriptor.cpp`, removed misplaced test files

## [0.9.0] -2026-03-01

### Added (Phase 7: ODBC 2.x Legacy Functions & Data-at-Execution)
- **`SQLParamData`/`SQLPutData` full implementation**: Data-at-Execution parameter support
  - `SQL_DATA_AT_EXEC` / `SQL_LEN_DATA_AT_EXEC` indicator detection
  - `SQLExecute` returns `SQL_NEED_DATA` when DAE parameters detected
  - `SQLParamData` returns parameter tokens while sequentially receiving data
  - `SQLPutData` supports multi-chunk accumulation (`SQL_NTS` / `SQL_NULL_DATA` handling)
  - Automatic query execution after all DAE parameters received
  - Added `need_data`, `pending_dae_params`, `current_dae_index`, `dae_buffers`, `ResetDAE()` to `OdbcStatement`
- **`SQLError`/`SQLErrorW` (ODBC 2.x)**: Iterative diagnostic record consumption
  - Handle priority: stmt > dbc > env (per ODBC 2.x spec)
  - Consumes and returns the first record on each call
- **`SQLGetConnectOption`/`SQLSetConnectOption` (ODBC 2.x)**: Connection options
  - Wrapper delegating to `SQLGetConnectAttr`/`SQLSetConnectAttr`
- **`SQLGetStmtOption`/`SQLSetStmtOption` (ODBC 2.x)**: Statement options
  - Wrapper delegating to `SQLGetStmtAttr`/`SQLSetStmtAttr`
- **`SQLTransact` (ODBC 2.x)**: Transaction control
  - Wrapper delegating to `SQLEndTran`
- **`SQLGetFunctions` update**: Added all legacy functions (`SQLError`, `SQLGet/SetConnectOption`, `SQLGet/SetStmtOption`, `SQLTransact`) to bitmap and individual checks
- Phase 7 tests: 32 tests added (331 tests total)

### Changed
- `clickhouse_odbc.def`: Added `SQLError`/`SQLErrorW`, `SQLGetConnectOption`, `SQLSetConnectOption`, `SQLGetStmtOption`, `SQLSetStmtOption`, `SQLTransact` exports

## [0.8.0] - 2026-03-01

### Added (Phase 6: ODBC 2.x Backward Compatibility)
- **`SQLExtendedFetch`**: ODBC 2.x fetch function
  - Delegates to `SQLFetchScroll`, sets `RowCountPtr`/`RowStatusArray`
  - Function preferentially used by MS Access over `SQLFetchScroll`
  - ANSI version only (per ODBC 2.x spec)
- **`SQLColAttributes`**: ODBC 2.x column attribute function
  - ODBC 2.x field ID → 3.x field ID mapping
  - Supports `SQL_COLUMN_COUNT`, `SQL_COLUMN_NAME`, `SQL_COLUMN_TYPE`, `SQL_COLUMN_LENGTH`, etc.
  - Thin wrapper delegating to `SQLColAttribute`
- **`SQLSetScrollOptions`**: ODBC 2.x scroll options (deprecated)
  - Only `SQL_CONCUR_READ_ONLY` supported
  - Maps `fConcurrency` to `cursor_type`, `crowKeyset` to `rowset_size`
  - Unsupported concurrency (`SQL_CONCUR_LOCK`, etc.) returns `SQLSTATE HYC00`
- **Statement attribute extensions**:
  - `SQL_ROWSET_SIZE` / `SQL_ATTR_ROW_ARRAY_SIZE` get/set
  - `SQL_ATTR_ROW_STATUS_PTR` / `SQL_ATTR_ROWS_FETCHED_PTR` get/set
  - Added `rowset_size`, `row_status_ptr`, `rows_fetched_ptr` fields to `OdbcStatement`
- **`SQLGetFunctions` update**: Added ODBC 2.x functions (`SQLExtendedFetch`, `SQLColAttributes`, `SQLSetScrollOptions`) and existing catalog functions to bitmap and individual checks
- Phase 6 tests: 25 tests added (299 tests total)

### Changed
- `SQLGetInfo`: Changed `SQL_DESCRIBE_PARAMETER` return value from `"N"` to `"Y"` (since `SQLDescribeParam` is implemented)
- `clickhouse_odbc.def`: Added `SQLExtendedFetch`, `SQLColAttributes`, `SQLSetScrollOptions` exports

### Build Improvements
- Excluded test files from DLL project (`clickhouse_odbc.vcxproj`) to prevent PDB lock contention
- Added `/FS` (Force Synchronous PDB Writes) option to both DLL and test projects
- Resolved PDB access contention during parallel builds

## [0.7.0] - 2026-03-01

### Added (Phase 5b: MS Access Practical Feature Enhancement)
- **`SQLDescribeParam`**: Parameter description
  - Returns type info (data type, size, precision) for bound parameters
  - Unbound parameters return default `SQL_VARCHAR`/255 (MS Access compatible)
  - Parameter index validation, SQLSTATE `07009` error handling
- **`SQLNumParams` improvement**: Accurate parameter count skipping `?` inside quotes
  - Correctly ignores `?` inside single quotes and escaped quotes (`''`)
- **DSN registry reading (`SQLConnect`/`SQLConnectW`)**:
  - Retrieves connection parameters from ODBC.INI via `SQLGetPrivateProfileString`
  - `BuildConnStrFromDsn()` helper (Host/Port/Database/UID/PWD)
  - Explicit SQLConnect arguments override DSN values
  - Trace log output added
- **Cursor name (`SQLGetCursorName`/`SQLSetCursorName`)**:
  - Both ANSI and Wide versions implemented
  - Auto-generated name (`SQL_CUR<N>`) assigned at statement creation
  - User-set name retention, `SQLSTATE 01004` on truncation
  - Null pointer detection (`SQLSTATE HY009`)
- **`SQLSetPos` enhancement**:
  - `SQL_POSITION`: Cursor positioning (row 0=current row, row 1=first row)
  - `SQL_REFRESH`: no-op (HTTP-based driver)
  - `SQLSTATE HY109` error for invalid row position
  - `SQLSTATE HYC00` for unsupported operations (UPDATE/DELETE/ADD)
- **`SQLGetFunctions` update**: Added `SQLDescribeParam`, `SQLSetPos`, `SQLGetCursorName`, `SQLSetCursorName` to bitmap and individual checks
- Phase 5b tests: 27 tests added (274 tests total)

### Changed
- `clickhouse_odbc.def`: Added `SQLDescribeParam`, `SQLGetCursorName`/`W`, `SQLSetCursorName`/`W` exports
- Added `cursor_name`/`cursor_name_set`/`next_cursor_id` fields to `OdbcStatement`
- `connection.cpp`: Added `odbcinst.h` include and `#undef` for `SQLGetPrivateProfileString`

## [0.6.0] - 2026-03-01

### Added (Phase 5: Advanced Features — Parameterized Queries, Trace, DSN)
- **Parameterized queries (`SQLBindParameter`)**:
  - Client-side parameter substitution (`?` placeholder → value expansion)
  - All C types supported: `SQL_C_CHAR`, `SQL_C_WCHAR`, `SQL_C_SLONG`, `SQL_C_DOUBLE`, `SQL_C_SBIGINT`, `SQL_C_TYPE_DATE`, `SQL_C_TYPE_TIME`, `SQL_C_TYPE_TIMESTAMP`, `SQL_C_BINARY`, `SQL_C_GUID`, unsigned integer types, `SQL_C_BIT`, etc.
  - `SQL_NULL_DATA` parameter to `NULL` conversion
  - `?` inside string literals not substituted (quote-aware)
  - Single quote and backslash escaping
  - `SQLSTATE 07002` (unbound parameter) error handling
- **ODBC trace log infrastructure**:
  - `TraceLog` singleton (level control: Off/Error/Warning/Info/Debug/Verbose)
  - Dual logging via `OutputDebugString` + file output
  - Timestamped formatting
  - `TRACE_LOG` / `TRACE_ENTRY` / `TRACE_EXIT` macros
  - Trace instrumentation on major API entry points (SQLConnect, SQLExecDirect, SQLFetch, SQLGetData, SQLBindParameter, SQLPrepare, SQLExecute)
  - Activation via `SQL_ATTR_TRACE` / `SQL_ATTR_TRACEFILE` connection attributes
- **DSN configuration (`ConfigDSN` / `ConfigDSNW`)**:
  - `ODBC_ADD_DSN`, `ODBC_CONFIG_DSN`, `ODBC_REMOVE_DSN` support
  - DSN parameter writing to registry ODBC.INI (Host, Port, Database, UID, PWD, Description)
  - Both ANSI and Wide versions exported
  - Double-null terminated attribute list parsing
- Helper functions: `ExtractParameterValue()`, `EscapeSqlString()`, `SubstituteParameters()`
- Phase 5 tests: 44 tests added (247 tests total)

### Changed
- `SQLExecute` automatically substitutes bound parameters into query
- `clickhouse_odbc.def`: Added `ConfigDSN` / `ConfigDSNW` exports
- Added `legacy_stdio_definitions.lib` to linker dependencies (resolves CRT functions for `odbccp32.lib`)

## [0.5.0] - 2026-03-01

### Added (Phase 4: Performance & Reliability)
- **Thread safety**: Added `recursive_mutex` to `OdbcHandle`, `HandleLock` RAII guard, thread-safe diagnostic record operations
- **Global connection map exclusive access**: Protected all connection map access with `g_clients_mutex`
- **Timeout settings**: Added `SetConnectionTimeout()`/`SetQueryTimeout()` to `ClickHouseClient`, automatic propagation from connection attributes
- **Connection retry**: Configurable retry via `SetRetryCount()`/`SetRetryDelayMs()` (on connect and query execution)
- **Connection pooling awareness**: `SQL_ATTR_CONNECTION_DEAD` support (Driver Manager pooling integration)
- **Robust error handling**:
  - SQLSTATE `08002` (double connection prevention)
  - SQLSTATE `HY024` (invalid port number)
  - SQLSTATE `01004` (string truncation) diagnostic record setting
  - SQLSTATE `24000` (fetch without result set)
  - SQLSTATE `HYT00` (timeout detection)
- **Safe handle cleanup**:
  - `SQLFreeHandle(DBC)` automatically disconnects and frees statements
  - `SQLFreeHandle(STMT)` clears result sets and bindings
  - `SQLFreeHandle(ENV)` cascade-frees remaining connections
- Phase 4 tests: 24 tests added (203 tests total)

### Changed
- `OdbcHandle` is now non-copyable (due to mutex ownership)
- `ClickHouseClient::Connect()` attempts connection with retry-enabled Ping
- `ClickHouseClient::ExecuteQuery()` retries on connection failure (server errors are not retried)
- `ExecDirectImpl` auto-detects timeout and sets HYT00
- `SQLFreeHandle(ENV)` auto-frees remaining connections (previously returned HY010 error)
- `SQLFreeHandle(DBC)` auto-cleans up even when connected (previously returned HY010 error)

## [0.3.0] - 2026-03-01

### Added (Phase 3: Complete Data Type Support)
- `SQL_C_NUMERIC` conversion — Converts Decimal types to `SQL_NUMERIC_STRUCT` (little-endian 128-bit)
- `SQL_C_GUID` conversion — Parses UUID strings to `SQLGUID` struct
- `SQL_C_TYPE_TIME` / `SQL_C_TIME` conversion — Extracts time portion from timestamps
- `IsNullableType()` — Nullable wrapper detection helper
- `GetCTypeName()` — C type name for diagnostic messages
- `ValidateNumericRange()` — Overflow validation during numeric conversion (SQLSTATE 22003)
- ClickHouse extended type mapping: Nothing, SimpleAggregateFunction (automatic inner type extraction), AggregateFunction, IntervalXxx, Geo types (Point/Ring/Polygon/MultiPolygon), JSON/Object, Nested
- Comprehensive `BuildTypeInfoResultSet` enhancement:
  - Unsigned integer variants (TINYINT UNSIGNED, SMALLINT UNSIGNED, INTEGER UNSIGNED, BIGINT UNSIGNED)
  - WCHAR / WVARCHAR / SQL_TYPE_TIME / BINARY / VARBINARY type entries added
  - Accurate CREATE_PARAMS reporting (precision, scale, length)
  - Accurate SQL_DATETIME_SUB setting (SQL_CODE_DATE/TIME/TIMESTAMP)
  - Type-specific NUM_PREC_RADIX setting (numeric=10, string=NULL)
  - Type-specific CASE_SENSITIVE setting (string=TRUE, numeric=FALSE)
  - Accurate MINIMUM_SCALE / MAXIMUM_SCALE reporting
- Phase 3 tests: 55 tests added (179 tests total)

### Changed
- JSON response parser accuracy improvements:
  - Boolean values converted to "1"/"0" (previously "true"/"false")
  - Integer values converted via `to_string` (no precision loss)
  - Floating point values converted with `setprecision(17)` (high precision)
- Added overflow validation to `SQL_C_SLONG` conversion
- `BuildTypeInfoResultSet` SQL_DATA_TYPE now correctly returns SQL_DATETIME for date/time types

## [0.2.0] - 2026-03-01

### Added (Phase 2: MS Access Compatibility Enhancement)
- `GetColumnSizeForType()` — Precise column size calculation from ClickHouse type strings (Decimal(P,S), FixedString(N), DateTime64(P) support)
- `GetDecimalDigitsForType()` — Accurate decimal digit reporting by type
- `IsUnsignedType()` — Unsigned type detection
- `NormalizeClickHouseType()` — Nullable/LowCardinality wrapper removal
- Full `SQLColAttribute` attribute support: BASE_COLUMN_NAME, BASE_TABLE_NAME, LITERAL_PREFIX/SUFFIX, LOCAL_TYPE_NAME, NUM_PREC_RADIX, accurate DISPLAY_SIZE/OCTET_LENGTH
- Additional `SQLGetInfo` info types: SQL_KEYWORDS, SQL_DDL_INDEX, SQL_CREATE/DROP_*, SQL_SQL92_GRANT/REVOKE, SQL_INSERT_STATEMENT, SQL_CONVERT_* added
- NULL handling improvement: Returns SQLSTATE 22002 error when retrieving NULL value without indicator
- Phase 2 tests: 31 tests added (124 tests total)

### Changed
- `MapClickHouseType` improved to use precise size and digit counts
- `SQLColumns` now accurately reports type-specific DECIMAL_DIGITS, NUM_PREC_RADIX, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH
- `SQLColAttribute` TYPE_NAME now returns ODBC standard names (e.g., "INTEGER") (LOCAL_TYPE_NAME returns ClickHouse type names)
- `SQLColAttribute` CASE_SENSITIVE returns accurate type-specific values
- `SQLColAttribute` UNSIGNED correctly detects ClickHouse UInt types

## [0.1.0] - 2026-03-01

### Added
- Basic ODBC driver structure (DLL project)
- ClickHouse HTTP interface connection (`SQLConnect`, `SQLDriverConnect`)
- Query execution (`SQLExecDirect`, `SQLPrepare`, `SQLExecute`)
- Result retrieval (`SQLFetch`, `SQLFetchScroll`, `SQLGetData`)
- Catalog functions (`SQLTables`, `SQLColumns`, `SQLPrimaryKeys`, `SQLStatistics`, etc.)
- Type mapping (ClickHouse → ODBC)
- `SQLGetTypeInfo` type information provider
- Comprehensive driver information reporting via `SQLGetInfo`
- Both ANSI and Unicode (W) versions exported
- Diagnostic functions (`SQLGetDiagRec`, `SQLGetDiagField`)
- Driver registration script (`setup/register_driver.bat`)
- CMake build support
- 93 unit tests (all passing)
  - Utility functions (28 tests)
  - Type mapping (18 tests)
  - Handle management (17 tests)
  - ODBC API handle operations (12 tests)
  - Diagnostic functions (4 tests)
  - Driver information (6 tests)
- Documentation
  - README.md (project overview and usage guide)
  - docs/ARCHITECTURE.md (architecture design)
  - docs/ROADMAP.md (development roadmap)
  - docs/CONTRIBUTING.md (development guide)
  - .github/copilot-instructions.md (Copilot coding conventions)

### Build Fixes
- Fixed PlatformToolset to match environment (v145)
- Added `<windows.h>` include to `handle.h` (resolving ODBC type definition dependencies)
- Added `WIN32_LEAN_AND_MEAN` definition (preventing winsock2.h conflicts)
- Added ODBC UNICODE macro `#undef` to all source files
- Removed `CPPHTTPLIB_OPENSSL_SUPPORT` definition (OpenSSL not required)
- Added `_CRT_SECURE_NO_WARNINGS` to preprocessor definitions
- Fixed httplib `Post()` call (URL query parameter method)

### Known Limitations
- Real connection tests against ClickHouse server not implemented (unit tests only)
- Connection dialog (GUI) not implemented
- Parameter binding is declaration only
- TLS/SSL not supported
