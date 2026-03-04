// Phase 13 Tests: ODBC escape sequence processing, environment attribute enhancement, driver compatibility improvement
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
#include "../src/include/util.h"

#ifdef UNICODE
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLGetInfo
#undef SQLNativeSql
#undef SQLPrepare
#undef SQLGetDiagField
#endif

#pragma warning(disable: 4996)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLGetInfo(SQLHDBC, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLNativeSql(SQLHDBC, SQLCHAR *, SQLINTEGER, SQLCHAR *, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// Helper: create env + dbc + stmt
static void CreateTestHandles(SQLHENV &env, SQLHDBC &dbc, SQLHSTMT &stmt) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
}

static void FreeTestHandles(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt) {
    if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (dbc) {
        auto *conn = static_cast<OdbcConnection *>(dbc);
        conn->connected = false;
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// ODBC Escape Sequence: Scalar Functions
// ============================================================================
TEST(Phase13_EscapeSeq, FnUcase) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn UCASE(name)} FROM t");
    AssertEqual(std::string("SELECT upper(name) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnLcase) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn LCASE(name)} FROM t");
    AssertEqual(std::string("SELECT lower(name) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnNow) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn NOW()}");
    AssertEqual(std::string("SELECT now()"), result);
}

TEST(Phase13_EscapeSeq, FnCurdate) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn CURDATE()}");
    AssertEqual(std::string("SELECT today()"), result);
}

TEST(Phase13_EscapeSeq, FnLength) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn LENGTH(name)} FROM t");
    AssertEqual(std::string("SELECT length(name) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnSubstring) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn SUBSTRING(name, 1, 3)} FROM t");
    AssertEqual(std::string("SELECT substring(name, 1, 3) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnConcat) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn CONCAT(a, b)} FROM t");
    AssertEqual(std::string("SELECT concat(a, b) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnAbs) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn ABS(-5)}");
    AssertEqual(std::string("SELECT abs(-5)"), result);
}

TEST(Phase13_EscapeSeq, FnFloor) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn FLOOR(3.7)}");
    AssertEqual(std::string("SELECT floor(3.7)"), result);
}

TEST(Phase13_EscapeSeq, FnRound) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn ROUND(val, 2)} FROM t");
    AssertEqual(std::string("SELECT round(val, 2) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnIfnull) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn IFNULL(col, 0)} FROM t");
    AssertEqual(std::string("SELECT ifNull(col, 0) FROM t"), result);
}

// Date/Time functions
TEST(Phase13_EscapeSeq, FnYear) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn YEAR(d)} FROM t");
    AssertEqual(std::string("SELECT toYear(d) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnMonth) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn MONTH(d)} FROM t");
    AssertEqual(std::string("SELECT toMonth(d) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnDayofmonth) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn DAYOFMONTH(d)} FROM t");
    AssertEqual(std::string("SELECT toDayOfMonth(d) FROM t"), result);
}

// CONVERT function
TEST(Phase13_EscapeSeq, FnConvertVarchar) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn CONVERT(col, SQL_VARCHAR)} FROM t");
    AssertEqual(std::string("SELECT toString(col) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnConvertInteger) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn CONVERT(col, SQL_INTEGER)} FROM t");
    AssertEqual(std::string("SELECT toInt32(col) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnConvertDouble) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn CONVERT(col, SQL_DOUBLE)} FROM t");
    AssertEqual(std::string("SELECT toFloat64(col) FROM t"), result);
}

// ============================================================================
// ODBC Escape Sequence: Date/Time/Timestamp Literals
// ============================================================================
TEST(Phase13_EscapeSeq, DateLiteral) {
    std::string result = ProcessOdbcEscapeSequences("SELECT * FROM t WHERE d = {d '2024-01-15'}");
    AssertEqual(std::string("SELECT * FROM t WHERE d = '2024-01-15'"), result);
}

TEST(Phase13_EscapeSeq, TimeLiteral) {
    std::string result = ProcessOdbcEscapeSequences("SELECT * FROM t WHERE t = {t '13:45:00'}");
    AssertEqual(std::string("SELECT * FROM t WHERE t = '13:45:00'"), result);
}

TEST(Phase13_EscapeSeq, TimestampLiteral) {
    std::string result = ProcessOdbcEscapeSequences("SELECT * FROM t WHERE ts = {ts '2024-01-15 13:45:00'}");
    AssertEqual(std::string("SELECT * FROM t WHERE ts = '2024-01-15 13:45:00'"), result);
}

// ============================================================================
// ODBC Escape Sequence: Outer Join
// ============================================================================
TEST(Phase13_EscapeSeq, OuterJoin) {
    std::string result = ProcessOdbcEscapeSequences("SELECT * FROM {oj t1 LEFT OUTER JOIN t2 ON t1.id = t2.id}");
    AssertEqual(std::string("SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id"), result);
}

// ============================================================================
// ODBC Escape Sequence: Escape clause
// ============================================================================
TEST(Phase13_EscapeSeq, EscapeClause) {
    std::string result = ProcessOdbcEscapeSequences("SELECT * FROM t WHERE name LIKE '%\\_%' {escape '\\'}");
    AssertEqual(std::string("SELECT * FROM t WHERE name LIKE '%\\_%' ESCAPE '\\'"), result);
}

// ============================================================================
// ODBC Escape Sequence: No change
// ============================================================================
TEST(Phase13_EscapeSeq, NoEscapeSequence) {
    std::string sql = "SELECT col1, col2 FROM table1 WHERE id = 42";
    std::string result = ProcessOdbcEscapeSequences(sql);
    AssertEqual(sql, result);
}

TEST(Phase13_EscapeSeq, EscapeInStringLiteral) {
    // Escape sequences inside string literals should NOT be processed
    std::string sql = "SELECT '{fn NOW()}' FROM t";
    std::string result = ProcessOdbcEscapeSequences(sql);
    AssertEqual(sql, result);
}

// ============================================================================
// ODBC Escape Sequence: Multiple/Nested
// ============================================================================
TEST(Phase13_EscapeSeq, MultipleEscapes) {
    std::string result = ProcessOdbcEscapeSequences(
        "SELECT {fn UCASE(name)}, {fn YEAR(d)} FROM t WHERE d > {d '2024-01-01'}");
    AssertEqual(std::string("SELECT upper(name), toYear(d) FROM t WHERE d > '2024-01-01'"), result);
}

TEST(Phase13_EscapeSeq, NestedFunctions) {
    std::string result = ProcessOdbcEscapeSequences(
        "SELECT {fn UCASE({fn SUBSTRING(name, 1, 3)})} FROM t");
    AssertEqual(std::string("SELECT upper(substring(name, 1, 3)) FROM t"), result);
}

// ============================================================================
// SQLNativeSql: Escape sequence processing
// ============================================================================
TEST(Phase13_NativeSql, ProcessesEscapeSequences) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    std::string input = "SELECT {fn NOW()}";
    char output[256] = {};
    SQLINTEGER out_len = 0;
    SQLRETURN ret = SQLNativeSql(dbc, (SQLCHAR *)input.c_str(), SQL_NTS,
                                  (SQLCHAR *)output, sizeof(output), &out_len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("SELECT now()"), std::string(output));

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase13_NativeSql, DateLiteralConversion) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    std::string input = "SELECT * FROM t WHERE d = {d '2024-06-15'}";
    char output[256] = {};
    SQLINTEGER out_len = 0;
    SQLRETURN ret = SQLNativeSql(dbc, (SQLCHAR *)input.c_str(), SQL_NTS,
                                  (SQLCHAR *)output, sizeof(output), &out_len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("SELECT * FROM t WHERE d = '2024-06-15'"), std::string(output));

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLPrepare: Escape sequence processing
// ============================================================================
TEST(Phase13_Prepare, ProcessesEscapeSequences) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    std::string input = "SELECT {fn UCASE(name)} FROM t WHERE d = {d '2024-01-01'}";
    SQLRETURN ret = SQLPrepare(stmt, (SQLCHAR *)input.c_str(), SQL_NTS);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertEqual(std::string("SELECT upper(name) FROM t WHERE d = '2024-01-01'"), s->query);

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// Environment Attributes: SQL_ATTR_CP_MATCH
// ============================================================================
TEST(Phase13_EnvAttr, GetConnectionPoolingDefault) {
    SQLHENV env = SQL_NULL_HANDLE;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetEnvAttr(env, SQL_ATTR_CONNECTION_POOLING, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)SQL_CP_OFF, (__int64)val);

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase13_EnvAttr, SetConnectionPooling) {
    SQLHENV env = SQL_NULL_HANDLE;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    SQLRETURN ret = SQLSetEnvAttr(env, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_ONE_PER_DRIVER, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    SQLUINTEGER val = 0;
    ret = SQLGetEnvAttr(env, SQL_ATTR_CONNECTION_POOLING, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)SQL_CP_ONE_PER_DRIVER, (__int64)val);

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase13_EnvAttr, GetCpMatchDefault) {
    SQLHENV env = SQL_NULL_HANDLE;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetEnvAttr(env, SQL_ATTR_CP_MATCH, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)SQL_CP_STRICT_MATCH, (__int64)val);

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase13_EnvAttr, SetCpMatch) {
    SQLHENV env = SQL_NULL_HANDLE;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

    SQLRETURN ret = SQLSetEnvAttr(env, SQL_ATTR_CP_MATCH, (SQLPOINTER)SQL_CP_RELAXED_MATCH, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret);

    SQLUINTEGER val = 0;
    ret = SQLGetEnvAttr(env, SQL_ATTR_CP_MATCH, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)SQL_CP_RELAXED_MATCH, (__int64)val);

    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// SQLGetInfo: Enhanced function bitmasks
// ============================================================================
TEST(Phase13_GetInfo, StringFunctionsEnhanced) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_STRING_FUNCTIONS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    // Check that new bits are present
    AssertTrue((val & SQL_FN_STR_LEFT) != 0);
    AssertTrue((val & SQL_FN_STR_RIGHT) != 0);
    AssertTrue((val & SQL_FN_STR_ASCII) != 0);
    AssertTrue((val & SQL_FN_STR_LOCATE) != 0);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase13_GetInfo, NumericFunctionsEnhanced) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_NUMERIC_FUNCTIONS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_FN_NUM_EXP) != 0);
    AssertTrue((val & SQL_FN_NUM_LOG10) != 0);
    AssertTrue((val & SQL_FN_NUM_SIN) != 0);
    AssertTrue((val & SQL_FN_NUM_COS) != 0);
    AssertTrue((val & SQL_FN_NUM_TRUNCATE) != 0);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase13_GetInfo, TimedateFunctionsEnhanced) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_TIMEDATE_FUNCTIONS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_FN_TD_DAYOFWEEK) != 0);
    AssertTrue((val & SQL_FN_TD_DAYOFYEAR) != 0);
    AssertTrue((val & SQL_FN_TD_QUARTER) != 0);
    AssertTrue((val & SQL_FN_TD_TIMESTAMPADD) != 0);
    AssertTrue((val & SQL_FN_TD_TIMESTAMPDIFF) != 0);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase13_GetInfo, SystemFunctionsEnhanced) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_SYSTEM_FUNCTIONS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_FN_SYS_IFNULL) != 0);
    AssertTrue((val & SQL_FN_SYS_DBNAME) != 0);
    AssertTrue((val & SQL_FN_SYS_USERNAME) != 0);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase13_GetInfo, ConvertFunctionsEnhanced) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_CONVERT_FUNCTIONS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_FN_CVT_CAST) != 0);
    AssertTrue((val & SQL_FN_CVT_CONVERT) != 0);

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetDiagField: SERVER_NAME improvement
// ============================================================================
TEST(Phase13_DiagField, ServerNameFromConnection) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // Set a known host
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->host = "test-server.example.com";

    // Set a diagnostic record on the connection
    conn->SetError("HY000", "Test error");

    char buf[256] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_DBC, dbc, 1, SQL_DIAG_SERVER_NAME,
                                     buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("test-server.example.com"), std::string(buf));

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase13_DiagField, ServerNameFromStatement) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->host = "clickhouse-host";

    // Set a diagnostic record on the statement
    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test error");

    char buf[256] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_SERVER_NAME,
                                     buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("clickhouse-host"), std::string(buf));

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase13_DiagField, ConnectionNameFromStatement) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->host = "my-host";

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->SetError("HY000", "Test error");

    char buf[256] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_CONNECTION_NAME,
                                     buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("my-host"), std::string(buf));

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// Additional scalar function tests
// ============================================================================
TEST(Phase13_EscapeSeq, FnLtrim) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn LTRIM(name)} FROM t");
    AssertEqual(std::string("SELECT trimLeft(name) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnRtrim) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn RTRIM(name)} FROM t");
    AssertEqual(std::string("SELECT trimRight(name) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnConvertDate) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn CONVERT(col, SQL_DATE)} FROM t");
    AssertEqual(std::string("SELECT toDate(col) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnConvertTimestamp) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn CONVERT(col, SQL_TIMESTAMP)} FROM t");
    AssertEqual(std::string("SELECT toDateTime(col) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnLeft) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn LEFT(name, 5)} FROM t");
    AssertEqual(std::string("SELECT left(name, 5) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnRight) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn RIGHT(name, 3)} FROM t");
    AssertEqual(std::string("SELECT right(name, 3) FROM t"), result);
}

TEST(Phase13_EscapeSeq, FnReplace) {
    std::string result = ProcessOdbcEscapeSequences("SELECT {fn REPLACE(name, 'a', 'b')} FROM t");
    AssertEqual(std::string("SELECT replaceAll(name, 'a', 'b') FROM t"), result);
}
