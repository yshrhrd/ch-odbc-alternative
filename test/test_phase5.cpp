// Phase 5 Tests: Parameterized queries, trace logging, ConfigDSN
#include "test_framework.h"

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include <string>
#include <unordered_map>
#include <cstring>
#include <optional>
#include <vector>

// Include driver headers
#include "../src/include/handle.h"
#include "../src/include/util.h"
#include "../src/include/trace.h"
#include "../src/include/type_mapping.h"

#ifdef UNICODE
#undef SQLSetConnectAttr
#undef SQLGetConnectAttr
#undef SQLPrepare
#undef SQLExecDirect
#undef SQLGetDiagRec
#undef SQLFreeStmt
#undef SQLDriverConnect
#endif

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLPrepare(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLBindParameter(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                               SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLPOINTER,
                                               SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLNumParams(SQLHSTMT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// Parameter extraction test
// ============================================================================

TEST(Phase5_Params, ExtractParameterValue_Char) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_CHAR;
    char val[] = "hello";
    bp.parameter_value = val;
    SQLLEN len = SQL_NTS;
    bp.str_len_or_ind = &len;
    bp.buffer_length = sizeof(val);

    std::string result = ExtractParameterValue(bp);
    AssertEqual("'hello'", result, "SQL_C_CHAR should be quoted string");
}

TEST(Phase5_Params, ExtractParameterValue_Char_WithLength) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_CHAR;
    char val[] = "hello world";
    bp.parameter_value = val;
    SQLLEN len = 5; // only "hello"
    bp.str_len_or_ind = &len;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("'hello'", result, "SQL_C_CHAR with explicit length");
}

TEST(Phase5_Params, ExtractParameterValue_NULL) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_CHAR;
    bp.parameter_value = (SQLPOINTER)"test";
    SQLLEN ind = SQL_NULL_DATA;
    bp.str_len_or_ind = &ind;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("NULL", result, "SQL_NULL_DATA should produce NULL");
}

TEST(Phase5_Params, ExtractParameterValue_NullPointer) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_CHAR;
    bp.parameter_value = nullptr;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("NULL", result, "null parameter_value should produce NULL");
}

TEST(Phase5_Params, ExtractParameterValue_SLONG) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_SLONG;
    SQLINTEGER val = 42;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("42", result, "SQL_C_SLONG integer");
}

TEST(Phase5_Params, ExtractParameterValue_NegativeLONG) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_SLONG;
    SQLINTEGER val = -12345;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("-12345", result, "SQL_C_SLONG negative");
}

TEST(Phase5_Params, ExtractParameterValue_SBIGINT) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_SBIGINT;
    int64_t val = 9876543210LL;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("9876543210", result, "SQL_C_SBIGINT large value");
}

TEST(Phase5_Params, ExtractParameterValue_Double) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_DOUBLE;
    double val = 3.14;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    // Just check it starts with 3.14
    AssertTrue(result.find("3.14") == 0, "SQL_C_DOUBLE should contain 3.14");
}

TEST(Phase5_Params, ExtractParameterValue_Double_WholeNumber) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_DOUBLE;
    double val = 12345.0;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("12345", result, "SQL_C_DOUBLE whole number should not have decimals");
}

TEST(Phase5_Params, ExtractParameterValue_Double_Zero) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_DOUBLE;
    double val = 0.0;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("0", result, "SQL_C_DOUBLE zero should be '0'");
}

TEST(Phase5_Params, ExtractParameterValue_Double_NegativeWhole) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_DOUBLE;
    double val = -999.0;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("-999", result, "SQL_C_DOUBLE negative whole number");
}

TEST(Phase5_Params, ExtractParameterValue_Double_LargeWhole) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_DOUBLE;
    double val = 9876543210.0;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("9876543210", result, "SQL_C_DOUBLE large whole number (Access product code pattern)");
}

TEST(Phase5_Params, ExtractParameterValue_Float_WholeNumber) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_FLOAT;
    float val = 42.0f;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("42", result, "SQL_C_FLOAT whole number should not have decimals");
}

TEST(Phase5_Params, ExtractParameterValue_CDefault_Double) {
    // Simulates Access binding: SQL_C_DEFAULT with parameter_type=SQL_DOUBLE
    BoundParameter bp = {};
    bp.value_type = SQL_C_DEFAULT;
    bp.parameter_type = SQL_DOUBLE;
    double val = 12345.0;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("12345", result, "SQL_C_DEFAULT/SQL_DOUBLE whole number (Access dynaset pattern)");
}

TEST(Phase5_Params, ExtractParameterValue_CDefault_Double_RealProductCode) {
    // Real product code value (900218158) bound by Access as SQL_C_DEFAULT/SQL_DOUBLE
    BoundParameter bp = {};
    bp.value_type = SQL_C_DEFAULT;
    bp.parameter_type = SQL_DOUBLE;
    double val = 900218158.0;
    bp.parameter_value = &val;
    SQLLEN len = sizeof(double);
    bp.str_len_or_ind = &len;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("900218158", result, "商品CD 900218158 should be exact integer string");
}

TEST(Phase5_Params, ExtractParameterValue_BIT) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_BIT;
    signed char val = 1;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("1", result, "SQL_C_BIT value 1");
}

TEST(Phase5_Params, ExtractParameterValue_DATE) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_TYPE_DATE;
    SQL_DATE_STRUCT ds = {};
    ds.year = 2025;
    ds.month = 7;
    ds.day = 12;
    bp.parameter_value = &ds;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("'2025-07-12'", result, "SQL_C_TYPE_DATE");
}

TEST(Phase5_Params, ExtractParameterValue_TIME) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_TYPE_TIME;
    SQL_TIME_STRUCT ts = {};
    ts.hour = 14;
    ts.minute = 30;
    ts.second = 45;
    bp.parameter_value = &ts;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("'14:30:45'", result, "SQL_C_TYPE_TIME");
}

TEST(Phase5_Params, ExtractParameterValue_TIMESTAMP) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_TYPE_TIMESTAMP;
    SQL_TIMESTAMP_STRUCT ts = {};
    ts.year = 2025;
    ts.month = 7;
    ts.day = 12;
    ts.hour = 14;
    ts.minute = 30;
    ts.second = 45;
    ts.fraction = 0;
    bp.parameter_value = &ts;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("'2025-07-12 14:30:45'", result, "SQL_C_TYPE_TIMESTAMP without fraction");
}

TEST(Phase5_Params, ExtractParameterValue_TIMESTAMP_WithFraction) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_TYPE_TIMESTAMP;
    SQL_TIMESTAMP_STRUCT ts = {};
    ts.year = 2025;
    ts.month = 1;
    ts.day = 1;
    ts.hour = 0;
    ts.minute = 0;
    ts.second = 0;
    ts.fraction = 123000000;
    bp.parameter_value = &ts;

    std::string result = ExtractParameterValue(bp);
    AssertTrue(result.find("123000000") != std::string::npos, "TIMESTAMP should include fraction");
}

TEST(Phase5_Params, ExtractParameterValue_GUID) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_GUID;
    SQLGUID guid = {};
    guid.Data1 = 0x12345678;
    guid.Data2 = 0xABCD;
    guid.Data3 = 0xEF01;
    guid.Data4[0] = 0x23;
    guid.Data4[1] = 0x45;
    guid.Data4[2] = 0x67;
    guid.Data4[3] = 0x89;
    guid.Data4[4] = 0xAB;
    guid.Data4[5] = 0xCD;
    guid.Data4[6] = 0xEF;
    guid.Data4[7] = 0x01;
    bp.parameter_value = &guid;

    std::string result = ExtractParameterValue(bp);
    AssertTrue(result.find("12345678") != std::string::npos, "GUID should contain Data1");
    AssertTrue(result.find("ABCD") != std::string::npos, "GUID should contain Data2");
}

// ============================================================================
// SQL string escape test
// ============================================================================

TEST(Phase5_Escape, EscapeSqlString_NoSpecialChars) {
    std::string result = EscapeSqlString("hello world");
    AssertEqual("hello world", result, "No escaping needed");
}

TEST(Phase5_Escape, EscapeSqlString_SingleQuote) {
    std::string result = EscapeSqlString("it's");
    AssertEqual("it\\'s", result, "Single quote escaped");
}

TEST(Phase5_Escape, EscapeSqlString_Backslash) {
    std::string result = EscapeSqlString("path\\to\\file");
    AssertEqual("path\\\\to\\\\file", result, "Backslash escaped");
}

TEST(Phase5_Escape, EscapeSqlString_Both) {
    std::string result = EscapeSqlString("it's a \\test");
    AssertEqual("it\\'s a \\\\test", result, "Both single quote and backslash escaped");
}

TEST(Phase5_Escape, EscapeSqlString_Empty) {
    std::string result = EscapeSqlString("");
    AssertEqual("", result, "Empty string unchanged");
}

// ============================================================================
// Parameter substitution test
// ============================================================================

TEST(Phase5_Substitute, SimpleIntParam) {
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    BoundParameter bp = {};
    bp.value_type = SQL_C_SLONG;
    SQLINTEGER val = 42;
    bp.parameter_value = &val;
    params[1] = bp;

    std::string error_msg;
    std::string result = SubstituteParameters("SELECT * FROM t WHERE id = ?", params, error_msg);
    AssertEqual("SELECT * FROM t WHERE id = 42", result, "Integer substitution");
    AssertTrue(error_msg.empty(), "No error expected");
}

TEST(Phase5_Substitute, MultipleParams) {
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;

    // Param 1: string
    BoundParameter bp1 = {};
    bp1.value_type = SQL_C_CHAR;
    char name[] = "Alice";
    bp1.parameter_value = name;
    SQLLEN len1 = SQL_NTS;
    bp1.str_len_or_ind = &len1;
    params[1] = bp1;

    // Param 2: int
    BoundParameter bp2 = {};
    bp2.value_type = SQL_C_SLONG;
    SQLINTEGER age = 30;
    bp2.parameter_value = &age;
    params[2] = bp2;

    std::string error_msg;
    std::string result = SubstituteParameters("SELECT * FROM t WHERE name = ? AND age > ?", params, error_msg);
    AssertEqual("SELECT * FROM t WHERE name = 'Alice' AND age > 30", result, "Multiple params");
}

TEST(Phase5_Substitute, NullParam) {
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    BoundParameter bp = {};
    bp.value_type = SQL_C_CHAR;
    bp.parameter_value = (SQLPOINTER)"x";
    SQLLEN ind = SQL_NULL_DATA;
    bp.str_len_or_ind = &ind;
    params[1] = bp;

    std::string error_msg;
    std::string result = SubstituteParameters("INSERT INTO t VALUES (?)", params, error_msg);
    AssertEqual("INSERT INTO t VALUES (NULL)", result, "NULL param substitution");
}

TEST(Phase5_Substitute, NoParams) {
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    std::string error_msg;
    std::string result = SubstituteParameters("SELECT 1", params, error_msg);
    AssertEqual("SELECT 1", result, "No params returns original query");
}

TEST(Phase5_Substitute, QuestionMarkInString) {
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    BoundParameter bp = {};
    bp.value_type = SQL_C_SLONG;
    SQLINTEGER val = 1;
    bp.parameter_value = &val;
    params[1] = bp;

    std::string error_msg;
    std::string result = SubstituteParameters("SELECT '?' AS q, ? AS val", params, error_msg);
    AssertEqual("SELECT '?' AS q, 1 AS val", result, "? inside string literal not substituted");
}

TEST(Phase5_Substitute, MissingParam) {
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    // Bind param 2 only, but query has 2 placeholders -> param 1 is missing
    BoundParameter bp = {};
    bp.value_type = SQL_C_SLONG;
    SQLINTEGER val = 99;
    bp.parameter_value = &val;
    params[2] = bp;

    std::string error_msg;
    std::string result = SubstituteParameters("SELECT ?, ?", params, error_msg);
    AssertTrue(result.empty(), "Result should be empty on error");
    AssertTrue(error_msg.find("not bound") != std::string::npos, "Error message should mention 'not bound'");
}

TEST(Phase5_Substitute, EscapedStringParam) {
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    BoundParameter bp = {};
    bp.value_type = SQL_C_CHAR;
    char val[] = "O'Brien";
    bp.parameter_value = val;
    SQLLEN len = SQL_NTS;
    bp.str_len_or_ind = &len;
    params[1] = bp;

    std::string error_msg;
    std::string result = SubstituteParameters("SELECT * FROM t WHERE name = ?", params, error_msg);
    AssertEqual("SELECT * FROM t WHERE name = 'O\\'Brien'", result, "Quote in param value escaped");
}

TEST(Phase5_Substitute, DateParam) {
    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    BoundParameter bp = {};
    bp.value_type = SQL_C_TYPE_DATE;
    SQL_DATE_STRUCT ds = {};
    ds.year = 2025;
    ds.month = 12;
    ds.day = 25;
    bp.parameter_value = &ds;
    params[1] = bp;

    std::string error_msg;
    std::string result = SubstituteParameters("SELECT * FROM t WHERE dt = ?", params, error_msg);
    AssertEqual("SELECT * FROM t WHERE dt = '2025-12-25'", result, "Date param substitution");
}

// ============================================================================
// Trace log test
// ============================================================================

TEST(Phase5_Trace, TraceLog_Singleton) {
    TraceLog &log1 = TraceLog::Instance();
    TraceLog &log2 = TraceLog::Instance();
    AssertTrue(&log1 == &log2, "TraceLog should be a singleton");
}

TEST(Phase5_Trace, TraceLog_EnableDisable) {
    TraceLog &log = TraceLog::Instance();

    log.SetEnabled(false);
    AssertFalse(log.IsEnabled(), "Trace should be disabled");

    log.SetEnabled(true);
    AssertTrue(log.IsEnabled(), "Trace should be enabled");

    log.SetEnabled(false);
    AssertFalse(log.IsEnabled(), "Trace should be disabled again");
}

TEST(Phase5_Trace, TraceLog_Level) {
    TraceLog &log = TraceLog::Instance();

    log.SetLevel(TraceLevel::Error);
    AssertTrue(log.GetLevel() == TraceLevel::Error, "Level should be Error");

    log.SetLevel(TraceLevel::Verbose);
    AssertTrue(log.GetLevel() == TraceLevel::Verbose, "Level should be Verbose");

    log.SetLevel(TraceLevel::Info);
    AssertTrue(log.GetLevel() == TraceLevel::Info, "Level should be Info (reset)");
}

TEST(Phase5_Trace, SqlReturnToString) {
    AssertEqual("SQL_SUCCESS", TraceLog::SqlReturnToString(SQL_SUCCESS));
    AssertEqual("SQL_SUCCESS_WITH_INFO", TraceLog::SqlReturnToString(SQL_SUCCESS_WITH_INFO));
    AssertEqual("SQL_ERROR", TraceLog::SqlReturnToString(SQL_ERROR));
    AssertEqual("SQL_INVALID_HANDLE", TraceLog::SqlReturnToString(SQL_INVALID_HANDLE));
    AssertEqual("SQL_NO_DATA", TraceLog::SqlReturnToString(SQL_NO_DATA));
    AssertEqual("SQL_NEED_DATA", TraceLog::SqlReturnToString(SQL_NEED_DATA));
}

TEST(Phase5_Trace, TraceLog_LogDoesNotCrash) {
    TraceLog &log = TraceLog::Instance();
    log.SetEnabled(true);
    log.SetLevel(TraceLevel::Verbose);

    // These should not crash
    log.Log(TraceLevel::Info, "TestFunc", "Test message");
    log.LogEntry("TestFunc", "param1=value1");
    log.LogExit("TestFunc", SQL_SUCCESS);

    log.SetEnabled(false);
}

TEST(Phase5_Trace, TraceLog_FileOutput) {
    TraceLog &log = TraceLog::Instance();

    // Test setting and closing trace file (with temp file)
    std::string tmp_path = "test_trace_output.log";
    log.SetTraceFile(tmp_path);
    log.SetEnabled(true);
    log.SetLevel(TraceLevel::Debug);

    log.Log(TraceLevel::Info, "TestFunc", "File output test");
    log.LogEntry("TestFunc2", "x=1");
    log.LogExit("TestFunc2", SQL_ERROR);

    log.CloseTraceFile();
    log.SetEnabled(false);

    // Verify file was created and has content
    FILE *f = fopen(tmp_path.c_str(), "r");
    AssertTrue(f != nullptr, "Trace log file should exist");
    if (f) {
        fseek(f, 0, SEEK_END);
        long size = ftell(f);
        fclose(f);
        AssertTrue(size > 0, "Trace log file should have content");
    }

    // Cleanup
    remove(tmp_path.c_str());
}

// ============================================================================
// ODBC API trace attribute test
// ============================================================================

TEST(Phase5_TraceAttr, SetTrace_EnableViaConnAttr) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);

    // Enable trace via attribute
    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_TRACE, (SQLPOINTER)SQL_OPT_TRACE_ON, 0);
    AssertTrue(ret == SQL_SUCCESS, "SQLSetConnectAttr TRACE should succeed");
    AssertTrue(conn->trace_enabled, "trace_enabled should be true");

    // Read back
    SQLUINTEGER trace_val = 0;
    ret = SQLGetConnectAttr(dbc, SQL_ATTR_TRACE, &trace_val, sizeof(trace_val), nullptr);
    AssertTrue(ret == SQL_SUCCESS, "SQLGetConnectAttr TRACE should succeed");
    AssertTrue(trace_val == SQL_OPT_TRACE_ON, "Trace should be ON");

    // Disable
    ret = SQLSetConnectAttr(dbc, SQL_ATTR_TRACE, (SQLPOINTER)SQL_OPT_TRACE_OFF, 0);
    AssertTrue(ret == SQL_SUCCESS, "Disable trace should succeed");
    AssertFalse(conn->trace_enabled, "trace_enabled should be false");

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase5_TraceAttr, SetTraceFile_ViaConnAttr) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);

    // Set trace file
    const char *path = "C:\\temp\\odbc_trace.log";
    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_TRACEFILE, (SQLPOINTER)path, SQL_NTS);
    AssertTrue(ret == SQL_SUCCESS, "SQLSetConnectAttr TRACEFILE should succeed");
    AssertEqual(std::string(path), conn->trace_file, "trace_file should match");

    // Read back
    char buf[256] = {};
    ret = SQLGetConnectAttr(dbc, SQL_ATTR_TRACEFILE, buf, sizeof(buf), nullptr);
    AssertTrue(ret == SQL_SUCCESS, "SQLGetConnectAttr TRACEFILE should succeed");
    AssertEqual(std::string(path), std::string(buf), "Trace file path should match");

    // Cleanup trace state
    TraceLog::Instance().CloseTraceFile();
    TraceLog::Instance().SetEnabled(false);

    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// SQLBindParameter ODBC API test
// ============================================================================

TEST(Phase5_BindParam, BindParameter_Basic) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLHSTMT stmt_h = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    // Manually set connected to allow stmt allocation
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    SQLINTEGER val = 42;
    SQLLEN len = sizeof(val);
    SQLRETURN ret = SQLBindParameter(stmt_h, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER,
                                      0, 0, &val, sizeof(val), &len);
    AssertTrue(ret == SQL_SUCCESS, "SQLBindParameter should succeed");

    // Check that parameter was stored
    auto *stmt = static_cast<OdbcStatement *>(stmt_h);
    AssertTrue(stmt->bound_parameters.count(1) == 1, "Parameter 1 should be bound");
    AssertTrue(stmt->bound_parameters[1].value_type == SQL_C_SLONG, "Value type should be SQL_C_SLONG");

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase5_BindParam, BindMultipleParams) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLHSTMT stmt_h = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    SQLINTEGER int_val = 100;
    SQLLEN int_len = sizeof(int_val);
    SQLBindParameter(stmt_h, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &int_val, sizeof(int_val), &int_len);

    char str_val[] = "test";
    SQLLEN str_len = SQL_NTS;
    SQLBindParameter(stmt_h, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 255, 0, str_val, sizeof(str_val), &str_len);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);
    AssertTrue(stmt->bound_parameters.size() == 2, "Should have 2 bound parameters");
    AssertTrue(stmt->bound_parameters[1].value_type == SQL_C_SLONG, "Param 1 type");
    AssertTrue(stmt->bound_parameters[2].value_type == SQL_C_CHAR, "Param 2 type");

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase5_BindParam, NumParams_Count) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLHSTMT stmt_h = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    // Prepare a query with 3 parameters
    SQLPrepare(stmt_h, (SQLCHAR *)"SELECT * FROM t WHERE a=? AND b=? AND c=?", SQL_NTS);

    SQLSMALLINT param_count = 0;
    SQLRETURN ret = SQLNumParams(stmt_h, &param_count);
    AssertTrue(ret == SQL_SUCCESS, "SQLNumParams should succeed");
    AssertEqual(3, (int)param_count, "Should have 3 parameters");

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// Parameter type variation test
// ============================================================================

TEST(Phase5_ParamTypes, UnsignedShort) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_USHORT;
    unsigned short val = 65535;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("65535", result, "SQL_C_USHORT max value");
}

TEST(Phase5_ParamTypes, UnsignedLong) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_ULONG;
    SQLUINTEGER val = 4294967295U;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("4294967295", result, "SQL_C_ULONG max value");
}

TEST(Phase5_ParamTypes, UnsignedBigInt) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_UBIGINT;
    uint64_t val = 18446744073709551615ULL;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("18446744073709551615", result, "SQL_C_UBIGINT max value");
}

TEST(Phase5_ParamTypes, Float) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_FLOAT;
    float val = 1.5f;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertTrue(result.find("1.5") != std::string::npos, "SQL_C_FLOAT 1.5");
}

TEST(Phase5_ParamTypes, UTinyInt) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_UTINYINT;
    unsigned char val = 255;
    bp.parameter_value = &val;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("255", result, "SQL_C_UTINYINT max value");
}

TEST(Phase5_ParamTypes, Binary) {
    BoundParameter bp = {};
    bp.value_type = SQL_C_BINARY;
    unsigned char data[] = {0xDE, 0xAD, 0xBE, 0xEF};
    bp.parameter_value = data;
    bp.buffer_length = 4;
    SQLLEN len = 4;
    bp.str_len_or_ind = &len;

    std::string result = ExtractParameterValue(bp);
    AssertEqual("unhex('DEADBEEF')", result, "SQL_C_BINARY as hex");
}

// ============================================================================
// prepared_query template preservation test
// Verify that prepared_query retains ? placeholders even after repeated SQLExecute calls
// ============================================================================

TEST(Phase5_PreparedQuery, TemplatePreservedAfterPrepare) {
    SQLHENV env = nullptr;
    SQLHDBC dbc = nullptr;
    SQLHSTMT stmt_h = nullptr;

    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt_h);

    // SQLPrepare should set both query and prepared_query
    SQLPrepare(stmt_h, (SQLCHAR *)"SELECT * FROM t WHERE id = ?", SQL_NTS);

    auto *stmt = static_cast<OdbcStatement *>(stmt_h);
    AssertEqual(std::string("SELECT * FROM t WHERE id = ?"), stmt->prepared_query,
                "prepared_query should contain ? placeholder");
    AssertTrue(stmt->prepared, "Statement should be marked as prepared");

    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_STMT, stmt_h);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

TEST(Phase5_PreparedQuery, SubstituteUsesTemplate) {
    // Verify that SubstituteParameters works correctly even after stmt->query is overwritten
    // This simulates the Access dynaset pattern:
    //   1. SQLPrepare("SELECT ... WHERE id = ?")
    //   2. Bind param 1 = 12345
    //   3. SQLExecute -> ExecDirectImpl overwrites stmt->query
    //   4. Change param value to 67890
    //   5. SQLExecute should use prepared_query (with ?) not stmt->query (without ?)

    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    BoundParameter bp = {};
    bp.value_type = SQL_C_DOUBLE;
    double val = 12345.0;
    bp.parameter_value = &val;
    params[1] = bp;

    std::string template_query = "SELECT * FROM M_ITEM WHERE 商品CD = ?";
    std::string error_msg;

    // First substitution
    std::string result1 = SubstituteParameters(template_query, params, error_msg);
    AssertEqual("SELECT * FROM M_ITEM WHERE 商品CD = 12345", result1, "First substitution");

    // Simulate ExecDirectImpl overwriting stmt->query (this is what was causing the bug)
    std::string overwritten_query = result1; // "SELECT * FROM M_ITEM WHERE 商品CD = 12345"

    // Second substitution using WRONG query (overwritten) - demonstrates the bug
    val = 67890.0;
    std::string result_wrong = SubstituteParameters(overwritten_query, params, error_msg);
    // This returns the old query because there's no ? to substitute
    AssertEqual("SELECT * FROM M_ITEM WHERE 商品CD = 12345", result_wrong,
                "Bug: overwritten query has no ? placeholder");

    // Second substitution using CORRECT query (prepared_query) - demonstrates the fix
    std::string result_correct = SubstituteParameters(template_query, params, error_msg);
    AssertEqual("SELECT * FROM M_ITEM WHERE 商品CD = 67890", result_correct,
                "Fix: prepared_query preserves ? placeholder");
}
