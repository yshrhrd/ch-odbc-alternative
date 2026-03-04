// Phase 16 Tests: Parameter bind type conversion (SQL_C_DEFAULT resolution, UTF-16 support)
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
#include <cmath>

// Include driver headers
#include "../src/include/handle.h"
#include "../src/include/util.h"

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// Helper: create a BoundParameter with given types and data pointer
// ============================================================================
static BoundParameter MakeParam(SQLSMALLINT value_type, SQLSMALLINT param_type,
                                 SQLPOINTER data, SQLLEN buffer_len, SQLLEN *str_len) {
    BoundParameter bp;
    bp.value_type = value_type;
    bp.parameter_type = param_type;
    bp.parameter_value = data;
    bp.buffer_length = buffer_len;
    bp.str_len_or_ind = str_len;
    return bp;
}

// ============================================================================
// 1. ResolveCDefaultType: SQL type → default C type mapping
// ============================================================================
TEST(Phase16_ResolveCDefault, DoubleType) {
    AssertEqual((int)SQL_C_DOUBLE, (int)ResolveCDefaultType(SQL_DOUBLE));
}

TEST(Phase16_ResolveCDefault, IntegerType) {
    AssertEqual((int)SQL_C_SLONG, (int)ResolveCDefaultType(SQL_INTEGER));
}

TEST(Phase16_ResolveCDefault, VarcharType) {
    AssertEqual((int)SQL_C_CHAR, (int)ResolveCDefaultType(SQL_VARCHAR));
}

TEST(Phase16_ResolveCDefault, WVarcharType) {
    AssertEqual((int)SQL_C_WCHAR, (int)ResolveCDefaultType(SQL_WVARCHAR));
}

TEST(Phase16_ResolveCDefault, SmallintType) {
    AssertEqual((int)SQL_C_SSHORT, (int)ResolveCDefaultType(SQL_SMALLINT));
}

TEST(Phase16_ResolveCDefault, BigintType) {
    AssertEqual((int)SQL_C_SBIGINT, (int)ResolveCDefaultType(SQL_BIGINT));
}

TEST(Phase16_ResolveCDefault, RealType) {
    AssertEqual((int)SQL_C_FLOAT, (int)ResolveCDefaultType(SQL_REAL));
}

TEST(Phase16_ResolveCDefault, FloatType) {
    // ODBC SQL_FLOAT is double precision (8 bytes)
    AssertEqual((int)SQL_C_DOUBLE, (int)ResolveCDefaultType(SQL_FLOAT));
}

TEST(Phase16_ResolveCDefault, TimestampType) {
    AssertEqual((int)SQL_C_TYPE_TIMESTAMP, (int)ResolveCDefaultType(SQL_TYPE_TIMESTAMP));
}

TEST(Phase16_ResolveCDefault, DateType) {
    AssertEqual((int)SQL_C_TYPE_DATE, (int)ResolveCDefaultType(SQL_TYPE_DATE));
}

TEST(Phase16_ResolveCDefault, BinaryType) {
    AssertEqual((int)SQL_C_BINARY, (int)ResolveCDefaultType(SQL_BINARY));
}

TEST(Phase16_ResolveCDefault, GuidType) {
    AssertEqual((int)SQL_C_GUID, (int)ResolveCDefaultType(SQL_GUID));
}

TEST(Phase16_ResolveCDefault, NumericType) {
    AssertEqual((int)SQL_C_CHAR, (int)ResolveCDefaultType(SQL_NUMERIC));
}

TEST(Phase16_ResolveCDefault, TinyintType) {
    AssertEqual((int)SQL_C_STINYINT, (int)ResolveCDefaultType(SQL_TINYINT));
}

TEST(Phase16_ResolveCDefault, BitType) {
    AssertEqual((int)SQL_C_BIT, (int)ResolveCDefaultType(SQL_BIT));
}

// ============================================================================
// 2. ExtractParameterValue with SQL_C_DEFAULT: binary → string conversion
//    This is the core fix for MS Access parameter binding
// ============================================================================
TEST(Phase16_ParamConvert, CDefault_Double) {
    // MS Access binds SQL_C_DEFAULT + SQL_DOUBLE with 8 bytes IEEE 754
    double val = 9.5;
    SQLLEN ind = sizeof(double);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_DOUBLE, &val, sizeof(double), &ind);
    std::string result = ExtractParameterValue(bp);
    // Should be numeric string, not raw bytes
    double parsed = std::stod(result);
    AssertTrue(std::abs(parsed - 9.5) < 0.001, "SQL_C_DEFAULT+SQL_DOUBLE should produce '9.5'");
}

TEST(Phase16_ParamConvert, CDefault_DoubleNegative) {
    double val = -123.456;
    SQLLEN ind = sizeof(double);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_DOUBLE, &val, sizeof(double), &ind);
    std::string result = ExtractParameterValue(bp);
    double parsed = std::stod(result);
    AssertTrue(std::abs(parsed - (-123.456)) < 0.001, "Negative double should convert correctly");
}

TEST(Phase16_ParamConvert, CDefault_Integer) {
    SQLINTEGER val = 42;
    SQLLEN ind = sizeof(SQLINTEGER);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_INTEGER, &val, sizeof(SQLINTEGER), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("42"), result);
}

TEST(Phase16_ParamConvert, CDefault_IntegerNegative) {
    SQLINTEGER val = -100;
    SQLLEN ind = sizeof(SQLINTEGER);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_INTEGER, &val, sizeof(SQLINTEGER), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("-100"), result);
}

TEST(Phase16_ParamConvert, CDefault_Smallint) {
    short val = 256;
    SQLLEN ind = sizeof(short);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_SMALLINT, &val, sizeof(short), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("256"), result);
}

TEST(Phase16_ParamConvert, CDefault_Bigint) {
    int64_t val = 1234567890123LL;
    SQLLEN ind = sizeof(int64_t);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_BIGINT, &val, sizeof(int64_t), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("1234567890123"), result);
}

TEST(Phase16_ParamConvert, CDefault_Real) {
    float val = 3.14f;
    SQLLEN ind = sizeof(float);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_REAL, &val, sizeof(float), &ind);
    std::string result = ExtractParameterValue(bp);
    float parsed = std::stof(result);
    AssertTrue(std::abs(parsed - 3.14f) < 0.01f, "SQL_C_DEFAULT+SQL_REAL should produce float value");
}

TEST(Phase16_ParamConvert, CDefault_Float) {
    // ODBC SQL_FLOAT = double precision
    double val = 2.71828;
    SQLLEN ind = sizeof(double);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_FLOAT, &val, sizeof(double), &ind);
    std::string result = ExtractParameterValue(bp);
    double parsed = std::stod(result);
    AssertTrue(std::abs(parsed - 2.71828) < 0.001, "SQL_C_DEFAULT+SQL_FLOAT should produce double value");
}

TEST(Phase16_ParamConvert, CDefault_Varchar) {
    // SQL_C_DEFAULT + SQL_VARCHAR should resolve to SQL_C_CHAR
    const char *val = "hello";
    SQLLEN ind = SQL_NTS;
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_VARCHAR, (SQLPOINTER)val, 6, &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("'hello'"), result);
}

TEST(Phase16_ParamConvert, CDefault_WVarchar) {
    // SQL_C_DEFAULT + SQL_WVARCHAR should resolve to SQL_C_WCHAR
    const wchar_t *val = L"world";
    SQLLEN ind = 5 * sizeof(SQLWCHAR);  // byte length
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_WVARCHAR, (SQLPOINTER)val, ind, &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("'world'"), result);
}

TEST(Phase16_ParamConvert, CDefault_Tinyint) {
    signed char val = -5;
    SQLLEN ind = sizeof(signed char);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_TINYINT, &val, sizeof(signed char), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("-5"), result);
}

TEST(Phase16_ParamConvert, CDefault_Bit) {
    signed char val = 1;
    SQLLEN ind = sizeof(signed char);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_BIT, &val, sizeof(signed char), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("1"), result);
}

// ============================================================================
// 3. Explicit type tests (non SQL_C_DEFAULT) still work
// ============================================================================
TEST(Phase16_ParamConvert, ExplicitDouble) {
    double val = 99.99;
    SQLLEN ind = sizeof(double);
    auto bp = MakeParam(SQL_C_DOUBLE, SQL_DOUBLE, &val, sizeof(double), &ind);
    std::string result = ExtractParameterValue(bp);
    double parsed = std::stod(result);
    AssertTrue(std::abs(parsed - 99.99) < 0.01, "Explicit SQL_C_DOUBLE works");
}

TEST(Phase16_ParamConvert, ExplicitInteger) {
    SQLINTEGER val = 7;
    SQLLEN ind = sizeof(SQLINTEGER);
    auto bp = MakeParam(SQL_C_SLONG, SQL_INTEGER, &val, sizeof(SQLINTEGER), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("7"), result);
}

TEST(Phase16_ParamConvert, ExplicitChar) {
    const char *val = "test'val";
    SQLLEN ind = SQL_NTS;
    auto bp = MakeParam(SQL_C_CHAR, SQL_VARCHAR, (SQLPOINTER)val, 9, &ind);
    std::string result = ExtractParameterValue(bp);
    // Single quote should be escaped
    AssertEqual(std::string("'test\\'val'"), result);
}

// ============================================================================
// 4. NULL parameter handling
// ============================================================================
TEST(Phase16_ParamConvert, NullData) {
    SQLLEN ind = SQL_NULL_DATA;
    int dummy = 0;
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_INTEGER, &dummy, sizeof(int), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("NULL"), result);
}

TEST(Phase16_ParamConvert, NullPointer) {
    SQLLEN ind = 0;
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_INTEGER, nullptr, 0, &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("NULL"), result);
}

// ============================================================================
// 5. SQL_C_WCHAR UTF-16 → UTF-8 conversion (explicit)
// ============================================================================
TEST(Phase16_WCharParam, BasicAscii) {
    const wchar_t *val = L"Hello";
    SQLLEN ind = 5 * sizeof(SQLWCHAR);
    auto bp = MakeParam(SQL_C_WCHAR, SQL_WVARCHAR, (SQLPOINTER)val, ind, &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("'Hello'"), result);
}

TEST(Phase16_WCharParam, JapaneseText) {
    // テスト = "test" in Japanese (UTF-16)
    const wchar_t *val = L"\u30C6\u30B9\u30C8";  // テスト
    SQLLEN ind = 3 * sizeof(SQLWCHAR);
    auto bp = MakeParam(SQL_C_WCHAR, SQL_WVARCHAR, (SQLPOINTER)val, ind, &ind);
    std::string result = ExtractParameterValue(bp);
    // Should be UTF-8 encoded Japanese in quotes
    AssertTrue(result.front() == '\'' && result.back() == '\'', "Should be quoted");
    // テスト in UTF-8 is 9 bytes (3 chars × 3 bytes each)
    // result = 'テスト' = 1 + 9 + 1 = 11 chars
    AssertEqual((int)11, (int)result.size());
}

TEST(Phase16_WCharParam, NullTerminated) {
    const wchar_t *val = L"NTS";
    SQLLEN ind = SQL_NTS;
    auto bp = MakeParam(SQL_C_WCHAR, SQL_WVARCHAR, (SQLPOINTER)val, 0, &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("'NTS'"), result);
}

TEST(Phase16_WCharParam, EmptyString) {
    const wchar_t *val = L"";
    SQLLEN ind = 0;
    auto bp = MakeParam(SQL_C_WCHAR, SQL_WVARCHAR, (SQLPOINTER)val, 0, &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("''"), result);
}

TEST(Phase16_WCharParam, SpecialCharsEscaped) {
    // Test that single quotes in wide strings are escaped
    const wchar_t *val = L"it's";
    SQLLEN ind = 4 * sizeof(SQLWCHAR);
    auto bp = MakeParam(SQL_C_WCHAR, SQL_WVARCHAR, (SQLPOINTER)val, ind, &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("'it\\'s'"), result);
}

// ============================================================================
// 6. SubstituteParameters integration test
// ============================================================================
TEST(Phase16_Substitute, DoubleParam) {
    double val = 9.5;
    SQLLEN ind = sizeof(double);
    BoundParameter bp;
    bp.value_type = SQL_C_DEFAULT;
    bp.parameter_type = SQL_DOUBLE;
    bp.parameter_value = &val;
    bp.buffer_length = sizeof(double);
    bp.str_len_or_ind = &ind;

    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    params[1] = bp;

    std::string error_msg;
    std::string result = SubstituteParameters("SELECT * FROM t WHERE x = ?", params, error_msg);
    AssertTrue(error_msg.empty(), "No error expected");
    AssertTrue(result.find("9.5") != std::string::npos,
               "Substituted query should contain '9.5'");
    // Should NOT contain raw binary bytes
    AssertTrue(result.find('\0') == std::string::npos,
               "Should not contain null bytes from raw binary");
}

TEST(Phase16_Substitute, MultipleParams) {
    SQLINTEGER id = 42;
    SQLLEN ind1 = sizeof(SQLINTEGER);
    BoundParameter bp1;
    bp1.value_type = SQL_C_DEFAULT;
    bp1.parameter_type = SQL_INTEGER;
    bp1.parameter_value = &id;
    bp1.buffer_length = sizeof(SQLINTEGER);
    bp1.str_len_or_ind = &ind1;

    const wchar_t *name = L"test";
    SQLLEN ind2 = 4 * sizeof(SQLWCHAR);
    BoundParameter bp2;
    bp2.value_type = SQL_C_WCHAR;
    bp2.parameter_type = SQL_WVARCHAR;
    bp2.parameter_value = (SQLPOINTER)name;
    bp2.buffer_length = ind2;
    bp2.str_len_or_ind = &ind2;

    std::unordered_map<SQLUSMALLINT, BoundParameter> params;
    params[1] = bp1;
    params[2] = bp2;

    std::string error_msg;
    std::string result = SubstituteParameters("INSERT INTO t VALUES (?, ?)", params, error_msg);
    AssertTrue(error_msg.empty(), "No error expected");
    AssertTrue(result.find("42") != std::string::npos, "Should contain integer value");
    AssertTrue(result.find("'test'") != std::string::npos, "Should contain quoted string value");
}

// ============================================================================
// 7. Date/Time with SQL_C_DEFAULT
// ============================================================================
TEST(Phase16_ParamConvert, CDefault_Date) {
    SQL_DATE_STRUCT ds;
    ds.year = 2025;
    ds.month = 6;
    ds.day = 15;
    SQLLEN ind = sizeof(SQL_DATE_STRUCT);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_TYPE_DATE, &ds, sizeof(SQL_DATE_STRUCT), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("'2025-06-15'"), result);
}

TEST(Phase16_ParamConvert, CDefault_Timestamp) {
    SQL_TIMESTAMP_STRUCT ts;
    ts.year = 2025;
    ts.month = 1;
    ts.day = 15;
    ts.hour = 10;
    ts.minute = 30;
    ts.second = 45;
    ts.fraction = 0;
    SQLLEN ind = sizeof(SQL_TIMESTAMP_STRUCT);
    auto bp = MakeParam(SQL_C_DEFAULT, SQL_TYPE_TIMESTAMP, &ts, sizeof(SQL_TIMESTAMP_STRUCT), &ind);
    std::string result = ExtractParameterValue(bp);
    AssertEqual(std::string("'2025-01-15 10:30:45'"), result);
}
