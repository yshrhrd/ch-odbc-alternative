#include "test_framework.h"
#include "../src/include/handle.h"
#include "../src/include/type_mapping.h"
#include "../src/include/util.h"

#include <sql.h>
#include <sqlext.h>

#ifdef UNICODE
#undef SQLColAttribute
#undef SQLDescribeCol
#undef SQLGetInfo
#undef SQLGetData
#endif

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// GetColumnSizeForType tests
// ============================================================================

TEST(Phase2_ColumnSize, Integer_types_use_display_size) {
    AssertEqual((long long)11, (long long)GetColumnSizeForType("Int32", SQL_INTEGER));
    AssertEqual((long long)20, (long long)GetColumnSizeForType("UInt64", SQL_BIGINT));
    AssertEqual((long long)4, (long long)GetColumnSizeForType("Int8", SQL_TINYINT));
    AssertEqual((long long)6, (long long)GetColumnSizeForType("Int16", SQL_SMALLINT));
}

TEST(Phase2_ColumnSize, FixedString_returns_exact_length) {
    AssertEqual((long long)32, (long long)GetColumnSizeForType("FixedString(32)", SQL_CHAR));
    AssertEqual((long long)100, (long long)GetColumnSizeForType("FixedString(100)", SQL_CHAR));
    AssertEqual((long long)1, (long long)GetColumnSizeForType("FixedString(1)", SQL_CHAR));
}

TEST(Phase2_ColumnSize, Decimal_precision) {
    AssertEqual((long long)10, (long long)GetColumnSizeForType("Decimal(10,2)", SQL_DECIMAL));
    AssertEqual((long long)38, (long long)GetColumnSizeForType("Decimal(38,10)", SQL_DECIMAL));
    AssertEqual((long long)9, (long long)GetColumnSizeForType("Decimal32", SQL_DECIMAL));
    AssertEqual((long long)18, (long long)GetColumnSizeForType("Decimal64", SQL_DECIMAL));
    AssertEqual((long long)38, (long long)GetColumnSizeForType("Decimal128", SQL_DECIMAL));
}

TEST(Phase2_ColumnSize, DateTime64_includes_fraction) {
    // DateTime64(3) = "YYYY-MM-DD HH:MM:SS.fff" = 20 + 3 = 23
    AssertEqual((long long)23, (long long)GetColumnSizeForType("DateTime64(3)", SQL_TYPE_TIMESTAMP));
    AssertEqual((long long)26, (long long)GetColumnSizeForType("DateTime64(6)", SQL_TYPE_TIMESTAMP));
}

TEST(Phase2_ColumnSize, Nullable_wrapping) {
    AssertEqual((long long)32, (long long)GetColumnSizeForType("Nullable(FixedString(32))", SQL_CHAR));
    AssertEqual((long long)10, (long long)GetColumnSizeForType("Nullable(Decimal(10,2))", SQL_DECIMAL));
}

TEST(Phase2_ColumnSize, LowCardinality_wrapping) {
    AssertEqual((long long)65535, (long long)GetColumnSizeForType("LowCardinality(String)", SQL_VARCHAR));
    AssertEqual((long long)32, (long long)GetColumnSizeForType("LowCardinality(FixedString(32))", SQL_CHAR));
}

// ============================================================================
// GetDecimalDigitsForType tests
// ============================================================================

TEST(Phase2_DecimalDigits, Decimal_scale) {
    AssertEqual((int)2, (int)GetDecimalDigitsForType("Decimal(10,2)", SQL_DECIMAL));
    AssertEqual((int)10, (int)GetDecimalDigitsForType("Decimal(38,10)", SQL_DECIMAL));
    AssertEqual((int)0, (int)GetDecimalDigitsForType("Decimal32", SQL_DECIMAL));
}

TEST(Phase2_DecimalDigits, DateTime64_precision) {
    AssertEqual((int)3, (int)GetDecimalDigitsForType("DateTime64(3)", SQL_TYPE_TIMESTAMP));
    AssertEqual((int)6, (int)GetDecimalDigitsForType("DateTime64(6)", SQL_TYPE_TIMESTAMP));
    AssertEqual((int)0, (int)GetDecimalDigitsForType("DateTime", SQL_TYPE_TIMESTAMP));
}

TEST(Phase2_DecimalDigits, Float_precision) {
    AssertEqual((int)7, (int)GetDecimalDigitsForType("Float32", SQL_REAL));
    AssertEqual((int)15, (int)GetDecimalDigitsForType("Float64", SQL_DOUBLE));
}

TEST(Phase2_DecimalDigits, Integer_returns_zero) {
    AssertEqual((int)0, (int)GetDecimalDigitsForType("Int32", SQL_INTEGER));
    AssertEqual((int)0, (int)GetDecimalDigitsForType("UInt64", SQL_BIGINT));
}

TEST(Phase2_DecimalDigits, Nullable_wrapping) {
    AssertEqual((int)2, (int)GetDecimalDigitsForType("Nullable(Decimal(10,2))", SQL_DECIMAL));
    AssertEqual((int)3, (int)GetDecimalDigitsForType("Nullable(DateTime64(3))", SQL_TYPE_TIMESTAMP));
}

// ============================================================================
// IsUnsignedType tests
// ============================================================================

TEST(Phase2_IsUnsigned, Unsigned_types) {
    AssertTrue(IsUnsignedType("UInt8"));
    AssertTrue(IsUnsignedType("UInt16"));
    AssertTrue(IsUnsignedType("UInt32"));
    AssertTrue(IsUnsignedType("UInt64"));
}

TEST(Phase2_IsUnsigned, Signed_types) {
    AssertFalse(IsUnsignedType("Int8"));
    AssertFalse(IsUnsignedType("Int32"));
    AssertFalse(IsUnsignedType("Float64"));
    AssertFalse(IsUnsignedType("String"));
}

TEST(Phase2_IsUnsigned, Nullable_unsigned) {
    AssertTrue(IsUnsignedType("Nullable(UInt32)"));
    AssertFalse(IsUnsignedType("Nullable(Int32)"));
}

// ============================================================================
// NormalizeClickHouseType tests
// ============================================================================

TEST(Phase2_Normalize, Strip_Nullable) {
    AssertEqual("Int32", NormalizeClickHouseType("Nullable(Int32)"));
    AssertEqual("String", NormalizeClickHouseType("Nullable(String)"));
}

TEST(Phase2_Normalize, Strip_LowCardinality) {
    AssertEqual("String", NormalizeClickHouseType("LowCardinality(String)"));
}

TEST(Phase2_Normalize, Strip_LowCardinality_Nullable) {
    AssertEqual("String", NormalizeClickHouseType("LowCardinality(Nullable(String))"));
}

TEST(Phase2_Normalize, Plain_type_unchanged) {
    AssertEqual("Int32", NormalizeClickHouseType("Int32"));
    AssertEqual("DateTime64(3)", NormalizeClickHouseType("DateTime64(3)"));
}

// ============================================================================
// SQLColAttribute enhanced coverage tests
// ============================================================================

// Helper to create a statement with one column of given type
static OdbcEnvironment test_env;
static OdbcConnection test_conn;

static OdbcStatement *CreateTestStmt(const std::string &ch_type) {
    static OdbcStatement stmt;
    stmt.handle_type = SQL_HANDLE_STMT;
    stmt.conn = &test_conn;
    test_conn.handle_type = SQL_HANDLE_DBC;
    test_conn.env = &test_env;
    test_env.handle_type = SQL_HANDLE_ENV;

    stmt.result_set.Reset();
    stmt.ClearDiagRecords();

    ColumnInfo ci;
    ci.name = "test_col";
    ci.clickhouse_type = ch_type;
    ci.sql_type = ClickHouseTypeToSqlType(ch_type);
    ci.column_size = GetColumnSizeForType(ch_type, ci.sql_type);
    ci.decimal_digits = GetDecimalDigitsForType(ch_type, ci.sql_type);
    ci.nullable = (ch_type.find("Nullable") != std::string::npos) ? SQL_NULLABLE : SQL_NO_NULLS;
    stmt.result_set.columns.push_back(ci);

    return &stmt;
}

extern "C" SQLRETURN SQL_API SQLColAttribute(SQLHSTMT, SQLUSMALLINT, SQLUSMALLINT,
    SQLPOINTER, SQLSMALLINT, SQLSMALLINT *, SQLLEN *);

TEST(Phase2_ColAttribute, Unsigned_detection_UInt32) {
    auto *stmt = CreateTestStmt("UInt32");
    SQLLEN value = -1;
    SQLRETURN ret = SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_UNSIGNED, nullptr, 0, nullptr, &value);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((long long)SQL_TRUE, (long long)value);
}

TEST(Phase2_ColAttribute, Unsigned_detection_Int32) {
    auto *stmt = CreateTestStmt("Int32");
    SQLLEN value = -1;
    SQLRETURN ret = SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_UNSIGNED, nullptr, 0, nullptr, &value);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((long long)SQL_FALSE, (long long)value);
}

TEST(Phase2_ColAttribute, Display_size_integer) {
    auto *stmt = CreateTestStmt("Int32");
    SQLLEN value = 0;
    SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_DISPLAY_SIZE, nullptr, 0, nullptr, &value);
    AssertEqual((long long)11, (long long)value);
}

TEST(Phase2_ColAttribute, Octet_length_integer) {
    auto *stmt = CreateTestStmt("Int32");
    SQLLEN value = 0;
    SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_OCTET_LENGTH, nullptr, 0, nullptr, &value);
    AssertEqual((long long)4, (long long)value);
}

TEST(Phase2_ColAttribute, Case_sensitive_string) {
    auto *stmt = CreateTestStmt("String");
    SQLLEN value = 0;
    SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_CASE_SENSITIVE, nullptr, 0, nullptr, &value);
    AssertEqual((long long)SQL_TRUE, (long long)value);
}

TEST(Phase2_ColAttribute, Case_insensitive_numeric) {
    auto *stmt = CreateTestStmt("Int32");
    SQLLEN value = -1;
    SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_CASE_SENSITIVE, nullptr, 0, nullptr, &value);
    AssertEqual((long long)SQL_FALSE, (long long)value);
}

TEST(Phase2_ColAttribute, Type_name_returns_sql_name) {
    auto *stmt = CreateTestStmt("Int32");
    char buf[128] = {};
    SQLSMALLINT len = 0;
    SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_TYPE_NAME, buf, sizeof(buf), &len, nullptr);
    AssertEqual("INTEGER", std::string(buf));
}

TEST(Phase2_ColAttribute, Local_type_name_returns_ch_type) {
    auto *stmt = CreateTestStmt("Nullable(DateTime64(3))");
    char buf[128] = {};
    SQLSMALLINT len = 0;
    SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_LOCAL_TYPE_NAME, buf, sizeof(buf), &len, nullptr);
    AssertEqual("Nullable(DateTime64(3))", std::string(buf));
}

TEST(Phase2_ColAttribute, Num_prec_radix_numeric) {
    auto *stmt = CreateTestStmt("Int32");
    SQLLEN value = 0;
    SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_NUM_PREC_RADIX, nullptr, 0, nullptr, &value);
    AssertEqual((long long)10, (long long)value);
}

TEST(Phase2_ColAttribute, Num_prec_radix_string) {
    auto *stmt = CreateTestStmt("String");
    SQLLEN value = -1;
    SQLColAttribute((SQLHSTMT)stmt, 1, SQL_DESC_NUM_PREC_RADIX, nullptr, 0, nullptr, &value);
    AssertEqual((long long)0, (long long)value);
}

// ============================================================================
// NULL handling tests
// ============================================================================

extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);

TEST(Phase2_NullHandling, GetData_NULL_with_indicator) {
    OdbcStatement stmt;
    stmt.handle_type = SQL_HANDLE_STMT;
    stmt.conn = &test_conn;

    // Add a column and a row with NULL value
    ColumnInfo ci;
    ci.name = "nullable_col";
    ci.clickhouse_type = "Nullable(Int32)";
    ci.sql_type = SQL_INTEGER;
    ci.column_size = 11;
    ci.decimal_digits = 0;
    ci.nullable = SQL_NULLABLE;
    stmt.result_set.columns.push_back(ci);

    ResultRow row;
    row.push_back(std::nullopt); // NULL value
    stmt.result_set.rows.push_back(row);
    stmt.result_set.current_row = 0;

    SQLLEN indicator = 0;
    SQLINTEGER value = 42;
    SQLRETURN ret = SQLGetData((SQLHSTMT)&stmt, 1, SQL_C_SLONG, &value, sizeof(value), &indicator);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((long long)SQL_NULL_DATA, (long long)indicator);
}

TEST(Phase2_NullHandling, GetData_NULL_without_indicator_returns_error) {
    OdbcStatement stmt;
    stmt.handle_type = SQL_HANDLE_STMT;
    stmt.conn = &test_conn;

    ColumnInfo ci;
    ci.name = "nullable_col";
    ci.clickhouse_type = "Nullable(Int32)";
    ci.sql_type = SQL_INTEGER;
    ci.column_size = 11;
    ci.nullable = SQL_NULLABLE;
    stmt.result_set.columns.push_back(ci);

    ResultRow row;
    row.push_back(std::nullopt);
    stmt.result_set.rows.push_back(row);
    stmt.result_set.current_row = 0;

    SQLINTEGER value = 42;
    SQLRETURN ret = SQLGetData((SQLHSTMT)&stmt, 1, SQL_C_SLONG, &value, sizeof(value), nullptr);
    AssertEqual((int)SQL_ERROR, (int)ret);
    // Verify SQLSTATE 22002 was set
    AssertTrue(!stmt.diag_records.empty(), "Expected diagnostic record");
    AssertEqual("22002", stmt.diag_records[0].sql_state);
}

TEST(Phase2_NullHandling, GetData_non_NULL_value_succeeds) {
    OdbcStatement stmt;
    stmt.handle_type = SQL_HANDLE_STMT;
    stmt.conn = &test_conn;

    ColumnInfo ci;
    ci.name = "int_col";
    ci.clickhouse_type = "Int32";
    ci.sql_type = SQL_INTEGER;
    ci.column_size = 11;
    stmt.result_set.columns.push_back(ci);

    ResultRow row;
    row.push_back("12345");
    stmt.result_set.rows.push_back(row);
    stmt.result_set.current_row = 0;

    SQLLEN indicator = 0;
    SQLINTEGER value = 0;
    SQLRETURN ret = SQLGetData((SQLHSTMT)&stmt, 1, SQL_C_SLONG, &value, sizeof(value), &indicator);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((long long)12345, (long long)value);
    AssertEqual((long long)sizeof(SQLINTEGER), (long long)indicator);
}
