#include "test_framework.h"
#include "../src/include/handle.h"
#include "../src/include/type_mapping.h"
#include "../src/include/util.h"

#include <sql.h>
#include <sqlext.h>
#include <cstring>

#ifdef UNICODE
#undef SQLColAttribute
#undef SQLDescribeCol
#undef SQLGetInfo
#undef SQLGetData
#undef SQLGetTypeInfo
#endif

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// ClickHouseTypeToSqlType - Extended type mapping test
// ============================================================================

TEST(Phase3_TypeMapping, Basic_integer_types) {
    AssertEqual((int)SQL_TINYINT, (int)ClickHouseTypeToSqlType("Int8"));
    AssertEqual((int)SQL_TINYINT, (int)ClickHouseTypeToSqlType("UInt8"));
    AssertEqual((int)SQL_SMALLINT, (int)ClickHouseTypeToSqlType("Int16"));
    AssertEqual((int)SQL_SMALLINT, (int)ClickHouseTypeToSqlType("UInt16"));
    AssertEqual((int)SQL_INTEGER, (int)ClickHouseTypeToSqlType("Int32"));
    AssertEqual((int)SQL_INTEGER, (int)ClickHouseTypeToSqlType("UInt32"));
    AssertEqual((int)SQL_BIGINT, (int)ClickHouseTypeToSqlType("Int64"));
    AssertEqual((int)SQL_BIGINT, (int)ClickHouseTypeToSqlType("UInt64"));
}

TEST(Phase3_TypeMapping, Large_integer_types_as_varchar) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Int128"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("UInt128"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Int256"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("UInt256"));
}

TEST(Phase3_TypeMapping, Float_types) {
    AssertEqual((int)SQL_REAL, (int)ClickHouseTypeToSqlType("Float32"));
    AssertEqual((int)SQL_DOUBLE, (int)ClickHouseTypeToSqlType("Float64"));
}

TEST(Phase3_TypeMapping, Decimal_types) {
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal(10,2)"));
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal32"));
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal64"));
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal128"));
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal256"));
}

TEST(Phase3_TypeMapping, String_types) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("String"));
    AssertEqual((int)SQL_CHAR, (int)ClickHouseTypeToSqlType("FixedString(32)"));
}

TEST(Phase3_TypeMapping, Date_time_types) {
    AssertEqual((int)SQL_TYPE_DATE, (int)ClickHouseTypeToSqlType("Date"));
    AssertEqual((int)SQL_TYPE_DATE, (int)ClickHouseTypeToSqlType("Date32"));
    AssertEqual((int)SQL_TYPE_TIMESTAMP, (int)ClickHouseTypeToSqlType("DateTime"));
    AssertEqual((int)SQL_TYPE_TIMESTAMP, (int)ClickHouseTypeToSqlType("DateTime('Asia/Tokyo')"));
    AssertEqual((int)SQL_TYPE_TIMESTAMP, (int)ClickHouseTypeToSqlType("DateTime64(3)"));
    AssertEqual((int)SQL_TYPE_TIMESTAMP, (int)ClickHouseTypeToSqlType("DateTime64(6, 'UTC')"));
}

TEST(Phase3_TypeMapping, Bool_type) {
    AssertEqual((int)SQL_BIT, (int)ClickHouseTypeToSqlType("Bool"));
}

TEST(Phase3_TypeMapping, UUID_type) {
    AssertEqual((int)SQL_GUID, (int)ClickHouseTypeToSqlType("UUID"));
}

TEST(Phase3_TypeMapping, Enum_types) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Enum8('a'=1,'b'=2)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Enum16('x'=1)"));
}

TEST(Phase3_TypeMapping, Array_Tuple_Map_types) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Array(Int32)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Tuple(String, Int32)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Map(String, Int32)"));
}

TEST(Phase3_TypeMapping, IPv4_IPv6_types) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("IPv4"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("IPv6"));
}

TEST(Phase3_TypeMapping, Nothing_type) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Nothing"));
}

TEST(Phase3_TypeMapping, SimpleAggregateFunction_extracts_inner) {
    AssertEqual((int)SQL_BIGINT, (int)ClickHouseTypeToSqlType("SimpleAggregateFunction(max, Int64)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("SimpleAggregateFunction(groupUniqArray, String)"));
}

TEST(Phase3_TypeMapping, AggregateFunction_returns_varbinary) {
    AssertEqual((int)SQL_VARBINARY, (int)ClickHouseTypeToSqlType("AggregateFunction(count, UInt64)"));
}

TEST(Phase3_TypeMapping, Interval_types) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("IntervalDay"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("IntervalHour"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("IntervalMonth"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("IntervalYear"));
}

TEST(Phase3_TypeMapping, Geo_types) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Point"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Ring"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Polygon"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("MultiPolygon"));
}

TEST(Phase3_TypeMapping, JSON_and_Nested_types) {
    AssertEqual((int)SQL_LONGVARCHAR, (int)ClickHouseTypeToSqlType("JSON"));
    AssertEqual((int)SQL_LONGVARCHAR, (int)ClickHouseTypeToSqlType("Object('json')"));
    AssertEqual((int)SQL_LONGVARCHAR, (int)ClickHouseTypeToSqlType("Nested(a Int32, b String)"));
}

// ============================================================================
// Nullable / LowCardinality wrapper handling test
// ============================================================================

TEST(Phase3_TypeMapping, Nullable_wrapping) {
    AssertEqual((int)SQL_INTEGER, (int)ClickHouseTypeToSqlType("Nullable(Int32)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Nullable(String)"));
    AssertEqual((int)SQL_GUID, (int)ClickHouseTypeToSqlType("Nullable(UUID)"));
}

TEST(Phase3_TypeMapping, LowCardinality_wrapping) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("LowCardinality(String)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("LowCardinality(Nullable(String))"));
    AssertEqual((int)SQL_CHAR, (int)ClickHouseTypeToSqlType("LowCardinality(FixedString(10))"));
}

TEST(Phase3_NormalizeType, Strip_Nullable) {
    AssertEqual(std::string("Int32"), NormalizeClickHouseType("Nullable(Int32)"));
    AssertEqual(std::string("String"), NormalizeClickHouseType("Nullable(String)"));
}

TEST(Phase3_NormalizeType, Strip_LowCardinality) {
    AssertEqual(std::string("String"), NormalizeClickHouseType("LowCardinality(String)"));
}

TEST(Phase3_NormalizeType, Strip_both) {
    AssertEqual(std::string("String"), NormalizeClickHouseType("LowCardinality(Nullable(String))"));
}

// ============================================================================
// IsNullableType test
// ============================================================================

TEST(Phase3_Nullable, Nullable_detected) {
    AssertTrue(IsNullableType("Nullable(Int32)"));
    AssertTrue(IsNullableType("LowCardinality(Nullable(String))"));
}

TEST(Phase3_Nullable, Non_nullable_not_detected) {
    AssertTrue(!IsNullableType("Int32"));
    AssertTrue(!IsNullableType("String"));
    AssertTrue(!IsNullableType("LowCardinality(String)"));
}

// ============================================================================
// IsUnsignedType test
// ============================================================================

TEST(Phase3_Unsigned, Unsigned_types_detected) {
    AssertTrue(IsUnsignedType("UInt8"));
    AssertTrue(IsUnsignedType("UInt16"));
    AssertTrue(IsUnsignedType("UInt32"));
    AssertTrue(IsUnsignedType("UInt64"));
    AssertTrue(IsUnsignedType("Nullable(UInt32)"));
}

TEST(Phase3_Unsigned, Signed_types_not_unsigned) {
    AssertTrue(!IsUnsignedType("Int8"));
    AssertTrue(!IsUnsignedType("Int32"));
    AssertTrue(!IsUnsignedType("Float64"));
    AssertTrue(!IsUnsignedType("String"));
}

// ============================================================================
// GetColumnSizeForType - Extended test
// ============================================================================

TEST(Phase3_ColumnSize, Enum_types_return_256) {
    AssertEqual((long long)256, (long long)GetColumnSizeForType("Enum8('a'=1)", SQL_VARCHAR));
    AssertEqual((long long)256, (long long)GetColumnSizeForType("Enum16('x'=1,'y'=2)", SQL_VARCHAR));
}

TEST(Phase3_ColumnSize, DateTime64_precision_variants) {
    AssertEqual((long long)23, (long long)GetColumnSizeForType("DateTime64(3)", SQL_TYPE_TIMESTAMP));
    AssertEqual((long long)26, (long long)GetColumnSizeForType("DateTime64(6)", SQL_TYPE_TIMESTAMP));
    AssertEqual((long long)29, (long long)GetColumnSizeForType("DateTime64(9)", SQL_TYPE_TIMESTAMP));
}

TEST(Phase3_ColumnSize, Decimal256_precision) {
    AssertEqual((long long)76, (long long)GetColumnSizeForType("Decimal256", SQL_DECIMAL));
}

// ============================================================================
// GetDecimalDigitsForType test
// ============================================================================

TEST(Phase3_DecimalDigits, Decimal_with_scale) {
    AssertEqual((int)2, (int)GetDecimalDigitsForType("Decimal(10,2)", SQL_DECIMAL));
    AssertEqual((int)10, (int)GetDecimalDigitsForType("Decimal(38,10)", SQL_DECIMAL));
    AssertEqual((int)0, (int)GetDecimalDigitsForType("Decimal(10,0)", SQL_DECIMAL));
}

TEST(Phase3_DecimalDigits, Decimal_aliases_zero_scale) {
    AssertEqual((int)0, (int)GetDecimalDigitsForType("Decimal32", SQL_DECIMAL));
    AssertEqual((int)0, (int)GetDecimalDigitsForType("Decimal64", SQL_DECIMAL));
    AssertEqual((int)0, (int)GetDecimalDigitsForType("Decimal128", SQL_DECIMAL));
}

TEST(Phase3_DecimalDigits, DateTime64_precision) {
    AssertEqual((int)3, (int)GetDecimalDigitsForType("DateTime64(3)", SQL_TYPE_TIMESTAMP));
    AssertEqual((int)6, (int)GetDecimalDigitsForType("DateTime64(6)", SQL_TYPE_TIMESTAMP));
}

TEST(Phase3_DecimalDigits, Float_precision) {
    AssertEqual((int)7, (int)GetDecimalDigitsForType("Float32", SQL_REAL));
    AssertEqual((int)15, (int)GetDecimalDigitsForType("Float64", SQL_DOUBLE));
}

TEST(Phase3_DecimalDigits, DateTime_zero) {
    AssertEqual((int)0, (int)GetDecimalDigitsForType("DateTime", SQL_TYPE_TIMESTAMP));
}

// ============================================================================
// GetDefaultCType test
// ============================================================================

TEST(Phase3_DefaultCType, Integer_types) {
    AssertEqual((int)SQL_C_STINYINT, (int)GetDefaultCType(SQL_TINYINT));
    AssertEqual((int)SQL_C_SSHORT, (int)GetDefaultCType(SQL_SMALLINT));
    AssertEqual((int)SQL_C_SLONG, (int)GetDefaultCType(SQL_INTEGER));
    AssertEqual((int)SQL_C_SBIGINT, (int)GetDefaultCType(SQL_BIGINT));
}

TEST(Phase3_DefaultCType, Float_types) {
    AssertEqual((int)SQL_C_FLOAT, (int)GetDefaultCType(SQL_REAL));
    AssertEqual((int)SQL_C_DOUBLE, (int)GetDefaultCType(SQL_DOUBLE));
}

TEST(Phase3_DefaultCType, String_types) {
    AssertEqual((int)SQL_C_CHAR, (int)GetDefaultCType(SQL_VARCHAR));
    AssertEqual((int)SQL_C_CHAR, (int)GetDefaultCType(SQL_CHAR));
    AssertEqual((int)SQL_C_WCHAR, (int)GetDefaultCType(SQL_WCHAR));
    AssertEqual((int)SQL_C_WCHAR, (int)GetDefaultCType(SQL_WVARCHAR));
}

TEST(Phase3_DefaultCType, DateTime_types) {
    AssertEqual((int)SQL_C_TYPE_DATE, (int)GetDefaultCType(SQL_TYPE_DATE));
    AssertEqual((int)SQL_C_TYPE_TIME, (int)GetDefaultCType(SQL_TYPE_TIME));
    AssertEqual((int)SQL_C_TYPE_TIMESTAMP, (int)GetDefaultCType(SQL_TYPE_TIMESTAMP));
}

TEST(Phase3_DefaultCType, Decimal_and_numeric) {
    AssertEqual((int)SQL_C_CHAR, (int)GetDefaultCType(SQL_DECIMAL));
    AssertEqual((int)SQL_C_CHAR, (int)GetDefaultCType(SQL_NUMERIC));
}

// ============================================================================
// GetSqlTypeName test
// ============================================================================

TEST(Phase3_TypeName, All_known_types) {
    AssertEqual(std::string("BIT"), GetSqlTypeName(SQL_BIT));
    AssertEqual(std::string("TINYINT"), GetSqlTypeName(SQL_TINYINT));
    AssertEqual(std::string("SMALLINT"), GetSqlTypeName(SQL_SMALLINT));
    AssertEqual(std::string("INTEGER"), GetSqlTypeName(SQL_INTEGER));
    AssertEqual(std::string("BIGINT"), GetSqlTypeName(SQL_BIGINT));
    AssertEqual(std::string("REAL"), GetSqlTypeName(SQL_REAL));
    AssertEqual(std::string("DOUBLE"), GetSqlTypeName(SQL_DOUBLE));
    AssertEqual(std::string("DECIMAL"), GetSqlTypeName(SQL_DECIMAL));
    AssertEqual(std::string("CHAR"), GetSqlTypeName(SQL_CHAR));
    AssertEqual(std::string("VARCHAR"), GetSqlTypeName(SQL_VARCHAR));
    AssertEqual(std::string("DATE"), GetSqlTypeName(SQL_TYPE_DATE));
    AssertEqual(std::string("TIMESTAMP"), GetSqlTypeName(SQL_TYPE_TIMESTAMP));
    AssertEqual(std::string("GUID"), GetSqlTypeName(SQL_GUID));
}

// ============================================================================
// GetCTypeName test
// ============================================================================

TEST(Phase3_CTypeName, Known_c_types) {
    AssertEqual(std::string("SQL_C_CHAR"), GetCTypeName(SQL_C_CHAR));
    AssertEqual(std::string("SQL_C_WCHAR"), GetCTypeName(SQL_C_WCHAR));
    AssertEqual(std::string("SQL_C_SLONG"), GetCTypeName(SQL_C_SLONG));
    AssertEqual(std::string("SQL_C_DOUBLE"), GetCTypeName(SQL_C_DOUBLE));
    AssertEqual(std::string("SQL_C_NUMERIC"), GetCTypeName(SQL_C_NUMERIC));
    AssertEqual(std::string("SQL_C_GUID"), GetCTypeName(SQL_C_GUID));
    AssertEqual(std::string("SQL_C_TYPE_DATE"), GetCTypeName(SQL_C_TYPE_DATE));
    AssertEqual(std::string("SQL_C_TYPE_TIME"), GetCTypeName(SQL_C_TYPE_TIME));
    AssertEqual(std::string("SQL_C_TYPE_TIMESTAMP"), GetCTypeName(SQL_C_TYPE_TIMESTAMP));
}

// ============================================================================
// ValidateNumericRange test
// ============================================================================

TEST(Phase3_NumericRange, STINYINT_range) {
    AssertTrue(ValidateNumericRange("0", SQL_C_STINYINT));
    AssertTrue(ValidateNumericRange("-128", SQL_C_STINYINT));
    AssertTrue(ValidateNumericRange("127", SQL_C_STINYINT));
    AssertTrue(!ValidateNumericRange("128", SQL_C_STINYINT));
    AssertTrue(!ValidateNumericRange("-129", SQL_C_STINYINT));
}

TEST(Phase3_NumericRange, UTINYINT_range) {
    AssertTrue(ValidateNumericRange("0", SQL_C_UTINYINT));
    AssertTrue(ValidateNumericRange("255", SQL_C_UTINYINT));
    AssertTrue(!ValidateNumericRange("256", SQL_C_UTINYINT));
    AssertTrue(!ValidateNumericRange("-1", SQL_C_UTINYINT));
}

TEST(Phase3_NumericRange, SSHORT_range) {
    AssertTrue(ValidateNumericRange("0", SQL_C_SSHORT));
    AssertTrue(ValidateNumericRange("-32768", SQL_C_SSHORT));
    AssertTrue(ValidateNumericRange("32767", SQL_C_SSHORT));
    AssertTrue(!ValidateNumericRange("32768", SQL_C_SSHORT));
}

TEST(Phase3_NumericRange, String_targets_always_valid) {
    AssertTrue(ValidateNumericRange("999999999999999999999", SQL_C_CHAR));
    AssertTrue(ValidateNumericRange("any_string", SQL_C_WCHAR));
}

TEST(Phase3_NumericRange, Empty_value_valid) {
    AssertTrue(ValidateNumericRange("", SQL_C_STINYINT));
}

// ============================================================================
// BuildTypeInfoResultSet test
// ============================================================================

TEST(Phase3_TypeInfo, All_types_returns_multiple_rows) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_ALL_TYPES);
    // Should have at least 15 type entries (including unsigned variants)
    AssertTrue(rs.rows.size() >= 15);
    // 19 columns per ODBC spec
    AssertEqual((int)19, (int)rs.columns.size());
}

TEST(Phase3_TypeInfo, Specific_type_filter) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_INTEGER);
    // INTEGER should have signed + unsigned variants
    AssertEqual((int)2, (int)rs.rows.size());
}

TEST(Phase3_TypeInfo, VARCHAR_single_row) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_VARCHAR);
    AssertEqual((int)1, (int)rs.rows.size());
    // TYPE_NAME should be "VARCHAR"
    AssertTrue(rs.rows[0][0].has_value());
    AssertEqual(std::string("VARCHAR"), rs.rows[0][0].value());
}

TEST(Phase3_TypeInfo, DATE_has_datetime_sub) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_TYPE_DATE);
    AssertEqual((int)1, (int)rs.rows.size());
    // SQL_DATA_TYPE (column 15) should be SQL_DATETIME (9)
    AssertTrue(rs.rows[0][15].has_value());
    AssertEqual(std::string("9"), rs.rows[0][15].value()); // SQL_DATETIME = 9
    // SQL_DATETIME_SUB (column 16) should be SQL_CODE_DATE (1)
    AssertTrue(rs.rows[0][16].has_value());
    AssertEqual(std::string("1"), rs.rows[0][16].value()); // SQL_CODE_DATE = 1
}

TEST(Phase3_TypeInfo, TIMESTAMP_has_create_params) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_TYPE_TIMESTAMP);
    AssertEqual((int)1, (int)rs.rows.size());
    // CREATE_PARAMS (column 5) should be "precision"
    AssertTrue(rs.rows[0][5].has_value());
    AssertEqual(std::string("precision"), rs.rows[0][5].value());
}

TEST(Phase3_TypeInfo, DECIMAL_has_precision_scale_params) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_DECIMAL);
    AssertEqual((int)1, (int)rs.rows.size());
    // CREATE_PARAMS (column 5) should be "precision,scale"
    AssertTrue(rs.rows[0][5].has_value());
    AssertEqual(std::string("precision,scale"), rs.rows[0][5].value());
}

TEST(Phase3_TypeInfo, BIGINT_unsigned_variant) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_BIGINT);
    AssertEqual((int)2, (int)rs.rows.size());
    // First row: BIGINT (signed)
    AssertEqual(std::string("BIGINT"), rs.rows[0][0].value());
    // UNSIGNED_ATTRIBUTE (column 9) = SQL_FALSE for signed
    AssertEqual(std::string("0"), rs.rows[0][9].value());
    // Second row: BIGINT UNSIGNED
    AssertEqual(std::string("BIGINT UNSIGNED"), rs.rows[1][0].value());
    // UNSIGNED_ATTRIBUTE (column 9) = SQL_TRUE for unsigned
    AssertEqual(std::string("1"), rs.rows[1][9].value());
}

// ============================================================================
// GetSqlTypeDisplaySize / GetSqlTypeOctetLength test
// ============================================================================

TEST(Phase3_DisplaySize, Common_types) {
    AssertEqual((long long)1, (long long)GetSqlTypeDisplaySize(SQL_BIT));
    AssertEqual((long long)11, (long long)GetSqlTypeDisplaySize(SQL_INTEGER));
    AssertEqual((long long)20, (long long)GetSqlTypeDisplaySize(SQL_BIGINT));
    AssertEqual((long long)10, (long long)GetSqlTypeDisplaySize(SQL_TYPE_DATE));
    AssertEqual((long long)8, (long long)GetSqlTypeDisplaySize(SQL_TYPE_TIME));
    AssertEqual((long long)26, (long long)GetSqlTypeDisplaySize(SQL_TYPE_TIMESTAMP));
    AssertEqual((long long)36, (long long)GetSqlTypeDisplaySize(SQL_GUID));
}

TEST(Phase3_OctetLength, Struct_sizes) {
    AssertEqual((long long)sizeof(SQL_DATE_STRUCT), (long long)GetSqlTypeOctetLength(SQL_TYPE_DATE));
    AssertEqual((long long)sizeof(SQL_TIME_STRUCT), (long long)GetSqlTypeOctetLength(SQL_TYPE_TIME));
    AssertEqual((long long)sizeof(SQL_TIMESTAMP_STRUCT), (long long)GetSqlTypeOctetLength(SQL_TYPE_TIMESTAMP));
    AssertEqual((long long)16, (long long)GetSqlTypeOctetLength(SQL_GUID));
}
