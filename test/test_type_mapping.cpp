#include "test_framework.h"
#include "../src/include/handle.h"
#include "../src/include/type_mapping.h"

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// ClickHouseTypeToSqlType
// ============================================================================

TEST(TypeMapping, IntegerTypes) {
    AssertEqual((int)SQL_TINYINT, (int)ClickHouseTypeToSqlType("Int8"));
    AssertEqual((int)SQL_TINYINT, (int)ClickHouseTypeToSqlType("UInt8"));
    AssertEqual((int)SQL_SMALLINT, (int)ClickHouseTypeToSqlType("Int16"));
    AssertEqual((int)SQL_SMALLINT, (int)ClickHouseTypeToSqlType("UInt16"));
    AssertEqual((int)SQL_INTEGER, (int)ClickHouseTypeToSqlType("Int32"));
    AssertEqual((int)SQL_INTEGER, (int)ClickHouseTypeToSqlType("UInt32"));
    AssertEqual((int)SQL_BIGINT, (int)ClickHouseTypeToSqlType("Int64"));
    AssertEqual((int)SQL_BIGINT, (int)ClickHouseTypeToSqlType("UInt64"));
}

TEST(TypeMapping, LargeIntegerTypes) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Int128"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("UInt256"));
}

TEST(TypeMapping, FloatTypes) {
    AssertEqual((int)SQL_REAL, (int)ClickHouseTypeToSqlType("Float32"));
    AssertEqual((int)SQL_DOUBLE, (int)ClickHouseTypeToSqlType("Float64"));
}

TEST(TypeMapping, DecimalTypes) {
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal(10,2)"));
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal32(4)"));
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal64(8)"));
    AssertEqual((int)SQL_DECIMAL, (int)ClickHouseTypeToSqlType("Decimal128(18)"));
}

TEST(TypeMapping, StringTypes) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("String"));
    AssertEqual((int)SQL_CHAR, (int)ClickHouseTypeToSqlType("FixedString(32)"));
}

TEST(TypeMapping, DateTimeTypes) {
    AssertEqual((int)SQL_TYPE_DATE, (int)ClickHouseTypeToSqlType("Date"));
    AssertEqual((int)SQL_TYPE_DATE, (int)ClickHouseTypeToSqlType("Date32"));
    AssertEqual((int)SQL_TYPE_TIMESTAMP, (int)ClickHouseTypeToSqlType("DateTime"));
    AssertEqual((int)SQL_TYPE_TIMESTAMP, (int)ClickHouseTypeToSqlType("DateTime('Asia/Tokyo')"));
    AssertEqual((int)SQL_TYPE_TIMESTAMP, (int)ClickHouseTypeToSqlType("DateTime64(3)"));
}

TEST(TypeMapping, BoolType) {
    AssertEqual((int)SQL_BIT, (int)ClickHouseTypeToSqlType("Bool"));
}

TEST(TypeMapping, UUIDType) {
    AssertEqual((int)SQL_GUID, (int)ClickHouseTypeToSqlType("UUID"));
}

TEST(TypeMapping, EnumTypes) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Enum8('a'=1,'b'=2)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Enum16('x'=1)"));
}

TEST(TypeMapping, ComplexTypes) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Array(Int32)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Tuple(Int32, String)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Map(String, Int32)"));
}

TEST(TypeMapping, NetworkTypes) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("IPv4"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("IPv6"));
}

TEST(TypeMapping, NullableWrapper) {
    AssertEqual((int)SQL_INTEGER, (int)ClickHouseTypeToSqlType("Nullable(Int32)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("Nullable(String)"));
    AssertEqual((int)SQL_TYPE_DATE, (int)ClickHouseTypeToSqlType("Nullable(Date)"));
}

TEST(TypeMapping, LowCardinalityWrapper) {
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("LowCardinality(String)"));
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("LowCardinality(Nullable(String))"));
}

TEST(TypeMapping, UnknownType) {
    // Unknown types should default to SQL_VARCHAR
    AssertEqual((int)SQL_VARCHAR, (int)ClickHouseTypeToSqlType("SomeUnknownType"));
}

// ============================================================================
// GetSqlTypeDisplaySize
// ============================================================================

TEST(TypeMapping, DisplaySize_Integer) {
    AssertTrue(GetSqlTypeDisplaySize(SQL_TINYINT) > 0, "TINYINT display size > 0");
    AssertTrue(GetSqlTypeDisplaySize(SQL_INTEGER) > 0, "INTEGER display size > 0");
    AssertTrue(GetSqlTypeDisplaySize(SQL_BIGINT) > GetSqlTypeDisplaySize(SQL_INTEGER),
               "BIGINT display size > INTEGER");
}

TEST(TypeMapping, DisplaySize_String) {
    AssertTrue(GetSqlTypeDisplaySize(SQL_VARCHAR) > 0, "VARCHAR display size > 0");
    AssertTrue(GetSqlTypeDisplaySize(SQL_VARCHAR) >= 65535, "VARCHAR display size >= 65535");
}

TEST(TypeMapping, DisplaySize_DateTime) {
    AssertEqual(10LL, (long long)GetSqlTypeDisplaySize(SQL_TYPE_DATE));
    AssertEqual(26LL, (long long)GetSqlTypeDisplaySize(SQL_TYPE_TIMESTAMP));
}

TEST(TypeMapping, DisplaySize_GUID) {
    AssertEqual(36LL, (long long)GetSqlTypeDisplaySize(SQL_GUID));
}

// ============================================================================
// GetSqlTypeName
// ============================================================================

TEST(TypeMapping, TypeName_Basic) {
    std::string name = GetSqlTypeName(SQL_VARCHAR);
    AssertFalse(name.empty(), "VARCHAR type name should not be empty");
}

TEST(TypeMapping, TypeName_Integer) {
    std::string name = GetSqlTypeName(SQL_INTEGER);
    AssertFalse(name.empty(), "INTEGER type name should not be empty");
}

// ============================================================================
// GetDefaultCType
// ============================================================================

TEST(TypeMapping, DefaultCType_Char) {
    AssertEqual((int)SQL_C_CHAR, (int)GetDefaultCType(SQL_VARCHAR));
    AssertEqual((int)SQL_C_CHAR, (int)GetDefaultCType(SQL_CHAR));
}

TEST(TypeMapping, DefaultCType_Integer) {
    AssertEqual((int)SQL_C_SLONG, (int)GetDefaultCType(SQL_INTEGER));
}

TEST(TypeMapping, DefaultCType_Bit) {
    AssertEqual((int)SQL_C_BIT, (int)GetDefaultCType(SQL_BIT));
}

// ============================================================================
// BuildTypeInfoResultSet
// ============================================================================

TEST(TypeMapping, BuildTypeInfo_AllTypes) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_ALL_TYPES);
    AssertTrue(rs.columns.size() == 19, "Type info should have 19 columns");
    AssertTrue(rs.rows.size() > 0, "Should have at least one type info row");
}

TEST(TypeMapping, BuildTypeInfo_SpecificType) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, SQL_VARCHAR);
    AssertTrue(rs.rows.size() >= 1, "Should have at least one row for VARCHAR");
}

TEST(TypeMapping, BuildTypeInfo_NoMatch) {
    ResultSet rs;
    BuildTypeInfoResultSet(rs, (SQLSMALLINT)9999);
    AssertEqual(0, (int)rs.rows.size(), "Unknown type should produce no rows");
}
