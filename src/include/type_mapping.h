#pragma once

#include "handle.h"
#include <string>
#include <vector>

namespace clickhouse_odbc {

// Map ClickHouse type string to ODBC SQL type
SQLSMALLINT ClickHouseTypeToSqlType(const std::string &ch_type);

// Get the display size for a given SQL type
SQLULEN GetSqlTypeDisplaySize(SQLSMALLINT sql_type);

// Get the octet length for a given SQL type
SQLULEN GetSqlTypeOctetLength(SQLSMALLINT sql_type);

// Get the type name for a given SQL type
std::string GetSqlTypeName(SQLSMALLINT sql_type);

// SQL_C_xxx default type for a given SQL type
SQLSMALLINT GetDefaultCType(SQLSMALLINT sql_type);

// Build type info result set for SQLGetTypeInfo
void BuildTypeInfoResultSet(ResultSet &result_set, SQLSMALLINT data_type);

// Get precise column size from ClickHouse type string (handles Decimal(P,S), FixedString(N), etc.)
SQLULEN GetColumnSizeForType(const std::string &ch_type, SQLSMALLINT sql_type);

// Get decimal digits from ClickHouse type string (handles Decimal(P,S), DateTime64(P), etc.)
SQLSMALLINT GetDecimalDigitsForType(const std::string &ch_type, SQLSMALLINT sql_type);

// Check if a ClickHouse type is unsigned
bool IsUnsignedType(const std::string &ch_type);

// Normalize ClickHouse type (strip Nullable/LowCardinality wrappers)
std::string NormalizeClickHouseType(const std::string &ch_type);

// Check if a ClickHouse type is Nullable
bool IsNullableType(const std::string &ch_type);

// Get the SQL_C_xxx type name for diagnostic messages
std::string GetCTypeName(SQLSMALLINT c_type);

// Validate numeric string conversion (returns true if safe)
bool ValidateNumericRange(const std::string &value, SQLSMALLINT target_c_type);

} // namespace clickhouse_odbc
