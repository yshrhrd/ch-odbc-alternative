#include "include/type_mapping.h"
#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdlib>
#include <cstring>

namespace clickhouse_odbc {

// Strip Nullable(...) wrapper
static std::string StripNullable(const std::string &ch_type) {
    if (ch_type.find("Nullable(") == 0 && ch_type.back() == ')') {
        return ch_type.substr(9, ch_type.size() - 10);
    }
    return ch_type;
}

// Strip LowCardinality(...) wrapper
static std::string StripLowCardinality(const std::string &ch_type) {
    if (ch_type.find("LowCardinality(") == 0 && ch_type.back() == ')') {
        return ch_type.substr(15, ch_type.size() - 16);
    }
    return ch_type;
}

static std::string NormalizeType(const std::string &ch_type) {
    std::string t = StripNullable(ch_type);
    t = StripLowCardinality(t);
    t = StripNullable(t); // LowCardinality(Nullable(X))
    return t;
}

std::string NormalizeClickHouseType(const std::string &ch_type) {
    return NormalizeType(ch_type);
}

SQLSMALLINT ClickHouseTypeToSqlType(const std::string &ch_type) {
    std::string t = NormalizeType(ch_type);

    // Integer types
    if (t == "UInt8" || t == "Int8") return SQL_TINYINT;
    if (t == "UInt16" || t == "Int16") return SQL_SMALLINT;
    if (t == "UInt32" || t == "Int32") return SQL_INTEGER;
    if (t == "UInt64" || t == "Int64") return SQL_BIGINT;
    if (t == "Int128" || t == "Int256" || t == "UInt128" || t == "UInt256") return SQL_VARCHAR;

    // Float types
    if (t == "Float32") return SQL_REAL;
    if (t == "Float64") return SQL_DOUBLE;

    // Decimal types
    if (t.find("Decimal") == 0) return SQL_DECIMAL;

    // String types
    if (t == "String") return SQL_VARCHAR;
    if (t == "FixedString" || t.find("FixedString(") == 0) return SQL_CHAR;

    // Date/time types
    if (t == "Date" || t == "Date32") return SQL_TYPE_DATE;
    if (t == "DateTime" || t.find("DateTime(") == 0) return SQL_TYPE_TIMESTAMP;
    if (t == "DateTime64" || t.find("DateTime64(") == 0) return SQL_TYPE_TIMESTAMP;

    // Boolean
    if (t == "Bool") return SQL_BIT;

    // UUID
    if (t == "UUID") return SQL_GUID;

    // Enum
    if (t.find("Enum8") == 0 || t.find("Enum16") == 0) return SQL_VARCHAR;

    // Array, Tuple, Map -> VARCHAR
    if (t.find("Array") == 0 || t.find("Tuple") == 0 || t.find("Map") == 0) return SQL_VARCHAR;

    // IPv4, IPv6
    if (t == "IPv4" || t == "IPv6") return SQL_VARCHAR;

    // Nothing type (empty/void)
    if (t == "Nothing") return SQL_VARCHAR;

    // SimpleAggregateFunction(...) / AggregateFunction(...)
    // Extract the inner type: SimpleAggregateFunction(func, Type) -> treat as the inner type
    if (t.find("SimpleAggregateFunction(") == 0) {
        auto comma = t.find(',');
        if (comma != std::string::npos) {
            auto inner = t.substr(comma + 1);
            // Remove trailing ')'
            if (!inner.empty() && inner.back() == ')') inner.pop_back();
            // Trim leading space
            auto sp = inner.find_first_not_of(' ');
            if (sp != std::string::npos) inner = inner.substr(sp);
            return ClickHouseTypeToSqlType(inner);
        }
        return SQL_VARCHAR;
    }
    if (t.find("AggregateFunction(") == 0) return SQL_VARBINARY;

    // Interval types -> VARCHAR (text representation)
    if (t.find("Interval") == 0) return SQL_VARCHAR;

    // Geo types -> VARCHAR (text representation)
    if (t == "Point" || t == "Ring" || t == "Polygon" || t == "MultiPolygon") return SQL_VARCHAR;

    // JSON / Object('json')
    if (t == "JSON" || t.find("Object(") == 0) return SQL_LONGVARCHAR;

    // Nested type -> VARCHAR
    if (t.find("Nested(") == 0) return SQL_LONGVARCHAR;

    // Default to VARCHAR
    return SQL_VARCHAR;
}

SQLULEN GetSqlTypeDisplaySize(SQLSMALLINT sql_type) {
    switch (sql_type) {
    case SQL_BIT: return 1;
    case SQL_TINYINT: return 4;
    case SQL_SMALLINT: return 6;
    case SQL_INTEGER: return 11;
    case SQL_BIGINT: return 20;
    case SQL_REAL: return 14;
    case SQL_FLOAT:
    case SQL_DOUBLE: return 24;
    case SQL_DECIMAL:
    case SQL_NUMERIC: return 38;
    case SQL_CHAR: return 256;
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR: return 65535;
    case SQL_WCHAR: return 256;
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR: return 65535;
    case SQL_TYPE_DATE: return 10;
    case SQL_TYPE_TIME: return 8;
    case SQL_TYPE_TIMESTAMP: return 26;
    case SQL_GUID: return 36;
    case SQL_BINARY:
    case SQL_VARBINARY: return 65535;
    default: return 65535;
    }
}

SQLULEN GetSqlTypeOctetLength(SQLSMALLINT sql_type) {
    switch (sql_type) {
    case SQL_BIT: return 1;
    case SQL_TINYINT: return 1;
    case SQL_SMALLINT: return 2;
    case SQL_INTEGER: return 4;
    case SQL_BIGINT: return 8;
    case SQL_REAL: return 4;
    case SQL_FLOAT:
    case SQL_DOUBLE: return 8;
    case SQL_DECIMAL:
    case SQL_NUMERIC: return 38;
    case SQL_TYPE_DATE: return sizeof(SQL_DATE_STRUCT);
    case SQL_TYPE_TIME: return sizeof(SQL_TIME_STRUCT);
    case SQL_TYPE_TIMESTAMP: return sizeof(SQL_TIMESTAMP_STRUCT);
    case SQL_GUID: return 16;
    default: return GetSqlTypeDisplaySize(sql_type);
    }
}

std::string GetSqlTypeName(SQLSMALLINT sql_type) {
    switch (sql_type) {
    case SQL_BIT: return "BIT";
    case SQL_TINYINT: return "TINYINT";
    case SQL_SMALLINT: return "SMALLINT";
    case SQL_INTEGER: return "INTEGER";
    case SQL_BIGINT: return "BIGINT";
    case SQL_REAL: return "REAL";
    case SQL_FLOAT: return "FLOAT";
    case SQL_DOUBLE: return "DOUBLE";
    case SQL_DECIMAL: return "DECIMAL";
    case SQL_NUMERIC: return "NUMERIC";
    case SQL_CHAR: return "CHAR";
    case SQL_VARCHAR: return "VARCHAR";
    case SQL_LONGVARCHAR: return "LONG VARCHAR";
    case SQL_WCHAR: return "WCHAR";
    case SQL_WVARCHAR: return "WVARCHAR";
    case SQL_TYPE_DATE: return "DATE";
    case SQL_TYPE_TIME: return "TIME";
    case SQL_TYPE_TIMESTAMP: return "TIMESTAMP";
    case SQL_GUID: return "GUID";
    case SQL_BINARY: return "BINARY";
    case SQL_VARBINARY: return "VARBINARY";
    default: return "VARCHAR";
    }
}

SQLSMALLINT GetDefaultCType(SQLSMALLINT sql_type) {
    switch (sql_type) {
    case SQL_BIT: return SQL_C_BIT;
    case SQL_TINYINT: return SQL_C_STINYINT;
    case SQL_SMALLINT: return SQL_C_SSHORT;
    case SQL_INTEGER: return SQL_C_SLONG;
    case SQL_BIGINT: return SQL_C_SBIGINT;
    case SQL_REAL: return SQL_C_FLOAT;
    case SQL_FLOAT:
    case SQL_DOUBLE: return SQL_C_DOUBLE;
    case SQL_DECIMAL:
    case SQL_NUMERIC: return SQL_C_CHAR;
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR: return SQL_C_CHAR;
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR: return SQL_C_WCHAR;
    case SQL_TYPE_DATE: return SQL_C_TYPE_DATE;
    case SQL_TYPE_TIME: return SQL_C_TYPE_TIME;
    case SQL_TYPE_TIMESTAMP: return SQL_C_TYPE_TIMESTAMP;
    case SQL_GUID: return SQL_C_CHAR;
    case SQL_BINARY:
    case SQL_VARBINARY: return SQL_C_BINARY;
    default: return SQL_C_CHAR;
    }
}

// Type info row helper (extended)
static void AddTypeInfoRow(ResultSet &rs, const std::string &type_name, SQLSMALLINT data_type,
                           SQLULEN column_size, const std::string &literal_prefix,
                           const std::string &literal_suffix,
                           const std::string &create_params,
                           SQLSMALLINT nullable, SQLSMALLINT case_sensitive,
                           SQLSMALLINT searchable, SQLSMALLINT unsigned_attr,
                           SQLSMALLINT fixed_prec_scale, SQLSMALLINT auto_increment,
                           SQLSMALLINT min_scale, SQLSMALLINT max_scale,
                           SQLSMALLINT sql_data_type, const std::optional<std::string> &datetime_sub,
                           const std::optional<std::string> &num_prec_radix) {
    ResultRow row;
    row.push_back(type_name);                                       // TYPE_NAME
    row.push_back(std::to_string(data_type));                       // DATA_TYPE
    row.push_back(std::to_string(column_size));                     // COLUMN_SIZE
    row.push_back(literal_prefix.empty() ? std::nullopt : std::optional(literal_prefix)); // LITERAL_PREFIX
    row.push_back(literal_suffix.empty() ? std::nullopt : std::optional(literal_suffix)); // LITERAL_SUFFIX
    row.push_back(create_params.empty() ? std::nullopt : std::optional(create_params));   // CREATE_PARAMS
    row.push_back(std::to_string(nullable));                        // NULLABLE
    row.push_back(std::to_string(case_sensitive));                  // CASE_SENSITIVE
    row.push_back(std::to_string(searchable));                      // SEARCHABLE
    row.push_back(std::to_string(unsigned_attr));                   // UNSIGNED_ATTRIBUTE
    row.push_back(std::to_string(fixed_prec_scale));                // FIXED_PREC_SCALE
    row.push_back(std::to_string(auto_increment));                  // AUTO_UNIQUE_VALUE
    row.push_back(type_name);                                       // LOCAL_TYPE_NAME
    row.push_back(std::to_string(min_scale));                       // MINIMUM_SCALE
    row.push_back(std::to_string(max_scale));                       // MAXIMUM_SCALE
    row.push_back(std::to_string(sql_data_type));                   // SQL_DATA_TYPE
    row.push_back(datetime_sub);                                    // SQL_DATETIME_SUB
    row.push_back(num_prec_radix);                                  // NUM_PREC_RADIX
    row.push_back(std::nullopt);                                    // INTERVAL_PRECISION
    rs.rows.push_back(std::move(row));
}

void BuildTypeInfoResultSet(ResultSet &result_set, SQLSMALLINT data_type) {
    result_set.Reset();

    // Standard type info columns
    result_set.columns = {
        {"TYPE_NAME", "String", SQL_VARCHAR, 128, 0, SQL_NO_NULLS},
        {"DATA_TYPE", "Int16", SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"COLUMN_SIZE", "Int32", SQL_INTEGER, 10, 0, SQL_NULLABLE},
        {"LITERAL_PREFIX", "String", SQL_VARCHAR, 128, 0, SQL_NULLABLE},
        {"LITERAL_SUFFIX", "String", SQL_VARCHAR, 128, 0, SQL_NULLABLE},
        {"CREATE_PARAMS", "String", SQL_VARCHAR, 128, 0, SQL_NULLABLE},
        {"NULLABLE", "Int16", SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"CASE_SENSITIVE", "Int16", SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"SEARCHABLE", "Int16", SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"UNSIGNED_ATTRIBUTE", "Int16", SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"FIXED_PREC_SCALE", "Int16", SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"AUTO_UNIQUE_VALUE", "Int16", SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"LOCAL_TYPE_NAME", "String", SQL_VARCHAR, 128, 0, SQL_NULLABLE},
        {"MINIMUM_SCALE", "Int16", SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"MAXIMUM_SCALE", "Int16", SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"SQL_DATA_TYPE", "Int16", SQL_SMALLINT, 5, 0, SQL_NO_NULLS},
        {"SQL_DATETIME_SUB", "Int16", SQL_SMALLINT, 5, 0, SQL_NULLABLE},
        {"NUM_PREC_RADIX", "Int32", SQL_INTEGER, 10, 0, SQL_NULLABLE},
        {"INTERVAL_PRECISION", "Int16", SQL_SMALLINT, 5, 0, SQL_NULLABLE}
    };

    auto addIf = [&](SQLSMALLINT type, auto &&fn) {
        if (data_type == SQL_ALL_TYPES || data_type == type) {
            fn();
        }
    };

    // --- Character types ---
    addIf(SQL_CHAR, [&] {
        AddTypeInfoRow(result_set, "CHAR", SQL_CHAR, 255,
            "'", "'", "length",
            SQL_NULLABLE, SQL_TRUE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_CHAR, std::nullopt, std::nullopt);
    });
    addIf(SQL_VARCHAR, [&] {
        AddTypeInfoRow(result_set, "VARCHAR", SQL_VARCHAR, 65535,
            "'", "'", "",
            SQL_NULLABLE, SQL_TRUE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_VARCHAR, std::nullopt, std::nullopt);
    });
    addIf(SQL_LONGVARCHAR, [&] {
        AddTypeInfoRow(result_set, "LONG VARCHAR", SQL_LONGVARCHAR, 2147483647,
            "'", "'", "",
            SQL_NULLABLE, SQL_TRUE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_LONGVARCHAR, std::nullopt, std::nullopt);
    });
    addIf(SQL_WCHAR, [&] {
        AddTypeInfoRow(result_set, "WCHAR", SQL_WCHAR, 255,
            "N'", "'", "length",
            SQL_NULLABLE, SQL_TRUE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_WCHAR, std::nullopt, std::nullopt);
    });
    addIf(SQL_WVARCHAR, [&] {
        AddTypeInfoRow(result_set, "WVARCHAR", SQL_WVARCHAR, 65535,
            "N'", "'", "",
            SQL_NULLABLE, SQL_TRUE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_WVARCHAR, std::nullopt, std::nullopt);
    });

    // --- Exact numeric types (signed) ---
    addIf(SQL_BIT, [&] {
        AddTypeInfoRow(result_set, "BIT", SQL_BIT, 1,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_TRUE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_BIT, std::nullopt, std::string("10"));
    });
    addIf(SQL_TINYINT, [&] {
        // Signed TINYINT (Int8)
        AddTypeInfoRow(result_set, "TINYINT", SQL_TINYINT, 3,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_TINYINT, std::nullopt, std::string("10"));
        // Unsigned TINYINT (UInt8)
        AddTypeInfoRow(result_set, "TINYINT UNSIGNED", SQL_TINYINT, 3,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_TRUE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_TINYINT, std::nullopt, std::string("10"));
    });
    addIf(SQL_SMALLINT, [&] {
        AddTypeInfoRow(result_set, "SMALLINT", SQL_SMALLINT, 5,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_SMALLINT, std::nullopt, std::string("10"));
        AddTypeInfoRow(result_set, "SMALLINT UNSIGNED", SQL_SMALLINT, 5,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_TRUE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_SMALLINT, std::nullopt, std::string("10"));
    });
    addIf(SQL_INTEGER, [&] {
        AddTypeInfoRow(result_set, "INTEGER", SQL_INTEGER, 10,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_INTEGER, std::nullopt, std::string("10"));
        AddTypeInfoRow(result_set, "INTEGER UNSIGNED", SQL_INTEGER, 10,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_TRUE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_INTEGER, std::nullopt, std::string("10"));
    });
    addIf(SQL_BIGINT, [&] {
        AddTypeInfoRow(result_set, "BIGINT", SQL_BIGINT, 19,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_BIGINT, std::nullopt, std::string("10"));
        AddTypeInfoRow(result_set, "BIGINT UNSIGNED", SQL_BIGINT, 19,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_TRUE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_BIGINT, std::nullopt, std::string("10"));
    });

    // --- Approximate numeric types ---
    addIf(SQL_REAL, [&] {
        AddTypeInfoRow(result_set, "REAL", SQL_REAL, 7,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_REAL, std::nullopt, std::string("10"));
    });
    addIf(SQL_DOUBLE, [&] {
        AddTypeInfoRow(result_set, "DOUBLE", SQL_DOUBLE, 15,
            "", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_DOUBLE, std::nullopt, std::string("10"));
    });

    // --- Decimal ---
    addIf(SQL_DECIMAL, [&] {
        AddTypeInfoRow(result_set, "DECIMAL", SQL_DECIMAL, 38,
            "", "", "precision,scale",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 38, SQL_DECIMAL, std::nullopt, std::string("10"));
    });

    // --- Date/time types ---
    addIf(SQL_TYPE_DATE, [&] {
        AddTypeInfoRow(result_set, "DATE", SQL_TYPE_DATE, 10,
            "'", "'", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_DATETIME, std::to_string(SQL_CODE_DATE), std::nullopt);
    });
    addIf(SQL_TYPE_TIME, [&] {
        AddTypeInfoRow(result_set, "TIME", SQL_TYPE_TIME, 8,
            "'", "'", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_DATETIME, std::to_string(SQL_CODE_TIME), std::nullopt);
    });
    addIf(SQL_TYPE_TIMESTAMP, [&] {
        AddTypeInfoRow(result_set, "TIMESTAMP", SQL_TYPE_TIMESTAMP, 26,
            "'", "'", "precision",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 9, SQL_DATETIME, std::to_string(SQL_CODE_TIMESTAMP), std::nullopt);
    });

    // --- GUID ---
    addIf(SQL_GUID, [&] {
        AddTypeInfoRow(result_set, "GUID", SQL_GUID, 36,
            "'", "'", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_GUID, std::nullopt, std::nullopt);
    });

    // --- Binary ---
    addIf(SQL_BINARY, [&] {
        AddTypeInfoRow(result_set, "BINARY", SQL_BINARY, 65535,
            "0x", "", "length",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_BINARY, std::nullopt, std::nullopt);
    });
    addIf(SQL_VARBINARY, [&] {
        AddTypeInfoRow(result_set, "VARBINARY", SQL_VARBINARY, 65535,
            "0x", "", "",
            SQL_NULLABLE, SQL_FALSE, SQL_SEARCHABLE, SQL_FALSE, SQL_FALSE, SQL_FALSE,
            0, 0, SQL_VARBINARY, std::nullopt, std::nullopt);
    });
}

// Parse precision from Decimal(P,S) or Decimal(P)
static int ParseDecimalPrecision(const std::string &t) {
    auto pos = t.find('(');
    if (pos == std::string::npos) return 38;
    auto end = t.find_first_of(",)", pos + 1);
    if (end == std::string::npos) return 38;
    return atoi(t.substr(pos + 1, end - pos - 1).c_str());
}

// Parse scale from Decimal(P,S)
static int ParseDecimalScale(const std::string &t) {
    auto comma = t.find(',');
    if (comma == std::string::npos) return 0;
    auto end = t.find(')', comma + 1);
    if (end == std::string::npos) return 0;
    return atoi(t.substr(comma + 1, end - comma - 1).c_str());
}

// Parse FixedString(N) length
static int ParseFixedStringLength(const std::string &t) {
    auto pos = t.find('(');
    if (pos == std::string::npos) return 256;
    auto end = t.find(')', pos + 1);
    if (end == std::string::npos) return 256;
    return atoi(t.substr(pos + 1, end - pos - 1).c_str());
}

// Parse DateTime64(P) precision
static int ParseDateTime64Precision(const std::string &t) {
    auto pos = t.find('(');
    if (pos == std::string::npos) return 3; // default
    auto end = t.find_first_of(",)", pos + 1);
    if (end == std::string::npos) return 3;
    return atoi(t.substr(pos + 1, end - pos - 1).c_str());
}

SQLULEN GetColumnSizeForType(const std::string &ch_type, SQLSMALLINT sql_type) {
    std::string t = NormalizeType(ch_type);

    // FixedString(N)
    if (t.find("FixedString(") == 0) {
        return (SQLULEN)ParseFixedStringLength(t);
    }

    // Decimal(P,S)
    if (t.find("Decimal") == 0) {
        if (t == "Decimal32") return 9;
        if (t == "Decimal64") return 18;
        if (t == "Decimal128") return 38;
        if (t == "Decimal256") return 76;
        return (SQLULEN)ParseDecimalPrecision(t);
    }

    // DateTime64(P)
    if (t.find("DateTime64") == 0) {
        int p = ParseDateTime64Precision(t);
        return (SQLULEN)(20 + p); // YYYY-MM-DD HH:MM:SS + .fraction
    }

    // Enum types - treat as VARCHAR with reasonable size
    if (t.find("Enum8") == 0 || t.find("Enum16") == 0) {
        return 256;
    }

    // Fall back to generic display size
    return GetSqlTypeDisplaySize(sql_type);
}

SQLSMALLINT GetDecimalDigitsForType(const std::string &ch_type, SQLSMALLINT sql_type) {
    std::string t = NormalizeType(ch_type);

    // Decimal(P,S)
    if (t.find("Decimal") == 0) {
        if (t == "Decimal32") return 0;
        if (t == "Decimal64") return 0;
        if (t == "Decimal128") return 0;
        if (t == "Decimal256") return 0;
        return (SQLSMALLINT)ParseDecimalScale(t);
    }

    // DateTime64(P)
    if (t.find("DateTime64") == 0) {
        return (SQLSMALLINT)ParseDateTime64Precision(t);
    }

    // DateTime -> seconds precision
    if (t == "DateTime" || t.find("DateTime(") == 0) {
        return 0;
    }

    // Float types
    if (t == "Float32") return 7;
    if (t == "Float64") return 15;

    return 0;
}

bool IsUnsignedType(const std::string &ch_type) {
    std::string t = NormalizeType(ch_type);
    return (t.find("UInt") == 0);
}

bool IsNullableType(const std::string &ch_type) {
    return (ch_type.find("Nullable(") != std::string::npos);
}

std::string GetCTypeName(SQLSMALLINT c_type) {
    switch (c_type) {
    case SQL_C_CHAR: return "SQL_C_CHAR";
    case SQL_C_WCHAR: return "SQL_C_WCHAR";
    case SQL_C_SLONG: return "SQL_C_SLONG";
    case SQL_C_ULONG: return "SQL_C_ULONG";
    case SQL_C_SSHORT: return "SQL_C_SSHORT";
    case SQL_C_USHORT: return "SQL_C_USHORT";
    case SQL_C_STINYINT: return "SQL_C_STINYINT";
    case SQL_C_UTINYINT: return "SQL_C_UTINYINT";
    case SQL_C_SBIGINT: return "SQL_C_SBIGINT";
    case SQL_C_UBIGINT: return "SQL_C_UBIGINT";
    case SQL_C_FLOAT: return "SQL_C_FLOAT";
    case SQL_C_DOUBLE: return "SQL_C_DOUBLE";
    case SQL_C_BIT: return "SQL_C_BIT";
    case SQL_C_NUMERIC: return "SQL_C_NUMERIC";
    case SQL_C_GUID: return "SQL_C_GUID";
    case SQL_C_TYPE_DATE: return "SQL_C_TYPE_DATE";
    case SQL_C_TYPE_TIME: return "SQL_C_TYPE_TIME";
    case SQL_C_TYPE_TIMESTAMP: return "SQL_C_TYPE_TIMESTAMP";
    case SQL_C_BINARY: return "SQL_C_BINARY";
    default: return "SQL_C_UNKNOWN(" + std::to_string(c_type) + ")";
    }
}

bool ValidateNumericRange(const std::string &value, SQLSMALLINT target_c_type) {
    if (value.empty()) return true;

    // For string/binary targets, always valid
    if (target_c_type == SQL_C_CHAR || target_c_type == SQL_C_WCHAR ||
        target_c_type == SQL_C_BINARY || target_c_type == SQL_C_DEFAULT) {
        return true;
    }

    // Try parsing as a double for range check
    char *endp = nullptr;
    double dval = strtod(value.c_str(), &endp);
    if (endp == value.c_str()) return true; // not a number, let conversion handle it

    switch (target_c_type) {
    case SQL_C_STINYINT:
    case SQL_C_TINYINT:
        return (dval >= -128.0 && dval <= 127.0);
    case SQL_C_UTINYINT:
        return (dval >= 0.0 && dval <= 255.0);
    case SQL_C_SSHORT:
    case SQL_C_SHORT:
        return (dval >= -32768.0 && dval <= 32767.0);
    case SQL_C_USHORT:
        return (dval >= 0.0 && dval <= 65535.0);
    case SQL_C_SLONG:
    case SQL_C_LONG:
        return (dval >= (double)LONG_MIN && dval <= (double)LONG_MAX);
    case SQL_C_ULONG:
        return (dval >= 0.0 && dval <= (double)ULONG_MAX);
    case SQL_C_SBIGINT:
        return (dval >= (double)LLONG_MIN && dval <= (double)LLONG_MAX);
    case SQL_C_UBIGINT:
        return (dval >= 0.0 && dval <= (double)ULLONG_MAX);
    default:
        return true;
    }
}

} // namespace clickhouse_odbc
