#include "include/util.h"
#include "include/trace.h"

#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <sstream>

namespace clickhouse_odbc {

std::string WideToUtf8(const SQLWCHAR *wide, SQLSMALLINT len) {
    if (!wide) return "";

    int wlen;
    if (len == SQL_NTS || len < 0) {
        wlen = (int)wcslen((const wchar_t *)wide);
    } else {
        wlen = len;
    }

    if (wlen == 0) return "";

    int size = WideCharToMultiByte(CP_UTF8, 0, (const wchar_t *)wide, wlen, nullptr, 0, nullptr, nullptr);
    if (size <= 0) return "";

    std::string result(size, '\0');
    WideCharToMultiByte(CP_UTF8, 0, (const wchar_t *)wide, wlen, &result[0], size, nullptr, nullptr);
    return result;
}

std::string WideToUtf8(const std::wstring &wide) {
    return WideToUtf8((const SQLWCHAR *)wide.c_str(), (SQLSMALLINT)wide.size());
}

std::wstring Utf8ToWide(const std::string &utf8) {
    if (utf8.empty()) return L"";

    int size = MultiByteToWideChar(CP_UTF8, 0, utf8.c_str(), (int)utf8.size(), nullptr, 0);
    if (size <= 0) return L"";

    std::wstring result(size, L'\0');
    MultiByteToWideChar(CP_UTF8, 0, utf8.c_str(), (int)utf8.size(), &result[0], size);
    return result;
}

SQLRETURN CopyStringToBuffer(const std::string &src, SQLCHAR *target, SQLSMALLINT buffer_length,
                             SQLSMALLINT *string_length) {
    SQLSMALLINT len = (SQLSMALLINT)src.size();
    if (string_length) *string_length = len;

    if (target && buffer_length > 0) {
        SQLSMALLINT copy_len = (len < buffer_length - 1) ? len : (buffer_length - 1);
        memcpy(target, src.c_str(), copy_len);
        target[copy_len] = '\0';
        if (len >= buffer_length) {
            return SQL_SUCCESS_WITH_INFO;
        }
    }
    return SQL_SUCCESS;
}

SQLRETURN CopyStringToBufferW(const std::string &src, SQLWCHAR *target, SQLSMALLINT buffer_length,
                              SQLSMALLINT *string_length) {
    std::wstring wide = Utf8ToWide(src);
    SQLSMALLINT char_count = (SQLSMALLINT)wide.size();
    if (string_length) *string_length = char_count;

    if (target && buffer_length > 0) {
        SQLSMALLINT copy_chars = (char_count < buffer_length - 1) ? char_count : (buffer_length - 1);
        memcpy(target, wide.c_str(), copy_chars * sizeof(SQLWCHAR));
        target[copy_chars] = L'\0';
        if (char_count >= buffer_length) {
            return SQL_SUCCESS_WITH_INFO;
        }
    }
    return SQL_SUCCESS;
}

std::unordered_map<std::string, std::string> ParseConnectionString(const std::string &conn_str) {
    std::unordered_map<std::string, std::string> params;

    std::istringstream stream(conn_str);
    std::string token;

    while (std::getline(stream, token, ';')) {
        auto eq_pos = token.find('=');
        if (eq_pos != std::string::npos) {
            std::string key = Trim(token.substr(0, eq_pos));
            std::string value = Trim(token.substr(eq_pos + 1));
            if (!key.empty()) {
                params[key] = value;
            }
        }
    }

    return params;
}

std::string ToUpper(const std::string &s) {
    std::string result = s;
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return (char)std::toupper(c); });
    return result;
}

std::string Trim(const std::string &s) {
    auto start = std::find_if_not(s.begin(), s.end(), [](unsigned char c) { return std::isspace(c); });
    auto end = std::find_if_not(s.rbegin(), s.rend(), [](unsigned char c) { return std::isspace(c); }).base();
    if (start >= end) return "";
    return std::string(start, end);
}

std::string EscapeSqlString(const std::string &value) {
    std::string escaped;
    escaped.reserve(value.size() + 10);
    for (char c : value) {
        if (c == '\'') {
            escaped += '\\';
            escaped += '\'';
        } else if (c == '\\') {
            escaped += '\\';
            escaped += '\\';
        } else {
            escaped += c;
        }
    }
    return escaped;
}

SQLSMALLINT ResolveCDefaultType(SQLSMALLINT sql_type) {
    switch (sql_type) {
    case SQL_CHAR:
    case SQL_VARCHAR:
    case SQL_LONGVARCHAR:
        return SQL_C_CHAR;
    case SQL_WCHAR:
    case SQL_WVARCHAR:
    case SQL_WLONGVARCHAR:
        return SQL_C_WCHAR;
    case SQL_SMALLINT:
        return SQL_C_SSHORT;
    case SQL_INTEGER:
        return SQL_C_SLONG;
    case SQL_REAL:
        return SQL_C_FLOAT;
    case SQL_FLOAT:
        return SQL_C_DOUBLE;
    case SQL_DOUBLE:
        return SQL_C_DOUBLE;
    case SQL_BIT:
        return SQL_C_BIT;
    case SQL_TINYINT:
        return SQL_C_STINYINT;
    case SQL_BIGINT:
        return SQL_C_SBIGINT;
    case SQL_BINARY:
    case SQL_VARBINARY:
    case SQL_LONGVARBINARY:
        return SQL_C_BINARY;
    case SQL_TYPE_DATE:
    case SQL_DATE:
        return SQL_C_TYPE_DATE;
    case SQL_TYPE_TIME:
    case SQL_TIME:
        return SQL_C_TYPE_TIME;
    case SQL_TYPE_TIMESTAMP:
    case SQL_TIMESTAMP:
        return SQL_C_TYPE_TIMESTAMP;
    case SQL_NUMERIC:
    case SQL_DECIMAL:
        return SQL_C_CHAR;
    case SQL_GUID:
        return SQL_C_GUID;
    default:
        return SQL_C_CHAR;
    }
}

std::string ExtractParameterValue(const BoundParameter &param) {
    // Check for NULL
    if (param.str_len_or_ind && *param.str_len_or_ind == SQL_NULL_DATA) {
        return "NULL";
    }

    if (!param.parameter_value) {
        return "NULL";
    }

    // Resolve SQL_C_DEFAULT to actual C type based on SQL parameter type
    SQLSMALLINT effective_type = param.value_type;
    if (effective_type == SQL_C_DEFAULT) {
        effective_type = ResolveCDefaultType(param.parameter_type);
        TRACE_LOG(TraceLevel::Debug, "ExtractParameterValue",
                  "SQL_C_DEFAULT resolved: parameter_type=" + std::to_string(param.parameter_type) +
                  " -> effective_type=" + std::to_string(effective_type));
    }

    switch (effective_type) {
    case SQL_C_CHAR: {
        SQLLEN len = 0;
        if (param.str_len_or_ind) {
            len = *param.str_len_or_ind;
            if (len == SQL_NTS) {
                len = (SQLLEN)strlen(static_cast<const char *>(param.parameter_value));
            }
        } else {
            len = (SQLLEN)strlen(static_cast<const char *>(param.parameter_value));
        }
        std::string val(static_cast<const char *>(param.parameter_value), len);
        return "'" + EscapeSqlString(val) + "'";
    }
    case SQL_C_WCHAR: {
        const SQLWCHAR *wval = static_cast<const SQLWCHAR *>(param.parameter_value);
        SQLSMALLINT wlen = SQL_NTS;
        if (param.str_len_or_ind) {
            SQLLEN byte_len = *param.str_len_or_ind;
            if (byte_len != SQL_NTS) {
                wlen = static_cast<SQLSMALLINT>(byte_len / sizeof(SQLWCHAR));
            }
        }
        std::string val = WideToUtf8(wval, wlen);
        return "'" + EscapeSqlString(val) + "'";
    }
    case SQL_C_SSHORT:
    case SQL_C_SHORT:
        return std::to_string(*static_cast<const short *>(param.parameter_value));
    case SQL_C_USHORT:
        return std::to_string(*static_cast<const unsigned short *>(param.parameter_value));
    case SQL_C_SLONG:
    case SQL_C_LONG:
        return std::to_string(*static_cast<const SQLINTEGER *>(param.parameter_value));
    case SQL_C_ULONG:
        return std::to_string(*static_cast<const SQLUINTEGER *>(param.parameter_value));
    case SQL_C_SBIGINT:
        return std::to_string(*static_cast<const int64_t *>(param.parameter_value));
    case SQL_C_UBIGINT:
        return std::to_string(*static_cast<const uint64_t *>(param.parameter_value));
    case SQL_C_FLOAT: {
        float fval = *static_cast<const float *>(param.parameter_value);
        if (fval == std::floor(fval) && std::abs(fval) < 1e15f) {
            return std::to_string(static_cast<int64_t>(fval));
        }
        char fbuf[64];
        snprintf(fbuf, sizeof(fbuf), "%.15g", static_cast<double>(fval));
        return std::string(fbuf);
    }
    case SQL_C_DOUBLE: {
        double dval = *static_cast<const double *>(param.parameter_value);
        if (dval == std::floor(dval) && std::abs(dval) < 1e18) {
            return std::to_string(static_cast<int64_t>(dval));
        }
        char dbuf[64];
        snprintf(dbuf, sizeof(dbuf), "%.15g", dval);
        return std::string(dbuf);
    }
    case SQL_C_BIT:
    case SQL_C_TINYINT:
    case SQL_C_STINYINT:
        return std::to_string(*static_cast<const signed char *>(param.parameter_value));
    case SQL_C_UTINYINT:
        return std::to_string(*static_cast<const unsigned char *>(param.parameter_value));
    case SQL_C_TYPE_DATE:
    case SQL_C_DATE: {
        const SQL_DATE_STRUCT *ds = static_cast<const SQL_DATE_STRUCT *>(param.parameter_value);
        char buf[32];
        snprintf(buf, sizeof(buf), "'%04d-%02u-%02u'", ds->year, ds->month, ds->day);
        return std::string(buf);
    }
    case SQL_C_TYPE_TIME:
    case SQL_C_TIME: {
        const SQL_TIME_STRUCT *ts = static_cast<const SQL_TIME_STRUCT *>(param.parameter_value);
        char buf[32];
        snprintf(buf, sizeof(buf), "'%02u:%02u:%02u'", ts->hour, ts->minute, ts->second);
        return std::string(buf);
    }
    case SQL_C_TYPE_TIMESTAMP:
    case SQL_C_TIMESTAMP: {
        const SQL_TIMESTAMP_STRUCT *ts = static_cast<const SQL_TIMESTAMP_STRUCT *>(param.parameter_value);
        char buf[64];
        if (ts->fraction > 0) {
            snprintf(buf, sizeof(buf), "'%04d-%02u-%02u %02u:%02u:%02u.%09u'",
                     ts->year, ts->month, ts->day,
                     ts->hour, ts->minute, ts->second, ts->fraction);
        } else {
            snprintf(buf, sizeof(buf), "'%04d-%02u-%02u %02u:%02u:%02u'",
                     ts->year, ts->month, ts->day,
                     ts->hour, ts->minute, ts->second);
        }
        return std::string(buf);
    }
    case SQL_C_BINARY: {
        // Encode as hex string for ClickHouse unhex() usage
        SQLLEN len = param.buffer_length;
        if (param.str_len_or_ind && *param.str_len_or_ind != SQL_NTS) {
            len = *param.str_len_or_ind;
        }
        const unsigned char *data = static_cast<const unsigned char *>(param.parameter_value);
        std::string hex = "unhex('";
        char hexbuf[3];
        for (SQLLEN i = 0; i < len; i++) {
            snprintf(hexbuf, sizeof(hexbuf), "%02X", data[i]);
            hex += hexbuf;
        }
        hex += "')";
        return hex;
    }
    case SQL_C_GUID: {
        const SQLGUID *guid = static_cast<const SQLGUID *>(param.parameter_value);
        char buf[64];
        snprintf(buf, sizeof(buf), "'%08lX-%04X-%04X-%02X%02X-%02X%02X%02X%02X%02X%02X'",
                 guid->Data1, guid->Data2, guid->Data3,
                 guid->Data4[0], guid->Data4[1], guid->Data4[2], guid->Data4[3],
                 guid->Data4[4], guid->Data4[5], guid->Data4[6], guid->Data4[7]);
        return std::string(buf);
    }
    default: {
        // Fallback: try to interpret as string
        if (param.parameter_value) {
            const char *str = static_cast<const char *>(param.parameter_value);
            SQLLEN len = param.buffer_length;
            if (param.str_len_or_ind) {
                len = *param.str_len_or_ind;
                if (len == SQL_NTS) {
                    len = (SQLLEN)strlen(str);
                }
            }
            std::string val(str, len > 0 ? len : 0);
            return "'" + EscapeSqlString(val) + "'";
        }
        return "NULL";
    }
    }
}

std::string SubstituteParameters(const std::string &query,
                                 const std::unordered_map<SQLUSMALLINT, BoundParameter> &params,
                                 std::string &error_msg) {
    if (params.empty()) {
        return query;
    }

    std::string result;
    result.reserve(query.size() * 2);

    SQLUSMALLINT param_index = 1;
    bool in_single_quote = false;
    bool in_double_quote = false;
    bool in_backtick = false;

    for (size_t i = 0; i < query.size(); i++) {
        char c = query[i];

        // Track string literals and quoted identifiers to avoid substituting '?' inside them
        if (c == '\'' && !in_double_quote && !in_backtick) {
            // Check for escaped quote
            if (i + 1 < query.size() && query[i + 1] == '\'') {
                result += c;
                result += query[++i];
                continue;
            }
            in_single_quote = !in_single_quote;
            result += c;
            continue;
        }
        if (c == '"' && !in_single_quote && !in_backtick) {
            in_double_quote = !in_double_quote;
            result += c;
            continue;
        }
        if (c == '`' && !in_single_quote && !in_double_quote) {
            in_backtick = !in_backtick;
            result += c;
            continue;
        }

        if (c == '?' && !in_single_quote && !in_double_quote && !in_backtick) {
            auto it = params.find(param_index);
            if (it == params.end()) {
                error_msg = "Parameter " + std::to_string(param_index) + " not bound";
                return "";
            }
            result += ExtractParameterValue(it->second);
            param_index++;
        } else {
            result += c;
        }
    }

    return result;
}

// ============================================================================
// ODBC Escape Sequence Processing
// ============================================================================

// Map of ODBC scalar function names to ClickHouse equivalents
static std::string TranslateScalarFunction(const std::string &func_name, const std::string &args) {
    std::string upper_name = func_name;
    std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(), ::toupper);

    // String functions
    if (upper_name == "UCASE") return "upper(" + args + ")";
    if (upper_name == "LCASE") return "lower(" + args + ")";
    if (upper_name == "LENGTH" || upper_name == "CHAR_LENGTH" || upper_name == "CHARACTER_LENGTH")
        return "length(" + args + ")";
    if (upper_name == "OCTET_LENGTH") return "lengthUTF8(" + args + ")";
    if (upper_name == "LTRIM") return "trimLeft(" + args + ")";
    if (upper_name == "RTRIM") return "trimRight(" + args + ")";
    if (upper_name == "SUBSTRING") return "substring(" + args + ")";
    if (upper_name == "CONCAT") return "concat(" + args + ")";
    if (upper_name == "LOCATE") {
        // ODBC: LOCATE(needle, haystack[, start]) → ClickHouse: positionUTF8(haystack, needle[, start])
        // Need to swap first two arguments
        // Parse args respecting nested parens and string literals
        std::vector<std::string> parts;
        std::string current;
        int paren_depth = 0;
        bool in_quote = false;
        for (size_t i = 0; i < args.size(); i++) {
            char c = args[i];
            if (c == '\'' && !in_quote) { in_quote = true; current += c; continue; }
            if (c == '\'' && in_quote) { in_quote = false; current += c; continue; }
            if (in_quote) { current += c; continue; }
            if (c == '(') { paren_depth++; current += c; continue; }
            if (c == ')') { paren_depth--; current += c; continue; }
            if (c == ',' && paren_depth == 0) {
                parts.push_back(current);
                current.clear();
                // Skip leading space after comma
                if (i + 1 < args.size() && args[i + 1] == ' ') i++;
                continue;
            }
            current += c;
        }
        if (!current.empty()) parts.push_back(current);

        if (parts.size() >= 2) {
            std::string result = "positionUTF8(" + parts[1] + ", " + parts[0];
            for (size_t i = 2; i < parts.size(); i++) {
                result += ", " + parts[i];
            }
            result += ")";
            return result;
        }
        return "positionUTF8(" + args + ")";
    }
    if (upper_name == "REPLACE") return "replaceAll(" + args + ")";
    if (upper_name == "LEFT") return "left(" + args + ")";
    if (upper_name == "RIGHT") return "right(" + args + ")";
    if (upper_name == "SPACE") return "repeat(' ', " + args + ")";
    if (upper_name == "REPEAT") return "repeat(" + args + ")";
    if (upper_name == "ASCII") return "toUInt8(substring(" + args + ", 1, 1))";
    if (upper_name == "CHAR") return "char(" + args + ")";
    if (upper_name == "INSERT") return "concat(left(" + args + "))"; // approximation
    if (upper_name == "BIT_LENGTH") return "(length(" + args + ") * 8)";

    // Numeric functions
    if (upper_name == "ABS") return "abs(" + args + ")";
    if (upper_name == "CEILING" || upper_name == "CEIL") return "ceil(" + args + ")";
    if (upper_name == "FLOOR") return "floor(" + args + ")";
    if (upper_name == "ROUND") return "round(" + args + ")";
    if (upper_name == "TRUNCATE") return "trunc(" + args + ")";
    if (upper_name == "SIGN") return "sign(" + args + ")";
    if (upper_name == "SQRT") return "sqrt(" + args + ")";
    if (upper_name == "POWER" || upper_name == "POW") return "pow(" + args + ")";
    if (upper_name == "EXP") return "exp(" + args + ")";
    if (upper_name == "LOG" || upper_name == "LN") return "log(" + args + ")";
    if (upper_name == "LOG10") return "log10(" + args + ")";
    if (upper_name == "LOG2") return "log2(" + args + ")";
    if (upper_name == "MOD") return "modulo(" + args + ")";
    if (upper_name == "PI") return "pi()";
    if (upper_name == "RAND") return "rand()";
    if (upper_name == "DEGREES") return "degrees(" + args + ")";
    if (upper_name == "RADIANS") return "radians(" + args + ")";
    if (upper_name == "SIN") return "sin(" + args + ")";
    if (upper_name == "COS") return "cos(" + args + ")";
    if (upper_name == "TAN") return "tan(" + args + ")";
    if (upper_name == "ASIN") return "asin(" + args + ")";
    if (upper_name == "ACOS") return "acos(" + args + ")";
    if (upper_name == "ATAN") return "atan(" + args + ")";
    if (upper_name == "ATAN2") return "atan2(" + args + ")";

    // Date/Time functions
    if (upper_name == "NOW") return "now()";
    if (upper_name == "CURDATE" || upper_name == "CURRENT_DATE") return "today()";
    if (upper_name == "CURTIME" || upper_name == "CURRENT_TIME") return "toTimeString(now())";
    if (upper_name == "CURRENT_TIMESTAMP") return "now()";
    if (upper_name == "DAYOFMONTH" || upper_name == "DAY") return "toDayOfMonth(" + args + ")";
    if (upper_name == "DAYOFWEEK") return "toDayOfWeek(" + args + ")";
    if (upper_name == "DAYOFYEAR") return "toDayOfYear(" + args + ")";
    if (upper_name == "MONTH") return "toMonth(" + args + ")";
    if (upper_name == "YEAR") return "toYear(" + args + ")";
    if (upper_name == "HOUR") return "toHour(" + args + ")";
    if (upper_name == "MINUTE") return "toMinute(" + args + ")";
    if (upper_name == "SECOND") return "toSecond(" + args + ")";
    if (upper_name == "WEEK") return "toISOWeek(" + args + ")";
    if (upper_name == "QUARTER") return "toQuarter(" + args + ")";
    if (upper_name == "TIMESTAMPADD") return "date_add(" + args + ")";
    if (upper_name == "TIMESTAMPDIFF") return "date_diff(" + args + ")";
    if (upper_name == "DAYNAME") return "dateName('day', " + args + ")";
    if (upper_name == "MONTHNAME") return "dateName('month', " + args + ")";
    if (upper_name == "EXTRACT") return "extract(" + args + ")";

    // System functions
    if (upper_name == "IFNULL") return "ifNull(" + args + ")";
    if (upper_name == "DATABASE") return "currentDatabase()";
    if (upper_name == "USER") return "currentUser()";

    // CONVERT function: {fn CONVERT(value, SQL_TYPE)} -> ClickHouse CAST
    if (upper_name == "CONVERT") {
        // Parse "value, SQL_TYPE" from args
        auto comma_pos = args.rfind(',');
        if (comma_pos != std::string::npos) {
            std::string value = args.substr(0, comma_pos);
            std::string type_str = args.substr(comma_pos + 1);
            // Trim whitespace
            while (!type_str.empty() && type_str.front() == ' ') type_str.erase(0, 1);
            while (!type_str.empty() && type_str.back() == ' ') type_str.pop_back();
            std::string upper_type = type_str;
            std::transform(upper_type.begin(), upper_type.end(), upper_type.begin(), ::toupper);

            if (upper_type == "SQL_VARCHAR" || upper_type == "SQL_CHAR" ||
                upper_type == "SQL_LONGVARCHAR" || upper_type == "SQL_WVARCHAR" ||
                upper_type == "SQL_WCHAR" || upper_type == "SQL_WLONGVARCHAR")
                return "toString(" + value + ")";
            if (upper_type == "SQL_INTEGER" || upper_type == "SQL_INT")
                return "toInt32(" + value + ")";
            if (upper_type == "SQL_SMALLINT")
                return "toInt16(" + value + ")";
            if (upper_type == "SQL_TINYINT")
                return "toInt8(" + value + ")";
            if (upper_type == "SQL_BIGINT")
                return "toInt64(" + value + ")";
            if (upper_type == "SQL_FLOAT" || upper_type == "SQL_DOUBLE")
                return "toFloat64(" + value + ")";
            if (upper_type == "SQL_REAL")
                return "toFloat32(" + value + ")";
            if (upper_type == "SQL_DECIMAL" || upper_type == "SQL_NUMERIC")
                return "toDecimal64(" + value + ", 6)";
            if (upper_type == "SQL_DATE")
                return "toDate(" + value + ")";
            if (upper_type == "SQL_TIMESTAMP")
                return "toDateTime(" + value + ")";
            if (upper_type == "SQL_TIME")
                return "toTimeString(" + value + ")";
            if (upper_type == "SQL_BIT")
                return "toUInt8(" + value + ")";
            // Unknown SQL type: fall through with CAST
            return "CAST(" + value + " AS String)";
        }
    }

    // Unknown function: pass through as-is
    return func_name + "(" + args + ")";
}

// Parse a single escape sequence starting after '{', return the translated content
// The position should be right after the opening '{'
static std::string ParseEscapeContent(const std::string &sql, size_t &pos) {
    // Skip leading whitespace
    while (pos < sql.size() && sql[pos] == ' ') pos++;

    if (pos >= sql.size()) return "";

    // Determine the escape type
    std::string keyword;
    size_t kw_start = pos;
    while (pos < sql.size() && sql[pos] != ' ' && sql[pos] != '\'' && sql[pos] != '}') {
        keyword += sql[pos++];
    }

    std::string upper_kw = keyword;
    std::transform(upper_kw.begin(), upper_kw.end(), upper_kw.begin(), ::toupper);

    // {fn function(args)} — scalar function
    if (upper_kw == "FN") {
        while (pos < sql.size() && sql[pos] == ' ') pos++;

        // Extract function name
        std::string func_name;
        while (pos < sql.size() && sql[pos] != '(' && sql[pos] != ' ' && sql[pos] != '}') {
            func_name += sql[pos++];
        }

        // Extract arguments (handle nested parens and escape sequences)
        std::string args;
        if (pos < sql.size() && sql[pos] == '(') {
            pos++; // skip '('
            int paren_depth = 1;
            while (pos < sql.size() && paren_depth > 0) {
                if (sql[pos] == '{') {
                    pos++; // skip '{'
                    args += ParseEscapeContent(sql, pos);
                    if (pos < sql.size() && sql[pos] == '}') pos++; // skip '}'
                } else if (sql[pos] == '(') {
                    paren_depth++;
                    args += sql[pos++];
                } else if (sql[pos] == ')') {
                    paren_depth--;
                    if (paren_depth > 0) args += sql[pos];
                    pos++;
                } else if (sql[pos] == '\'') {
                    // Copy string literal as-is
                    args += sql[pos++];
                    while (pos < sql.size()) {
                        args += sql[pos];
                        if (sql[pos] == '\'' && (pos + 1 >= sql.size() || sql[pos + 1] != '\'')) {
                            pos++;
                            break;
                        }
                        if (sql[pos] == '\'' && pos + 1 < sql.size() && sql[pos + 1] == '\'') {
                            args += sql[++pos]; // escaped quote
                        }
                        pos++;
                    }
                } else {
                    args += sql[pos++];
                }
            }
        }

        return TranslateScalarFunction(func_name, args);
    }

    // {d 'yyyy-mm-dd'} — date literal
    if (upper_kw == "D") {
        while (pos < sql.size() && sql[pos] == ' ') pos++;
        // Just extract the quoted value and pass through
        std::string literal;
        if (pos < sql.size() && sql[pos] == '\'') {
            literal += sql[pos++];
            while (pos < sql.size()) {
                literal += sql[pos];
                if (sql[pos] == '\'') { pos++; break; }
                pos++;
            }
        }
        return literal;
    }

    // {t 'hh:mm:ss'} — time literal
    if (upper_kw == "T") {
        while (pos < sql.size() && sql[pos] == ' ') pos++;
        std::string literal;
        if (pos < sql.size() && sql[pos] == '\'') {
            literal += sql[pos++];
            while (pos < sql.size()) {
                literal += sql[pos];
                if (sql[pos] == '\'') { pos++; break; }
                pos++;
            }
        }
        return literal;
    }

    // {ts 'yyyy-mm-dd hh:mm:ss'} — timestamp literal
    if (upper_kw == "TS") {
        while (pos < sql.size() && sql[pos] == ' ') pos++;
        std::string literal;
        if (pos < sql.size() && sql[pos] == '\'') {
            literal += sql[pos++];
            while (pos < sql.size()) {
                literal += sql[pos];
                if (sql[pos] == '\'') { pos++; break; }
                pos++;
            }
        }
        return literal;
    }

    // {oj ...} — outer join (pass through content)
    if (upper_kw == "OJ") {
        while (pos < sql.size() && sql[pos] == ' ') pos++;
        std::string content;
        int brace_depth = 1;
        while (pos < sql.size() && brace_depth > 0) {
            if (sql[pos] == '{') brace_depth++;
            else if (sql[pos] == '}') {
                brace_depth--;
                if (brace_depth == 0) break;
            }
            content += sql[pos++];
        }
        return content;
    }

    // {escape 'x'} — escape character for LIKE
    if (upper_kw == "ESCAPE") {
        while (pos < sql.size() && sql[pos] == ' ') pos++;
        std::string content = "ESCAPE ";
        // Copy the rest until '}'
        while (pos < sql.size() && sql[pos] != '}') {
            content += sql[pos++];
        }
        return content;
    }

    // {call procedure_name(...)} — procedure call (pass through)
    if (upper_kw == "CALL") {
        while (pos < sql.size() && sql[pos] == ' ') pos++;
        std::string content;
        int brace_depth = 1;
        while (pos < sql.size() && brace_depth > 0) {
            if (sql[pos] == '{') brace_depth++;
            else if (sql[pos] == '}') {
                brace_depth--;
                if (brace_depth == 0) break;
            }
            content += sql[pos++];
        }
        return content;
    }

    // Unknown escape type: pass through
    std::string content = keyword;
    while (pos < sql.size() && sql[pos] != '}') {
        content += sql[pos++];
    }
    return content;
}

std::string ProcessOdbcEscapeSequences(const std::string &sql) {
    if (sql.find('{') == std::string::npos) {
        return sql; // Fast path: no escape sequences
    }

    std::string result;
    result.reserve(sql.size());

    bool in_single_quote = false;
    bool in_double_quote = false;

    for (size_t i = 0; i < sql.size(); i++) {
        char c = sql[i];

        // Track string literals to avoid processing '{' inside them
        if (c == '\'' && !in_double_quote) {
            if (i + 1 < sql.size() && sql[i + 1] == '\'') {
                result += c;
                result += sql[++i];
                continue;
            }
            in_single_quote = !in_single_quote;
            result += c;
            continue;
        }
        if (c == '"' && !in_single_quote) {
            in_double_quote = !in_double_quote;
            result += c;
            continue;
        }

        if (c == '{' && !in_single_quote && !in_double_quote) {
            size_t pos = i + 1;
            result += ParseEscapeContent(sql, pos);
            if (pos < sql.size() && sql[pos] == '}') pos++;
            i = pos - 1; // -1 because for-loop will increment
        } else {
            result += c;
        }
    }

    return result;
}

// ============================================================================
// ConvertTopToLimit: SELECT TOP N → SELECT ... LIMIT N
// Converts SELECT TOP N syntax generated by MS Access to ClickHouse's LIMIT N.
// ============================================================================
std::string ConvertTopToLimit(const std::string &sql) {
    // Fast path: short queries can't contain TOP
    if (sql.size() < 14) return sql; // "SELECT TOP 1 x" minimum

    std::string upper = sql;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);

    // Find SELECT
    auto select_pos = upper.find("SELECT");
    if (select_pos == std::string::npos) return sql;

    size_t pos = select_pos + 6;
    // Skip whitespace
    while (pos < sql.size() && (sql[pos] == ' ' || sql[pos] == '\t')) pos++;

    // Check for optional DISTINCT / ALL
    if (pos + 8 <= upper.size() && upper.substr(pos, 8) == "DISTINCT") {
        pos += 8;
        while (pos < sql.size() && (sql[pos] == ' ' || sql[pos] == '\t')) pos++;
    } else if (pos + 3 <= upper.size() && upper.substr(pos, 3) == "ALL") {
        pos += 3;
        while (pos < sql.size() && (sql[pos] == ' ' || sql[pos] == '\t')) pos++;
    }

    // Check for TOP keyword
    if (pos + 3 > upper.size()) return sql;
    if (upper.substr(pos, 3) != "TOP") return sql;
    // TOP must be followed by whitespace (not part of a longer identifier like "TOPAZ")
    if (pos + 3 < sql.size() && sql[pos + 3] != ' ' && sql[pos + 3] != '\t') return sql;

    size_t top_start = pos;
    pos += 3;
    while (pos < sql.size() && (sql[pos] == ' ' || sql[pos] == '\t')) pos++;

    // Parse the number N
    size_t num_start = pos;
    while (pos < sql.size() && sql[pos] >= '0' && sql[pos] <= '9') pos++;
    if (pos == num_start) return sql; // No number found

    std::string limit_num = sql.substr(num_start, pos - num_start);

    // Skip whitespace after number
    while (pos < sql.size() && (sql[pos] == ' ' || sql[pos] == '\t')) pos++;

    // Check for PERCENT (not supported, return as-is)
    if (pos + 7 <= upper.size() && upper.substr(pos, 7) == "PERCENT") {
        return sql;
    }

    // Build result: remove "TOP N " from the query
    std::string result = sql.substr(0, top_start) + sql.substr(pos);

    // Remove trailing whitespace and semicolons
    size_t end = result.size();
    while (end > 0 && (result[end - 1] == ' ' || result[end - 1] == '\t' ||
                       result[end - 1] == '\n' || result[end - 1] == '\r' ||
                       result[end - 1] == ';')) {
        end--;
    }
    result = result.substr(0, end);

    // Only add LIMIT if not already present
    std::string upper_result = result;
    std::transform(upper_result.begin(), upper_result.end(), upper_result.begin(), ::toupper);
    if (upper_result.find("LIMIT") == std::string::npos) {
        result += " LIMIT " + limit_num;
    }

    return result;
}

} // namespace clickhouse_odbc
