#include "include/handle.h"
#include "include/util.h"
#include "include/clickhouse_client.h"

#include <sql.h>
#include <sqlext.h>
#include <cstring>

#ifdef UNICODE
#undef SQLGetInfo
#undef SQLGetDiagField
#endif

namespace clickhouse_odbc {
extern ClickHouseClient *GetClient(OdbcConnection *conn);
}

using namespace clickhouse_odbc;

// Helper: copy SQLUSMALLINT to output
static SQLRETURN ReturnUSmallInt(SQLUSMALLINT value, SQLPOINTER InfoValue,
                                  SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr) {
    if (InfoValue) *(SQLUSMALLINT *)InfoValue = value;
    if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUSMALLINT);
    return SQL_SUCCESS;
}

static SQLRETURN ReturnUInteger(SQLUINTEGER value, SQLPOINTER InfoValue,
                                 SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr) {
    if (InfoValue) *(SQLUINTEGER *)InfoValue = value;
    if (StringLengthPtr) *StringLengthPtr = sizeof(SQLUINTEGER);
    return SQL_SUCCESS;
}

static SQLRETURN ReturnString(const std::string &value, SQLPOINTER InfoValue,
                               SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr) {
    if (StringLengthPtr) *StringLengthPtr = (SQLSMALLINT)value.size();
    if (InfoValue && BufferLength > 0) {
        CopyStringToBuffer(value, (SQLCHAR *)InfoValue, BufferLength, StringLengthPtr);
    }
    return SQL_SUCCESS;
}

static SQLRETURN ReturnStringW(const std::string &value, SQLPOINTER InfoValue,
                                SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr) {
    std::wstring wide = Utf8ToWide(value);
    SQLSMALLINT char_count = (SQLSMALLINT)wide.size();
    if (StringLengthPtr) *StringLengthPtr = char_count * sizeof(SQLWCHAR);
    if (InfoValue && BufferLength > 0) {
        SQLSMALLINT max_chars = (BufferLength / sizeof(SQLWCHAR)) - 1;
        SQLSMALLINT copy_chars = (char_count < max_chars) ? char_count : max_chars;
        memcpy(InfoValue, wide.c_str(), copy_chars * sizeof(SQLWCHAR));
        ((SQLWCHAR *)InfoValue)[copy_chars] = L'\0';
    }
    return SQL_SUCCESS;
}

// ============================================================================
// Core SQLGetInfo implementation
// ============================================================================
static SQLRETURN GetInfoImpl(OdbcConnection *conn, SQLUSMALLINT InfoType,
                             SQLPOINTER InfoValue, SQLSMALLINT BufferLength,
                             SQLSMALLINT *StringLengthPtr, bool is_wide) {

    auto returnStr = [&](const std::string &val) -> SQLRETURN {
        return is_wide ? ReturnStringW(val, InfoValue, BufferLength, StringLengthPtr)
                       : ReturnString(val, InfoValue, BufferLength, StringLengthPtr);
    };

    switch (InfoType) {
    // === Driver information ===
    case SQL_DRIVER_NAME:
        return returnStr("ch-odbc-alternative.dll");
    case SQL_DRIVER_VER:
        return returnStr("01.01.0000");
    case SQL_DRIVER_ODBC_VER:
        return returnStr("03.80");
    case SQL_ODBC_VER:
        return returnStr("03.80");

    // === DBMS information ===
    case SQL_DBMS_NAME:
        return returnStr("ClickHouse");
    case SQL_DBMS_VER: {
        auto *client = GetClient(conn);
        std::string ver = client ? client->GetServerVersion() : "unknown";
        return returnStr(ver);
    }
    case SQL_SERVER_NAME:
        return returnStr(conn->host);
    case SQL_DATABASE_NAME:
        return returnStr(conn->database);
    case SQL_USER_NAME:
        return returnStr(conn->user);

    // === Data source information ===
    case SQL_DATA_SOURCE_NAME:
        return returnStr("ClickHouse");
    case SQL_DATA_SOURCE_READ_ONLY:
        return returnStr("Y");
    case SQL_ACCESSIBLE_TABLES:
        return returnStr("Y");
    case SQL_ACCESSIBLE_PROCEDURES:
        return returnStr("N");

    // === SQL conformance (critical for MS Access) ===
    case SQL_ODBC_SQL_CONFORMANCE:
        return ReturnUSmallInt(SQL_OSC_CORE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_SQL_CONFORMANCE:
        return ReturnUInteger(SQL_SC_SQL92_ENTRY, InfoValue, BufferLength, StringLengthPtr);
    case SQL_ODBC_API_CONFORMANCE:
        return ReturnUSmallInt(SQL_OAC_LEVEL1, InfoValue, BufferLength, StringLengthPtr);
    case SQL_ODBC_INTERFACE_CONFORMANCE:
        return ReturnUInteger(SQL_OIC_CORE, InfoValue, BufferLength, StringLengthPtr);

    // === Catalog support ===
    // ClickHouse's HTTP interface specifies the database via URL parameters.
    // Setting SQL_CATALOG_USAGE=0 prevents Access from generating catalog-qualified
    // queries (e.g., `default`.`M_ITEM`).
    case SQL_CATALOG_NAME:
        return returnStr("N");
    case SQL_CATALOG_NAME_SEPARATOR:
        return returnStr("");
    case SQL_CATALOG_TERM:
        return returnStr("");
    case SQL_CATALOG_USAGE:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CATALOG_LOCATION:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);

    case SQL_SCHEMA_TERM:
        return returnStr("");
    case SQL_SCHEMA_USAGE:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);

    case SQL_TABLE_TERM:
        return returnStr("table");
    case SQL_PROCEDURE_TERM:
        return returnStr("procedure");

    // === Identifier quoting ===
    case SQL_IDENTIFIER_QUOTE_CHAR:
        return returnStr("`");
    case SQL_IDENTIFIER_CASE:
        return ReturnUSmallInt(SQL_IC_SENSITIVE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_QUOTED_IDENTIFIER_CASE:
        return ReturnUSmallInt(SQL_IC_SENSITIVE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_SPECIAL_CHARACTERS:
        return returnStr("_");

    // === Search patterns ===
    case SQL_SEARCH_PATTERN_ESCAPE:
        return returnStr("\\");
    case SQL_LIKE_ESCAPE_CLAUSE:
        return returnStr("Y");

    // === Max lengths (MS Access queries these) ===
    case SQL_MAX_CATALOG_NAME_LEN:
        return ReturnUSmallInt(128, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_SCHEMA_NAME_LEN:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_TABLE_NAME_LEN:
        return ReturnUSmallInt(128, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_COLUMN_NAME_LEN:
        return ReturnUSmallInt(128, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_IDENTIFIER_LEN:
        return ReturnUSmallInt(128, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_COLUMNS_IN_SELECT:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr); // no limit
    case SQL_MAX_COLUMNS_IN_TABLE:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_COLUMNS_IN_GROUP_BY:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_COLUMNS_IN_ORDER_BY:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_COLUMNS_IN_INDEX:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_TABLES_IN_SELECT:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_CHAR_LITERAL_LEN:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_STATEMENT_LEN:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_ROW_SIZE:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
        return returnStr("Y");
    case SQL_MAX_CURSOR_NAME_LEN:
        return ReturnUSmallInt(128, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_INDEX_SIZE:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);

    // === Transaction support ===
    case SQL_TXN_CAPABLE:
        return ReturnUSmallInt(SQL_TC_NONE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_TXN_ISOLATION_OPTION:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DEFAULT_TXN_ISOLATION:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MULTIPLE_ACTIVE_TXN:
        return returnStr("N");

    // === Cursor and scrolling ===
    case SQL_SCROLL_OPTIONS:
        return ReturnUInteger(SQL_SO_FORWARD_ONLY | SQL_SO_STATIC, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CURSOR_COMMIT_BEHAVIOR:
        return ReturnUSmallInt(SQL_CB_PRESERVE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CURSOR_ROLLBACK_BEHAVIOR:
        return ReturnUSmallInt(SQL_CB_PRESERVE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CURSOR_SENSITIVITY:
        return ReturnUInteger(SQL_INSENSITIVE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
        return ReturnUInteger(SQL_CA1_NEXT | SQL_CA1_POSITIONED_UPDATE | SQL_CA1_POSITIONED_DELETE,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
        return ReturnUInteger(SQL_CA2_READ_ONLY_CONCURRENCY | SQL_CA2_CRC_EXACT,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_STATIC_CURSOR_ATTRIBUTES1:
        return ReturnUInteger(SQL_CA1_NEXT | SQL_CA1_ABSOLUTE | SQL_CA1_RELATIVE |
                              SQL_CA1_POSITIONED_UPDATE | SQL_CA1_POSITIONED_DELETE,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_STATIC_CURSOR_ATTRIBUTES2:
        return ReturnUInteger(SQL_CA2_READ_ONLY_CONCURRENCY | SQL_CA2_CRC_EXACT,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_KEYSET_CURSOR_ATTRIBUTES1:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_KEYSET_CURSOR_ATTRIBUTES2:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);

    // === SQL functionality ===
    case SQL_EXPRESSIONS_IN_ORDERBY:
        return returnStr("Y");
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:
        return returnStr("N");
    case SQL_GROUP_BY:
        return ReturnUSmallInt(SQL_GB_GROUP_BY_EQUALS_SELECT, InfoValue, BufferLength, StringLengthPtr);
    case SQL_COLUMN_ALIAS:
        return returnStr("Y");
    case SQL_NULL_COLLATION:
        return ReturnUSmallInt(SQL_NC_LOW, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CONCAT_NULL_BEHAVIOR:
        return ReturnUSmallInt(SQL_CB_NULL, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CORRELATION_NAME:
        return ReturnUSmallInt(SQL_CN_ANY, InfoValue, BufferLength, StringLengthPtr);

    // === Supported SQL (MS Access critical) ===
    case SQL_SQL92_STRING_FUNCTIONS:
        return ReturnUInteger(SQL_SSF_LOWER | SQL_SSF_UPPER | SQL_SSF_SUBSTRING | SQL_SSF_TRIM_BOTH |
                              SQL_SSF_TRIM_LEADING | SQL_SSF_TRIM_TRAILING,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
        return ReturnUInteger(SQL_SNVF_CHAR_LENGTH | SQL_SNVF_CHARACTER_LENGTH | SQL_SNVF_EXTRACT |
                              SQL_SNVF_OCTET_LENGTH | SQL_SNVF_POSITION,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_SQL92_DATETIME_FUNCTIONS:
        return ReturnUInteger(SQL_SDF_CURRENT_DATE | SQL_SDF_CURRENT_TIMESTAMP,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_SQL92_PREDICATES:
        return ReturnUInteger(SQL_SP_BETWEEN | SQL_SP_COMPARISON | SQL_SP_EXISTS | SQL_SP_IN |
                              SQL_SP_ISNOTNULL | SQL_SP_ISNULL | SQL_SP_LIKE,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
        return ReturnUInteger(SQL_SRJO_CROSS_JOIN | SQL_SRJO_INNER_JOIN |
                              SQL_SRJO_LEFT_OUTER_JOIN | SQL_SRJO_RIGHT_OUTER_JOIN | SQL_SRJO_FULL_OUTER_JOIN,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_SQL92_VALUE_EXPRESSIONS:
        return ReturnUInteger(SQL_SVE_CASE | SQL_SVE_CAST | SQL_SVE_COALESCE | SQL_SVE_NULLIF,
                              InfoValue, BufferLength, StringLengthPtr);

    case SQL_NUMERIC_FUNCTIONS:
        return ReturnUInteger(SQL_FN_NUM_ABS | SQL_FN_NUM_CEILING | SQL_FN_NUM_FLOOR |
                              SQL_FN_NUM_LOG | SQL_FN_NUM_MOD | SQL_FN_NUM_POWER |
                              SQL_FN_NUM_ROUND | SQL_FN_NUM_SQRT | SQL_FN_NUM_SIGN |
                              SQL_FN_NUM_EXP | SQL_FN_NUM_LOG10 | SQL_FN_NUM_PI |
                              SQL_FN_NUM_RAND | SQL_FN_NUM_DEGREES | SQL_FN_NUM_RADIANS |
                              SQL_FN_NUM_SIN | SQL_FN_NUM_COS | SQL_FN_NUM_TAN |
                              SQL_FN_NUM_ASIN | SQL_FN_NUM_ACOS | SQL_FN_NUM_ATAN |
                              SQL_FN_NUM_ATAN2 | SQL_FN_NUM_TRUNCATE,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_STRING_FUNCTIONS:
        return ReturnUInteger(SQL_FN_STR_CONCAT | SQL_FN_STR_LENGTH | SQL_FN_STR_LCASE |
                              SQL_FN_STR_UCASE | SQL_FN_STR_SUBSTRING | SQL_FN_STR_LTRIM |
                              SQL_FN_STR_RTRIM | SQL_FN_STR_REPLACE | SQL_FN_STR_LEFT |
                              SQL_FN_STR_RIGHT | SQL_FN_STR_REPEAT | SQL_FN_STR_SPACE |
                              SQL_FN_STR_ASCII | SQL_FN_STR_CHAR | SQL_FN_STR_LOCATE |
                              SQL_FN_STR_BIT_LENGTH | SQL_FN_STR_CHAR_LENGTH | SQL_FN_STR_OCTET_LENGTH,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_TIMEDATE_FUNCTIONS:
        return ReturnUInteger(SQL_FN_TD_NOW | SQL_FN_TD_CURDATE | SQL_FN_TD_YEAR |
                              SQL_FN_TD_MONTH | SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_HOUR |
                              SQL_FN_TD_MINUTE | SQL_FN_TD_SECOND | SQL_FN_TD_DAYOFWEEK |
                              SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_WEEK | SQL_FN_TD_QUARTER |
                              SQL_FN_TD_CURTIME | SQL_FN_TD_DAYNAME | SQL_FN_TD_MONTHNAME |
                              SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME |
                              SQL_FN_TD_CURRENT_TIMESTAMP | SQL_FN_TD_EXTRACT |
                              SQL_FN_TD_TIMESTAMPADD | SQL_FN_TD_TIMESTAMPDIFF,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_SYSTEM_FUNCTIONS:
        return ReturnUInteger(SQL_FN_SYS_IFNULL | SQL_FN_SYS_DBNAME | SQL_FN_SYS_USERNAME, InfoValue, BufferLength, StringLengthPtr);

    case SQL_AGGREGATE_FUNCTIONS:
        return ReturnUInteger(SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM |
                              SQL_AF_DISTINCT | SQL_AF_ALL,
                              InfoValue, BufferLength, StringLengthPtr);

    // === Conversion support ===
    case SQL_CONVERT_FUNCTIONS:
        return ReturnUInteger(SQL_FN_CVT_CAST | SQL_FN_CVT_CONVERT, InfoValue, BufferLength, StringLengthPtr);

    case SQL_CONVERT_BIGINT:
    case SQL_CONVERT_INTEGER:
    case SQL_CONVERT_SMALLINT:
    case SQL_CONVERT_TINYINT:
    case SQL_CONVERT_FLOAT:
    case SQL_CONVERT_DOUBLE:
    case SQL_CONVERT_REAL:
    case SQL_CONVERT_DECIMAL:
    case SQL_CONVERT_NUMERIC:
    case SQL_CONVERT_CHAR:
    case SQL_CONVERT_VARCHAR:
    case SQL_CONVERT_LONGVARCHAR:
    case SQL_CONVERT_DATE:
    case SQL_CONVERT_TIMESTAMP:
        return ReturnUInteger(SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR |
                              SQL_CVT_INTEGER | SQL_CVT_BIGINT | SQL_CVT_FLOAT | SQL_CVT_DOUBLE,
                              InfoValue, BufferLength, StringLengthPtr);

    // === Subqueries ===
    case SQL_SUBQUERIES:
        return ReturnUInteger(SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON | SQL_SQ_EXISTS | SQL_SQ_IN,
                              InfoValue, BufferLength, StringLengthPtr);

    // === Union ===
    case SQL_UNION:
        return ReturnUInteger(SQL_U_UNION | SQL_U_UNION_ALL, InfoValue, BufferLength, StringLengthPtr);

    // === ALTER TABLE ===
    case SQL_ALTER_TABLE:
        return ReturnUInteger(SQL_AT_ADD_COLUMN | SQL_AT_DROP_COLUMN, InfoValue, BufferLength, StringLengthPtr);

    // === Positioned operations ===
    case SQL_POSITIONED_STATEMENTS:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);

    // === Bookmark ===
    case SQL_BOOKMARK_PERSISTENCE:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);

    // === Batch support ===
    case SQL_BATCH_SUPPORT:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_BATCH_ROW_COUNT:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);

    // === Misc ===
    case SQL_NEED_LONG_DATA_LEN:
        return returnStr("N");
    case SQL_NON_NULLABLE_COLUMNS:
        return ReturnUSmallInt(SQL_NNC_NON_NULL, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MULT_RESULT_SETS:
        return returnStr("N");
    case SQL_PROCEDURES:
        return returnStr("N");
    case SQL_DESCRIBE_PARAMETER:
        return returnStr("Y");
    case SQL_INTEGRITY:
        return returnStr("N");
    case SQL_ROW_UPDATES:
        return returnStr("N");
    case SQL_MAX_CONCURRENT_ACTIVITIES:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_ASYNC_MODE:
        return ReturnUInteger(SQL_AM_NONE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_GETDATA_EXTENSIONS:
        return ReturnUInteger(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER | SQL_GD_BLOCK | SQL_GD_BOUND, InfoValue, BufferLength, StringLengthPtr);
    case SQL_OJ_CAPABILITIES:
        return ReturnUInteger(SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL | SQL_OJ_NESTED |
                              SQL_OJ_NOT_ORDERED | SQL_OJ_INNER | SQL_OJ_ALL_COMPARISON_OPS,
                              InfoValue, BufferLength, StringLengthPtr);

    // === Keywords (MS Access uses to determine reserved words) ===
    case SQL_KEYWORDS:
        return returnStr("ARRAY,GLOBAL,LOCAL,MATERIALIZED,SAMPLE,SETTINGS,TOTALS,FINAL,PREWHERE");

    // === DDL support (MS Access needs these) ===
    case SQL_DDL_INDEX:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CREATE_ASSERTION:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CREATE_CHARACTER_SET:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CREATE_COLLATION:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CREATE_DOMAIN:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CREATE_SCHEMA:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CREATE_TRANSLATION:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DROP_ASSERTION:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DROP_CHARACTER_SET:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DROP_COLLATION:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DROP_DOMAIN:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DROP_SCHEMA:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DROP_TRANSLATION:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);

    // === GRANT/REVOKE (Access checks these) ===
    case SQL_SQL92_GRANT:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_SQL92_REVOKE:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);

    // === INSERT statement support ===
    case SQL_INSERT_STATEMENT:
        return ReturnUInteger(SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED | SQL_IS_SELECT_INTO,
                              InfoValue, BufferLength, StringLengthPtr);

    // === Info type bitmask ===
    case SQL_INFO_SCHEMA_VIEWS:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_PARAM_ARRAY_ROW_COUNTS:
        return ReturnUInteger(SQL_PARC_NO_BATCH, InfoValue, BufferLength, StringLengthPtr);
    case SQL_PARAM_ARRAY_SELECTS:
        return ReturnUInteger(SQL_PAS_NO_SELECT, InfoValue, BufferLength, StringLengthPtr);

    // === SQL_OWNER_TERM (deprecated alias for SQL_SCHEMA_TERM, Access still uses it) ===
    // SQL_OWNER_TERM == SQL_SCHEMA_TERM (handled above at case SQL_SCHEMA_TERM)

    // === Additional conversions Access may check ===
    case SQL_CONVERT_BIT:
    case SQL_CONVERT_WCHAR:
    case SQL_CONVERT_WVARCHAR:
    case SQL_CONVERT_WLONGVARCHAR:
    case SQL_CONVERT_BINARY:
    case SQL_CONVERT_VARBINARY:
    case SQL_CONVERT_LONGVARBINARY:
    case SQL_CONVERT_TIME:
        return ReturnUInteger(SQL_CVT_CHAR | SQL_CVT_VARCHAR | SQL_CVT_LONGVARCHAR,
                              InfoValue, BufferLength, StringLengthPtr);

    // === CREATE TABLE support ===
    case SQL_CREATE_TABLE:
        return ReturnUInteger(SQL_CT_CREATE_TABLE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DROP_TABLE:
        return ReturnUInteger(SQL_DT_DROP_TABLE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CREATE_VIEW:
        return ReturnUInteger(SQL_CV_CREATE_VIEW, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DROP_VIEW:
        return ReturnUInteger(SQL_DV_DROP_VIEW, InfoValue, BufferLength, StringLengthPtr);

    // === Additional info types requested by ODBC apps ===
    case SQL_ACTIVE_ENVIRONMENTS:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr); // no limit
    case SQL_MAX_DRIVER_CONNECTIONS:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr); // no limit
    case SQL_FETCH_DIRECTION:
        return ReturnUInteger(SQL_FD_FETCH_NEXT | SQL_FD_FETCH_FIRST | SQL_FD_FETCH_LAST |
                              SQL_FD_FETCH_ABSOLUTE | SQL_FD_FETCH_RELATIVE,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_FILE_USAGE:
        return ReturnUSmallInt(SQL_FILE_NOT_SUPPORTED, InfoValue, BufferLength, StringLengthPtr);
    case SQL_POS_OPERATIONS:
        return ReturnUInteger(SQL_POS_POSITION, InfoValue, BufferLength, StringLengthPtr);
    case SQL_LOCK_TYPES:
        return ReturnUInteger(SQL_LCK_NO_CHANGE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_SCROLL_CONCURRENCY:
        return ReturnUInteger(SQL_SCCO_READ_ONLY, InfoValue, BufferLength, StringLengthPtr);
    case SQL_STATIC_SENSITIVITY:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr); // no sensitivity
    case SQL_DATETIME_LITERALS:
        return ReturnUInteger(SQL_DL_SQL92_DATE | SQL_DL_SQL92_TIMESTAMP, InfoValue, BufferLength, StringLengthPtr);
    case SQL_XOPEN_CLI_YEAR:
        return returnStr("1995");
    case SQL_TIMEDATE_ADD_INTERVALS:
    case SQL_TIMEDATE_DIFF_INTERVALS:
        return ReturnUInteger(SQL_FN_TSI_YEAR | SQL_FN_TSI_MONTH | SQL_FN_TSI_DAY |
                              SQL_FN_TSI_HOUR | SQL_FN_TSI_MINUTE | SQL_FN_TSI_SECOND,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_SQL92_ROW_VALUE_CONSTRUCTOR:
        return ReturnUInteger(SQL_SRVC_VALUE_EXPRESSION | SQL_SRVC_NULL | SQL_SRVC_DEFAULT | SQL_SRVC_ROW_SUBQUERY,
                              InfoValue, BufferLength, StringLengthPtr);
    case SQL_STANDARD_CLI_CONFORMANCE:
        return ReturnUInteger(SQL_SCC_XOPEN_CLI_VERSION1, InfoValue, BufferLength, StringLengthPtr);
    // Note: SQL_OWNER_TERM == SQL_SCHEMA_TERM, SQL_OWNER_USAGE == SQL_SCHEMA_USAGE (handled above)
    // Note: SQL_QUALIFIER_TERM == SQL_CATALOG_TERM, SQL_QUALIFIER_NAME_SEPARATOR == SQL_CATALOG_NAME_SEPARATOR (handled above)
    // Note: SQL_QUALIFIER_USAGE == SQL_CATALOG_USAGE, SQL_QUALIFIER_LOCATION == SQL_CATALOG_LOCATION (handled above)
    case SQL_CONVERT_INTERVAL_YEAR_MONTH:
    case SQL_CONVERT_INTERVAL_DAY_TIME:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    case SQL_CONVERT_GUID:
        return ReturnUInteger(SQL_CVT_CHAR | SQL_CVT_VARCHAR, InfoValue, BufferLength, StringLengthPtr);
    case SQL_DM_VER:
        return returnStr("03.80.0000");
    case SQL_COLLATION_SEQ:
        return returnStr("UTF-8");
    case SQL_ASYNC_DBC_FUNCTIONS:
        return ReturnUInteger(SQL_ASYNC_DBC_NOT_CAPABLE, InfoValue, BufferLength, StringLengthPtr);
    case SQL_MAX_BINARY_LITERAL_LEN:
        return ReturnUInteger(0, InfoValue, BufferLength, StringLengthPtr);
    // Note: SQL_MAX_COLUMNS_IN_GROUP_BY already handled above
    case SQL_MAX_PROCEDURE_NAME_LEN:
        return ReturnUSmallInt(0, InfoValue, BufferLength, StringLengthPtr);

    default:
        // Return 0/empty for unknown info types (compatibility)
        if (InfoValue) {
            memset(InfoValue, 0, BufferLength > 0 ? BufferLength : 0);
        }
        if (StringLengthPtr) *StringLengthPtr = 0;
        return SQL_SUCCESS;
    }
}

// ============================================================================
// SQLGetInfo / SQLGetInfoW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType,
                                         SQLPOINTER InfoValue, SQLSMALLINT BufferLength,
                                         SQLSMALLINT *StringLengthPtr) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    return GetInfoImpl(conn, InfoType, InfoValue, BufferLength, StringLengthPtr, false);
}

extern "C" SQLRETURN SQL_API SQLGetInfoW(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType,
                                           SQLPOINTER InfoValue, SQLSMALLINT BufferLength,
                                           SQLSMALLINT *StringLengthPtr) {
    if (!IsValidDbcHandle(ConnectionHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *conn = static_cast<OdbcConnection *>(ConnectionHandle);
    HandleLock lock(conn);
    return GetInfoImpl(conn, InfoType, InfoValue, BufferLength, StringLengthPtr, true);
}
