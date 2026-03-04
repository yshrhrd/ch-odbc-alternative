// Phase 11 Tests: Connection attribute completion, SQLGetInfo enhancement, driver quality improvement
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
#undef SQLGetDiagRec
#endif

#pragma warning(disable: 4996)
#pragma warning(disable: 4995)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetInfo(SQLHDBC, SQLUSMALLINT, SQLPOINTER, SQLSMALLINT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// Helper: create env + dbc
static void CreateConnHandles(SQLHENV &env, SQLHDBC &dbc) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
}

static void FreeConnHandles(SQLHENV env, SQLHDBC dbc) {
    if (dbc) SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// Connection attributes test: Default values
// ============================================================================
TEST(Phase11_ConnAttrDefaults, MetadataId) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 99;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_METADATA_ID, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_FALSE, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrDefaults, TxnIsolation) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_TXN_ISOLATION, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_TXN_READ_COMMITTED, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrDefaults, PacketSize) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 99;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_PACKET_SIZE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(0, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrDefaults, AsyncEnable) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLULEN val = 99;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_ASYNC_ENABLE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)SQL_ASYNC_ENABLE_OFF, (__int64)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrDefaults, QuietMode) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLHWND val = (SQLHWND)0x1234;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_QUIET_MODE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue(val == nullptr);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrDefaults, OdbcCursors) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLULEN val = 99;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_ODBC_CURSORS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((__int64)SQL_CUR_USE_DRIVER, (__int64)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrDefaults, TranslateOption) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 99;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_TRANSLATE_OPTION, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(0, (int)val);
    FreeConnHandles(env, dbc);
}

// ============================================================================
// Connection attributes test: Set + Get roundtrip
// ============================================================================
TEST(Phase11_ConnAttrRoundtrip, MetadataId) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLSetConnectAttr(dbc, SQL_ATTR_METADATA_ID, (SQLPOINTER)(uintptr_t)SQL_TRUE, 0);
    SQLUINTEGER val = 0;
    SQLGetConnectAttr(dbc, SQL_ATTR_METADATA_ID, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_TRUE, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrRoundtrip, TxnIsolation) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLSetConnectAttr(dbc, SQL_ATTR_TXN_ISOLATION, (SQLPOINTER)(uintptr_t)SQL_TXN_SERIALIZABLE, 0);
    SQLUINTEGER val = 0;
    SQLGetConnectAttr(dbc, SQL_ATTR_TXN_ISOLATION, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_TXN_SERIALIZABLE, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrRoundtrip, PacketSize) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLSetConnectAttr(dbc, SQL_ATTR_PACKET_SIZE, (SQLPOINTER)(uintptr_t)32768, 0);
    SQLUINTEGER val = 0;
    SQLGetConnectAttr(dbc, SQL_ATTR_PACKET_SIZE, &val, sizeof(val), nullptr);
    AssertEqual(32768, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrRoundtrip, AsyncEnable) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLSetConnectAttr(dbc, SQL_ATTR_ASYNC_ENABLE, (SQLPOINTER)(uintptr_t)SQL_ASYNC_ENABLE_ON, 0);
    SQLULEN val = 0;
    SQLGetConnectAttr(dbc, SQL_ATTR_ASYNC_ENABLE, &val, sizeof(val), nullptr);
    AssertEqual((__int64)SQL_ASYNC_ENABLE_ON, (__int64)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrRoundtrip, QuietMode) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    HWND fakeWnd = (HWND)0xABCD;
    SQLSetConnectAttr(dbc, SQL_ATTR_QUIET_MODE, (SQLPOINTER)fakeWnd, 0);
    SQLHWND val = nullptr;
    SQLGetConnectAttr(dbc, SQL_ATTR_QUIET_MODE, &val, sizeof(val), nullptr);
    AssertTrue(val == fakeWnd);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrRoundtrip, OdbcCursors) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLSetConnectAttr(dbc, SQL_ATTR_ODBC_CURSORS, (SQLPOINTER)(uintptr_t)SQL_CUR_USE_ODBC, 0);
    SQLULEN val = 0;
    SQLGetConnectAttr(dbc, SQL_ATTR_ODBC_CURSORS, &val, sizeof(val), nullptr);
    AssertEqual((__int64)SQL_CUR_USE_ODBC, (__int64)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrRoundtrip, TranslateOption) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLSetConnectAttr(dbc, SQL_ATTR_TRANSLATE_OPTION, (SQLPOINTER)(uintptr_t)42, 0);
    SQLUINTEGER val = 0;
    SQLGetConnectAttr(dbc, SQL_ATTR_TRANSLATE_OPTION, &val, sizeof(val), nullptr);
    AssertEqual(42, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrRoundtrip, TranslateLib) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    const char *lib = "mylib.dll";
    SQLSetConnectAttr(dbc, SQL_ATTR_TRANSLATE_LIB, (SQLPOINTER)lib, SQL_NTS);
    char buf[64] = {};
    SQLGetConnectAttr(dbc, SQL_ATTR_TRANSLATE_LIB, buf, sizeof(buf), nullptr);
    AssertEqual(std::string("mylib.dll"), std::string(buf));
    FreeConnHandles(env, dbc);
}

// ============================================================================
// Connection attributes test: Default handling for unknown attributes
// ============================================================================
TEST(Phase11_ConnAttrUnknown, GetReturnsSuccess) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 99;
    SQLRETURN ret = SQLGetConnectAttr(dbc, 99999, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(0, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_ConnAttrUnknown, SetReturnsSuccess) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLRETURN ret = SQLSetConnectAttr(dbc, 99999, (SQLPOINTER)(uintptr_t)123, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    FreeConnHandles(env, dbc);
}

// ============================================================================
// SQLGetInfo test: New information types
// ============================================================================
TEST(Phase11_GetInfo, DriverVer) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    char buf[64] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_DRIVER_VER, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("01.00.0000"), std::string(buf));
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, FetchDirection) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_FETCH_DIRECTION, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_FD_FETCH_NEXT) != 0);
    AssertTrue((val & SQL_FD_FETCH_FIRST) != 0);
    AssertTrue((val & SQL_FD_FETCH_LAST) != 0);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, FileUsage) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUSMALLINT val = 99;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_FILE_USAGE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_FILE_NOT_SUPPORTED, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, PosOperations) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_POS_OPERATIONS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_POS_POSITION) != 0);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, LockTypes) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_LOCK_TYPES, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_LCK_NO_CHANGE) != 0);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, XopenCliYear) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    char buf[32] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_XOPEN_CLI_YEAR, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("1995"), std::string(buf));
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, CollationSeq) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    char buf[32] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_COLLATION_SEQ, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string("UTF-8"), std::string(buf));
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, DatetimeLiterals) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_DATETIME_LITERALS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_DL_SQL92_DATE) != 0);
    AssertTrue((val & SQL_DL_SQL92_TIMESTAMP) != 0);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, MaxDriverConnections) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUSMALLINT val = 99;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_MAX_DRIVER_CONNECTIONS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(0, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, ActiveEnvironments) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUSMALLINT val = 99;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_ACTIVE_ENVIRONMENTS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(0, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, SchemaTerm) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    char buf[32] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_SCHEMA_TERM, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(std::string(""), std::string(buf));
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, CatalogTerm) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    char buf[32] = {};
    SQLSMALLINT len = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_CATALOG_TERM, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    // Catalog disabled: return empty string so Access does not generate catalog-qualified queries
    AssertEqual(std::string(""), std::string(buf));
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, ConvertGuid) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_CONVERT_GUID, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_CVT_CHAR) != 0);
    AssertTrue((val & SQL_CVT_VARCHAR) != 0);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, StaticSensitivity) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 99;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_STATIC_SENSITIVITY, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual(0, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, AsyncDbcFunctions) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 99;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_ASYNC_DBC_FUNCTIONS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertEqual((int)SQL_ASYNC_DBC_NOT_CAPABLE, (int)val);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, UnknownInfoType) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 99;
    SQLSMALLINT len = -1;
    SQLRETURN ret = SQLGetInfo(dbc, 65000, &val, sizeof(val), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    FreeConnHandles(env, dbc);
}

TEST(Phase11_GetInfo, TimedateAddIntervals) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);
    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_TIMEDATE_ADD_INTERVALS, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret);
    AssertTrue((val & SQL_FN_TSI_YEAR) != 0);
    AssertTrue((val & SQL_FN_TSI_MONTH) != 0);
    AssertTrue((val & SQL_FN_TSI_DAY) != 0);
    FreeConnHandles(env, dbc);
}
