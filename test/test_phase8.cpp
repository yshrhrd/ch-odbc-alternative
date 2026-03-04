// Phase 8 Tests: Descriptor handles & SQLBulkOperations
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
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLGetDescField
#undef SQLSetDescField
#undef SQLGetDescRec
#undef SQLSetDescRec
#undef SQLGetDiagRec
#undef SQLDescribeCol
#undef SQLColAttribute
#undef SQLExecDirect
#endif

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetDescField(SQLHDESC, SQLSMALLINT, SQLSMALLINT, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLGetDescFieldW(SQLHDESC, SQLSMALLINT, SQLSMALLINT, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetDescField(SQLHDESC, SQLSMALLINT, SQLSMALLINT, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLSetDescFieldW(SQLHDESC, SQLSMALLINT, SQLSMALLINT, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetDescRec(SQLHDESC, SQLSMALLINT, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *,
                                            SQLSMALLINT *, SQLSMALLINT *, SQLLEN *, SQLSMALLINT *,
                                            SQLSMALLINT *, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetDescRecW(SQLHDESC, SQLSMALLINT, SQLWCHAR *, SQLSMALLINT, SQLSMALLINT *,
                                             SQLSMALLINT *, SQLSMALLINT *, SQLLEN *, SQLSMALLINT *,
                                             SQLSMALLINT *, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLSetDescRec(SQLHDESC, SQLSMALLINT, SQLSMALLINT, SQLSMALLINT,
                                            SQLLEN, SQLSMALLINT, SQLSMALLINT, SQLPOINTER,
                                            SQLLEN *, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLCopyDesc(SQLHDESC, SQLHDESC);
extern "C" SQLRETURN SQL_API SQLBulkOperations(SQLHSTMT, SQLSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetFunctions(SQLHDBC, SQLUSMALLINT, SQLUSMALLINT *);
extern "C" SQLRETURN SQL_API SQLBindCol(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLExecDirect(SQLHSTMT, SQLCHAR *, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);

using namespace test_framework;
using namespace clickhouse_odbc;

// Helper: create env + dbc + stmt handles
static void CreateTestHandles(SQLHENV &env, SQLHDBC &dbc, SQLHSTMT &stmt) {
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
}

static void FreeTestHandles(SQLHENV env, SQLHDBC dbc, SQLHSTMT stmt) {
    if (stmt) SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    if (dbc) {
        auto *conn = static_cast<OdbcConnection *>(dbc);
        conn->connected = false;
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    }
    if (env) SQLFreeHandle(SQL_HANDLE_ENV, env);
}

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
// Descriptor: Auto-allocation test
// ============================================================================

TEST(Phase8_Descriptor, AutoDescriptorsCreatedWithStatement) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertNotNull(s->apd, "APD should not be null");
    AssertNotNull(s->ipd, "IPD should not be null");
    AssertNotNull(s->ard, "ARD should not be null");
    AssertNotNull(s->ird, "IRD should not be null");

    // Auto descriptors should be same as current
    AssertTrue(s->apd == s->auto_apd.get(), "APD should point to auto_apd");
    AssertTrue(s->ipd == s->auto_ipd.get(), "IPD should point to auto_ipd");
    AssertTrue(s->ard == s->auto_ard.get(), "ARD should point to auto_ard");
    AssertTrue(s->ird == s->auto_ird.get(), "IRD should point to auto_ird");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, DescriptorTypesCorrect) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertTrue(s->apd->desc_type == DescriptorType::APD, "APD type");
    AssertTrue(s->ipd->desc_type == DescriptorType::IPD, "IPD type");
    AssertTrue(s->ard->desc_type == DescriptorType::ARD, "ARD type");
    AssertTrue(s->ird->desc_type == DescriptorType::IRD, "IRD type");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, DescriptorsAreAutoAllocated) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    AssertTrue(s->apd->is_auto, "APD should be auto");
    AssertTrue(s->ipd->is_auto, "IPD should be auto");
    AssertTrue(s->ard->is_auto, "ARD should be auto");
    AssertTrue(s->ird->is_auto, "IRD should be auto");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetStmtAttr — Descriptor attributes
// ============================================================================

TEST(Phase8_Descriptor, GetStmtAttrReturnsDescriptors) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLPOINTER desc_ptr = nullptr;

    SQLRETURN ret = SQLGetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, &desc_ptr, SQL_IS_POINTER, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get ARD");
    AssertTrue(desc_ptr == (SQLPOINTER)s->ard, "ARD pointer match");

    ret = SQLGetStmtAttr(stmt, SQL_ATTR_APP_PARAM_DESC, &desc_ptr, SQL_IS_POINTER, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get APD");
    AssertTrue(desc_ptr == (SQLPOINTER)s->apd, "APD pointer match");

    ret = SQLGetStmtAttr(stmt, SQL_ATTR_IMP_ROW_DESC, &desc_ptr, SQL_IS_POINTER, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get IRD");
    AssertTrue(desc_ptr == (SQLPOINTER)s->ird, "IRD pointer match");

    ret = SQLGetStmtAttr(stmt, SQL_ATTR_IMP_PARAM_DESC, &desc_ptr, SQL_IS_POINTER, nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get IPD");
    AssertTrue(desc_ptr == (SQLPOINTER)s->ipd, "IPD pointer match");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetStmtAttrRejectsImpDescriptors) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    OdbcDescriptor dummy(DescriptorType::IRD);

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_IMP_ROW_DESC, (SQLPOINTER)&dummy, SQL_IS_POINTER);
    AssertEqual((int)SQL_ERROR, (int)ret, "Cannot set IRD");

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_IMP_PARAM_DESC, (SQLPOINTER)&dummy, SQL_IS_POINTER);
    AssertEqual((int)SQL_ERROR, (int)ret, "Cannot set IPD");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetStmtAttrResetsToAutoWithNull) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);

    // Set to a different descriptor
    OdbcDescriptor user_ard(DescriptorType::ARD);
    user_ard.is_auto = false;
    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, (SQLPOINTER)&user_ard, SQL_IS_POINTER);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set user ARD");
    AssertTrue(s->ard == &user_ard, "ARD should point to user descriptor");

    // Reset to auto by passing NULL
    ret = SQLSetStmtAttr(stmt, SQL_ATTR_APP_ROW_DESC, nullptr, SQL_IS_POINTER);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Reset ARD to auto");
    AssertTrue(s->ard == s->auto_ard.get(), "ARD should be auto again");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetDescField test
// ============================================================================

TEST(Phase8_Descriptor, GetDescFieldInvalidHandle) {
    SQLRETURN ret = SQLGetDescField(SQL_NULL_HANDLE, 1, SQL_DESC_TYPE, nullptr, 0, nullptr);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Invalid handle");
}

TEST(Phase8_Descriptor, GetDescFieldAllocType) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLSMALLINT alloc_type = 0;
    SQLRETURN ret = SQLGetDescField((SQLHDESC)s->ard, 0, SQL_DESC_ALLOC_TYPE, &alloc_type, sizeof(alloc_type), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get alloc type");
    AssertEqual((int)SQL_DESC_ALLOC_AUTO, (int)alloc_type, "Should be auto");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, GetDescFieldCount) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLSMALLINT count = -1;
    SQLRETURN ret = SQLGetDescField((SQLHDESC)s->ird, 0, SQL_DESC_COUNT, &count, sizeof(count), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get count");
    AssertEqual(0, (int)count, "IRD should start empty");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLSetDescField test
// ============================================================================

TEST(Phase8_Descriptor, SetDescFieldType) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLRETURN ret = SQLSetDescField((SQLHDESC)s->ard, 1, SQL_DESC_TYPE, (SQLPOINTER)(intptr_t)SQL_INTEGER, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set type");

    // Verify
    SQLSMALLINT type_val = 0;
    ret = SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_TYPE, &type_val, sizeof(type_val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get type");
    AssertEqual((int)SQL_INTEGER, (int)type_val, "Type should be SQL_INTEGER");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetDescFieldUpdatesCount) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Set record 3
    SQLSetDescField((SQLHDESC)s->ard, 3, SQL_DESC_TYPE, (SQLPOINTER)(intptr_t)SQL_VARCHAR, 0);

    SQLSMALLINT count = 0;
    SQLGetDescField((SQLHDESC)s->ard, 0, SQL_DESC_COUNT, &count, sizeof(count), nullptr);
    AssertEqual(3, (int)count, "Count should be 3 after setting record 3");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetDescFieldIRDReadOnly) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLRETURN ret = SQLSetDescField((SQLHDESC)s->ird, 1, SQL_DESC_TYPE, (SQLPOINTER)(intptr_t)SQL_INTEGER, 0);
    AssertEqual((int)SQL_ERROR, (int)ret, "IRD should be read-only");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetDescFieldInvalidIndex) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLRETURN ret = SQLSetDescField((SQLHDESC)s->ard, 0, SQL_DESC_TYPE, (SQLPOINTER)(intptr_t)SQL_INTEGER, 0);
    AssertEqual((int)SQL_ERROR, (int)ret, "RecNumber 0 is invalid for record fields");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetDescFieldConciseTypeAndLength) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLSetDescField((SQLHDESC)s->apd, 1, SQL_DESC_CONCISE_TYPE, (SQLPOINTER)(intptr_t)SQL_C_CHAR, 0);
    SQLSetDescField((SQLHDESC)s->apd, 1, SQL_DESC_LENGTH, (SQLPOINTER)(uintptr_t)256, 0);
    SQLSetDescField((SQLHDESC)s->apd, 1, SQL_DESC_PRECISION, (SQLPOINTER)(intptr_t)10, 0);
    SQLSetDescField((SQLHDESC)s->apd, 1, SQL_DESC_SCALE, (SQLPOINTER)(intptr_t)2, 0);

    SQLSMALLINT concise = 0;
    SQLULEN length = 0;
    SQLSMALLINT prec = 0;
    SQLSMALLINT scale = 0;
    SQLGetDescField((SQLHDESC)s->apd, 1, SQL_DESC_CONCISE_TYPE, &concise, sizeof(concise), nullptr);
    SQLGetDescField((SQLHDESC)s->apd, 1, SQL_DESC_LENGTH, &length, sizeof(length), nullptr);
    SQLGetDescField((SQLHDESC)s->apd, 1, SQL_DESC_PRECISION, &prec, sizeof(prec), nullptr);
    SQLGetDescField((SQLHDESC)s->apd, 1, SQL_DESC_SCALE, &scale, sizeof(scale), nullptr);

    AssertEqual((int)SQL_C_CHAR, (int)concise, "Concise type");
    AssertEqual((__int64)256, (__int64)length, "Length");
    AssertEqual(10, (int)prec, "Precision");
    AssertEqual(2, (int)scale, "Scale");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetDescRec test
// ============================================================================

TEST(Phase8_Descriptor, GetDescRecNoData) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLCHAR name[128];
    SQLSMALLINT nameLen;
    SQLSMALLINT type, subtype, prec, scale, nullable;
    SQLLEN length;

    SQLRETURN ret = SQLGetDescRec((SQLHDESC)s->ird, 1, name, sizeof(name), &nameLen,
                                  &type, &subtype, &length, &prec, &scale, &nullable);
    AssertEqual((int)SQL_NO_DATA, (int)ret, "No data for empty descriptor");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, GetDescRecAfterSetField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Set up a record in ARD
    auto &rec = s->ard->records[1];
    rec.name = "test_col";
    rec.type = SQL_INTEGER;
    rec.concise_type = SQL_INTEGER;
    rec.length = 4;
    rec.precision = 10;
    rec.scale = 0;
    rec.nullable = SQL_NO_NULLS;
    s->ard->count = 1;

    SQLCHAR name[128] = {};
    SQLSMALLINT nameLen = 0;
    SQLSMALLINT type = 0, subtype = 0, prec = 0, scale = 0, nullable = 0;
    SQLLEN length = 0;

    SQLRETURN ret = SQLGetDescRec((SQLHDESC)s->ard, 1, name, sizeof(name), &nameLen,
                                  &type, &subtype, &length, &prec, &scale, &nullable);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetDescRec success");
    AssertEqual(std::string("test_col"), std::string((char *)name), "Name");
    AssertEqual((int)SQL_INTEGER, (int)type, "Type");
    AssertEqual((__int64)4, (__int64)length, "Length");
    AssertEqual(10, (int)prec, "Precision");
    AssertEqual(0, (int)scale, "Scale");
    AssertEqual((int)SQL_NO_NULLS, (int)nullable, "Nullable");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLSetDescRec test
// ============================================================================

TEST(Phase8_Descriptor, SetDescRecSuccess) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    int data_buf = 42;
    SQLLEN indicator = sizeof(int);

    SQLRETURN ret = SQLSetDescRec((SQLHDESC)s->ard, 1, SQL_INTEGER, SQL_INTEGER,
                                  4, 10, 0, &data_buf, &indicator, &indicator);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetDescRec success");

    // Verify record was set
    AssertEqual(1, (int)s->ard->count, "Count should be 1");
    auto it = s->ard->records.find(1);
    AssertTrue(it != s->ard->records.end(), "Record 1 should exist");
    AssertEqual((int)SQL_INTEGER, (int)it->second.type, "Type");
    AssertTrue(it->second.data_ptr == &data_buf, "Data ptr");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetDescRecIRDReadOnly) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLRETURN ret = SQLSetDescRec((SQLHDESC)s->ird, 1, SQL_INTEGER, SQL_INTEGER,
                                  4, 10, 0, nullptr, nullptr, nullptr);
    AssertEqual((int)SQL_ERROR, (int)ret, "Cannot set IRD records");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLCopyDesc test
// ============================================================================

TEST(Phase8_Descriptor, CopyDescSuccess) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);

    // Set up source records
    s->apd->count = 2;
    s->apd->records[1].type = SQL_INTEGER;
    s->apd->records[1].length = 4;
    s->apd->records[2].type = SQL_VARCHAR;
    s->apd->records[2].length = 255;

    // Copy to ARD
    SQLRETURN ret = SQLCopyDesc((SQLHDESC)s->apd, (SQLHDESC)s->ard);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "CopyDesc success");
    AssertEqual(2, (int)s->ard->count, "Count copied");
    AssertEqual((int)SQL_INTEGER, (int)s->ard->records[1].type, "Record 1 type copied");
    AssertEqual((int)SQL_VARCHAR, (int)s->ard->records[2].type, "Record 2 type copied");
    AssertEqual((__int64)255, (__int64)s->ard->records[2].length, "Record 2 length copied");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, CopyDescToIRDFails) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->apd->count = 1;
    s->apd->records[1].type = SQL_INTEGER;

    SQLRETURN ret = SQLCopyDesc((SQLHDESC)s->apd, (SQLHDESC)s->ird);
    AssertEqual((int)SQL_ERROR, (int)ret, "Cannot copy to IRD");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, CopyDescInvalidHandle) {
    SQLRETURN ret = SQLCopyDesc(SQL_NULL_HANDLE, SQL_NULL_HANDLE);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Invalid handle");
}

// ============================================================================
// SQLGetDescField — String field test
// ============================================================================

TEST(Phase8_Descriptor, GetDescFieldStringFields) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Populate a record directly
    auto &rec = s->ard->records[1];
    rec.name = "column_A";
    rec.label = "Label_A";
    rec.type_name = "VARCHAR";
    rec.table_name = "my_table";
    rec.base_column_name = "col_a";
    rec.base_table_name = "base_tbl";
    s->ard->count = 1;

    SQLCHAR buf[128] = {};
    SQLINTEGER len = 0;

    SQLRETURN ret = SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_NAME, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get name");
    AssertEqual(std::string("column_A"), std::string((char *)buf), "Name value");

    ret = SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_LABEL, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get label");
    AssertEqual(std::string("Label_A"), std::string((char *)buf), "Label value");

    ret = SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_TYPE_NAME, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get type name");
    AssertEqual(std::string("VARCHAR"), std::string((char *)buf), "Type name value");

    ret = SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_TABLE_NAME, buf, sizeof(buf), &len);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Get table name");
    AssertEqual(std::string("my_table"), std::string((char *)buf), "Table name value");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetDescField — Pointer field test
// ============================================================================

TEST(Phase8_Descriptor, GetDescFieldPointerFields) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    int data_buf = 100;
    SQLLEN indicator = 4;
    auto &rec = s->ard->records[1];
    rec.data_ptr = &data_buf;
    rec.indicator_ptr = &indicator;
    rec.octet_length_ptr = &indicator;
    s->ard->count = 1;

    SQLPOINTER ptr = nullptr;
    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_DATA_PTR, &ptr, sizeof(ptr), nullptr);
    AssertTrue(ptr == &data_buf, "Data ptr match");

    SQLLEN *ind_ptr = nullptr;
    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_INDICATOR_PTR, &ind_ptr, sizeof(ind_ptr), nullptr);
    AssertTrue(ind_ptr == &indicator, "Indicator ptr match");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetDescField — Numeric attribute field test
// ============================================================================

TEST(Phase8_Descriptor, GetDescFieldNumericAttributes) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    auto &rec = s->ard->records[1];
    rec.nullable = SQL_NO_NULLS;
    rec.searchable = SQL_PRED_BASIC;
    rec.updatable = SQL_ATTR_READWRITE_UNKNOWN;
    rec.case_sensitive = SQL_FALSE;
    rec.fixed_prec_scale = SQL_TRUE;
    rec.unnamed = SQL_UNNAMED;
    s->ard->count = 1;

    SQLSMALLINT val = 0;

    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_NULLABLE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_NO_NULLS, (int)val, "Nullable");

    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_SEARCHABLE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_PRED_BASIC, (int)val, "Searchable");

    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_CASE_SENSITIVE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_FALSE, (int)val, "Case sensitive");

    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_FIXED_PREC_SCALE, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_TRUE, (int)val, "Fixed prec scale");

    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_UNNAMED, &val, sizeof(val), nullptr);
    AssertEqual((int)SQL_UNNAMED, (int)val, "Unnamed");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLSetDescField — String/pointer field test
// ============================================================================

TEST(Phase8_Descriptor, SetDescFieldName) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    const char *name = "new_name";
    SQLRETURN ret = SQLSetDescField((SQLHDESC)s->ard, 1, SQL_DESC_NAME, (SQLPOINTER)name, SQL_NTS);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set name");

    SQLCHAR buf[128] = {};
    SQLINTEGER len = 0;
    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_NAME, buf, sizeof(buf), &len);
    AssertEqual(std::string("new_name"), std::string((char *)buf), "Name matches");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetDescFieldDataPtr) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    int data = 42;
    SQLRETURN ret = SQLSetDescField((SQLHDESC)s->ard, 1, SQL_DESC_DATA_PTR, &data, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set data ptr");

    SQLPOINTER ptr = nullptr;
    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_DATA_PTR, &ptr, sizeof(ptr), nullptr);
    AssertTrue(ptr == &data, "Data ptr round-trip");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SetDescFieldIndicatorPtr) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLLEN indicator = 0;
    SQLSetDescField((SQLHDESC)s->ard, 1, SQL_DESC_INDICATOR_PTR, &indicator, 0);
    SQLSetDescField((SQLHDESC)s->ard, 1, SQL_DESC_OCTET_LENGTH_PTR, &indicator, 0);

    SQLLEN *ptr = nullptr;
    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_INDICATOR_PTR, &ptr, sizeof(ptr), nullptr);
    AssertTrue(ptr == &indicator, "Indicator ptr");

    ptr = nullptr;
    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_OCTET_LENGTH_PTR, &ptr, sizeof(ptr), nullptr);
    AssertTrue(ptr == &indicator, "Octet length ptr");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLSetDescField — Header field (SQL_DESC_COUNT)
// ============================================================================

TEST(Phase8_Descriptor, SetDescFieldCount) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLRETURN ret = SQLSetDescField((SQLHDESC)s->ard, 0, SQL_DESC_COUNT, (SQLPOINTER)(intptr_t)5, 0);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Set count");

    SQLSMALLINT count = 0;
    SQLGetDescField((SQLHDESC)s->ard, 0, SQL_DESC_COUNT, &count, sizeof(count), nullptr);
    AssertEqual(5, (int)count, "Count should be 5");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetDescFieldW test
// ============================================================================

TEST(Phase8_Descriptor, GetDescFieldWStringField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->ard->records[1].name = "wide_test";
    s->ard->count = 1;

    SQLWCHAR wbuf[128] = {};
    SQLINTEGER wlen = 0;
    SQLRETURN ret = SQLGetDescFieldW((SQLHDESC)s->ard, 1, SQL_DESC_NAME, wbuf, sizeof(wbuf), &wlen);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetDescFieldW name");
    // Convert back and verify
    std::string result = WideToUtf8(wbuf, -1);
    AssertEqual(std::string("wide_test"), result, "Wide name value");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, GetDescFieldWNonStringField) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    s->ard->records[1].type = SQL_DOUBLE;
    s->ard->count = 1;

    SQLSMALLINT type_val = 0;
    SQLRETURN ret = SQLGetDescFieldW((SQLHDESC)s->ard, 1, SQL_DESC_TYPE, &type_val, sizeof(type_val), nullptr);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetDescFieldW type");
    AssertEqual((int)SQL_DOUBLE, (int)type_val, "Type via W version");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetDescRecW test
// ============================================================================

TEST(Phase8_Descriptor, GetDescRecWSuccess) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    auto &rec = s->ard->records[1];
    rec.name = "wide_col";
    rec.type = SQL_FLOAT;
    rec.concise_type = SQL_FLOAT;
    rec.length = 8;
    rec.precision = 15;
    rec.scale = 0;
    rec.nullable = SQL_NULLABLE;
    s->ard->count = 1;

    SQLWCHAR wname[128] = {};
    SQLSMALLINT nameLen = 0;
    SQLSMALLINT type = 0, subtype = 0, prec = 0, scale = 0, nullable = 0;
    SQLLEN length = 0;

    SQLRETURN ret = SQLGetDescRecW((SQLHDESC)s->ard, 1, wname, 128, &nameLen,
                                   &type, &subtype, &length, &prec, &scale, &nullable);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "GetDescRecW");
    std::string name_str = WideToUtf8(wname, -1);
    AssertEqual(std::string("wide_col"), name_str, "Wide name");
    AssertEqual((int)SQL_FLOAT, (int)type, "Type");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SyncDescriptorsFromStatement test
// ============================================================================

TEST(Phase8_Descriptor, SyncIRDFromResultSet) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Simulate result set
    ColumnInfo col1;
    col1.name = "id";
    col1.clickhouse_type = "Int32";
    col1.sql_type = SQL_INTEGER;
    col1.column_size = 10;
    col1.decimal_digits = 0;
    col1.nullable = SQL_NO_NULLS;

    ColumnInfo col2;
    col2.name = "name";
    col2.clickhouse_type = "String";
    col2.sql_type = SQL_VARCHAR;
    col2.column_size = 255;
    col2.decimal_digits = 0;
    col2.nullable = SQL_NULLABLE;

    s->result_set.columns.push_back(col1);
    s->result_set.columns.push_back(col2);

    SyncDescriptorsFromStatement(s);

    // Verify IRD
    AssertEqual(2, (int)s->ird->count, "IRD count");

    SQLSMALLINT type_val = 0;
    SQLGetDescField((SQLHDESC)s->ird, 1, SQL_DESC_TYPE, &type_val, sizeof(type_val), nullptr);
    AssertEqual((int)SQL_INTEGER, (int)type_val, "IRD col 1 type");

    SQLCHAR name_buf[128] = {};
    SQLINTEGER name_len = 0;
    SQLGetDescField((SQLHDESC)s->ird, 1, SQL_DESC_NAME, name_buf, sizeof(name_buf), &name_len);
    AssertEqual(std::string("id"), std::string((char *)name_buf), "IRD col 1 name");

    type_val = 0;
    SQLGetDescField((SQLHDESC)s->ird, 2, SQL_DESC_TYPE, &type_val, sizeof(type_val), nullptr);
    AssertEqual((int)SQL_VARCHAR, (int)type_val, "IRD col 2 type");

    memset(name_buf, 0, sizeof(name_buf));
    SQLGetDescField((SQLHDESC)s->ird, 2, SQL_DESC_NAME, name_buf, sizeof(name_buf), &name_len);
    AssertEqual(std::string("name"), std::string((char *)name_buf), "IRD col 2 name");

    SQLSMALLINT nullable_val = 0;
    SQLGetDescField((SQLHDESC)s->ird, 2, SQL_DESC_NULLABLE, &nullable_val, sizeof(nullable_val), nullptr);
    AssertEqual((int)SQL_NULLABLE, (int)nullable_val, "IRD col 2 nullable");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_Descriptor, SyncARDFromBoundColumns) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Bind a column
    char buf[100];
    SQLLEN ind = 0;
    s->bound_columns[1] = {SQL_C_CHAR, buf, sizeof(buf), &ind};

    SyncDescriptorsFromStatement(s);

    AssertEqual(1, (int)s->ard->count, "ARD count");
    auto it = s->ard->records.find(1);
    AssertTrue(it != s->ard->records.end(), "ARD record 1 exists");
    AssertEqual((int)SQL_C_CHAR, (int)it->second.data_type, "ARD data type");
    AssertTrue(it->second.data_ptr == buf, "ARD data ptr");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLBulkOperations test
// ============================================================================

TEST(Phase8_BulkOps, InvalidHandle) {
    SQLRETURN ret = SQLBulkOperations(SQL_NULL_HANDLE, SQL_ADD);
    AssertEqual((int)SQL_INVALID_HANDLE, (int)ret, "Invalid handle");
}

TEST(Phase8_BulkOps, NoResultSetError) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    // No result set, so SQL_ADD should fail
    SQLRETURN ret = SQLBulkOperations(stmt, SQL_ADD);
    AssertEqual((int)SQL_ERROR, (int)ret, "No result set");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_BulkOps, BookmarkOperationsNotSupported) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLRETURN ret = SQLBulkOperations(stmt, SQL_UPDATE_BY_BOOKMARK);
    AssertEqual((int)SQL_ERROR, (int)ret, "Update by bookmark not supported");

    ret = SQLBulkOperations(stmt, SQL_DELETE_BY_BOOKMARK);
    AssertEqual((int)SQL_ERROR, (int)ret, "Delete by bookmark not supported");

    ret = SQLBulkOperations(stmt, SQL_FETCH_BY_BOOKMARK);
    AssertEqual((int)SQL_ERROR, (int)ret, "Fetch by bookmark not supported");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_BulkOps, InvalidOperationError) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    SQLRETURN ret = SQLBulkOperations(stmt, 999);
    AssertEqual((int)SQL_ERROR, (int)ret, "Invalid operation");

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase8_BulkOps, AddWithResultSetButNoColumns) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    // Add columns to result set but don't bind
    ColumnInfo col;
    col.name = "id";
    col.sql_type = SQL_INTEGER;
    col.column_size = 10;
    s->result_set.columns.push_back(col);
    s->query = "SELECT id FROM test_table";

    SQLRETURN ret = SQLBulkOperations(stmt, SQL_ADD);
    AssertEqual((int)SQL_ERROR, (int)ret, "No bound columns");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SQLGetFunctions — Descriptor function test
// ============================================================================

TEST(Phase8_Functions, DescriptorFunctionsSupported) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT supported = SQL_FALSE;

    SQLGetFunctions(dbc, SQL_API_SQLGETDESCFIELD, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetDescField supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLSETDESCFIELD, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetDescField supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLGETDESCREC, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLGetDescRec supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLSETDESCREC, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLSetDescRec supported");

    supported = SQL_FALSE;
    SQLGetFunctions(dbc, SQL_API_SQLCOPYDESC, &supported);
    AssertEqual((int)SQL_TRUE, (int)supported, "SQLCopyDesc supported");

    FreeConnHandles(env, dbc);
}

TEST(Phase8_Functions, DescriptorFunctionsInBitmap) {
    SQLHENV env; SQLHDBC dbc;
    CreateConnHandles(env, dbc);

    SQLUSMALLINT bitmap[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE] = {};
    SQLRETURN ret = SQLGetFunctions(dbc, SQL_API_ODBC3_ALL_FUNCTIONS, bitmap);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "Bitmap query");

    auto checkFunc = [&](SQLUSMALLINT id) -> bool {
        return (bitmap[id >> 4] & (1 << (id & 0x000F))) != 0;
    };

    AssertTrue(checkFunc(SQL_API_SQLGETDESCFIELD), "Bitmap: SQLGetDescField");
    AssertTrue(checkFunc(SQL_API_SQLSETDESCFIELD), "Bitmap: SQLSetDescField");
    AssertTrue(checkFunc(SQL_API_SQLGETDESCREC), "Bitmap: SQLGetDescRec");
    AssertTrue(checkFunc(SQL_API_SQLSETDESCREC), "Bitmap: SQLSetDescRec");
    AssertTrue(checkFunc(SQL_API_SQLCOPYDESC), "Bitmap: SQLCopyDesc");

    FreeConnHandles(env, dbc);
}

// ============================================================================
// DescriptorRecord default values test
// ============================================================================

TEST(Phase8_Descriptor, DefaultRecordValues) {
    DescriptorRecord rec;
    AssertEqual((int)SQL_VARCHAR, (int)rec.type, "Default type");
    AssertEqual((int)SQL_VARCHAR, (int)rec.concise_type, "Default concise type");
    AssertEqual((__int64)0, (__int64)rec.length, "Default length");
    AssertEqual(0, (int)rec.precision, "Default precision");
    AssertEqual(0, (int)rec.scale, "Default scale");
    AssertEqual((int)SQL_NULLABLE, (int)rec.nullable, "Default nullable");
    AssertEqual((int)SQL_NAMED, (int)rec.unnamed, "Default unnamed");
    AssertEqual((int)SQL_PRED_SEARCHABLE, (int)rec.searchable, "Default searchable");
    AssertEqual((int)SQL_TRUE, (int)rec.case_sensitive, "Default case sensitive");
    AssertEqual((int)SQL_ATTR_READONLY, (int)rec.updatable, "Default updatable");
    AssertEqual((int)SQL_FALSE, (int)rec.auto_unique_value, "Default auto unique");
    AssertEqual((int)SQL_FALSE, (int)rec.fixed_prec_scale, "Default fixed prec scale");
    AssertNull(rec.data_ptr, "Default data_ptr null");
    AssertNull(rec.indicator_ptr, "Default indicator_ptr null");
    AssertNull(rec.octet_length_ptr, "Default octet_length_ptr null");
}

// ============================================================================
// Multiple records test
// ============================================================================

TEST(Phase8_Descriptor, MultipleRecordsRoundTrip) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);

    // Set 3 records
    for (int i = 1; i <= 3; i++) {
        SQLSetDescField((SQLHDESC)s->ard, (SQLSMALLINT)i, SQL_DESC_TYPE,
                        (SQLPOINTER)(intptr_t)(SQL_INTEGER + i - 1), 0);
        SQLSetDescField((SQLHDESC)s->ard, (SQLSMALLINT)i, SQL_DESC_LENGTH,
                        (SQLPOINTER)(uintptr_t)(i * 10), 0);
    }

    // Verify count
    SQLSMALLINT count = 0;
    SQLGetDescField((SQLHDESC)s->ard, 0, SQL_DESC_COUNT, &count, sizeof(count), nullptr);
    AssertEqual(3, (int)count, "Count is 3");

    // Verify each record
    for (int i = 1; i <= 3; i++) {
        SQLSMALLINT type_val = 0;
        SQLULEN len_val = 0;
        SQLGetDescField((SQLHDESC)s->ard, (SQLSMALLINT)i, SQL_DESC_TYPE, &type_val, sizeof(type_val), nullptr);
        SQLGetDescField((SQLHDESC)s->ard, (SQLSMALLINT)i, SQL_DESC_LENGTH, &len_val, sizeof(len_val), nullptr);
        AssertEqual((int)(SQL_INTEGER + i - 1), (int)type_val, "Record type");
        AssertEqual((__int64)(i * 10), (__int64)len_val, "Record length");
    }

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SetDescField — Parameter type test
// ============================================================================

TEST(Phase8_Descriptor, SetDescFieldParameterType) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    SQLSetDescField((SQLHDESC)s->ipd, 1, SQL_DESC_PARAMETER_TYPE,
                    (SQLPOINTER)(uintptr_t)SQL_PARAM_INPUT_OUTPUT, 0);

    SQLSMALLINT param_type = 0;
    SQLGetDescField((SQLHDESC)s->ipd, 1, SQL_DESC_PARAMETER_TYPE, &param_type, sizeof(param_type), nullptr);
    AssertEqual((int)SQL_PARAM_INPUT_OUTPUT, (int)param_type, "Parameter type");

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// SetDescFieldW test
// ============================================================================

TEST(Phase8_Descriptor, SetDescFieldWName) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *s = static_cast<OdbcStatement *>(stmt);
    const wchar_t *wname = L"wide_name";
    SQLRETURN ret = SQLSetDescFieldW((SQLHDESC)s->ard, 1, SQL_DESC_NAME,
                                     (SQLPOINTER)wname, SQL_NTS);
    AssertEqual((int)SQL_SUCCESS, (int)ret, "SetDescFieldW name");

    // Read back via ANSI
    SQLCHAR buf[128] = {};
    SQLINTEGER len = 0;
    SQLGetDescField((SQLHDESC)s->ard, 1, SQL_DESC_NAME, buf, sizeof(buf), &len);
    AssertEqual(std::string("wide_name"), std::string((char *)buf), "Name via ANSI");

    FreeTestHandles(env, dbc, stmt);
}
