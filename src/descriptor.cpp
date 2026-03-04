#include "include/handle.h"
#include "include/util.h"
#include "include/type_mapping.h"

#include <sql.h>
#include <sqlext.h>
#include <cstring>

#ifdef UNICODE
#undef SQLGetDescField
#undef SQLSetDescField
#undef SQLGetDescRec
#undef SQLSetDescRec
#endif

using namespace clickhouse_odbc;

// ============================================================================
// Helper: Validate descriptor handle
// ============================================================================
static bool IsValidDescHandle(SQLHANDLE handle) {
    if (!handle) return false;
    auto *h = static_cast<OdbcHandle *>(handle);
    return h->handle_type == SQL_HANDLE_DESC;
}

// Helper: Get descriptor from statement handle (for SQL_ATTR_APP_ROW_DESC etc.)
static OdbcDescriptor *GetDescFromStmt(OdbcStatement *stmt, DescriptorType type) {
    switch (type) {
    case DescriptorType::APD: return stmt->apd;
    case DescriptorType::IPD: return stmt->ipd;
    case DescriptorType::ARD: return stmt->ard;
    case DescriptorType::IRD: return stmt->ird;
    default: return nullptr;
    }
}

// Helper: Sync IRD from result_set columns
static void SyncIRD(OdbcStatement *stmt) {
    auto *ird = stmt->ird;
    if (!ird) return;
    ird->records.clear();
    ird->count = static_cast<SQLSMALLINT>(stmt->result_set.columns.size());
    for (SQLUSMALLINT i = 0; i < stmt->result_set.columns.size(); i++) {
        const auto &col = stmt->result_set.columns[i];
        DescriptorRecord rec;
        rec.name = col.name;
        rec.base_column_name = col.name;
        rec.label = col.name;
        rec.type = col.sql_type;
        rec.concise_type = col.sql_type;
        rec.type_name = col.clickhouse_type;
        rec.length = col.column_size;
        rec.precision = static_cast<SQLSMALLINT>(col.column_size);
        rec.scale = col.decimal_digits;
        rec.nullable = col.nullable;
        rec.display_size = col.column_size;
        rec.octet_length = col.column_size;
        rec.unnamed = col.name.empty() ? SQL_UNNAMED : SQL_NAMED;
        ird->records[i + 1] = rec;
    }
}

// Helper: Sync ARD from bound_columns
static void SyncARD(OdbcStatement *stmt) {
    auto *ard = stmt->ard;
    if (!ard) return;
    ard->records.clear();
    ard->count = 0;
    for (auto &[idx, bc] : stmt->bound_columns) {
        DescriptorRecord rec;
        rec.data_type = bc.target_type;
        rec.concise_type = bc.target_type;
        rec.data_ptr = bc.target_value;
        rec.buffer_length = bc.buffer_length;
        rec.indicator_ptr = bc.str_len_or_ind;
        rec.octet_length_ptr = bc.str_len_or_ind;
        ard->records[idx] = rec;
        if (idx > ard->count) ard->count = idx;
    }
}

// Helper: Sync APD from bound_parameters
static void SyncAPD(OdbcStatement *stmt) {
    auto *apd = stmt->apd;
    if (!apd) return;
    apd->records.clear();
    apd->count = 0;
    for (auto &[idx, bp] : stmt->bound_parameters) {
        DescriptorRecord rec;
        rec.data_type = bp.value_type;
        rec.concise_type = bp.value_type;
        rec.data_ptr = bp.parameter_value;
        rec.buffer_length = bp.buffer_length;
        rec.indicator_ptr = bp.str_len_or_ind;
        rec.octet_length_ptr = bp.str_len_or_ind;
        apd->records[idx] = rec;
        if (idx > apd->count) apd->count = idx;
    }
}

// Helper: Sync IPD from bound_parameters
static void SyncIPD(OdbcStatement *stmt) {
    auto *ipd = stmt->ipd;
    if (!ipd) return;
    ipd->records.clear();
    ipd->count = 0;
    for (auto &[idx, bp] : stmt->bound_parameters) {
        DescriptorRecord rec;
        rec.type = bp.parameter_type;
        rec.concise_type = bp.parameter_type;
        rec.length = bp.column_size;
        rec.precision = static_cast<SQLSMALLINT>(bp.column_size);
        rec.scale = bp.decimal_digits;
        rec.parameter_type = bp.input_output_type;
        ipd->records[idx] = rec;
        if (idx > ipd->count) ipd->count = idx;
    }
}

// ============================================================================
// SQLGetDescField / SQLGetDescFieldW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                              SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
                                              SQLINTEGER BufferLength, SQLINTEGER *StringLength) {
    if (!IsValidDescHandle(DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *desc = static_cast<OdbcDescriptor *>(DescriptorHandle);
    HandleLock lock(desc);

    // Header fields (RecNumber == 0 or header field IDs)
    switch (FieldIdentifier) {
    case SQL_DESC_COUNT:
        if (Value) *(SQLSMALLINT *)Value = desc->count;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_ALLOC_TYPE:
        if (Value) *(SQLSMALLINT *)Value = desc->is_auto ? SQL_DESC_ALLOC_AUTO : SQL_DESC_ALLOC_USER;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    }

    // Record fields require RecNumber >= 1
    if (RecNumber < 1) {
        desc->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    auto it = desc->records.find(static_cast<SQLUSMALLINT>(RecNumber));
    if (it == desc->records.end()) {
        // Return defaults for unset records
        DescriptorRecord defaultRec;
        switch (FieldIdentifier) {
        case SQL_DESC_TYPE:
        case SQL_DESC_CONCISE_TYPE:
            if (Value) *(SQLSMALLINT *)Value = SQL_VARCHAR;
            return SQL_SUCCESS;
        case SQL_DESC_LENGTH:
            if (Value) *(SQLULEN *)Value = 0;
            return SQL_SUCCESS;
        case SQL_DESC_NULLABLE:
            if (Value) *(SQLSMALLINT *)Value = SQL_NULLABLE;
            return SQL_SUCCESS;
        default:
            if (Value && BufferLength > 0) memset(Value, 0, BufferLength);
            return SQL_SUCCESS;
        }
    }

    const auto &rec = it->second;

    switch (FieldIdentifier) {
    case SQL_DESC_TYPE:
        if (Value) *(SQLSMALLINT *)Value = rec.type;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_CONCISE_TYPE:
        if (Value) *(SQLSMALLINT *)Value = rec.concise_type;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_LENGTH:
        if (Value) *(SQLULEN *)Value = rec.length;
        if (StringLength) *StringLength = sizeof(SQLULEN);
        return SQL_SUCCESS;
    case SQL_DESC_OCTET_LENGTH:
        if (Value) *(SQLLEN *)Value = rec.octet_length;
        if (StringLength) *StringLength = sizeof(SQLLEN);
        return SQL_SUCCESS;
    case SQL_DESC_PRECISION:
        if (Value) *(SQLSMALLINT *)Value = rec.precision;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_SCALE:
        if (Value) *(SQLSMALLINT *)Value = rec.scale;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_NULLABLE:
        if (Value) *(SQLSMALLINT *)Value = rec.nullable;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_NAME:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(rec.name, (SQLCHAR *)Value, static_cast<SQLSMALLINT>(BufferLength), nullptr);
        }
        if (StringLength) *StringLength = static_cast<SQLINTEGER>(rec.name.size());
        return SQL_SUCCESS;
    case SQL_DESC_BASE_COLUMN_NAME:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(rec.base_column_name, (SQLCHAR *)Value, static_cast<SQLSMALLINT>(BufferLength), nullptr);
        }
        if (StringLength) *StringLength = static_cast<SQLINTEGER>(rec.base_column_name.size());
        return SQL_SUCCESS;
    case SQL_DESC_BASE_TABLE_NAME:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(rec.base_table_name, (SQLCHAR *)Value, static_cast<SQLSMALLINT>(BufferLength), nullptr);
        }
        if (StringLength) *StringLength = static_cast<SQLINTEGER>(rec.base_table_name.size());
        return SQL_SUCCESS;
    case SQL_DESC_TABLE_NAME:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(rec.table_name, (SQLCHAR *)Value, static_cast<SQLSMALLINT>(BufferLength), nullptr);
        }
        if (StringLength) *StringLength = static_cast<SQLINTEGER>(rec.table_name.size());
        return SQL_SUCCESS;
    case SQL_DESC_LABEL:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(rec.label, (SQLCHAR *)Value, static_cast<SQLSMALLINT>(BufferLength), nullptr);
        }
        if (StringLength) *StringLength = static_cast<SQLINTEGER>(rec.label.size());
        return SQL_SUCCESS;
    case SQL_DESC_TYPE_NAME:
        if (Value && BufferLength > 0) {
            CopyStringToBuffer(rec.type_name, (SQLCHAR *)Value, static_cast<SQLSMALLINT>(BufferLength), nullptr);
        }
        if (StringLength) *StringLength = static_cast<SQLINTEGER>(rec.type_name.size());
        return SQL_SUCCESS;
    case SQL_DESC_UNNAMED:
        if (Value) *(SQLSMALLINT *)Value = rec.unnamed;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_DISPLAY_SIZE:
        if (Value) *(SQLLEN *)Value = static_cast<SQLLEN>(rec.display_size);
        if (StringLength) *StringLength = sizeof(SQLLEN);
        return SQL_SUCCESS;
    case SQL_DESC_SEARCHABLE:
        if (Value) *(SQLSMALLINT *)Value = rec.searchable;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_FIXED_PREC_SCALE:
        if (Value) *(SQLSMALLINT *)Value = rec.fixed_prec_scale;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_CASE_SENSITIVE:
        if (Value) *(SQLSMALLINT *)Value = rec.case_sensitive;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_UPDATABLE:
        if (Value) *(SQLSMALLINT *)Value = rec.updatable;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    case SQL_DESC_AUTO_UNIQUE_VALUE:
        if (Value) *(SQLINTEGER *)Value = rec.auto_unique_value;
        if (StringLength) *StringLength = sizeof(SQLINTEGER);
        return SQL_SUCCESS;
    case SQL_DESC_DATA_PTR:
        if (Value) *(SQLPOINTER *)Value = rec.data_ptr;
        if (StringLength) *StringLength = sizeof(SQLPOINTER);
        return SQL_SUCCESS;
    case SQL_DESC_INDICATOR_PTR:
        if (Value) *(SQLLEN **)Value = rec.indicator_ptr;
        if (StringLength) *StringLength = sizeof(SQLLEN *);
        return SQL_SUCCESS;
    case SQL_DESC_OCTET_LENGTH_PTR:
        if (Value) *(SQLLEN **)Value = rec.octet_length_ptr;
        if (StringLength) *StringLength = sizeof(SQLLEN *);
        return SQL_SUCCESS;
    case SQL_DESC_PARAMETER_TYPE:
        if (Value) *(SQLSMALLINT *)Value = rec.parameter_type;
        if (StringLength) *StringLength = sizeof(SQLSMALLINT);
        return SQL_SUCCESS;
    default:
        desc->SetError("HY091", "Invalid descriptor field identifier");
        return SQL_ERROR;
    }
}

extern "C" SQLRETURN SQL_API SQLGetDescFieldW(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                               SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
                                               SQLINTEGER BufferLength, SQLINTEGER *StringLength) {
    if (!IsValidDescHandle(DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *desc = static_cast<OdbcDescriptor *>(DescriptorHandle);
    HandleLock lock(desc);

    // For string fields, handle wide conversion
    bool isStringField = false;
    switch (FieldIdentifier) {
    case SQL_DESC_NAME:
    case SQL_DESC_BASE_COLUMN_NAME:
    case SQL_DESC_BASE_TABLE_NAME:
    case SQL_DESC_TABLE_NAME:
    case SQL_DESC_LABEL:
    case SQL_DESC_TYPE_NAME:
        isStringField = true;
        break;
    }

    if (!isStringField) {
        return SQLGetDescField(DescriptorHandle, RecNumber, FieldIdentifier, Value, BufferLength, StringLength);
    }

    // Get ANSI value first
    char ansiBuffer[512] = {};
    SQLINTEGER ansiLen = 0;
    SQLRETURN ret = SQLGetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                                    ansiBuffer, sizeof(ansiBuffer), &ansiLen);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) return ret;

    std::string ansiStr(ansiBuffer, ansiLen > 0 ? ansiLen : strlen(ansiBuffer));
    if (Value && BufferLength > 0) {
        SQLSMALLINT wideLen = 0;
        CopyStringToBufferW(ansiStr, (SQLWCHAR *)Value, static_cast<SQLSMALLINT>(BufferLength / sizeof(SQLWCHAR)), &wideLen);
        if (StringLength) *StringLength = wideLen * sizeof(SQLWCHAR);
    } else if (StringLength) {
        *StringLength = static_cast<SQLINTEGER>(ansiStr.size() * sizeof(SQLWCHAR));
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLSetDescField / SQLSetDescFieldW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLSetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                              SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
                                              SQLINTEGER BufferLength) {
    if (!IsValidDescHandle(DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *desc = static_cast<OdbcDescriptor *>(DescriptorHandle);
    HandleLock lock(desc);

    // IRD is read-only
    if (desc->desc_type == DescriptorType::IRD && RecNumber > 0) {
        desc->SetError("HY016", "Cannot modify an implementation row descriptor");
        return SQL_ERROR;
    }

    // Header field
    if (FieldIdentifier == SQL_DESC_COUNT) {
        desc->count = (SQLSMALLINT)(intptr_t)Value;
        return SQL_SUCCESS;
    }

    if (RecNumber < 1) {
        desc->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    auto &rec = desc->records[static_cast<SQLUSMALLINT>(RecNumber)];
    if (RecNumber > desc->count) desc->count = RecNumber;

    switch (FieldIdentifier) {
    case SQL_DESC_TYPE:
        rec.type = (SQLSMALLINT)(intptr_t)Value;
        return SQL_SUCCESS;
    case SQL_DESC_CONCISE_TYPE:
        rec.concise_type = (SQLSMALLINT)(intptr_t)Value;
        return SQL_SUCCESS;
    case SQL_DESC_LENGTH:
        rec.length = (SQLULEN)(uintptr_t)Value;
        return SQL_SUCCESS;
    case SQL_DESC_OCTET_LENGTH:
        rec.octet_length = (SQLLEN)(intptr_t)Value;
        return SQL_SUCCESS;
    case SQL_DESC_PRECISION:
        rec.precision = (SQLSMALLINT)(intptr_t)Value;
        return SQL_SUCCESS;
    case SQL_DESC_SCALE:
        rec.scale = (SQLSMALLINT)(intptr_t)Value;
        return SQL_SUCCESS;
    case SQL_DESC_NULLABLE:
        rec.nullable = (SQLSMALLINT)(intptr_t)Value;
        return SQL_SUCCESS;
    case SQL_DESC_NAME:
        if (Value) rec.name = std::string(reinterpret_cast<const char *>(Value),
                                          BufferLength == SQL_NTS ? strlen(reinterpret_cast<const char *>(Value)) : BufferLength);
        return SQL_SUCCESS;
    case SQL_DESC_DATA_PTR:
        rec.data_ptr = Value;
        return SQL_SUCCESS;
    case SQL_DESC_INDICATOR_PTR:
        rec.indicator_ptr = static_cast<SQLLEN *>(Value);
        return SQL_SUCCESS;
    case SQL_DESC_OCTET_LENGTH_PTR:
        rec.octet_length_ptr = static_cast<SQLLEN *>(Value);
        return SQL_SUCCESS;
    case SQL_DESC_PARAMETER_TYPE:
        rec.parameter_type = (SQLUSMALLINT)(uintptr_t)Value;
        return SQL_SUCCESS;
    default:
        // Silently accept unknown fields for compatibility
        return SQL_SUCCESS;
    }
}

extern "C" SQLRETURN SQL_API SQLSetDescFieldW(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                                SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
                                                SQLINTEGER BufferLength) {
    if (!IsValidDescHandle(DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *desc = static_cast<OdbcDescriptor *>(DescriptorHandle);
    HandleLock lock(desc);

    // For string fields, convert wide to UTF-8
    if (FieldIdentifier == SQL_DESC_NAME && Value) {
        std::string utf8 = WideToUtf8((SQLWCHAR *)Value,
                                      BufferLength == SQL_NTS ? -1 : static_cast<SQLSMALLINT>(BufferLength / sizeof(SQLWCHAR)));
        return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier,
                               (SQLPOINTER)utf8.c_str(), static_cast<SQLINTEGER>(utf8.size()));
    }

    return SQLSetDescField(DescriptorHandle, RecNumber, FieldIdentifier, Value, BufferLength);
}

// ============================================================================
// SQLGetDescRec / SQLGetDescRecW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                            SQLCHAR *Name, SQLSMALLINT BufferLength,
                                            SQLSMALLINT *StringLength, SQLSMALLINT *Type,
                                            SQLSMALLINT *SubType, SQLLEN *Length,
                                            SQLSMALLINT *Precision, SQLSMALLINT *Scale,
                                            SQLSMALLINT *Nullable) {
    if (!IsValidDescHandle(DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *desc = static_cast<OdbcDescriptor *>(DescriptorHandle);
    HandleLock lock(desc);

    if (RecNumber < 1 || RecNumber > desc->count) {
        return SQL_NO_DATA;
    }

    auto it = desc->records.find(static_cast<SQLUSMALLINT>(RecNumber));
    if (it == desc->records.end()) {
        return SQL_NO_DATA;
    }

    const auto &rec = it->second;

    if (Name && BufferLength > 0) {
        CopyStringToBuffer(rec.name, Name, BufferLength, StringLength);
    } else if (StringLength) {
        *StringLength = static_cast<SQLSMALLINT>(rec.name.size());
    }

    if (Type) *Type = rec.type;
    if (SubType) *SubType = rec.concise_type;
    if (Length) *Length = static_cast<SQLLEN>(rec.length);
    if (Precision) *Precision = rec.precision;
    if (Scale) *Scale = rec.scale;
    if (Nullable) *Nullable = rec.nullable;

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLGetDescRecW(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                             SQLWCHAR *Name, SQLSMALLINT BufferLength,
                                             SQLSMALLINT *StringLength, SQLSMALLINT *Type,
                                             SQLSMALLINT *SubType, SQLLEN *Length,
                                             SQLSMALLINT *Precision, SQLSMALLINT *Scale,
                                             SQLSMALLINT *Nullable) {
    if (!IsValidDescHandle(DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }
    auto *desc = static_cast<OdbcDescriptor *>(DescriptorHandle);
    HandleLock lock(desc);

    // Get non-string fields via ANSI version
    SQLCHAR nameBuf[512] = {};
    SQLSMALLINT nameLen = 0;
    SQLRETURN ret = SQLGetDescRec(DescriptorHandle, RecNumber, nameBuf, sizeof(nameBuf),
                                  &nameLen, Type, SubType, Length, Precision, Scale, Nullable);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) return ret;

    // Convert name to wide
    if (Name && BufferLength > 0) {
        std::string nameStr(reinterpret_cast<const char *>(nameBuf), nameLen);
        CopyStringToBufferW(nameStr, Name, BufferLength, StringLength);
    } else if (StringLength) {
        *StringLength = nameLen;
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLSetDescRec
// ============================================================================
extern "C" SQLRETURN SQL_API SQLSetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecNumber,
                                            SQLSMALLINT Type, SQLSMALLINT SubType,
                                            SQLLEN Length, SQLSMALLINT Precision,
                                            SQLSMALLINT Scale, SQLPOINTER Data,
                                            SQLLEN *StringLength, SQLLEN *Indicator) {
    if (!IsValidDescHandle(DescriptorHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *desc = static_cast<OdbcDescriptor *>(DescriptorHandle);
    HandleLock lock(desc);

    if (desc->desc_type == DescriptorType::IRD) {
        desc->SetError("HY016", "Cannot modify an implementation row descriptor");
        return SQL_ERROR;
    }

    if (RecNumber < 1) {
        desc->SetError("07009", "Invalid descriptor index");
        return SQL_ERROR;
    }

    auto &rec = desc->records[static_cast<SQLUSMALLINT>(RecNumber)];
    if (RecNumber > desc->count) desc->count = RecNumber;

    rec.type = Type;
    rec.concise_type = SubType;
    rec.length = static_cast<SQLULEN>(Length);
    rec.octet_length = Length;
    rec.precision = Precision;
    rec.scale = Scale;
    rec.data_ptr = Data;
    rec.octet_length_ptr = StringLength;
    rec.indicator_ptr = Indicator;

    return SQL_SUCCESS;
}

// ============================================================================
// SQLCopyDesc
// ============================================================================
extern "C" SQLRETURN SQL_API SQLCopyDesc(SQLHDESC SourceDescHandle, SQLHDESC TargetDescHandle) {
    if (!IsValidDescHandle(SourceDescHandle) || !IsValidDescHandle(TargetDescHandle)) {
        return SQL_INVALID_HANDLE;
    }

    auto *src = static_cast<OdbcDescriptor *>(SourceDescHandle);
    auto *tgt = static_cast<OdbcDescriptor *>(TargetDescHandle);
    // Lock both descriptors in pointer order to avoid deadlock
    OdbcDescriptor *first = (src < tgt) ? src : tgt;
    OdbcDescriptor *second = (src < tgt) ? tgt : src;
    HandleLock lock1(first);
    HandleLock lock2(second);

    // Cannot write to IRD
    if (tgt->desc_type == DescriptorType::IRD) {
        tgt->SetError("HY016", "Cannot modify an implementation row descriptor");
        return SQL_ERROR;
    }

    tgt->count = src->count;
    tgt->records = src->records;

    return SQL_SUCCESS;
}

// ============================================================================
// Sync functions (called from statement operations)
// ============================================================================
namespace clickhouse_odbc {

void SyncDescriptorsFromStatement(OdbcStatement *stmt) {
    SyncIRD(stmt);
    SyncARD(stmt);
    SyncAPD(stmt);
    SyncIPD(stmt);
}

} // namespace clickhouse_odbc
