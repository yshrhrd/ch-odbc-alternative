#include "include/handle.h"
#include "include/util.h"

#include <sql.h>
#include <sqlext.h>
#include <cstring>

#ifdef UNICODE
#undef SQLGetDiagRec
#undef SQLGetDiagField
#undef SQLError
#endif

using namespace clickhouse_odbc;

// ============================================================================
// SQLGetDiagRec / SQLGetDiagRecW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                            SQLSMALLINT RecNumber, SQLCHAR *Sqlstate,
                                            SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                            SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
    if (!Handle || RecNumber < 1) {
        return SQL_INVALID_HANDLE;
    }

    auto *h = static_cast<OdbcHandle *>(Handle);
    HandleLock lock(h);
    if (RecNumber > static_cast<SQLSMALLINT>(h->diag_records.size())) {
        return SQL_NO_DATA;
    }

    const auto &rec = h->diag_records[RecNumber - 1];

    if (Sqlstate) {
        memset(Sqlstate, 0, 6);
        strncpy(reinterpret_cast<char *>(Sqlstate), rec.sql_state.c_str(), 5);
    }

    if (NativeError) {
        *NativeError = rec.native_error;
    }

    if (MessageText && BufferLength > 0) {
        CopyStringToBuffer(rec.message, MessageText, BufferLength, TextLength);
    } else if (TextLength) {
        *TextLength = (SQLSMALLINT)rec.message.size();
    }

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                             SQLSMALLINT RecNumber, SQLWCHAR *Sqlstate,
                                             SQLINTEGER *NativeError, SQLWCHAR *MessageText,
                                             SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
    if (!Handle || RecNumber < 1) {
        return SQL_INVALID_HANDLE;
    }

    auto *h = static_cast<OdbcHandle *>(Handle);
    HandleLock lock(h);
    if (RecNumber > static_cast<SQLSMALLINT>(h->diag_records.size())) {
        return SQL_NO_DATA;
    }

    const auto &rec = h->diag_records[RecNumber - 1];

    if (Sqlstate) {
        std::wstring wide_state = Utf8ToWide(rec.sql_state);
        memset(Sqlstate, 0, 6 * sizeof(SQLWCHAR));
        wcsncpy((wchar_t *)Sqlstate, wide_state.c_str(), 5);
    }

    if (NativeError) {
        *NativeError = rec.native_error;
    }

    if (MessageText && BufferLength > 0) {
        CopyStringToBufferW(rec.message, MessageText, BufferLength, TextLength);
    } else if (TextLength) {
        std::wstring wide_msg = Utf8ToWide(rec.message);
        *TextLength = (SQLSMALLINT)wide_msg.size();
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLGetDiagField / SQLGetDiagFieldW
// ============================================================================
extern "C" SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                              SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
                                              SQLPOINTER DiagInfo, SQLSMALLINT BufferLength,
                                              SQLSMALLINT *StringLength) {
    if (!Handle) {
        return SQL_INVALID_HANDLE;
    }

    auto *h = static_cast<OdbcHandle *>(Handle);
    HandleLock lock(h);

    // Header fields (RecNumber = 0)
    if (RecNumber == 0) {
        switch (DiagIdentifier) {
        case SQL_DIAG_NUMBER:
            if (DiagInfo) *(SQLINTEGER *)DiagInfo = (SQLINTEGER)h->diag_records.size();
            return SQL_SUCCESS;
        case SQL_DIAG_RETURNCODE:
            if (DiagInfo) *(SQLRETURN *)DiagInfo = h->diag_records.empty() ? SQL_SUCCESS : SQL_ERROR;
            return SQL_SUCCESS;
        case SQL_DIAG_ROW_COUNT:
            if (DiagInfo) {
                if (HandleType == SQL_HANDLE_STMT) {
                    auto *stmt = static_cast<OdbcStatement *>(Handle);
                    *(SQLLEN *)DiagInfo = (stmt->affected_rows >= 0) ? stmt->affected_rows
                                        : static_cast<SQLLEN>(stmt->result_set.RowCount());
                } else {
                    *(SQLLEN *)DiagInfo = 0;
                }
            }
            return SQL_SUCCESS;
        case SQL_DIAG_CURSOR_ROW_COUNT:
            if (DiagInfo) {
                if (HandleType == SQL_HANDLE_STMT) {
                    auto *stmt = static_cast<OdbcStatement *>(Handle);
                    *(SQLLEN *)DiagInfo = static_cast<SQLLEN>(stmt->result_set.RowCount());
                } else {
                    *(SQLLEN *)DiagInfo = 0;
                }
            }
            return SQL_SUCCESS;
        case SQL_DIAG_DYNAMIC_FUNCTION:
            if (DiagInfo && BufferLength > 0) {
                CopyStringToBuffer("", (SQLCHAR *)DiagInfo, BufferLength, StringLength);
            }
            return SQL_SUCCESS;
        case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
            if (DiagInfo) *(SQLINTEGER *)DiagInfo = SQL_DIAG_UNKNOWN_STATEMENT;
            return SQL_SUCCESS;
        default:
            return SQL_ERROR;
        }
    }

    if (RecNumber > static_cast<SQLSMALLINT>(h->diag_records.size())) {
        return SQL_NO_DATA;
    }

    const auto &rec = h->diag_records[RecNumber - 1];

    switch (DiagIdentifier) {
    case SQL_DIAG_SQLSTATE:
        if (DiagInfo && BufferLength > 0) {
            CopyStringToBuffer(rec.sql_state, (SQLCHAR *)DiagInfo, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DIAG_NATIVE:
        if (DiagInfo) *(SQLINTEGER *)DiagInfo = rec.native_error;
        return SQL_SUCCESS;
    case SQL_DIAG_MESSAGE_TEXT:
        if (DiagInfo && BufferLength > 0) {
            CopyStringToBuffer(rec.message, (SQLCHAR *)DiagInfo, BufferLength, StringLength);
        } else if (StringLength) {
            *StringLength = (SQLSMALLINT)rec.message.size();
        }
        return SQL_SUCCESS;
    case SQL_DIAG_CLASS_ORIGIN:
    case SQL_DIAG_SUBCLASS_ORIGIN:
        if (DiagInfo && BufferLength > 0) {
            CopyStringToBuffer("ODBC 3.0", (SQLCHAR *)DiagInfo, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    case SQL_DIAG_CONNECTION_NAME:
    case SQL_DIAG_SERVER_NAME: {
        // Try to get server name from the associated connection
        std::string server_name;
        if (HandleType == SQL_HANDLE_STMT) {
            auto *stmt = static_cast<OdbcStatement *>(Handle);
            if (stmt->conn) server_name = stmt->conn->host;
        } else if (HandleType == SQL_HANDLE_DBC) {
            auto *conn = static_cast<OdbcConnection *>(Handle);
            server_name = conn->host;
        }
        if (DiagInfo && BufferLength > 0) {
            CopyStringToBuffer(server_name, (SQLCHAR *)DiagInfo, BufferLength, StringLength);
        }
        return SQL_SUCCESS;
    }
    default:
        return SQL_ERROR;
    }
}

extern "C" SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT HandleType, SQLHANDLE Handle,
                                                SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
                                                SQLPOINTER DiagInfo, SQLSMALLINT BufferLength,
                                                SQLSMALLINT *StringLength) {
    if (!Handle) {
        return SQL_INVALID_HANDLE;
    }

    auto *h = static_cast<OdbcHandle *>(Handle);
    HandleLock lock(h);

    // Check if this is a string field that needs wide conversion
    bool isStringField = false;
    switch (DiagIdentifier) {
    case SQL_DIAG_SQLSTATE:
    case SQL_DIAG_MESSAGE_TEXT:
    case SQL_DIAG_CLASS_ORIGIN:
    case SQL_DIAG_SUBCLASS_ORIGIN:
    case SQL_DIAG_CONNECTION_NAME:
    case SQL_DIAG_SERVER_NAME:
    case SQL_DIAG_DYNAMIC_FUNCTION:
        isStringField = true;
        break;
    }

    // Non-string fields: delegate to ANSI version directly
    if (!isStringField) {
        return SQLGetDiagField(HandleType, Handle, RecNumber, DiagIdentifier, DiagInfo, BufferLength, StringLength);
    }

    // String fields: get ANSI value first, then convert to wide
    char ansiBuf[1024] = {};
    SQLSMALLINT ansiLen = 0;
    SQLRETURN ret = SQLGetDiagField(HandleType, Handle, RecNumber, DiagIdentifier,
                                    ansiBuf, sizeof(ansiBuf), &ansiLen);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) return ret;

    std::string ansiStr(ansiBuf, ansiLen > 0 ? ansiLen : strlen(ansiBuf));
    if (DiagInfo && BufferLength > 0) {
        SQLSMALLINT wideLen = 0;
        CopyStringToBufferW(ansiStr, (SQLWCHAR *)DiagInfo,
                            BufferLength / (SQLSMALLINT)sizeof(SQLWCHAR), &wideLen);
        if (StringLength) *StringLength = wideLen * sizeof(SQLWCHAR);
    } else if (StringLength) {
        std::wstring wide = Utf8ToWide(ansiStr);
        *StringLength = static_cast<SQLSMALLINT>(wide.size() * sizeof(SQLWCHAR));
    }

    return SQL_SUCCESS;
}

// ============================================================================
// SQLError / SQLErrorW  (ODBC 2.x)
// ============================================================================

// ODBC 2.x SQLError iterates diagnostic records sequentially.
// Each call returns the next record; returns SQL_NO_DATA when exhausted.
// We track position via a simple counter stored in diag_records iteration.

extern "C" SQLRETURN SQL_API SQLError(SQLHENV EnvironmentHandle, SQLHDBC ConnectionHandle,
                                       SQLHSTMT StatementHandle, SQLCHAR *Sqlstate,
                                       SQLINTEGER *NativeError, SQLCHAR *MessageText,
                                       SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
    // Determine which handle to use (ODBC 2.x picks the most specific non-null handle)
    SQLHANDLE handle = nullptr;
    SQLSMALLINT handleType = 0;
    if (StatementHandle) {
        handle = StatementHandle;
        handleType = SQL_HANDLE_STMT;
    } else if (ConnectionHandle) {
        handle = ConnectionHandle;
        handleType = SQL_HANDLE_DBC;
    } else if (EnvironmentHandle) {
        handle = EnvironmentHandle;
        handleType = SQL_HANDLE_ENV;
    } else {
        return SQL_INVALID_HANDLE;
    }

    auto *h = static_cast<OdbcHandle *>(handle);
    HandleLock lock(h);
    if (h->diag_records.empty()) {
        return SQL_NO_DATA;
    }

    // Return the first record and remove it (ODBC 2.x iterative consumption)
    const auto &rec = h->diag_records.front();

    if (Sqlstate) {
        CopyStringToBuffer(rec.sql_state, Sqlstate, 6, nullptr);
    }
    if (NativeError) {
        *NativeError = rec.native_error;
    }
    if (MessageText && BufferLength > 0) {
        CopyStringToBuffer(rec.message, MessageText, BufferLength, TextLength);
    } else if (TextLength) {
        *TextLength = static_cast<SQLSMALLINT>(rec.message.size());
    }

    // Remove the consumed record
    h->diag_records.erase(h->diag_records.begin());

    return SQL_SUCCESS;
}

extern "C" SQLRETURN SQL_API SQLErrorW(SQLHENV EnvironmentHandle, SQLHDBC ConnectionHandle,
                                        SQLHSTMT StatementHandle, SQLWCHAR *Sqlstate,
                                        SQLINTEGER *NativeError, SQLWCHAR *MessageText,
                                        SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
    SQLHANDLE handle = nullptr;
    SQLSMALLINT handleType = 0;
    if (StatementHandle) {
        handle = StatementHandle;
        handleType = SQL_HANDLE_STMT;
    } else if (ConnectionHandle) {
        handle = ConnectionHandle;
        handleType = SQL_HANDLE_DBC;
    } else if (EnvironmentHandle) {
        handle = EnvironmentHandle;
        handleType = SQL_HANDLE_ENV;
    } else {
        return SQL_INVALID_HANDLE;
    }

    auto *h = static_cast<OdbcHandle *>(handle);
    HandleLock lock(h);
    if (h->diag_records.empty()) {
        return SQL_NO_DATA;
    }

    const auto &rec = h->diag_records.front();

    if (Sqlstate) {
        CopyStringToBufferW(rec.sql_state, Sqlstate, 6, nullptr);
    }
    if (NativeError) {
        *NativeError = rec.native_error;
    }
    if (MessageText && BufferLength > 0) {
        CopyStringToBufferW(rec.message, MessageText, BufferLength, TextLength);
    } else if (TextLength) {
        std::wstring wide_msg = Utf8ToWide(rec.message);
        *TextLength = static_cast<SQLSMALLINT>(wide_msg.size());
    }

    h->diag_records.erase(h->diag_records.begin());

    return SQL_SUCCESS;
}
