#include "include/handle.h"
#include "include/util.h"
#include "include/clickhouse_client.h"
#include "resource.h"

#include <sql.h>
#include <sqlext.h>
#include <odbcinst.h>
#include <commctrl.h>

#ifdef UNICODE
#undef SQLConfigDataSource
#undef SQLWriteDSNToIni
#undef SQLRemoveDSNFromIni
#undef SQLWritePrivateProfileString
#undef SQLGetPrivateProfileString
#undef SQLPostInstallerError
#undef ConfigDSN
#endif

using namespace clickhouse_odbc;

// ============================================================================
// Data structure for DSN configuration dialog
// ============================================================================
struct DsnDialogData {
    std::string dsn_name;
    std::string description;
    std::string host = "localhost";
    std::string port = "8123";
    std::string database = "default";
    std::string user = "default";
    std::string password;
    std::string compression = "1";
    std::string page_size = "10000";
    std::string lazy_paging = "1";
    std::string max_lazy_rows = "0";
    std::string max_rows = "0";
    std::string ssl = "0";
    std::string ssl_verify = "1";
    std::string driver_name;
    bool is_new = true;       // true when ODBC_ADD_DSN
    bool result_ok = false;   // true when OK was pressed in dialog
};

// ============================================================================
// Helper: Read existing DSN settings from registry
// ============================================================================
static std::string ReadDsnValue(const std::string &dsn, const std::string &key, const std::string &default_val) {
    char buf[256] = {};
    SQLGetPrivateProfileString(dsn.c_str(), key.c_str(), default_val.c_str(), buf, sizeof(buf), "ODBC.INI");
    return std::string(buf);
}

// ============================================================================
// Helper: Get text from dialog control (ANSI)
// ============================================================================
static std::string GetDlgItemTextA_Str(HWND hDlg, int id) {
    char buf[512] = {};
    ::GetDlgItemTextA(hDlg, id, buf, sizeof(buf));
    return std::string(buf);
}

// ============================================================================
// Dialog procedure
// ============================================================================
static INT_PTR CALLBACK DsnDlgProc(HWND hDlg, UINT msg, WPARAM wParam, LPARAM lParam) {
    DsnDialogData *data = reinterpret_cast<DsnDialogData *>(GetWindowLongPtr(hDlg, GWLP_USERDATA));

    switch (msg) {
    case WM_INITDIALOG: {
        data = reinterpret_cast<DsnDialogData *>(lParam);
        SetWindowLongPtr(hDlg, GWLP_USERDATA, reinterpret_cast<LONG_PTR>(data));

        // Set initial values in fields
        ::SetDlgItemTextA(hDlg, IDC_EDIT_DSN, data->dsn_name.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_DESCRIPTION, data->description.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_HOST, data->host.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_PORT, data->port.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_DATABASE, data->database.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_UID, data->user.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_PWD, data->password.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_PAGESIZE, data->page_size.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_MAXROWS, data->max_rows.c_str());

        // Checkboxes
        CheckDlgButton(hDlg, IDC_CHECK_COMPRESSION,
                        (data->compression != "0") ? BST_CHECKED : BST_UNCHECKED);
        CheckDlgButton(hDlg, IDC_CHECK_LAZYPAGING,
                        (data->lazy_paging != "0") ? BST_CHECKED : BST_UNCHECKED);
        CheckDlgButton(hDlg, IDC_CHECK_SSL,
                        (data->ssl != "0") ? BST_CHECKED : BST_UNCHECKED);
        CheckDlgButton(hDlg, IDC_CHECK_SSL_VERIFY,
                        (data->ssl_verify != "0") ? BST_CHECKED : BST_UNCHECKED);

        // Disable DSN name editing when editing an existing DSN
        if (!data->is_new) {
            EnableWindow(GetDlgItem(hDlg, IDC_EDIT_DSN), FALSE);
        }

        return TRUE;
    }

    case WM_COMMAND:
        switch (LOWORD(wParam)) {
        case IDC_BTN_TEST: {
            // Connection test
            std::string test_host = GetDlgItemTextA_Str(hDlg, IDC_EDIT_HOST);
            std::string test_port_str = GetDlgItemTextA_Str(hDlg, IDC_EDIT_PORT);
            std::string test_db = GetDlgItemTextA_Str(hDlg, IDC_EDIT_DATABASE);
            std::string test_user = GetDlgItemTextA_Str(hDlg, IDC_EDIT_UID);
            std::string test_pwd = GetDlgItemTextA_Str(hDlg, IDC_EDIT_PWD);

            uint16_t test_port = 8123;
            try { test_port = static_cast<uint16_t>(std::stoi(test_port_str)); } catch (...) {}

            ClickHouseClient client;
            bool compress_checked = (IsDlgButtonChecked(hDlg, IDC_CHECK_COMPRESSION) == BST_CHECKED);
            client.SetCompressionEnabled(compress_checked);

            bool ssl_checked = (IsDlgButtonChecked(hDlg, IDC_CHECK_SSL) == BST_CHECKED);
            bool ssl_verify_checked = (IsDlgButtonChecked(hDlg, IDC_CHECK_SSL_VERIFY) == BST_CHECKED);
            client.SetSSLEnabled(ssl_checked);
            client.SetSSLVerify(ssl_verify_checked);

            if (client.Connect(test_host, test_port, test_db, test_user, test_pwd)) {
                client.Disconnect();
                MessageBoxA(hDlg, "Connection successful.", "Connection Test", MB_OK | MB_ICONINFORMATION);
            } else {
                std::string err_msg = "Connection failed.\n\nHost: " + test_host + ":" + test_port_str;
                MessageBoxA(hDlg, err_msg.c_str(), "Connection Test", MB_OK | MB_ICONERROR);
            }
            return TRUE;
        }

        case IDOK: {
            // Get input values
            data->dsn_name = GetDlgItemTextA_Str(hDlg, IDC_EDIT_DSN);
            data->description = GetDlgItemTextA_Str(hDlg, IDC_EDIT_DESCRIPTION);
            data->host = GetDlgItemTextA_Str(hDlg, IDC_EDIT_HOST);
            data->port = GetDlgItemTextA_Str(hDlg, IDC_EDIT_PORT);
            data->database = GetDlgItemTextA_Str(hDlg, IDC_EDIT_DATABASE);
            data->user = GetDlgItemTextA_Str(hDlg, IDC_EDIT_UID);
            data->password = GetDlgItemTextA_Str(hDlg, IDC_EDIT_PWD);
            data->page_size = GetDlgItemTextA_Str(hDlg, IDC_EDIT_PAGESIZE);
            data->max_rows = GetDlgItemTextA_Str(hDlg, IDC_EDIT_MAXROWS);
            data->compression = (IsDlgButtonChecked(hDlg, IDC_CHECK_COMPRESSION) == BST_CHECKED) ? "1" : "0";
            data->lazy_paging = (IsDlgButtonChecked(hDlg, IDC_CHECK_LAZYPAGING) == BST_CHECKED) ? "1" : "0";
            data->ssl = (IsDlgButtonChecked(hDlg, IDC_CHECK_SSL) == BST_CHECKED) ? "1" : "0";
            data->ssl_verify = (IsDlgButtonChecked(hDlg, IDC_CHECK_SSL_VERIFY) == BST_CHECKED) ? "1" : "0";

            // Validation
            if (data->dsn_name.empty()) {
                MessageBoxA(hDlg, "Please enter a DSN name.", "Validation Error", MB_OK | MB_ICONWARNING);
                SetFocus(GetDlgItem(hDlg, IDC_EDIT_DSN));
                return TRUE;
            }
            if (data->host.empty()) {
                MessageBoxA(hDlg, "Please enter a host name.", "Validation Error", MB_OK | MB_ICONWARNING);
                SetFocus(GetDlgItem(hDlg, IDC_EDIT_HOST));
                return TRUE;
            }

            data->result_ok = true;
            EndDialog(hDlg, IDOK);
            return TRUE;
        }

        case IDCANCEL:
            data->result_ok = false;
            EndDialog(hDlg, IDCANCEL);
            return TRUE;
        }
        break;
    }
    return FALSE;
}

// ============================================================================
// Write DSN to registry (common processing)
// ============================================================================
static BOOL WriteDsnToRegistry(const DsnDialogData &data) {
    if (!SQLWriteDSNToIni(data.dsn_name.c_str(), data.driver_name.c_str())) {
        return FALSE;
    }
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "Host", data.host.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "Port", data.port.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "Database", data.database.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "UID", data.user.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "PWD", data.password.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "Compression", data.compression.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "PageSize", data.page_size.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "LazyPaging", data.lazy_paging.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "MaxLazyRows", data.max_lazy_rows.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "DefaultMaxRows", data.max_rows.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "SSL", data.ssl.c_str(), "ODBC.INI");
    SQLWritePrivateProfileString(data.dsn_name.c_str(), "SSL_Verify", data.ssl_verify.c_str(), "ODBC.INI");
    if (!data.description.empty()) {
        SQLWritePrivateProfileString(data.dsn_name.c_str(), "Description", data.description.c_str(), "ODBC.INI");
    }
    return TRUE;
}

// ============================================================================
// Parse attribute string (double-null terminated) into DsnDialogData
// ============================================================================
static void ParseAttributes(const char *attrs, DsnDialogData &data) {
    if (!attrs) return;
    const char *p = attrs;
    while (*p) {
        std::string attr(p);
        p += attr.size() + 1;

        auto eq = attr.find('=');
        if (eq == std::string::npos) continue;

        std::string key = ToUpper(Trim(attr.substr(0, eq)));
        std::string val = Trim(attr.substr(eq + 1));

        if (key == "DSN") data.dsn_name = val;
        else if (key == "HOST" || key == "SERVER") data.host = val;
        else if (key == "PORT") data.port = val;
        else if (key == "DATABASE" || key == "DB") data.database = val;
        else if (key == "UID" || key == "USER") data.user = val;
        else if (key == "PWD" || key == "PASSWORD") data.password = val;
        else if (key == "DESCRIPTION") data.description = val;
        else if (key == "COMPRESSION" || key == "COMPRESS") data.compression = val;
        else if (key == "PAGESIZE" || key == "PAGE_SIZE") data.page_size = val;
        else if (key == "LAZYPAGING" || key == "LAZY_PAGING") data.lazy_paging = val;
        else if (key == "MAXLAZYROWS" || key == "MAX_LAZY_ROWS") data.max_lazy_rows = val;
        else if (key == "DEFAULTMAXROWS" || key == "DEFAULT_MAX_ROWS" || key == "MAXROWS") data.max_rows = val;
        else if (key == "SSL" || key == "SSLMODE") data.ssl = val;
        else if (key == "SSL_VERIFY" || key == "SSLVERIFY") data.ssl_verify = val;
    }
}

// ============================================================================
// Update DsnDialogData with existing DSN registry values
// ============================================================================
static void LoadExistingDsnValues(DsnDialogData &data) {
    if (data.dsn_name.empty()) return;
    data.host = ReadDsnValue(data.dsn_name, "Host", data.host);
    data.port = ReadDsnValue(data.dsn_name, "Port", data.port);
    data.database = ReadDsnValue(data.dsn_name, "Database", data.database);
    data.user = ReadDsnValue(data.dsn_name, "UID", data.user);
    data.password = ReadDsnValue(data.dsn_name, "PWD", data.password);
    data.description = ReadDsnValue(data.dsn_name, "Description", data.description);
    data.compression = ReadDsnValue(data.dsn_name, "Compression", data.compression);
    data.page_size = ReadDsnValue(data.dsn_name, "PageSize", data.page_size);
    data.lazy_paging = ReadDsnValue(data.dsn_name, "LazyPaging", data.lazy_paging);
    data.max_lazy_rows = ReadDsnValue(data.dsn_name, "MaxLazyRows", data.max_lazy_rows);
    data.max_rows = ReadDsnValue(data.dsn_name, "DefaultMaxRows", data.max_rows);
    data.ssl = ReadDsnValue(data.dsn_name, "SSL", data.ssl);
    data.ssl_verify = ReadDsnValue(data.dsn_name, "SSL_Verify", data.ssl_verify);
}

// DLL instance handle (set in dllmain.cpp)
#ifdef CLICKHOUSE_ODBC_EXPORTS
extern HINSTANCE g_hModule;
#else
static HINSTANCE g_hModule = nullptr;
#endif

// ============================================================================
// ConfigDSN
// ============================================================================
extern "C" BOOL INSTAPI ConfigDSN(HWND hwndParent, WORD fRequest,
                                   LPCSTR lpszDriver, LPCSTR lpszAttributes) {
    DsnDialogData data;
    data.driver_name = lpszDriver ? lpszDriver : "";
    ParseAttributes(lpszAttributes, data);

    switch (fRequest) {
    case ODBC_ADD_DSN:
        data.is_new = true;
        break;
    case ODBC_CONFIG_DSN:
        data.is_new = false;
        // Load existing settings from registry
        LoadExistingDsnValues(data);
        break;
    case ODBC_REMOVE_DSN:
        if (data.dsn_name.empty()) {
            SQLPostInstallerError(ODBC_ERROR_INVALID_DSN, "DSN name is required");
            return FALSE;
        }
        return SQLRemoveDSNFromIni(data.dsn_name.c_str());
    default:
        return FALSE;
    }

    // Show GUI dialog
    if (hwndParent) {
        INT_PTR ret = DialogBoxParam(g_hModule, MAKEINTRESOURCE(IDD_DSN_CONFIG),
                                     hwndParent, DsnDlgProc, reinterpret_cast<LPARAM>(&data));
        if (ret != IDOK || !data.result_ok) {
            return FALSE;
        }
    } else {
        // Silent mode when hwndParent is NULL (process with attribute values only)
        if (data.dsn_name.empty()) {
            SQLPostInstallerError(ODBC_ERROR_INVALID_DSN, "DSN name is required");
            return FALSE;
        }
    }

    return WriteDsnToRegistry(data);
}

// ============================================================================
// ConfigDSNW — ODBC DSN configuration (Unicode version)
// ============================================================================
extern "C" BOOL INSTAPI ConfigDSNW(HWND hwndParent, WORD fRequest,
                                    LPCWSTR lpszDriver, LPCWSTR lpszAttributes) {
    DsnDialogData data;
    if (lpszDriver) {
        data.driver_name = WideToUtf8(std::wstring(lpszDriver));
    }

    // Parse Unicode attributes
    if (lpszAttributes) {
        const wchar_t *p = lpszAttributes;
        while (*p) {
            std::wstring attr(p);
            p += attr.size() + 1;

            std::string attr_utf8 = WideToUtf8(attr);
            auto eq = attr_utf8.find('=');
            if (eq == std::string::npos) continue;

            std::string key = ToUpper(Trim(attr_utf8.substr(0, eq)));
            std::string val = Trim(attr_utf8.substr(eq + 1));

            if (key == "DSN") data.dsn_name = val;
            else if (key == "HOST" || key == "SERVER") data.host = val;
            else if (key == "PORT") data.port = val;
            else if (key == "DATABASE" || key == "DB") data.database = val;
            else if (key == "UID" || key == "USER") data.user = val;
            else if (key == "PWD" || key == "PASSWORD") data.password = val;
            else if (key == "DESCRIPTION") data.description = val;
            else if (key == "COMPRESSION" || key == "COMPRESS") data.compression = val;
            else if (key == "PAGESIZE" || key == "PAGE_SIZE") data.page_size = val;
            else if (key == "LAZYPAGING" || key == "LAZY_PAGING") data.lazy_paging = val;
            else if (key == "MAXLAZYROWS" || key == "MAX_LAZY_ROWS") data.max_lazy_rows = val;
            else if (key == "DEFAULTMAXROWS" || key == "DEFAULT_MAX_ROWS" || key == "MAXROWS") data.max_rows = val;
            else if (key == "SSL" || key == "SSLMODE") data.ssl = val;
            else if (key == "SSL_VERIFY" || key == "SSLVERIFY") data.ssl_verify = val;
        }
    }

    switch (fRequest) {
    case ODBC_ADD_DSN:
        data.is_new = true;
        break;
    case ODBC_CONFIG_DSN:
        data.is_new = false;
        LoadExistingDsnValues(data);
        break;
    case ODBC_REMOVE_DSN:
        if (data.dsn_name.empty()) {
            SQLPostInstallerErrorW(ODBC_ERROR_INVALID_DSN, L"DSN name is required");
            return FALSE;
        }
        return SQLRemoveDSNFromIni(data.dsn_name.c_str());
    default:
        return FALSE;
    }

    // Show GUI dialog
    if (hwndParent) {
        INT_PTR ret = DialogBoxParam(g_hModule, MAKEINTRESOURCE(IDD_DSN_CONFIG),
                                     hwndParent, DsnDlgProc, reinterpret_cast<LPARAM>(&data));
        if (ret != IDOK || !data.result_ok) {
            return FALSE;
        }
    } else {
        if (data.dsn_name.empty()) {
            SQLPostInstallerErrorW(ODBC_ERROR_INVALID_DSN, L"DSN name is required");
            return FALSE;
        }
    }

    return WriteDsnToRegistry(data);
}
