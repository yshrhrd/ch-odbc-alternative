// CH ODBC Alternative - GUI Installer
// Copies DLL, registers ODBC driver, optionally creates DSN.
// Requires Administrator privileges for HKLM registry writes.

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <shellapi.h>
#include <commctrl.h>
#include <shlobj.h>
#include <shlwapi.h>
#include <odbcinst.h>

// Force ANSI versions of ODBC installer APIs
#ifdef UNICODE
#undef SQLInstallDriverEx
#undef SQLRemoveDriver
#undef SQLWriteDSNToIni
#undef SQLRemoveDSNFromIni
#undef SQLWritePrivateProfileString
#undef SQLGetPrivateProfileString
#endif

#include <string>
#include <vector>

#include "resource.h"

#pragma comment(lib, "comctl32.lib")
#pragma comment(lib, "shlwapi.lib")
#pragma comment(lib, "shell32.lib")
#pragma comment(lib, "odbccp32.lib")
#pragma comment(linker, "/manifestdependency:\"type='win32' name='Microsoft.Windows.Common-Controls' version='6.0.0.0' processorArchitecture='*' publicKeyToken='6595b64144ccf1df' language='*'\"")

static HINSTANCE g_hInst = nullptr;

static const char *DRIVER_NAME = "CH ODBC Alternative";
static const char *DLL_FILENAME = "ch-odbc-alternative.dll";

// Resolve default install directory under %LOCALAPPDATA%
static std::string GetDefaultInstallDir() {
    char buf[MAX_PATH] = {};
    if (SUCCEEDED(SHGetFolderPathA(nullptr, CSIDL_LOCAL_APPDATA, nullptr, 0, buf))) {
        return std::string(buf) + "\\CH ODBC Alternative";
    }
    return "C:\\Users\\Default\\AppData\\Local\\CH ODBC Alternative";
}

// ============================================================================
// Helpers
// ============================================================================
static std::string GetDlgItemStr(HWND hDlg, int id) {
    char buf[MAX_PATH] = {};
    ::GetDlgItemTextA(hDlg, id, buf, sizeof(buf));
    return std::string(buf);
}

static void SetStatus(HWND hDlg, const char *msg) {
    ::SetDlgItemTextA(hDlg, IDC_STATIC_STATUS, msg);
    // Force immediate repaint
    HWND hCtrl = GetDlgItem(hDlg, IDC_STATIC_STATUS);
    if (hCtrl) {
        InvalidateRect(hCtrl, nullptr, TRUE);
        UpdateWindow(hCtrl);
    }
}

static void SetProgress(HWND hDlg, int pos) {
    SendDlgItemMessage(hDlg, IDC_PROGRESS, PBM_SETPOS, pos, 0);
}

static void EnableDsnControls(HWND hDlg, BOOL enable) {
    EnableWindow(GetDlgItem(hDlg, IDC_EDIT_DSN_NAME), enable);
    EnableWindow(GetDlgItem(hDlg, IDC_EDIT_HOST), enable);
    EnableWindow(GetDlgItem(hDlg, IDC_EDIT_PORT), enable);
    EnableWindow(GetDlgItem(hDlg, IDC_EDIT_DATABASE), enable);
    EnableWindow(GetDlgItem(hDlg, IDC_EDIT_UID), enable);
    EnableWindow(GetDlgItem(hDlg, IDC_EDIT_PWD), enable);
}

static void EnableActionButtons(HWND hDlg, BOOL enable) {
    EnableWindow(GetDlgItem(hDlg, IDC_BTN_INSTALL), enable);
    EnableWindow(GetDlgItem(hDlg, IDC_BTN_UNINSTALL), enable);
}

// Find DLL source: look next to the installer exe
static std::string FindSourceDll() {
    char exe_path[MAX_PATH] = {};
    GetModuleFileNameA(nullptr, exe_path, MAX_PATH);
    PathRemoveFileSpecA(exe_path);

    std::string dll_path = std::string(exe_path) + "\\" + DLL_FILENAME;
    if (GetFileAttributesA(dll_path.c_str()) != INVALID_FILE_ATTRIBUTES) {
        return dll_path;
    }

    // Also try parent directory (for dev builds: out\Release\installer.exe, dll in out\Release\)
    PathRemoveFileSpecA(exe_path);
    dll_path = std::string(exe_path) + "\\" + DLL_FILENAME;
    if (GetFileAttributesA(dll_path.c_str()) != INVALID_FILE_ATTRIBUTES) {
        return dll_path;
    }

    return "";
}

// ============================================================================
// Install
// ============================================================================
static bool DoInstall(HWND hDlg) {
    std::string install_dir = GetDlgItemStr(hDlg, IDC_EDIT_INSTALLDIR);
    if (install_dir.empty()) {
        MessageBoxA(hDlg, "Please specify an install directory.", "Error", MB_OK | MB_ICONWARNING);
        return false;
    }

    // Remove trailing backslash
    while (!install_dir.empty() && (install_dir.back() == '\\' || install_dir.back() == '/')) {
        install_dir.pop_back();
    }

    EnableActionButtons(hDlg, FALSE);
    SendDlgItemMessage(hDlg, IDC_PROGRESS, PBM_SETRANGE, 0, MAKELPARAM(0, 100));
    SetProgress(hDlg, 0);

    // Step 1: Find source DLL
    SetStatus(hDlg, "Locating driver DLL...");
    SetProgress(hDlg, 10);

    std::string source_dll = FindSourceDll();
    if (source_dll.empty()) {
        MessageBoxA(hDlg, "Could not find ch-odbc-alternative.dll.\n\n"
                          "Place the installer in the same directory as the DLL.",
                    "Error", MB_OK | MB_ICONERROR);
        EnableActionButtons(hDlg, TRUE);
        SetStatus(hDlg, "Install failed.");
        SetProgress(hDlg, 0);
        return false;
    }

    // Step 2: Create install directory
    SetStatus(hDlg, "Creating install directory...");
    SetProgress(hDlg, 20);

    // SHCreateDirectoryExA creates intermediate directories
    int shr = SHCreateDirectoryExA(hDlg, install_dir.c_str(), nullptr);
    if (shr != ERROR_SUCCESS && shr != ERROR_ALREADY_EXISTS && shr != ERROR_FILE_EXISTS) {
        std::string msg = "Failed to create directory:\n" + install_dir +
                          "\n\nError code: " + std::to_string(shr);
        MessageBoxA(hDlg, msg.c_str(), "Error", MB_OK | MB_ICONERROR);
        EnableActionButtons(hDlg, TRUE);
        SetStatus(hDlg, "Install failed.");
        SetProgress(hDlg, 0);
        return false;
    }

    // Step 3: Copy DLL
    SetStatus(hDlg, "Copying driver DLL...");
    SetProgress(hDlg, 40);

    std::string dest_dll = install_dir + "\\" + DLL_FILENAME;
    if (!CopyFileA(source_dll.c_str(), dest_dll.c_str(), FALSE)) {
        DWORD err = GetLastError();
        std::string msg = "Failed to copy DLL.\n\nSource: " + source_dll +
                          "\nDest: " + dest_dll +
                          "\nError: " + std::to_string(err);
        MessageBoxA(hDlg, msg.c_str(), "Error", MB_OK | MB_ICONERROR);
        EnableActionButtons(hDlg, TRUE);
        SetStatus(hDlg, "Install failed.");
        SetProgress(hDlg, 0);
        return false;
    }

    // Step 4: Register ODBC driver
    SetStatus(hDlg, "Registering ODBC driver...");
    SetProgress(hDlg, 60);

    // Use ODBC Installer API: SQLInstallDriverEx
    // First parameter: "DriverName\0key=value\0key=value\0\0" (double-null-terminated)
    std::string driver_spec;
    driver_spec += DRIVER_NAME;
    driver_spec.push_back('\0');
    driver_spec += std::string("Driver=") + dest_dll;
    driver_spec.push_back('\0');
    driver_spec += std::string("Setup=") + dest_dll;
    driver_spec.push_back('\0');
    driver_spec += "APILevel=1";
    driver_spec.push_back('\0');
    driver_spec += "ConnectFunctions=YYY";
    driver_spec.push_back('\0');
    driver_spec += "DriverODBCVer=03.80";
    driver_spec.push_back('\0');
    driver_spec += "FileUsage=0";
    driver_spec.push_back('\0');
    driver_spec += "SQLLevel=1";
    driver_spec.push_back('\0');
    driver_spec.push_back('\0');

    char installed_path[MAX_PATH] = {};
    WORD path_len = 0;
    DWORD usage_count = 0;
    BOOL api_ok = SQLInstallDriverEx(driver_spec.c_str(), install_dir.c_str(),
                            installed_path, MAX_PATH, &path_len,
                            ODBC_INSTALL_COMPLETE, &usage_count);
    if (!api_ok) {
        // Fallback: register via registry directly
        HKEY hKey = nullptr;
        LONG rc;
        std::string key_path = "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers";
        rc = RegCreateKeyExA(HKEY_LOCAL_MACHINE, key_path.c_str(), 0, nullptr,
                        0, KEY_SET_VALUE, nullptr, &hKey, nullptr);
        if (rc != ERROR_SUCCESS) {
            std::string msg = "Failed to register driver.\n\n"
                              "Registry write to HKLM failed (error " + std::to_string(rc) + ").\n"
                              "Please run as administrator.";
            MessageBoxA(hDlg, msg.c_str(), "Error", MB_OK | MB_ICONERROR);
            EnableActionButtons(hDlg, TRUE);
            SetStatus(hDlg, "Install failed.");
            SetProgress(hDlg, 0);
            return false;
        }
        RegSetValueExA(hKey, DRIVER_NAME, 0, REG_SZ,
                       (const BYTE *)"Installed", 10);
        RegCloseKey(hKey);

        key_path = std::string("SOFTWARE\\ODBC\\ODBCINST.INI\\") + DRIVER_NAME;
        RegCreateKeyExA(HKEY_LOCAL_MACHINE, key_path.c_str(), 0, nullptr,
                        0, KEY_SET_VALUE, nullptr, &hKey, nullptr);
        RegSetValueExA(hKey, "Driver", 0, REG_SZ,
                       (const BYTE *)dest_dll.c_str(), (DWORD)(dest_dll.size() + 1));
        RegSetValueExA(hKey, "Setup", 0, REG_SZ,
                       (const BYTE *)dest_dll.c_str(), (DWORD)(dest_dll.size() + 1));
        RegSetValueExA(hKey, "APILevel", 0, REG_SZ, (const BYTE *)"1", 2);
        RegSetValueExA(hKey, "ConnectFunctions", 0, REG_SZ, (const BYTE *)"YYY", 4);
        RegSetValueExA(hKey, "DriverODBCVer", 0, REG_SZ, (const BYTE *)"03.80", 6);
        RegSetValueExA(hKey, "FileUsage", 0, REG_SZ, (const BYTE *)"0", 2);
        RegSetValueExA(hKey, "SQLLevel", 0, REG_SZ, (const BYTE *)"1", 2);
        RegCloseKey(hKey);
    }

    // Step 5: Save install path for uninstall
    {
        HKEY hKey;
        std::string unreg_key = "SOFTWARE\\CH ODBC Alternative";
        RegCreateKeyExA(HKEY_LOCAL_MACHINE, unreg_key.c_str(), 0, nullptr,
                        0, KEY_SET_VALUE, nullptr, &hKey, nullptr);
        RegSetValueExA(hKey, "InstallDir", 0, REG_SZ,
                       (const BYTE *)install_dir.c_str(), (DWORD)(install_dir.size() + 1));
        RegCloseKey(hKey);
    }

    SetProgress(hDlg, 80);

    // Step 6: Create DSN (optional)
    bool create_dsn = (IsDlgButtonChecked(hDlg, IDC_CHECK_CREATE_DSN) == BST_CHECKED);
    if (create_dsn) {
        SetStatus(hDlg, "Creating DSN...");

        std::string dsn_name = GetDlgItemStr(hDlg, IDC_EDIT_DSN_NAME);
        std::string host = GetDlgItemStr(hDlg, IDC_EDIT_HOST);
        std::string port = GetDlgItemStr(hDlg, IDC_EDIT_PORT);
        std::string database = GetDlgItemStr(hDlg, IDC_EDIT_DATABASE);
        std::string uid = GetDlgItemStr(hDlg, IDC_EDIT_UID);
        std::string pwd = GetDlgItemStr(hDlg, IDC_EDIT_PWD);

        if (!dsn_name.empty()) {
            // Write DSN via ODBC Installer API
            SQLWriteDSNToIni(dsn_name.c_str(), DRIVER_NAME);
            SQLWritePrivateProfileString(dsn_name.c_str(), "Host",
                                         host.empty() ? "localhost" : host.c_str(), "ODBC.INI");
            SQLWritePrivateProfileString(dsn_name.c_str(), "Port",
                                         port.empty() ? "8123" : port.c_str(), "ODBC.INI");
            SQLWritePrivateProfileString(dsn_name.c_str(), "Database",
                                         database.empty() ? "default" : database.c_str(), "ODBC.INI");
            SQLWritePrivateProfileString(dsn_name.c_str(), "UID",
                                         uid.empty() ? "default" : uid.c_str(), "ODBC.INI");
            SQLWritePrivateProfileString(dsn_name.c_str(), "PWD", pwd.c_str(), "ODBC.INI");
            SQLWritePrivateProfileString(dsn_name.c_str(), "Compression", "1", "ODBC.INI");
            SQLWritePrivateProfileString(dsn_name.c_str(), "LazyPaging", "1", "ODBC.INI");
            SQLWritePrivateProfileString(dsn_name.c_str(), "PageSize", "10000", "ODBC.INI");
        }
    }

    SetProgress(hDlg, 100);
    SetStatus(hDlg, "Installation completed successfully.");
    EnableActionButtons(hDlg, TRUE);

    std::string success_msg = "CH ODBC Alternative installed successfully.\n\n"
                              "Install directory: " + install_dir + "\n\n"
                              "You can now configure DSN via odbcad32.exe\n"
                              "or use a connection string:\n"
                              "Driver={CH ODBC Alternative};Host=...;Port=8123;";
    if (create_dsn) {
        std::string dsn_name = GetDlgItemStr(hDlg, IDC_EDIT_DSN_NAME);
        if (!dsn_name.empty()) {
            success_msg += "\n\nDSN \"" + dsn_name + "\" created.";
        }
    }
    MessageBoxA(hDlg, success_msg.c_str(), "Success", MB_OK | MB_ICONINFORMATION);
    return true;
}

// ============================================================================
// Uninstall
// ============================================================================
static bool DoUninstall(HWND hDlg) {
    int ret = MessageBoxA(hDlg,
        "This will unregister the CH ODBC Alternative\n"
        "and remove the driver DLL.\n\n"
        "DSN entries will NOT be removed.\n\n"
        "Continue?",
        "Uninstall", MB_YESNO | MB_ICONQUESTION);
    if (ret != IDYES) return false;

    EnableActionButtons(hDlg, FALSE);
    SendDlgItemMessage(hDlg, IDC_PROGRESS, PBM_SETRANGE, 0, MAKELPARAM(0, 100));
    SetProgress(hDlg, 0);

    // Step 1: Read install directory from registry
    SetStatus(hDlg, "Reading install information...");
    SetProgress(hDlg, 10);

    std::string install_dir;
    {
        HKEY hKey;
        if (RegOpenKeyExA(HKEY_LOCAL_MACHINE, "SOFTWARE\\CH ODBC Alternative",
                          0, KEY_READ, &hKey) == ERROR_SUCCESS) {
            char buf[MAX_PATH] = {};
            DWORD buf_size = sizeof(buf);
            DWORD type = 0;
            if (RegQueryValueExA(hKey, "InstallDir", nullptr, &type,
                                 (BYTE *)buf, &buf_size) == ERROR_SUCCESS) {
                install_dir = buf;
            }
            RegCloseKey(hKey);
        }
    }

    // Step 2: Unregister driver
    SetStatus(hDlg, "Unregistering ODBC driver...");
    SetProgress(hDlg, 30);

    DWORD usage_count = 0;
    if (!SQLRemoveDriver(DRIVER_NAME, FALSE, &usage_count)) {
        // Fallback: direct registry cleanup
        RegDeleteKeyA(HKEY_LOCAL_MACHINE,
                      "SOFTWARE\\ODBC\\ODBCINST.INI\\CH ODBC Alternative");
        HKEY hKey;
        if (RegOpenKeyExA(HKEY_LOCAL_MACHINE,
                          "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers",
                          0, KEY_SET_VALUE, &hKey) == ERROR_SUCCESS) {
            RegDeleteValueA(hKey, DRIVER_NAME);
            RegCloseKey(hKey);
        }
    }

    SetProgress(hDlg, 60);

    // Step 3: Delete DLL
    SetStatus(hDlg, "Removing driver files...");
    if (!install_dir.empty()) {
        std::string dll_path = install_dir + "\\" + DLL_FILENAME;
        DeleteFileA(dll_path.c_str());
        // Try to remove directory (only succeeds if empty)
        RemoveDirectoryA(install_dir.c_str());
    }

    // Step 4: Remove install registry key
    RegDeleteKeyA(HKEY_LOCAL_MACHINE, "SOFTWARE\\CH ODBC Alternative");

    SetProgress(hDlg, 100);
    SetStatus(hDlg, "Uninstall completed.");
    EnableActionButtons(hDlg, TRUE);

    MessageBoxA(hDlg,
        "CH ODBC Alternative has been unregistered.\n\n"
        "Note: DSN entries were not removed.\n"
        "Use odbcad32.exe to remove them manually.",
        "Uninstall Complete", MB_OK | MB_ICONINFORMATION);
    return true;
}

// ============================================================================
// Browse folder
// ============================================================================
static void BrowseFolder(HWND hDlg) {
    BROWSEINFOA bi = {};
    bi.hwndOwner = hDlg;
    bi.lpszTitle = "Select install directory:";
    bi.ulFlags = BIF_RETURNONLYFSDIRS | BIF_NEWDIALOGSTYLE | BIF_USENEWUI;

    LPITEMIDLIST pidl = SHBrowseForFolderA(&bi);
    if (pidl) {
        char path[MAX_PATH] = {};
        if (SHGetPathFromIDListA(pidl, path)) {
            // Append driver folder name
            std::string full_path = std::string(path) + "\\CH ODBC Alternative";
            ::SetDlgItemTextA(hDlg, IDC_EDIT_INSTALLDIR, full_path.c_str());
        }
        CoTaskMemFree(pidl);
    }
}

// ============================================================================
// Dialog procedure
// ============================================================================
static INT_PTR CALLBACK InstallerDlgProc(HWND hDlg, UINT msg, WPARAM wParam, LPARAM lParam) {
    switch (msg) {
    case WM_INITDIALOG: {
        // Set icon (use system icon)
        SendMessage(hDlg, WM_SETICON, ICON_BIG,
                    (LPARAM)LoadIcon(nullptr, IDI_APPLICATION));

        // Default values
        std::string default_dir = GetDefaultInstallDir();
        ::SetDlgItemTextA(hDlg, IDC_EDIT_INSTALLDIR, default_dir.c_str());
        ::SetDlgItemTextA(hDlg, IDC_EDIT_DSN_NAME, "ClickHouse");
        ::SetDlgItemTextA(hDlg, IDC_EDIT_HOST, "localhost");
        ::SetDlgItemTextA(hDlg, IDC_EDIT_PORT, "8123");
        ::SetDlgItemTextA(hDlg, IDC_EDIT_DATABASE, "default");
        ::SetDlgItemTextA(hDlg, IDC_EDIT_UID, "default");
        ::SetDlgItemTextA(hDlg, IDC_EDIT_PWD, "");

        // DSN controls disabled by default
        CheckDlgButton(hDlg, IDC_CHECK_CREATE_DSN, BST_UNCHECKED);
        EnableDsnControls(hDlg, FALSE);

        // Progress bar
        SendDlgItemMessage(hDlg, IDC_PROGRESS, PBM_SETRANGE, 0, MAKELPARAM(0, 100));
        SendDlgItemMessage(hDlg, IDC_PROGRESS, PBM_SETPOS, 0, 0);

        // Check if source DLL exists
        std::string src = FindSourceDll();
        if (src.empty()) {
            SetStatus(hDlg, "Warning: ch-odbc-alternative.dll not found next to installer.");
        } else {
            std::string status_msg = "Ready. DLL found: " + src;
            // Truncate if too long
            if (status_msg.size() > 80) {
                status_msg = "Ready. DLL found.";
            }
            SetStatus(hDlg, status_msg.c_str());
        }

        return TRUE;
    }

    case WM_COMMAND:
        switch (LOWORD(wParam)) {
        case IDC_CHECK_CREATE_DSN:
            EnableDsnControls(hDlg,
                (IsDlgButtonChecked(hDlg, IDC_CHECK_CREATE_DSN) == BST_CHECKED) ? TRUE : FALSE);
            return TRUE;

        case IDC_BTN_BROWSE:
            BrowseFolder(hDlg);
            return TRUE;

        case IDC_BTN_INSTALL:
            DoInstall(hDlg);
            return TRUE;

        case IDC_BTN_UNINSTALL:
            DoUninstall(hDlg);
            return TRUE;

        case IDCANCEL:
            EndDialog(hDlg, IDCANCEL);
            return TRUE;
        }
        break;

    case WM_CLOSE:
        EndDialog(hDlg, IDCANCEL);
        return TRUE;
    }
    return FALSE;
}

// ============================================================================
// Check if running as administrator
// ============================================================================
static bool IsRunAsAdmin() {
    BOOL isAdmin = FALSE;
    PSID adminGroup = nullptr;
    SID_IDENTIFIER_AUTHORITY ntAuthority = SECURITY_NT_AUTHORITY;
    if (AllocateAndInitializeSid(&ntAuthority, 2,
            SECURITY_BUILTIN_DOMAIN_RID, DOMAIN_ALIAS_RID_ADMINS,
            0, 0, 0, 0, 0, 0, &adminGroup)) {
        CheckTokenMembership(nullptr, adminGroup, &isAdmin);
        FreeSid(adminGroup);
    }
    return isAdmin != FALSE;
}

// Re-launch self with UAC elevation via ShellExecuteEx
static bool RelaunchElevated() {
    char exe_path[MAX_PATH] = {};
    GetModuleFileNameA(nullptr, exe_path, MAX_PATH);

    SHELLEXECUTEINFOA sei = {};
    sei.cbSize = sizeof(sei);
    sei.lpVerb = "runas";
    sei.lpFile = exe_path;
    sei.nShow = SW_SHOWNORMAL;
    sei.fMask = SEE_MASK_NOCLOSEPROCESS;
    if (ShellExecuteExA(&sei)) {
        if (sei.hProcess) {
            WaitForSingleObject(sei.hProcess, INFINITE);
            CloseHandle(sei.hProcess);
        }
        return true;
    }
    return false;
}

// ============================================================================
// Entry point
// ============================================================================
int WINAPI WinMain(HINSTANCE hInstance, HINSTANCE, LPSTR, int) {
    g_hInst = hInstance;

    // Ensure administrator privileges (fallback if manifest elevation failed)
    if (!IsRunAsAdmin()) {
        if (RelaunchElevated()) {
            return 0;  // Elevated child process launched; exit this instance
        }
        // User declined UAC or error; show message and exit
        MessageBoxA(nullptr,
            "This installer requires administrator privileges.\n\n"
            "Please right-click the installer and select\n"
            "\"Run as administrator\".",
            "CH ODBC Alternative Installer", MB_OK | MB_ICONWARNING);
        return 1;
    }

    // Initialize common controls (for progress bar)
    INITCOMMONCONTROLSEX icc = {};
    icc.dwSize = sizeof(icc);
    icc.dwICC = ICC_PROGRESS_CLASS;
    InitCommonControlsEx(&icc);

    CoInitializeEx(nullptr, COINIT_APARTMENTTHREADED);

    DialogBoxParam(hInstance, MAKEINTRESOURCE(IDD_INSTALLER), nullptr,
                   InstallerDlgProc, 0);

    CoUninitialize();
    return 0;
}
