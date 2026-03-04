#include "include/handle.h"
#include "include/trace.h"
#include <cstdlib>

// DLL instance handle (for ConfigDSN dialog resource)
HINSTANCE g_hModule = nullptr;

// Auto-enable tracing via CLICKHOUSE_ODBC_TRACE environment variable at DLL load time
// Value is the trace log file path. "1" uses the default path.
static void InitTraceFromEnv() {
    char buf[MAX_PATH] = {};
    size_t len = 0;
    if (getenv_s(&len, buf, sizeof(buf), "CLICKHOUSE_ODBC_TRACE") != 0 || len == 0) {
        return;
    }
    std::string val(buf);
    if (val.empty()) return;

    auto &trace = clickhouse_odbc::TraceLog::Instance();
    trace.SetEnabled(true);
    trace.SetLevel(clickhouse_odbc::TraceLevel::Verbose);

    if (val == "1" || val == "debug") {
        // OutputDebugString only (viewable with DebugView)
        trace.Log(clickhouse_odbc::TraceLevel::Info, "DllMain",
                  "Trace enabled (OutputDebugString only). Set CLICKHOUSE_ODBC_TRACE=<filepath> for file output.");
    } else {
        // File output
        trace.SetTraceFile(val);
        trace.Log(clickhouse_odbc::TraceLevel::Info, "DllMain",
                  "Trace enabled. Log file: " + val);
    }
}

BOOL APIENTRY DllMain(HMODULE hModule, DWORD ul_reason_for_call, LPVOID lpReserved) {
    switch (ul_reason_for_call) {
    case DLL_PROCESS_ATTACH:
        g_hModule = hModule;
        DisableThreadLibraryCalls(hModule);
        InitTraceFromEnv();
        break;
    case DLL_THREAD_ATTACH:
    case DLL_THREAD_DETACH:
    case DLL_PROCESS_DETACH:
        break;
    }
    return TRUE;
}
