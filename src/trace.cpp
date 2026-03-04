#include "include/handle.h"
#include "include/trace.h"

#include <sql.h>
#include <sqlext.h>
#include <chrono>
#include <iomanip>
#include <sstream>

namespace clickhouse_odbc {

void TraceLog::SetTraceFile(const std::string &path) {
    std::lock_guard<std::mutex> lock(log_mutex_);
    if (trace_file_.is_open()) {
        trace_file_.close();
    }
    if (!path.empty()) {
        trace_file_.open(path, std::ios::out | std::ios::app);
    }
}

void TraceLog::CloseTraceFile() {
    std::lock_guard<std::mutex> lock(log_mutex_);
    if (trace_file_.is_open()) {
        trace_file_.close();
    }
}

void TraceLog::WriteOutput(const std::string &msg) {
    // Output to OutputDebugString
    OutputDebugStringA(msg.c_str());
    OutputDebugStringA("\n");

    // Output to file
    if (trace_file_.is_open()) {
        trace_file_ << msg << std::endl;
    }
}

std::string TraceLog::SqlReturnToString(SQLRETURN ret) {
    switch (ret) {
    case SQL_SUCCESS:           return "SQL_SUCCESS";
    case SQL_SUCCESS_WITH_INFO: return "SQL_SUCCESS_WITH_INFO";
    case SQL_ERROR:             return "SQL_ERROR";
    case SQL_INVALID_HANDLE:    return "SQL_INVALID_HANDLE";
    case SQL_NO_DATA:           return "SQL_NO_DATA";
    case SQL_NEED_DATA:         return "SQL_NEED_DATA";
    case SQL_STILL_EXECUTING:   return "SQL_STILL_EXECUTING";
    default:                    return "UNKNOWN(" + std::to_string(ret) + ")";
    }
}

void TraceLog::Log(TraceLevel level, const char *func_name, const std::string &message) {
    if (!enabled_ || level > level_) return;

    std::lock_guard<std::mutex> lock(log_mutex_);

    // Timestamp
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()) % 1000;

    struct tm local_time;
    localtime_s(&local_time, &time_t_now);

    std::ostringstream oss;
    oss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S")
        << '.' << std::setfill('0') << std::setw(3) << ms.count();

    // Level string
    const char *level_str = "???";
    switch (level) {
    case TraceLevel::Error:   level_str = "ERR"; break;
    case TraceLevel::Warning: level_str = "WRN"; break;
    case TraceLevel::Info:    level_str = "INF"; break;
    case TraceLevel::Debug:   level_str = "DBG"; break;
    case TraceLevel::Verbose: level_str = "VRB"; break;
    default: break;
    }

    oss << " [" << level_str << "] ";
    if (func_name) {
        oss << func_name << ": ";
    }
    oss << message;

    WriteOutput(oss.str());
}

void TraceLog::LogEntry(const char *func_name, const std::string &params) {
    if (!enabled_) return;

    std::string msg = "ENTER";
    if (!params.empty()) {
        msg += " (" + params + ")";
    }
    Log(TraceLevel::Debug, func_name, msg);
}

void TraceLog::LogExit(const char *func_name, SQLRETURN ret) {
    if (!enabled_) return;

    Log(TraceLevel::Debug, func_name, "EXIT -> " + SqlReturnToString(ret));
}

} // namespace clickhouse_odbc
