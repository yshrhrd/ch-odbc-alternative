#pragma once

#include "handle.h"
#include <string>
#include <fstream>
#include <mutex>

namespace clickhouse_odbc {

// Trace log level
enum class TraceLevel {
    Off = 0,
    Error = 1,
    Warning = 2,
    Info = 3,
    Debug = 4,
    Verbose = 5
};

// ODBC driver trace log
class TraceLog {
public:
    static TraceLog &Instance() {
        static TraceLog instance;
        return instance;
    }

    // Enable/disable tracing
    void SetEnabled(bool enabled) { enabled_ = enabled; }
    bool IsEnabled() const { return enabled_; }

    // Set trace level
    void SetLevel(TraceLevel level) { level_ = level; }
    TraceLevel GetLevel() const { return level_; }

    // Set trace file (empty string for OutputDebugString only)
    void SetTraceFile(const std::string &path);
    void CloseTraceFile();

    // Log output
    void Log(TraceLevel level, const char *func_name, const std::string &message);
    void LogEntry(const char *func_name, const std::string &params = "");
    void LogExit(const char *func_name, SQLRETURN ret);

    // Convert SQLRETURN to string
    static std::string SqlReturnToString(SQLRETURN ret);

private:
    TraceLog() = default;
    ~TraceLog() { CloseTraceFile(); }

    TraceLog(const TraceLog &) = delete;
    TraceLog &operator=(const TraceLog &) = delete;

    void WriteOutput(const std::string &msg);

    bool enabled_ = false;
    TraceLevel level_ = TraceLevel::Info;
    std::mutex log_mutex_;
    std::ofstream trace_file_;
};

// Macro: conditional trace logging
#define TRACE_LOG(level, func, msg) \
    do { \
        if (clickhouse_odbc::TraceLog::Instance().IsEnabled()) { \
            clickhouse_odbc::TraceLog::Instance().Log(level, func, msg); \
        } \
    } while (0)

#define TRACE_ENTRY(func, params) \
    do { \
        if (clickhouse_odbc::TraceLog::Instance().IsEnabled()) { \
            clickhouse_odbc::TraceLog::Instance().LogEntry(func, params); \
        } \
    } while (0)

#define TRACE_EXIT(func, ret) \
    do { \
        if (clickhouse_odbc::TraceLog::Instance().IsEnabled()) { \
            clickhouse_odbc::TraceLog::Instance().LogExit(func, ret); \
        } \
    } while (0)

} // namespace clickhouse_odbc
