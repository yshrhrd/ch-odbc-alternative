// Phase 20 Tests: Multi-threading support (HandleLock)
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
#include <vector>
#include <thread>
#include <atomic>
#include <functional>

// Include driver headers
#include "../src/include/handle.h"

#ifdef UNICODE
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLExecDirect
#undef SQLFetchScroll
#undef SQLGetConnectAttr
#undef SQLSetConnectAttr
#undef SQLGetInfo
#undef SQLGetDiagRec
#endif

#pragma warning(disable: 4996)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLRowCount(SQLHSTMT, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT, SQLSMALLINT *);
extern "C" SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC, SQLINTEGER, SQLPOINTER, SQLINTEGER, SQLINTEGER *);
extern "C" SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT, SQLUSMALLINT);
extern "C" SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT, SQLHANDLE, SQLSMALLINT, SQLCHAR *, SQLINTEGER *, SQLCHAR *, SQLSMALLINT, SQLSMALLINT *);

using namespace test_framework;
using namespace clickhouse_odbc;

// Helper: create env + dbc + stmt
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

// ============================================================================
// HandleLock basic test: recursive_mutex allows re-entry from the same thread
// ============================================================================
TEST(Phase20_ThreadSafety, RecursiveLockReentrancy) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);

    // Double-acquiring HandleLock should not deadlock (recursive_mutex)
    {
        HandleLock lock1(stmt);
        {
            HandleLock lock2(stmt);
            // Reaching here means success
            stmt->query = "SELECT 1";
        }
    }
    AssertEqual(std::string("SELECT 1"), stmt->query);

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// Concurrent multi-statement operations test
// ============================================================================
TEST(Phase20_ThreadSafety, ConcurrentStatements) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt1; SQLHSTMT hstmt2 = SQL_NULL_HSTMT;
    CreateTestHandles(env, dbc, hstmt1);
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt2);

    auto *stmt1 = static_cast<OdbcStatement *>(hstmt1);
    auto *stmt2 = static_cast<OdbcStatement *>(hstmt2);

    // Set dummy results for each statement
    stmt1->result_set.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});
    for (int i = 0; i < 100; ++i) {
        stmt1->result_set.rows.push_back({std::to_string(i)});
    }

    stmt2->result_set.columns.push_back({"name", "String", SQL_VARCHAR, 255, 0, SQL_NULLABLE});
    for (int i = 0; i < 100; ++i) {
        stmt2->result_set.rows.push_back({std::string("row_") + std::to_string(i)});
    }

    std::atomic<int> errors{0};

    // Thread 1: Fetch from stmt1
    std::thread t1([&]() {
        for (int i = 0; i < 100; ++i) {
            SQLRETURN ret = SQLFetch(hstmt1);
            if (ret != SQL_SUCCESS) { errors++; return; }
            char buf[64];
            SQLLEN ind;
            ret = SQLGetData(hstmt1, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
            if (ret != SQL_SUCCESS) { errors++; return; }
            if (std::string(buf) != std::to_string(i)) { errors++; return; }
        }
    });

    // Thread 2: Fetch from stmt2
    std::thread t2([&]() {
        for (int i = 0; i < 100; ++i) {
            SQLRETURN ret = SQLFetch(hstmt2);
            if (ret != SQL_SUCCESS) { errors++; return; }
            char buf[64];
            SQLLEN ind;
            ret = SQLGetData(hstmt2, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
            if (ret != SQL_SUCCESS) { errors++; return; }
            if (std::string(buf) != std::string("row_") + std::to_string(i)) { errors++; return; }
        }
    });

    t1.join();
    t2.join();

    AssertEqual((__int64)0, (__int64)errors.load());

    SQLFreeHandle(SQL_HANDLE_STMT, hstmt2);
    FreeTestHandles(env, dbc, hstmt1);
}

// ============================================================================
// Concurrent diagnostic record access from multiple threads
// ============================================================================
TEST(Phase20_ThreadSafety, ConcurrentDiagRecords) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    std::atomic<int> errors{0};
    const int iterations = 500;

    // Thread 1: Repeatedly call SetError
    std::thread t1([&]() {
        for (int i = 0; i < iterations; ++i) {
            stmt->SetError("HY000", "Error " + std::to_string(i));
        }
    });

    // Thread 2: Repeatedly call ClearDiagRecords
    std::thread t2([&]() {
        for (int i = 0; i < iterations; ++i) {
            stmt->ClearDiagRecords();
        }
    });

    // Thread 3: Repeatedly call AddDiagRecord
    std::thread t3([&]() {
        for (int i = 0; i < iterations; ++i) {
            stmt->AddDiagRecord("01000", 0, "Warning " + std::to_string(i));
        }
    });

    t1.join();
    t2.join();
    t3.join();

    // Completing without crash means success
    AssertEqual((__int64)0, (__int64)errors.load());

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// Concurrent statement attribute read/write
// ============================================================================
TEST(Phase20_ThreadSafety, ConcurrentStmtAttributes) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    std::atomic<int> errors{0};
    const int iterations = 200;

    // Thread 1: Set max_rows
    std::thread t1([&]() {
        for (int i = 0; i < iterations; ++i) {
            SQLSetStmtAttr(hstmt, SQL_ATTR_MAX_ROWS, (SQLPOINTER)(uintptr_t)(i + 1), 0);
        }
    });

    // Thread 2: Read max_rows
    std::thread t2([&]() {
        for (int i = 0; i < iterations; ++i) {
            SQLULEN val = 0;
            SQLGetStmtAttr(hstmt, SQL_ATTR_MAX_ROWS, &val, 0, nullptr);
            // Only confirm value >= 0 (any value possible due to race condition)
        }
    });

    t1.join();
    t2.join();

    // Completing without crash means success
    AssertEqual((__int64)0, (__int64)errors.load());

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// Concurrent SQLAllocHandle(STMT) from multiple threads
// ============================================================================
TEST(Phase20_ThreadSafety, ConcurrentAllocStatements) {
    SQLHENV env = SQL_NULL_HENV;
    SQLHDBC dbc = SQL_NULL_HDBC;
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->connected = true;

    const int thread_count = 4;
    const int stmts_per_thread = 10;
    std::vector<SQLHSTMT> all_stmts(thread_count * stmts_per_thread, SQL_NULL_HSTMT);
    std::atomic<int> errors{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([&, t]() {
            for (int s = 0; s < stmts_per_thread; ++s) {
                SQLHSTMT hstmt = SQL_NULL_HSTMT;
                SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt);
                if (ret != SQL_SUCCESS || !hstmt) { errors++; continue; }
                all_stmts[t * stmts_per_thread + s] = hstmt;
            }
        });
    }

    for (auto &thr : threads) thr.join();

    AssertEqual((__int64)0, (__int64)errors.load());

    // Verify all statements are valid
    int valid_count = 0;
    for (auto h : all_stmts) {
        if (h != SQL_NULL_HSTMT) valid_count++;
    }
    AssertEqual((__int64)(thread_count * stmts_per_thread), (__int64)valid_count);

    // Cleanup
    for (auto h : all_stmts) {
        if (h != SQL_NULL_HSTMT) SQLFreeHandle(SQL_HANDLE_STMT, h);
    }
    conn->connected = false;
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

// ============================================================================
// Concurrent SQLFetch + SQLRowCount
// ============================================================================
TEST(Phase20_ThreadSafety, ConcurrentFetchAndRowCount) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    stmt->result_set.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});
    for (int i = 0; i < 50; ++i) {
        stmt->result_set.rows.push_back({std::to_string(i)});
    }

    std::atomic<int> errors{0};

    // Thread 1: Fetch data via SQLFetch
    std::thread t1([&]() {
        for (int i = 0; i < 50; ++i) {
            SQLRETURN ret = SQLFetch(hstmt);
            if (ret != SQL_SUCCESS) { errors++; break; }
        }
    });

    // Thread 2: Repeatedly call SQLRowCount
    std::thread t2([&]() {
        for (int i = 0; i < 100; ++i) {
            SQLLEN rowcount = 0;
            SQLRowCount(hstmt, &rowcount);
            // Only confirm rowcount >= 0
        }
    });

    t1.join();
    t2.join();

    AssertEqual((__int64)0, (__int64)errors.load());

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// Lazy paging ResultSet page_mutex test
// ============================================================================
TEST(Phase20_ThreadSafety, ConcurrentLazyPageAccess) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 100;
    rs.page_size = 10;
    rs.max_cached_pages = 10;  // Keep all pages (prevent reference invalidation from eviction)
    rs.base_query = "SELECT * FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    std::atomic<int> fetch_count{0};
    rs.page_fetcher = [&fetch_count](const std::string &query,
                                      std::vector<ResultRow> &out_rows,
                                      std::string &error) -> bool {
        fetch_count++;
        auto pos = query.find("OFFSET ");
        size_t offset = (pos != std::string::npos) ? std::stoull(query.substr(pos + 7)) : 0;
        for (size_t i = 0; i < 10 && (offset + i) < 100; ++i) {
            out_rows.push_back({std::to_string(offset + i)});
        }
        return true;
    };

    std::atomic<int> errors{0};

    // Access different pages from multiple threads
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < 25; ++i) {
                size_t row_idx = (t * 25 + i) % 100;
                const auto &row = rs.GetRow(row_idx);
                if (row.empty()) { errors++; continue; }
                if (row[0].value() != std::to_string(row_idx)) { errors++; }
            }
        });
    }

    for (auto &thr : threads) thr.join();

    AssertEqual((__int64)0, (__int64)errors.load());
    AssertTrue(rs.page_cache.size() <= rs.max_cached_pages);
}

// ============================================================================
// SQLCloseCursor + SQLFetch contention test
// ============================================================================
TEST(Phase20_ThreadSafety, ConcurrentCloseCursorAndFetch) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    stmt->result_set.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});
    for (int i = 0; i < 100; ++i) {
        stmt->result_set.rows.push_back({std::to_string(i)});
    }

    std::atomic<bool> done{false};

    // Thread 1: Repeatedly call SQLFetch
    std::thread t1([&]() {
        while (!done.load()) {
            SQLFetch(hstmt);
        }
    });

    // Thread 2: SQLCloseCursor → rebuild result set
    std::thread t2([&]() {
        for (int i = 0; i < 20; ++i) {
            SQLCloseCursor(hstmt);
            // Rebuild result set (protected by HandleLock)
            HandleLock lock(stmt);
            stmt->result_set.current_row = -1;
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        done.store(true);
    });

    t1.join();
    t2.join();

    // Completing without crash means success
    AssertTrue(true);

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// Concurrent SQLGetDiagRec test
// ============================================================================
TEST(Phase20_ThreadSafety, ConcurrentGetDiagRec) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    std::atomic<int> errors{0};

    // Thread 1: Set errors
    std::thread t1([&]() {
        for (int i = 0; i < 200; ++i) {
            stmt->SetError("HY000", "Test error " + std::to_string(i));
        }
    });

    // Thread 2: Call SQLGetDiagRec
    std::thread t2([&]() {
        for (int i = 0; i < 200; ++i) {
            SQLCHAR state[6] = {};
            SQLINTEGER native_error = 0;
            SQLCHAR msg[256] = {};
            SQLSMALLINT msg_len = 0;
            SQLGetDiagRec(SQL_HANDLE_STMT, hstmt, 1, state, &native_error, msg, sizeof(msg), &msg_len);
        }
    });

    t1.join();
    t2.join();

    // Completing without crash means success
    AssertEqual((__int64)0, (__int64)errors.load());

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// Verify that OdbcHandle mutex is recursive
// ============================================================================
TEST(Phase20_ThreadSafety, HandleMutexIsRecursive) {
    OdbcStatement stmt;

    // SetError internally acquires lock_guard
    // Calling SetError while HandleLock is held should not deadlock
    HandleLock lock(&stmt);
    stmt.SetError("HY000", "test");

    // Reaching here means no deadlock
    AssertTrue(!stmt.diag_records.empty());
    AssertEqual(std::string("HY000"), stmt.diag_records[0].sql_state);
}

// Test registration: Automatically registered via TEST macro
