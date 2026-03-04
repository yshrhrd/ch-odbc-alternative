// Phase 19 Tests: Lazy paging ResultSet
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
#include <functional>

// Include driver headers
#include "../src/include/handle.h"
#include "../src/include/clickhouse_client.h"

#ifdef UNICODE
#undef SQLGetStmtAttr
#undef SQLSetStmtAttr
#undef SQLExecDirect
#undef SQLFetchScroll
#endif

#pragma warning(disable: 4996)

// Forward declarations
extern "C" SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT, SQLHANDLE, SQLHANDLE *);
extern "C" SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT, SQLHANDLE);
extern "C" SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV, SQLINTEGER, SQLPOINTER, SQLINTEGER);
extern "C" SQLRETURN SQL_API SQLFetch(SQLHSTMT);
extern "C" SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT, SQLSMALLINT, SQLLEN);
extern "C" SQLRETURN SQL_API SQLGetData(SQLHSTMT, SQLUSMALLINT, SQLSMALLINT, SQLPOINTER, SQLLEN, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLRowCount(SQLHSTMT, SQLLEN *);
extern "C" SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT);

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
// ResultSet RowCount() test
// ============================================================================
TEST(Phase19_LazyPaging, RowCountNormalMode) {
    ResultSet rs;
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});
    rs.rows.push_back({std::string("1")});
    rs.rows.push_back({std::string("2")});
    rs.rows.push_back({std::string("3")});

    AssertEqual((__int64)3, (__int64)rs.RowCount());
    AssertTrue(!rs.lazy);
}

TEST(Phase19_LazyPaging, RowCountLazyMode) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 1000000;
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    AssertEqual((__int64)1000000, (__int64)rs.RowCount());
}

// ============================================================================
// GetRow() test (normal mode)
// ============================================================================
TEST(Phase19_LazyPaging, GetRowNormalMode) {
    ResultSet rs;
    rs.columns.push_back({"val", "String", SQL_VARCHAR, 255, 0, SQL_NULLABLE});
    rs.rows.push_back({std::string("hello")});
    rs.rows.push_back({std::string("world")});

    const auto &row0 = rs.GetRow(0);
    AssertTrue(row0.size() == 1);
    AssertEqual(std::string("hello"), row0[0].value());

    const auto &row1 = rs.GetRow(1);
    AssertEqual(std::string("world"), row1[0].value());

    // Out of range returns empty row
    const auto &row2 = rs.GetRow(2);
    AssertTrue(row2.empty());
}

// ============================================================================
// GetRow() test (lazy mode with mock page_fetcher)
// ============================================================================
TEST(Phase19_LazyPaging, GetRowLazyMode) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 30;
    rs.page_size = 10;
    rs.base_query = "SELECT * FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    // Mock page_fetcher: parse LIMIT/OFFSET and return dummy data
    int fetch_count = 0;
    rs.page_fetcher = [&fetch_count](const std::string &query,
                                      std::vector<ResultRow> &out_rows,
                                      std::string &error) -> bool {
        fetch_count++;
        // query contains "LIMIT 10 OFFSET X"
        // Extract offset
        auto pos = query.find("OFFSET ");
        size_t offset = 0;
        if (pos != std::string::npos) {
            offset = std::stoull(query.substr(pos + 7));
        }
        // Generate rows: value = offset + i
        for (size_t i = 0; i < 10; ++i) {
            out_rows.push_back({std::to_string(offset + i)});
        }
        return true;
    };

    // Page 0 (rows 0-9)
    const auto &row0 = rs.GetRow(0);
    AssertTrue(row0.size() == 1);
    AssertEqual(std::string("0"), row0[0].value());
    AssertEqual((__int64)1, (__int64)fetch_count);

    // Same page → no fetch needed
    const auto &row5 = rs.GetRow(5);
    AssertEqual(std::string("5"), row5[0].value());
    AssertEqual((__int64)1, (__int64)fetch_count); // still 1

    // Page 1 (rows 10-19)
    const auto &row10 = rs.GetRow(10);
    AssertEqual(std::string("10"), row10[0].value());
    AssertEqual((__int64)2, (__int64)fetch_count);

    // Page 2 (rows 20-29)
    const auto &row20 = rs.GetRow(20);
    AssertEqual(std::string("20"), row20[0].value());
    AssertEqual((__int64)3, (__int64)fetch_count);

    // Out of range
    const auto &row30 = rs.GetRow(30);
    AssertTrue(row30.empty());
}

// ============================================================================
// Fetch() test (lazy mode)
// ============================================================================
TEST(Phase19_LazyPaging, FetchLazyMode) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 5;
    rs.page_size = 3;
    rs.base_query = "SELECT * FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    rs.page_fetcher = [](const std::string &query,
                          std::vector<ResultRow> &out_rows,
                          std::string &error) -> bool {
        auto pos = query.find("OFFSET ");
        size_t offset = 0;
        if (pos != std::string::npos) {
            offset = std::stoull(query.substr(pos + 7));
        }
        auto lpos = query.find("LIMIT ");
        size_t limit = 3;
        if (lpos != std::string::npos) {
            limit = std::stoull(query.substr(lpos + 6));
        }
        for (size_t i = 0; i < limit && (offset + i) < 5; ++i) {
            out_rows.push_back({std::to_string(offset + i)});
        }
        return true;
    };

    // Fetch 5 rows
    for (int i = 0; i < 5; ++i) {
        AssertTrue(rs.Fetch());
        AssertEqual((__int64)i, (__int64)rs.current_row);
    }
    // 6th fetch fails
    AssertTrue(!rs.Fetch());
}

// ============================================================================
// LRU page eviction test
// ============================================================================
TEST(Phase19_LazyPaging, PageEviction) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 100;
    rs.page_size = 10;
    rs.max_cached_pages = 3;
    rs.base_query = "SELECT * FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    rs.page_fetcher = [](const std::string &query,
                          std::vector<ResultRow> &out_rows,
                          std::string &error) -> bool {
        auto pos = query.find("OFFSET ");
        size_t offset = (pos != std::string::npos) ? std::stoull(query.substr(pos + 7)) : 0;
        for (size_t i = 0; i < 10; ++i) {
            out_rows.push_back({std::to_string(offset + i)});
        }
        return true;
    };

    // Load pages 0, 1, 2 → 3 pages in cache
    rs.GetRow(0);
    rs.GetRow(10);
    rs.GetRow(20);
    AssertEqual((__int64)3, (__int64)rs.page_cache.size());

    // Read page 3 → page 0 is evicted
    rs.GetRow(30);
    AssertEqual((__int64)3, (__int64)rs.page_cache.size());
    AssertTrue(rs.page_cache.count(0) == 0); // page 0 evicted
    AssertTrue(rs.page_cache.count(1) == 1);
    AssertTrue(rs.page_cache.count(2) == 1);
    AssertTrue(rs.page_cache.count(3) == 1);

    // Access page 1 (LRU update) → add page 4 → page 2 is evicted
    rs.GetRow(15); // touch page 1
    rs.GetRow(40); // page 4
    AssertEqual((__int64)3, (__int64)rs.page_cache.size());
    AssertTrue(rs.page_cache.count(2) == 0); // page 2 evicted (was LRU)
    AssertTrue(rs.page_cache.count(1) == 1); // page 1 still cached (was touched)
}

// ============================================================================
// Reset() test (lazy mode)
// ============================================================================
TEST(Phase19_LazyPaging, ResetClearsLazy) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 1000;
    rs.page_size = 100;
    rs.base_query = "SELECT * FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});
    rs.page_cache[0] = {{std::string("dummy")}};

    rs.Reset();

    AssertTrue(!rs.lazy);
    AssertEqual((__int64)0, (__int64)rs.total_row_count);
    AssertTrue(rs.base_query.empty());
    AssertTrue(rs.page_cache.empty());
    AssertTrue(rs.columns.empty());
}

// ============================================================================
// CloseCursor() test (lazy mode: preserves column information)
// ============================================================================
TEST(Phase19_LazyPaging, CloseCursorPreservesColumns) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 1000;
    rs.page_size = 100;
    rs.base_query = "SELECT * FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});
    rs.page_cache[0] = {{std::string("dummy")}};

    rs.CloseCursor();

    AssertTrue(rs.lazy); // lazy mode preserved
    AssertTrue(rs.page_cache.empty()); // cache cleared
    AssertTrue(!rs.columns.empty()); // columns preserved
    AssertEqual((__int64)-1, (__int64)rs.current_row); // cursor reset
}

// ============================================================================
// OdbcConnection default settings test
// ============================================================================
TEST(Phase19_LazyPaging, ConnectionDefaultPageSize) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    AssertEqual((__int64)10000, (__int64)conn->page_size);
    AssertTrue(conn->lazy_paging);

    FreeTestHandles(env, dbc, stmt);
}

TEST(Phase19_LazyPaging, ConnectionCustomPageSize) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT stmt;
    CreateTestHandles(env, dbc, stmt);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->page_size = 5000;
    conn->lazy_paging = false;

    AssertEqual((__int64)5000, (__int64)conn->page_size);
    AssertTrue(!conn->lazy_paging);

    FreeTestHandles(env, dbc, stmt);
}

// ============================================================================
// Safety test when page_fetcher is not set
// ============================================================================
TEST(Phase19_LazyPaging, GetRowWithoutFetcher) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 10;
    rs.page_size = 5;
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});
    // page_fetcher is null

    const auto &row = rs.GetRow(0);
    AssertTrue(row.empty()); // should return empty_row_ safely
}

// ============================================================================
// Safety test when page_fetcher returns an error
// ============================================================================
TEST(Phase19_LazyPaging, GetRowFetcherError) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 10;
    rs.page_size = 5;
    rs.base_query = "SELECT * FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    rs.page_fetcher = [](const std::string &, std::vector<ResultRow> &, std::string &error) -> bool {
        error = "Network error";
        return false;
    };

    const auto &row = rs.GetRow(0);
    AssertTrue(row.empty()); // should return empty_row_ safely
}

// ============================================================================
// SQLFetch with lazy ResultSet (ODBC handle integration)
// ============================================================================
TEST(Phase19_LazyPaging, SQLFetchWithLazyResultSet) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    auto &rs = stmt->result_set;

    // Lazy paging configuration
    rs.lazy = true;
    rs.total_row_count = 5;
    rs.page_size = 3;
    rs.base_query = "SELECT id FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    rs.page_fetcher = [](const std::string &query,
                          std::vector<ResultRow> &out_rows,
                          std::string &error) -> bool {
        auto pos = query.find("OFFSET ");
        size_t offset = (pos != std::string::npos) ? std::stoull(query.substr(pos + 7)) : 0;
        auto lpos = query.find("LIMIT ");
        size_t limit = 3;
        if (lpos != std::string::npos) {
            limit = std::stoull(query.substr(lpos + 6));
        }
        for (size_t i = 0; i < limit && (offset + i) < 5; ++i) {
            out_rows.push_back({std::to_string(offset + i + 100)});
        }
        return true;
    };

    // Fetch 5 rows via SQLFetch
    char buf[32];
    SQLLEN ind;
    for (int i = 0; i < 5; ++i) {
        SQLRETURN ret = SQLFetch(hstmt);
        AssertEqual((__int64)SQL_SUCCESS, (__int64)ret);

        ret = SQLGetData(hstmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
        AssertEqual((__int64)SQL_SUCCESS, (__int64)ret);
        AssertEqual(std::string(std::to_string(i + 100)), std::string(buf));
    }

    // 6th fetch → SQL_NO_DATA
    SQLRETURN ret = SQLFetch(hstmt);
    AssertEqual((__int64)SQL_NO_DATA, (__int64)ret);

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// SQLRowCount with lazy ResultSet
// ============================================================================
TEST(Phase19_LazyPaging, SQLRowCountLazy) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    auto &rs = stmt->result_set;

    rs.lazy = true;
    rs.total_row_count = 50000;
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    SQLLEN rowcount = 0;
    SQLRowCount(hstmt, &rowcount);
    AssertEqual((__int64)50000, (__int64)rowcount);

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// SQLFetchScroll ABSOLUTE with lazy ResultSet
// ============================================================================
TEST(Phase19_LazyPaging, FetchScrollAbsoluteLazy) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *stmt = static_cast<OdbcStatement *>(hstmt);
    auto &rs = stmt->result_set;

    rs.lazy = true;
    rs.total_row_count = 100;
    rs.page_size = 10;
    rs.base_query = "SELECT id FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    rs.page_fetcher = [](const std::string &query,
                          std::vector<ResultRow> &out_rows,
                          std::string &error) -> bool {
        auto pos = query.find("OFFSET ");
        size_t offset = (pos != std::string::npos) ? std::stoull(query.substr(pos + 7)) : 0;
        for (size_t i = 0; i < 10 && (offset + i) < 100; ++i) {
            out_rows.push_back({std::to_string(offset + i)});
        }
        return true;
    };

    // Jump to row 50 (absolute, 1-based)
    SQLRETURN ret = SQLFetchScroll(hstmt, SQL_FETCH_ABSOLUTE, 50);
    AssertEqual((__int64)SQL_SUCCESS, (__int64)ret);

    char buf[32];
    SQLLEN ind;
    ret = SQLGetData(hstmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((__int64)SQL_SUCCESS, (__int64)ret);
    AssertEqual(std::string("49"), std::string(buf));

    // Jump to last row (absolute -1)
    ret = SQLFetchScroll(hstmt, SQL_FETCH_ABSOLUTE, -1);
    AssertEqual((__int64)SQL_SUCCESS, (__int64)ret);

    ret = SQLGetData(hstmt, 1, SQL_C_CHAR, buf, sizeof(buf), &ind);
    AssertEqual((__int64)SQL_SUCCESS, (__int64)ret);
    AssertEqual(std::string("99"), std::string(buf));

    FreeTestHandles(env, dbc, hstmt);
}

// ============================================================================
// Large row count lazy paging simulation test
// ============================================================================
TEST(Phase19_LazyPaging, LargeTableSimulation) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 10000000; // 10 million rows
    rs.page_size = 100;            // Test: lightweight pages
    rs.max_cached_pages = 5;
    rs.base_query = "SELECT * FROM huge_table";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});
    rs.columns.push_back({"name", "String", SQL_VARCHAR, 255, 0, SQL_NULLABLE});

    int fetch_count = 0;
    rs.page_fetcher = [&fetch_count](const std::string &query,
                                      std::vector<ResultRow> &out_rows,
                                      std::string &error) -> bool {
        fetch_count++;
        auto pos = query.find("OFFSET ");
        size_t offset = (pos != std::string::npos) ? std::stoull(query.substr(pos + 7)) : 0;
        // Test: generate dummy rows of page_size
        for (size_t i = 0; i < 100; ++i) {
            size_t row_num = offset + i;
            out_rows.push_back({std::to_string(row_num), std::string("name_") + std::to_string(row_num)});
        }
        return true;
    };

    // RowCount is 10 million
    AssertEqual((__int64)10000000, (__int64)rs.RowCount());

    // First row
    const auto &row0 = rs.GetRow(0);
    AssertEqual(std::string("0"), row0[0].value());
    AssertEqual(std::string("name_0"), row0[1].value());
    AssertEqual((__int64)1, (__int64)fetch_count);

    // Row at 5 million
    const auto &row5m = rs.GetRow(5000000);
    AssertEqual(std::string("5000000"), row5m[0].value());
    AssertEqual(std::string("name_5000000"), row5m[1].value());
    AssertEqual((__int64)2, (__int64)fetch_count);

    // Last row
    const auto &rowLast = rs.GetRow(9999999);
    AssertEqual(std::string("9999999"), rowLast[0].value());
    AssertEqual((__int64)3, (__int64)fetch_count);

    // Should not exceed page cache limit
    AssertTrue(rs.page_cache.size() <= rs.max_cached_pages);
}

// ============================================================================
// Sequential Fetch + page transition test
// ============================================================================
TEST(Phase19_LazyPaging, SequentialFetchAcrossPages) {
    ResultSet rs;
    rs.lazy = true;
    rs.total_row_count = 25;
    rs.page_size = 10;
    rs.base_query = "SELECT id FROM test";
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 10, 0, SQL_NO_NULLS});

    int fetch_count = 0;
    rs.page_fetcher = [&fetch_count](const std::string &query,
                                      std::vector<ResultRow> &out_rows,
                                      std::string &error) -> bool {
        fetch_count++;
        auto pos = query.find("OFFSET ");
        size_t offset = (pos != std::string::npos) ? std::stoull(query.substr(pos + 7)) : 0;
        for (size_t i = 0; i < 10 && (offset + i) < 25; ++i) {
            out_rows.push_back({std::to_string(offset + i)});
        }
        return true;
    };

    // Fetch all 25 rows
    for (size_t i = 0; i < 25; ++i) {
        AssertTrue(rs.Fetch());
        const auto &row = rs.GetRow(static_cast<size_t>(rs.current_row));
        AssertEqual(std::to_string(i), row[0].value());
    }
    AssertTrue(!rs.Fetch()); // 26th fails

    // 3 pages fetched (0-9, 10-19, 20-24)
    AssertEqual((__int64)3, (__int64)fetch_count);
}

// ============================================================================
// default_max_rows application test
// ============================================================================
TEST(Phase19_LazyPaging, DefaultMaxRowsApplied) {
    SQLHENV env; SQLHDBC dbc; SQLHSTMT hstmt;
    CreateTestHandles(env, dbc, hstmt);

    auto *conn = static_cast<OdbcConnection *>(dbc);
    conn->default_max_rows = 500;

    // Create a new statement → default_max_rows is applied
    SQLHSTMT hstmt2 = SQL_NULL_HSTMT;
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hstmt2);

    auto *stmt2 = static_cast<OdbcStatement *>(hstmt2);
    AssertEqual((__int64)500, (__int64)stmt2->max_rows);

    SQLFreeHandle(SQL_HANDLE_STMT, hstmt2);
    FreeTestHandles(env, dbc, hstmt);
}

// Test registration: Automatically registered via TEST macro
