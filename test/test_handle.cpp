#include "test_framework.h"
#include "../src/include/handle.h"

using namespace test_framework;
using namespace clickhouse_odbc;

// ============================================================================
// OdbcHandle basic operations
// ============================================================================

TEST(Handle, OdbcEnvironment_Create) {
    OdbcEnvironment env;
    AssertEqual((int)SQL_HANDLE_ENV, (int)env.handle_type);
    AssertEqual((int)SQL_OV_ODBC3, (int)env.odbc_version);
    AssertTrue(env.connections.empty(), "New env should have no connections");
}

TEST(Handle, OdbcConnection_Create) {
    OdbcConnection conn;
    AssertEqual((int)SQL_HANDLE_DBC, (int)conn.handle_type);
    AssertEqual("localhost", conn.host);
    AssertEqual(8123, (int)conn.port);
    AssertEqual("default", conn.database);
    AssertEqual("default", conn.user);
    AssertFalse(conn.connected, "New connection should not be connected");
}

TEST(Handle, OdbcStatement_Create) {
    OdbcStatement stmt;
    AssertEqual((int)SQL_HANDLE_STMT, (int)stmt.handle_type);
    AssertTrue(stmt.query.empty(), "New statement should have empty query");
    AssertFalse(stmt.prepared, "New statement should not be prepared");
    AssertTrue(stmt.bound_columns.empty(), "New statement should have no bound columns");
    AssertTrue(stmt.bound_parameters.empty(), "New statement should have no bound parameters");
}

// ============================================================================
// Diagnostic records
// ============================================================================

TEST(Handle, DiagRecord_SetError) {
    OdbcStatement stmt;
    stmt.SetError("HY000", "General error", 42);

    AssertEqual(1, (int)stmt.diag_records.size());
    AssertEqual("HY000", stmt.diag_records[0].sql_state);
    AssertEqual("General error", stmt.diag_records[0].message);
    AssertEqual(42, (int)stmt.diag_records[0].native_error);
}

TEST(Handle, DiagRecord_SetError_ClearsPrevious) {
    OdbcStatement stmt;
    stmt.SetError("HY000", "Error 1");
    stmt.SetError("01004", "Error 2");

    AssertEqual(1, (int)stmt.diag_records.size());
    AssertEqual("01004", stmt.diag_records[0].sql_state);
}

TEST(Handle, DiagRecord_AddMultiple) {
    OdbcStatement stmt;
    stmt.AddDiagRecord("HY000", 0, "Error 1");
    stmt.AddDiagRecord("01004", 0, "Warning 1");

    AssertEqual(2, (int)stmt.diag_records.size());
}

TEST(Handle, DiagRecord_Clear) {
    OdbcStatement stmt;
    stmt.SetError("HY000", "Error");
    stmt.ClearDiagRecords();

    AssertTrue(stmt.diag_records.empty(), "After clear, diag records should be empty");
}

// ============================================================================
// ResultSet
// ============================================================================

TEST(Handle, ResultSet_Empty) {
    ResultSet rs;
    AssertFalse(rs.HasData(), "Empty result set should not have data");
    AssertFalse(rs.Fetch(), "Fetch on empty result set should return false");
}

TEST(Handle, ResultSet_Fetch) {
    ResultSet rs;
    rs.columns.push_back({"col1", "Int32", SQL_INTEGER, 11, 0, SQL_NO_NULLS});

    ResultRow row1 = {std::string("1")};
    ResultRow row2 = {std::string("2")};
    ResultRow row3 = {std::string("3")};
    rs.rows.push_back(row1);
    rs.rows.push_back(row2);
    rs.rows.push_back(row3);

    AssertTrue(rs.HasData(), "Result set with columns should have data");
    AssertEqual(-1LL, (long long)rs.current_row);

    AssertTrue(rs.Fetch(), "First fetch");
    AssertEqual(0LL, (long long)rs.current_row);

    AssertTrue(rs.Fetch(), "Second fetch");
    AssertEqual(1LL, (long long)rs.current_row);

    AssertTrue(rs.Fetch(), "Third fetch");
    AssertEqual(2LL, (long long)rs.current_row);

    AssertFalse(rs.Fetch(), "Fourth fetch should fail (past end)");
}

TEST(Handle, ResultSet_Reset) {
    ResultSet rs;
    rs.columns.push_back({"col1", "String", SQL_VARCHAR, 255, 0, SQL_NULLABLE});
    rs.rows.push_back({std::string("value")});
    rs.Fetch();

    rs.Reset();

    AssertFalse(rs.HasData(), "After reset, should not have data");
    AssertTrue(rs.columns.empty(), "After reset, columns should be empty");
    AssertTrue(rs.rows.empty(), "After reset, rows should be empty");
    AssertEqual(-1LL, (long long)rs.current_row);
}

TEST(Handle, ResultSet_NullValues) {
    ResultSet rs;
    rs.columns.push_back({"col1", "Nullable(String)", SQL_VARCHAR, 255, 0, SQL_NULLABLE});

    ResultRow row = {std::nullopt};
    rs.rows.push_back(row);

    AssertTrue(rs.Fetch(), "Should fetch row with null");
    AssertFalse(rs.rows[0][0].has_value(), "Null column should not have value");
}

TEST(Handle, ResultSet_MultipleColumns) {
    ResultSet rs;
    rs.columns.push_back({"id", "Int32", SQL_INTEGER, 11, 0, SQL_NO_NULLS});
    rs.columns.push_back({"name", "String", SQL_VARCHAR, 255, 0, SQL_NULLABLE});
    rs.columns.push_back({"value", "Float64", SQL_DOUBLE, 24, 0, SQL_NULLABLE});

    ResultRow row = {std::string("1"), std::string("test"), std::string("3.14")};
    rs.rows.push_back(row);

    AssertTrue(rs.Fetch());
    AssertEqual("1", rs.rows[0][0].value());
    AssertEqual("test", rs.rows[0][1].value());
    AssertEqual("3.14", rs.rows[0][2].value());
}

// ============================================================================
// Handle validation
// ============================================================================

TEST(Handle, IsValidEnvHandle) {
    OdbcEnvironment env;
    AssertTrue(IsValidEnvHandle(&env));
    AssertFalse(IsValidEnvHandle(nullptr));

    OdbcConnection conn;
    AssertFalse(IsValidEnvHandle(&conn));
}

TEST(Handle, IsValidDbcHandle) {
    OdbcConnection conn;
    AssertTrue(IsValidDbcHandle(&conn));
    AssertFalse(IsValidDbcHandle(nullptr));

    OdbcEnvironment env;
    AssertFalse(IsValidDbcHandle(&env));
}

TEST(Handle, IsValidStmtHandle) {
    OdbcStatement stmt;
    AssertTrue(IsValidStmtHandle(&stmt));
    AssertFalse(IsValidStmtHandle(nullptr));

    OdbcConnection conn;
    AssertFalse(IsValidStmtHandle(&conn));
}

// ============================================================================
// Connection attributes
// ============================================================================

TEST(Handle, ConnectionDefaults) {
    OdbcConnection conn;
    AssertEqual((int)SQL_MODE_READ_WRITE, (int)conn.access_mode);
    AssertEqual((int)SQL_AUTOCOMMIT_ON, (int)conn.autocommit);
    AssertEqual(0, (int)conn.login_timeout);
    AssertEqual(0, (int)conn.connection_timeout);
}

// ============================================================================
// Statement attributes
// ============================================================================

TEST(Handle, StatementDefaults) {
    OdbcStatement stmt;
    AssertEqual(0LL, (long long)stmt.max_rows);
    AssertEqual(0LL, (long long)stmt.query_timeout);
    AssertEqual((long long)SQL_CURSOR_FORWARD_ONLY, (long long)stmt.cursor_type);
}
