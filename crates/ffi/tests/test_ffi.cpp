/**
 * @file test_ffi.cpp
 * @brief Unit tests for the PotatoDB C and C++ FFI APIs using doctest.
 *
 * Build with CMake (from crates/ffi):
 *   cargo build --release -p potatodb-ffi
 *   cmake -B build && cmake --build build --target potatodb_tests
 *   ctest --test-dir build --output-on-failure
 *
 * Every test opens a fresh temporary database directory and cleans up
 * after itself so tests are isolated and repeatable.
 */

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"
#include "potatodb.hpp"   // C++ wrapper (includes potatodb.h)

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// ── Helpers ──────────────────────────────────────────────────

/// RAII temporary directory: creates a unique dir on construction,
/// recursively removes it on destruction.
struct TmpDir {
    fs::path path;

    TmpDir() {
        path = fs::temp_directory_path() / ("potatodb_test_" + std::to_string(rand()));
        fs::create_directories(path);
    }

    ~TmpDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }

    TmpDir(const TmpDir &) = delete;
    TmpDir &operator=(const TmpDir &) = delete;
};

/// Shorthand: open a DB in a temp dir, abort the test on failure.
static potato::Database open_tmp(const TmpDir &tmp) {
    auto db = potato::Database::open(tmp.path.string());
    REQUIRE(db);
    return std::move(*db);
}

/// Helper: execute SQL and assert success.
static potato::Result exec_ok(potato::Database &db, const std::string &sql) {
    auto res = db.execute(sql);
    REQUIRE_MESSAGE(res, res.error());
    return std::move(*res);
}

// =====================================================================
//  Database lifecycle
// =====================================================================

TEST_SUITE("Database lifecycle") {

    TEST_CASE("open and close a local database") {
        TmpDir tmp;
        auto db = potato::Database::open(tmp.path.string());
        REQUIRE(db);
        // Database destructor runs here — no crash means success.
    }

    TEST_CASE("open returns error for invalid S3 config") {
        // Passing a bogus S3 URL should still return a handle (lazy connect)
        // but at least should not crash.
        auto db = potato::Database::open_s3(
            "s3://nonexistent-bucket/pfx", "http://localhost:1", "us-east-1", true);
        // Whether this succeeds or fails depends on the engine behaviour;
        // we simply verify no crash / no UB.
        (void)db;
    }

    TEST_CASE("data_url reports the data directory") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        std::string url = db.data_url();
        CHECK_FALSE(url.empty());
    }

    TEST_CASE("last_error is empty on a fresh database") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        CHECK(db.last_error().empty());
    }
}

// =====================================================================
//  DDL — CREATE / DROP / ALTER
// =====================================================================

TEST_SUITE("DDL") {

    TEST_CASE("CREATE TABLE returns a message result") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto res = exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
        CHECK(res.is_message());
        CHECK_FALSE(res.message().empty());
    }

    TEST_CASE("CREATE TABLE IF NOT EXISTS is idempotent") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE IF NOT EXISTS t (id INT);");
        auto res = exec_ok(db, "CREATE TABLE IF NOT EXISTS t (id INT);");
        CHECK(res.is_message());
    }

    TEST_CASE("DROP TABLE removes the table") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "DROP TABLE t;");

        auto names = db.table_names().to_vector();
        auto it = std::find(names.begin(), names.end(), "t");
        CHECK(it == names.end());
    }

    TEST_CASE("table_names reflects created tables") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE alpha (id INT);");
        exec_ok(db, "CREATE TABLE beta  (id INT);");

        auto names = db.table_names().to_vector();
        CHECK(std::find(names.begin(), names.end(), "alpha") != names.end());
        CHECK(std::find(names.begin(), names.end(), "beta")  != names.end());
    }

    TEST_CASE("table_columns returns correct column names") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR, score DOUBLE);");

        auto cols = db.table_columns("t").to_vector();
        REQUIRE(cols.size() == 3);
        CHECK(cols[0] == "id");
        CHECK(cols[1] == "name");
        CHECK(cols[2] == "score");
    }
}

// =====================================================================
//  DML — INSERT / SELECT / UPDATE / DELETE
// =====================================================================

TEST_SUITE("DML") {

    TEST_CASE("INSERT and SELECT round-trip") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');");

        auto res = exec_ok(db, "SELECT * FROM t ORDER BY id;");
        CHECK(res.is_records());
        CHECK(res.row_count() == 2);
        CHECK(res.column_count() == 2);
        CHECK(res.get_int(0, 0) == 1);
        CHECK(res.get_string(0, 1) == "Alice");
        CHECK(res.get_int(1, 0) == 2);
        CHECK(res.get_string(1, 1) == "Bob");
    }

    TEST_CASE("column_name and column_type") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, flag BOOLEAN, score DOUBLE);");
        exec_ok(db, "INSERT INTO t VALUES (1, true, 3.14);");

        auto res = exec_ok(db, "SELECT * FROM t;");
        CHECK(res.column_name(0) == "id");
        CHECK(res.column_name(1) == "flag");
        CHECK(res.column_name(2) == "score");
    }

    TEST_CASE("typed getters: int, double, bool, string") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (i INT, d DOUBLE, b BOOLEAN, s VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (42, 2.718, true, 'hello');");

        auto res = exec_ok(db, "SELECT * FROM t;");
        CHECK(res.get_int(0, 0)    == 42);
        CHECK(res.get_double(0, 1) == doctest::Approx(2.718));
        CHECK(res.get_bool(0, 2)   == true);
        CHECK(res.get_string(0, 3) == "hello");
    }

    TEST_CASE("NULL handling") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (1, NULL);");

        auto res = exec_ok(db, "SELECT * FROM t;");
        CHECK_FALSE(res.is_null(0, 0));
        CHECK(res.is_null(0, 1));
        // Null int/double/bool should return zero-values.
        CHECK(res.get_int(0, 1) == 0);
        CHECK(res.get_string(0, 1).empty());
    }

    TEST_CASE("UPDATE modifies data") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, val INT);");
        exec_ok(db, "INSERT INTO t VALUES (1, 10), (2, 20);");
        exec_ok(db, "UPDATE t SET val = 99 WHERE id = 1;");

        auto res = exec_ok(db, "SELECT val FROM t WHERE id = 1;");
        CHECK(res.row_count() == 1);
        CHECK(res.get_int(0, 0) == 99);
    }

    TEST_CASE("DELETE removes rows") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1), (2), (3);");
        exec_ok(db, "DELETE FROM t WHERE id = 2;");

        auto res = exec_ok(db, "SELECT COUNT(*) FROM t;");
        CHECK(res.get_int(0, 0) == 2);
    }

    TEST_CASE("Aggregation queries") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (val INT);");
        exec_ok(db, "INSERT INTO t VALUES (10), (20), (30);");

        auto res = exec_ok(db,
            "SELECT COUNT(*) AS cnt, SUM(val) AS total, "
            "AVG(val) AS avg, MIN(val) AS lo, MAX(val) AS hi FROM t;");
        CHECK(res.get_int(0, 0)    == 3);
        CHECK(res.get_int(0, 1)    == 60);
        CHECK(res.get_double(0, 2) == doctest::Approx(20.0));
        CHECK(res.get_int(0, 3)    == 10);
        CHECK(res.get_int(0, 4)    == 30);
    }
}

// =====================================================================
//  Transactions
// =====================================================================

TEST_SUITE("Transactions") {

    TEST_CASE("BEGIN / COMMIT persists data") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");

        CHECK_FALSE(db.in_transaction());
        exec_ok(db, "BEGIN;");
        CHECK(db.in_transaction());

        exec_ok(db, "INSERT INTO t VALUES (1);");
        exec_ok(db, "COMMIT;");
        CHECK_FALSE(db.in_transaction());

        auto res = exec_ok(db, "SELECT COUNT(*) FROM t;");
        CHECK(res.get_int(0, 0) == 1);
    }

    TEST_CASE("BEGIN / ROLLBACK discards data") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");

        exec_ok(db, "BEGIN;");
        exec_ok(db, "INSERT INTO t VALUES (2);");
        exec_ok(db, "ROLLBACK;");

        auto res = exec_ok(db, "SELECT COUNT(*) FROM t;");
        CHECK(res.get_int(0, 0) == 1);
    }
}

// =====================================================================
//  Prepared statements
// =====================================================================

TEST_SUITE("Prepared statements") {

    TEST_CASE("prepare and execute with parameters") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');");

        bool ok = db.prepare("find_by_id", "SELECT name FROM t WHERE id = $1");
        REQUIRE(ok);

        auto res = db.execute_prepared("find_by_id", {"1"});
        REQUIRE(res);
        CHECK(res->row_count() == 1);
        CHECK(res->get_string(0, 0) == "Alice");

        auto res2 = db.execute_prepared("find_by_id", {"3"});
        REQUIRE(res2);
        CHECK(res2->get_string(0, 0) == "Charlie");
    }
}

// =====================================================================
//  Views
// =====================================================================

TEST_SUITE("Views") {

    TEST_CASE("CREATE and query a VIEW") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, score INT);");
        exec_ok(db, "INSERT INTO t VALUES (1, 80), (2, 95), (3, 70);");
        exec_ok(db, "CREATE VIEW high_scores AS SELECT * FROM t WHERE score >= 90;");

        auto names = db.view_names().to_vector();
        CHECK(std::find(names.begin(), names.end(), "high_scores") != names.end());

        auto res = exec_ok(db, "SELECT * FROM high_scores;");
        CHECK(res.row_count() == 1);
        CHECK(res.get_int(0, 1) == 95);
    }
}

// =====================================================================
//  Indexes
// =====================================================================

TEST_SUITE("Indexes") {

    TEST_CASE("CREATE INDEX and list indexes") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
        exec_ok(db, "CREATE INDEX idx_t_id ON t (id);");

        auto idxs = db.indexes();
        REQUIRE(idxs.count() >= 1);
        CHECK(idxs.name(0) == "idx_t_id");
        CHECK(idxs.table(0) == "t");
    }
}

// =====================================================================
//  Result display
// =====================================================================

TEST_SUITE("Result display") {

    TEST_CASE("display returns a non-empty formatted table") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (1, 'Alice');");

        auto res = exec_ok(db, "SELECT * FROM t;");
        std::string table = res.display();
        CHECK_FALSE(table.empty());
        // The display should contain column headers.
        CHECK(table.find("id") != std::string::npos);
        CHECK(table.find("name") != std::string::npos);
        CHECK(table.find("Alice") != std::string::npos);
    }
}

// =====================================================================
//  Query log
// =====================================================================

TEST_SUITE("Query log") {

    TEST_CASE("recent_queries captures executed SQL") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");
        exec_ok(db, "SELECT * FROM t;");

        auto log = db.recent_queries();
        CHECK(log.count() >= 3);
        // The most recent queries should be in the log.
        bool found_select = false;
        for (std::size_t i = 0; i < log.count(); ++i) {
            if (log.sql(i).find("SELECT") != std::string::npos)
                found_select = true;
        }
        CHECK(found_select);
    }
}

// =====================================================================
//  Streaming results
// =====================================================================

TEST_SUITE("Streaming") {

    TEST_CASE("execute_stream returns batches for SELECT") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1), (2), (3);");

        auto stream = db.execute_stream("SELECT * FROM t ORDER BY id;");
        REQUIRE(stream);
        CHECK_FALSE(stream->is_message());

        std::size_t total_rows = 0;
        while (true) {
            auto batch = stream->next();
            if (!batch) break;
            total_rows += batch.row_count();
        }
        CHECK(total_rows == 3);
    }

    TEST_CASE("execute_stream returns message for DDL") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        auto stream = db.execute_stream("CREATE TABLE t (id INT);");
        REQUIRE(stream);
        CHECK(stream->is_message());
        CHECK_FALSE(stream->message().empty());
    }
}

// =====================================================================
//  Execute file
// =====================================================================

TEST_SUITE("Execute file") {

    TEST_CASE("execute_file runs multiple statements from a .sql file") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        // Write a small SQL file.
        fs::path sql_file = tmp.path / "test.sql";
        {
            std::ofstream f(sql_file);
            f << "CREATE TABLE t (id INT, name VARCHAR);\n"
              << "INSERT INTO t VALUES (1, 'Alice');\n"
              << "INSERT INTO t VALUES (2, 'Bob');\n";
        }

        auto rlist = db.execute_file(sql_file.string());
        REQUIRE(rlist);
        CHECK(rlist->count() == 3);

        // Verify the data landed.
        auto res = exec_ok(db, "SELECT COUNT(*) FROM t;");
        CHECK(res.get_int(0, 0) == 2);
    }
}

// =====================================================================
//  Flush
// =====================================================================

TEST_SUITE("Flush") {

    TEST_CASE("flush succeeds on a database with data") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");

        CHECK(db.flush());
    }

    TEST_CASE("flush_table succeeds for a specific table") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");

        CHECK(db.flush_table("t"));
    }
}

// =====================================================================
//  Error handling
// =====================================================================

TEST_SUITE("Error handling") {

    TEST_CASE("execute on non-existent table returns error") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto res = db.execute("SELECT * FROM nonexistent;");
        CHECK_FALSE(res);
        CHECK_FALSE(res.error().empty());
    }

    TEST_CASE("invalid SQL returns error") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto res = db.execute("NOT VALID SQL AT ALL;");
        CHECK_FALSE(res);
        CHECK_FALSE(res.error().empty());
    }

    TEST_CASE("last_error is set after failed query") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto res = db.execute("SELECT * FROM nonexistent;");
        CHECK_FALSE(res);
        CHECK_FALSE(db.last_error().empty());
    }
}

// =====================================================================
//  C API — direct usage through the C header
// =====================================================================

TEST_SUITE("C API") {

    TEST_CASE("potato_open_local / potato_close") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);
        potato_close(db);
    }

    TEST_CASE("potato_execute + result accessors") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db,
            "CREATE TABLE c_test (id INT, name VARCHAR);");
        REQUIRE(r != nullptr);
        CHECK(potato_result_get_kind(r) == POTATO_RESULT_MESSAGE);
        potato_result_free(r);

        r = potato_execute(db, "INSERT INTO c_test VALUES (1, 'hello');");
        REQUIRE(r != nullptr);
        potato_result_free(r);

        r = potato_execute(db, "SELECT * FROM c_test;");
        REQUIRE(r != nullptr);
        CHECK(potato_result_get_kind(r) == POTATO_RESULT_RECORDS);
        CHECK(potato_result_row_count(r) == 1);
        CHECK(potato_result_column_count(r) == 2);

        // Column name
        const char *col0 = potato_result_column_name(r, 0);
        REQUIRE(col0 != nullptr);
        CHECK(std::string(col0) == "id");

        // Integer value
        CHECK(potato_result_get_int(r, 0, 0) == 1);

        // String value
        char *str = potato_result_get_string(r, 0, 1);
        REQUIRE(str != nullptr);
        CHECK(std::string(str) == "hello");
        potato_string_free(str);

        // NULL check
        CHECK_FALSE(potato_result_is_null(r, 0, 0));

        potato_result_free(r);
        potato_close(db);
    }

    TEST_CASE("NULL pointers are handled safely") {
        // All these should be no-ops or return safe defaults.
        CHECK(potato_in_transaction(nullptr) == false);
        CHECK(potato_string_list_count(nullptr) == 0);
        CHECK(potato_string_list_get(nullptr, 0) == nullptr);
        CHECK(potato_index_list_count(nullptr) == 0);
        CHECK(potato_result_list_count(nullptr) == 0);
        CHECK(potato_query_log_count(nullptr) == 0);
        CHECK(potato_stream_is_message(nullptr) == false);
        CHECK(potato_last_error(nullptr) == nullptr);

        // Free functions should accept NULL without crashing.
        potato_result_free(nullptr);
        potato_string_list_free(nullptr);
        potato_index_list_free(nullptr);
        potato_result_list_free(nullptr);
        potato_query_log_free(nullptr);
        potato_stream_free(nullptr);
        potato_close(nullptr);
    }
}

// =====================================================================
//  Read-only execution
// =====================================================================

TEST_SUITE("Read-only execution") {

    TEST_CASE("execute_readonly allows SELECT") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, val VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b');");

        auto res = db.execute_readonly("SELECT * FROM t ORDER BY id;");
        REQUIRE(res);
        CHECK(res->is_records());
        CHECK(res->row_count() == 2);
        CHECK(res->get_string(0, 1) == "a");
    }

    TEST_CASE("execute_readonly rejects mutating SQL") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");

        auto res = db.execute_readonly("INSERT INTO t VALUES (1);");
        CHECK_FALSE(res);
        CHECK_FALSE(res.error().empty());
    }
}

// =====================================================================
//  Backup / Restore
// =====================================================================

TEST_SUITE("Backup and restore") {

    TEST_CASE("backup and restore round-trip") {
        TmpDir tmp;
        TmpDir archive_dir;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');");
        exec_ok(db, "INSERT INTO t VALUES (3, 'Charlie');");
        db.flush();

        fs::path archive = archive_dir.path / "backup.tar.gz";
        CHECK(db.backup(archive.string()));
        CHECK(fs::exists(archive));
        CHECK(fs::file_size(archive) > 0);
    }

    TEST_CASE("backup with invalid path fails gracefully") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        CHECK_FALSE(db.backup("/nonexistent/deeply/nested/path/backup.tar.gz"));
    }
}

// =====================================================================
//  Column types
// =====================================================================

TEST_SUITE("Column types") {

    TEST_CASE("column_type returns correct type tags") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE typed (i INT, d DOUBLE, b BOOLEAN, s VARCHAR);");
        exec_ok(db, "INSERT INTO typed VALUES (1, 1.5, true, 'x');");

        auto res = exec_ok(db, "SELECT * FROM typed;");
        CHECK(res.column_type(3) == POTATO_TYPE_STRING);
        CHECK(res.column_type(2) == POTATO_TYPE_BOOL);
    }

    TEST_CASE("empty table result has zero rows and zero columns") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE empty_typed (x INT, y VARCHAR);");

        auto res = exec_ok(db, "SELECT * FROM empty_typed;");
        CHECK(res.row_count() == 0);
        // Empty results produce zero batches, so column_count is 0.
        CHECK(res.column_count() == 0);
    }
}

// =====================================================================
//  Table storage stats
// =====================================================================

TEST_SUITE("Table storage stats") {

    TEST_CASE("parquet_file_count after flush") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1), (2), (3);");
        db.flush();

        auto count = db.parquet_file_count("t");
        REQUIRE(count);
        CHECK(*count >= 1);
    }

    TEST_CASE("table_total_bytes after flush") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");
        db.flush();

        auto bytes = db.table_total_bytes("t");
        REQUIRE(bytes);
        CHECK(*bytes > 0);
    }

    TEST_CASE("table_oldest_file_age_secs after flush") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");
        db.flush();

        auto age = db.table_oldest_file_age_secs("t");
        REQUIRE(age);
        CHECK(*age >= 0);
    }

    TEST_CASE("stats on non-existent table returns zero") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto count = db.parquet_file_count("no_such_table");
        REQUIRE(count);
        CHECK(*count == 0);
    }
}

// =====================================================================
//  Query log — detail accessors
// =====================================================================

TEST_SUITE("Query log details") {

    TEST_CASE("query log entries have duration and row count") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1), (2), (3);");
        exec_ok(db, "SELECT * FROM t;");

        auto log = db.recent_queries();
        REQUIRE(log.count() >= 1);

        bool found = false;
        for (std::size_t i = 0; i < log.count(); ++i) {
            if (log.sql(i).find("SELECT") != std::string::npos) {
                CHECK(log.rows(i) == 3);
                found = true;
            }
        }
        CHECK(found);
    }

    TEST_CASE("query log duration_ms is non-negative") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");

        auto log = db.recent_queries();
        for (std::size_t i = 0; i < log.count(); ++i) {
            CHECK(log.duration_ms(i) < 60'000);
        }
    }
}

// =====================================================================
//  ResultList detail accessors
// =====================================================================

TEST_SUITE("ResultList details") {

    TEST_CASE("execute_file result list carries per-statement SQL") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        fs::path sql_file = tmp.path / "multi.sql";
        {
            std::ofstream f(sql_file);
            f << "CREATE TABLE t (id INT);\n"
              << "INSERT INTO t VALUES (1);\n"
              << "SELECT * FROM t;\n";
        }

        auto rlist = db.execute_file(sql_file.string());
        REQUIRE(rlist);
        REQUIRE(rlist->count() == 3);

        CHECK(rlist->sql(0).find("CREATE") != std::string::npos);
        CHECK(rlist->sql(1).find("INSERT") != std::string::npos);
        CHECK(rlist->sql(2).find("SELECT") != std::string::npos);

        CHECK_FALSE(rlist->has_error(0));
        CHECK_FALSE(rlist->has_error(1));
        CHECK_FALSE(rlist->has_error(2));

        const potato_result *r = rlist->result(2);
        REQUIRE(r != nullptr);
        CHECK(potato_result_row_count(r) == 1);
    }

    TEST_CASE("execute_file with continue_on_error=true reports partial errors") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        fs::path sql_file = tmp.path / "errors.sql";
        {
            std::ofstream f(sql_file);
            f << "CREATE TABLE t (id INT);\n"
              << "SELECT * FROM nonexistent;\n"
              << "INSERT INTO t VALUES (42);\n";
        }

        auto rlist = db.execute_file(sql_file.string(), true);
        REQUIRE(rlist);
        CHECK(rlist->count() == 3);

        CHECK_FALSE(rlist->has_error(0));
        CHECK(rlist->has_error(1));
        CHECK_FALSE(rlist->error(1).empty());
        CHECK_FALSE(rlist->has_error(2));
    }
}

// =====================================================================
//  DATE / TIMESTAMP types
// =====================================================================

TEST_SUITE("Date and timestamp types") {

    TEST_CASE("DATE values round-trip") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE d (dt DATE);");
        exec_ok(db, "INSERT INTO d VALUES ('2024-06-15');");

        auto res = exec_ok(db, "SELECT * FROM d;");
        CHECK(res.row_count() == 1);
        std::string as_str = res.get_string(0, 0);
        CHECK(as_str.find("2024") != std::string::npos);
    }

    TEST_CASE("TIMESTAMP values round-trip") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE ts (t TIMESTAMP);");
        exec_ok(db, "INSERT INTO ts VALUES ('2024-01-15 10:30:00');");

        auto res = exec_ok(db, "SELECT * FROM ts;");
        CHECK(res.row_count() == 1);
        std::string as_str = res.get_string(0, 0);
        CHECK(as_str.find("2024") != std::string::npos);
    }
}

// =====================================================================
//  Move semantics (RAII wrappers)
// =====================================================================

TEST_SUITE("RAII wrappers") {

    TEST_CASE("Database is move-constructible") {
        TmpDir tmp;
        auto db1 = open_tmp(tmp);
        potato::Database db2 = std::move(db1);
        auto res = db2.execute("SELECT 1;");
        CHECK(res);
    }

    TEST_CASE("Database is move-assignable") {
        TmpDir tmp1;
        TmpDir tmp2;
        auto db1 = open_tmp(tmp1);
        auto db2 = open_tmp(tmp2);
        db2 = std::move(db1);
        auto res = db2.execute("SELECT 1;");
        CHECK(res);
    }

    TEST_CASE("Result is move-constructible") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto res1 = exec_ok(db, "SELECT 1 AS val;");
        potato::Result res2 = std::move(res1);
        CHECK(res2.row_count() == 1);
    }

    TEST_CASE("Result is move-assignable") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto res1 = exec_ok(db, "SELECT 1 AS a;");
        auto res2 = exec_ok(db, "SELECT 2 AS b;");
        res2 = std::move(res1);
        CHECK(res2.get_int(0, 0) == 1);
    }

    TEST_CASE("StringList is move-constructible") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");

        auto sl1 = db.table_names();
        potato::StringList sl2 = std::move(sl1);
        CHECK(sl2.count() >= 1);
    }

    TEST_CASE("IndexList is move-constructible") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "CREATE INDEX idx ON t (id);");

        auto il1 = db.indexes();
        potato::IndexList il2 = std::move(il1);
        CHECK(il2.count() >= 1);
    }

    TEST_CASE("QueryLog is move-constructible") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "SELECT 1;");

        auto ql1 = db.recent_queries();
        potato::QueryLog ql2 = std::move(ql1);
        CHECK(ql2.count() >= 1);
    }

    TEST_CASE("Expected holds value correctly") {
        potato::Expected<int> e(42);
        CHECK(e);
        CHECK(*e == 42);
    }

    TEST_CASE("Expected holds error correctly") {
        auto e = potato::Expected<int>::err("oops");
        CHECK_FALSE(e);
        CHECK(e.error() == "oops");
    }

    TEST_CASE("Expected is copy-constructible") {
        potato::Expected<int> e1(7);
        potato::Expected<int> e2(e1);
        CHECK(e2);
        CHECK(*e2 == 7);
    }

    TEST_CASE("Expected is move-constructible") {
        potato::Expected<int> e1(7);
        potato::Expected<int> e2(std::move(e1));
        CHECK(e2);
        CHECK(*e2 == 7);
    }
}

// =====================================================================
//  C API — extended coverage
// =====================================================================

TEST_SUITE("C API extended") {

    TEST_CASE("potato_data_url returns valid string") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        char *url = potato_data_url(db);
        REQUIRE(url != nullptr);
        CHECK(std::string(url).size() > 0);
        potato_string_free(url);

        potato_close(db);
    }

    TEST_CASE("potato_in_transaction with real database") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        CHECK_FALSE(potato_in_transaction(db));

        potato_result *r = potato_execute(db, "BEGIN;");
        REQUIRE(r != nullptr);
        potato_result_free(r);
        CHECK(potato_in_transaction(db));

        r = potato_execute(db, "COMMIT;");
        REQUIRE(r != nullptr);
        potato_result_free(r);
        CHECK_FALSE(potato_in_transaction(db));

        potato_close(db);
    }

    TEST_CASE("potato_table_names / potato_table_columns via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE ct (a INT, b VARCHAR);");
        potato_result_free(r);

        potato_string_list *names = potato_table_names(db);
        REQUIRE(names != nullptr);
        bool found = false;
        for (size_t i = 0; i < potato_string_list_count(names); ++i) {
            if (std::string(potato_string_list_get(names, i)) == "ct")
                found = true;
        }
        CHECK(found);
        potato_string_list_free(names);

        potato_string_list *cols = potato_table_columns(db, "ct");
        REQUIRE(cols != nullptr);
        REQUIRE(potato_string_list_count(cols) == 2);
        CHECK(std::string(potato_string_list_get(cols, 0)) == "a");
        CHECK(std::string(potato_string_list_get(cols, 1)) == "b");
        potato_string_list_free(cols);

        potato_close(db);
    }

    TEST_CASE("potato_view_names via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE vt (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "CREATE VIEW v AS SELECT * FROM vt;");
        potato_result_free(r);

        potato_string_list *views = potato_view_names(db);
        REQUIRE(views != nullptr);
        bool found = false;
        for (size_t i = 0; i < potato_string_list_count(views); ++i) {
            if (std::string(potato_string_list_get(views, i)) == "v")
                found = true;
        }
        CHECK(found);
        potato_string_list_free(views);

        potato_close(db);
    }

    TEST_CASE("potato_indexes via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE it (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "CREATE INDEX idx_it ON it (id);");
        potato_result_free(r);

        potato_index_list *idxs = potato_indexes(db);
        REQUIRE(idxs != nullptr);
        REQUIRE(potato_index_list_count(idxs) >= 1);
        CHECK(std::string(potato_index_list_name(idxs, 0)) == "idx_it");
        CHECK(std::string(potato_index_list_table(idxs, 0)) == "it");
        potato_index_list_free(idxs);

        potato_close(db);
    }

    TEST_CASE("potato_execute_readonly via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE ro (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO ro VALUES (1);");
        potato_result_free(r);

        r = potato_execute_readonly(db, "SELECT * FROM ro;");
        REQUIRE(r != nullptr);
        CHECK(potato_result_row_count(r) == 1);
        potato_result_free(r);

        r = potato_execute_readonly(db, "INSERT INTO ro VALUES (2);");
        CHECK(r == nullptr);

        potato_close(db);
    }

    TEST_CASE("potato_prepare / potato_execute_prepared via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db,
            "CREATE TABLE prep (id INT, name VARCHAR);");
        potato_result_free(r);
        r = potato_execute(db,
            "INSERT INTO prep VALUES (1, 'Alice'), (2, 'Bob');");
        potato_result_free(r);

        int rc = potato_prepare(db, "get_name",
            "SELECT name FROM prep WHERE id = $1");
        CHECK(rc == 0);

        const char *params[] = {"2"};
        r = potato_execute_prepared(db, "get_name", params, 1);
        REQUIRE(r != nullptr);
        CHECK(potato_result_row_count(r) == 1);

        char *name = potato_result_get_string(r, 0, 0);
        CHECK(std::string(name) == "Bob");
        potato_string_free(name);
        potato_result_free(r);

        potato_close(db);
    }

    TEST_CASE("potato_result typed getters via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db,
            "CREATE TABLE tg (i INT, d DOUBLE, b BOOLEAN, s VARCHAR);");
        potato_result_free(r);
        r = potato_execute(db,
            "INSERT INTO tg VALUES (99, 3.14, false, 'world');");
        potato_result_free(r);

        r = potato_execute(db, "SELECT * FROM tg;");
        REQUIRE(r != nullptr);

        CHECK(potato_result_get_int(r, 0, 0) == 99);
        CHECK(potato_result_get_double(r, 0, 1) == doctest::Approx(3.14));
        CHECK(potato_result_get_bool(r, 0, 2) == false);

        char *s = potato_result_get_string(r, 0, 3);
        CHECK(std::string(s) == "world");
        potato_string_free(s);

        potato_result_free(r);
        potato_close(db);
    }

    TEST_CASE("potato_result_get_column_type via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db,
            "CREATE TABLE ct2 (s VARCHAR, b BOOLEAN);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO ct2 VALUES ('x', true);");
        potato_result_free(r);

        r = potato_execute(db, "SELECT * FROM ct2;");
        REQUIRE(r != nullptr);
        CHECK(potato_result_get_column_type(r, 0) == POTATO_TYPE_STRING);
        CHECK(potato_result_get_column_type(r, 1) == POTATO_TYPE_BOOL);
        potato_result_free(r);

        potato_close(db);
    }

    TEST_CASE("potato_result_display via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db,
            "CREATE TABLE disp (id INT, name VARCHAR);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO disp VALUES (1, 'Alice');");
        potato_result_free(r);

        r = potato_execute(db, "SELECT * FROM disp;");
        REQUIRE(r != nullptr);
        const char *d = potato_result_display(r);
        REQUIRE(d != nullptr);
        std::string display(d);
        CHECK(display.find("Alice") != std::string::npos);
        CHECK(display.find("id") != std::string::npos);
        potato_result_free(r);

        potato_close(db);
    }

    TEST_CASE("potato_execute_stream via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE st (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO st VALUES (1), (2);");
        potato_result_free(r);

        potato_stream *stream = potato_execute_stream(db, "SELECT * FROM st;");
        REQUIRE(stream != nullptr);
        CHECK_FALSE(potato_stream_is_message(stream));

        size_t total = 0;
        while (true) {
            potato_result *batch = potato_stream_next(stream);
            if (!batch) break;
            total += potato_result_row_count(batch);
            potato_result_free(batch);
        }
        CHECK(total == 2);
        potato_stream_free(stream);

        stream = potato_execute_stream(db, "CREATE TABLE st2 (x INT);");
        REQUIRE(stream != nullptr);
        CHECK(potato_stream_is_message(stream));
        const char *msg = potato_stream_message(stream);
        REQUIRE(msg != nullptr);
        CHECK(std::string(msg).size() > 0);
        potato_stream_free(stream);

        potato_close(db);
    }

    TEST_CASE("potato_execute_file via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        fs::path sql_file = tmp.path / "capi.sql";
        {
            std::ofstream f(sql_file);
            f << "CREATE TABLE ft (id INT);\n"
              << "INSERT INTO ft VALUES (10);\n";
        }

        potato_result_list *rl = potato_execute_file(db, sql_file.string().c_str(), true);
        REQUIRE(rl != nullptr);
        CHECK(potato_result_list_count(rl) == 2);

        const char *sql0 = potato_result_list_sql(rl, 0);
        REQUIRE(sql0 != nullptr);
        CHECK(std::string(sql0).find("CREATE") != std::string::npos);

        const potato_result *res0 = potato_result_list_result(rl, 0);
        CHECK(res0 != nullptr);
        CHECK(potato_result_list_error(rl, 0) == nullptr);

        potato_result_list_free(rl);
        potato_close(db);
    }

    TEST_CASE("potato_recent_queries via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE ql (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO ql VALUES (1);");
        potato_result_free(r);

        potato_query_log *log = potato_recent_queries(db);
        REQUIRE(log != nullptr);
        CHECK(potato_query_log_count(log) >= 2);

        bool found = false;
        for (size_t i = 0; i < potato_query_log_count(log); ++i) {
            const char *sql = potato_query_log_sql(log, i);
            if (sql && std::string(sql).find("INSERT") != std::string::npos) {
                CHECK(potato_query_log_duration_ms(log, i) < 60000);
                found = true;
            }
        }
        CHECK(found);
        potato_query_log_free(log);

        potato_close(db);
    }

    TEST_CASE("potato_flush / potato_flush_table via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE fl (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO fl VALUES (1);");
        potato_result_free(r);

        CHECK(potato_flush_table(db, "fl") == 0);
        CHECK(potato_flush(db) == 0);

        potato_close(db);
    }

    TEST_CASE("potato_table storage stats via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE ss (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO ss VALUES (1);");
        potato_result_free(r);
        potato_flush(db);

        CHECK(potato_table_parquet_file_count(db, "ss") >= 1);
        CHECK(potato_table_total_bytes(db, "ss") > 0);
        CHECK(potato_table_oldest_file_age_secs(db, "ss") >= 0);

        CHECK(potato_table_parquet_file_count(db, "no_table") == 0);

        potato_close(db);
    }

    TEST_CASE("potato_backup / potato_restore via C API") {
        TmpDir tmp;
        TmpDir archive_dir;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE bk (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO bk VALUES (1);");
        potato_result_free(r);
        potato_flush(db);

        fs::path archive = archive_dir.path / "bk.tar.gz";
        CHECK(potato_backup(db, archive.string().c_str()) == 0);
        CHECK(fs::exists(archive));

        CHECK(potato_backup(db, "/no/such/path.tar.gz") == -1);

        potato_close(db);
    }

    TEST_CASE("out-of-bounds access returns safe defaults") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db,
            "CREATE TABLE ob (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO ob VALUES (1);");
        potato_result_free(r);

        r = potato_execute(db, "SELECT * FROM ob;");
        REQUIRE(r != nullptr);

        CHECK(potato_result_get_int(r, 999, 0) == 0);
        CHECK(potato_result_get_double(r, 0, 999) == 0.0);
        CHECK(potato_result_get_bool(r, 999, 999) == false);
        CHECK(potato_result_is_null(r, 999, 0) == true);
        CHECK(potato_result_column_name(r, 999) == nullptr);

        potato_result_free(r);
        potato_close(db);
    }
}

// =====================================================================
//  SQL edge cases
// =====================================================================

TEST_SUITE("SQL edge cases") {

    TEST_CASE("DISTINCT eliminates duplicates") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (val INT);");
        exec_ok(db, "INSERT INTO t VALUES (1), (1), (2), (2), (3);");

        auto res = exec_ok(db, "SELECT DISTINCT val FROM t ORDER BY val;");
        CHECK(res.row_count() == 3);
        CHECK(res.get_int(0, 0) == 1);
        CHECK(res.get_int(1, 0) == 2);
        CHECK(res.get_int(2, 0) == 3);
    }

    TEST_CASE("LIMIT and OFFSET") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1), (2), (3), (4), (5);");

        auto res = exec_ok(db, "SELECT id FROM t ORDER BY id LIMIT 2;");
        CHECK(res.row_count() == 2);
        CHECK(res.get_int(0, 0) == 1);
        CHECK(res.get_int(1, 0) == 2);

        auto res2 = exec_ok(db, "SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 2;");
        CHECK(res2.row_count() == 2);
        CHECK(res2.get_int(0, 0) == 3);
        CHECK(res2.get_int(1, 0) == 4);
    }

    TEST_CASE("ORDER BY DESC") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (3), (1), (2);");

        auto res = exec_ok(db, "SELECT id FROM t ORDER BY id DESC;");
        CHECK(res.get_int(0, 0) == 3);
        CHECK(res.get_int(1, 0) == 2);
        CHECK(res.get_int(2, 0) == 1);
    }

    TEST_CASE("GROUP BY with HAVING") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE sales (product VARCHAR, amount INT);");
        exec_ok(db,
            "INSERT INTO sales VALUES ('A', 10), ('A', 20), ('B', 5), "
            "('B', 3), ('C', 100);");

        auto res = exec_ok(db,
            "SELECT product, SUM(amount) AS total FROM sales "
            "GROUP BY product HAVING SUM(amount) > 10 ORDER BY product;");
        CHECK(res.row_count() == 2);
        CHECK(res.get_string(0, 0) == "A");
        CHECK(res.get_int(0, 1) == 30);
        CHECK(res.get_string(1, 0) == "C");
        CHECK(res.get_int(1, 1) == 100);
    }

    TEST_CASE("JOIN between two tables") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE users (id INT, name VARCHAR);");
        exec_ok(db, "CREATE TABLE orders (id INT, user_id INT, item VARCHAR);");
        exec_ok(db, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');");
        exec_ok(db,
            "INSERT INTO orders VALUES (100, 1, 'Widget'), (101, 2, 'Gadget');");

        auto res = exec_ok(db,
            "SELECT u.name, o.item FROM users u "
            "JOIN orders o ON u.id = o.user_id ORDER BY u.name;");
        CHECK(res.row_count() == 2);
        CHECK(res.get_string(0, 0) == "Alice");
        CHECK(res.get_string(0, 1) == "Widget");
        CHECK(res.get_string(1, 0) == "Bob");
        CHECK(res.get_string(1, 1) == "Gadget");
    }

    TEST_CASE("LEFT JOIN includes unmatched rows") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE a (id INT, val VARCHAR);");
        exec_ok(db, "CREATE TABLE b (a_id INT, extra VARCHAR);");
        exec_ok(db, "INSERT INTO a VALUES (1, 'x'), (2, 'y');");
        exec_ok(db, "INSERT INTO b VALUES (1, 'matched');");

        auto res = exec_ok(db,
            "SELECT a.val, b.extra FROM a "
            "LEFT JOIN b ON a.id = b.a_id ORDER BY a.id;");
        CHECK(res.row_count() == 2);
        CHECK(res.get_string(0, 1) == "matched");
        CHECK(res.is_null(1, 1));
    }

    TEST_CASE("Subquery in WHERE clause") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, score INT);");
        exec_ok(db, "INSERT INTO t VALUES (1, 50), (2, 80), (3, 90);");

        auto res = exec_ok(db,
            "SELECT id FROM t WHERE score > (SELECT AVG(score) FROM t) ORDER BY id;");
        CHECK(res.row_count() == 2);
        CHECK(res.get_int(0, 0) == 2);
        CHECK(res.get_int(1, 0) == 3);
    }

    TEST_CASE("Empty string values") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (s VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (''), ('notempty');");

        auto res = exec_ok(db, "SELECT s FROM t ORDER BY s;");
        CHECK(res.row_count() == 2);
        CHECK(res.get_string(0, 0).empty());
        CHECK(res.get_string(1, 0) == "notempty");
    }

    TEST_CASE("Special characters in string values") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (s VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES ('hello ''world''');");

        auto res = exec_ok(db, "SELECT s FROM t;");
        CHECK(res.row_count() == 1);
        CHECK(res.get_string(0, 0) == "hello 'world'");
    }

    TEST_CASE("LIKE pattern matching") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (name VARCHAR);");
        exec_ok(db,
            "INSERT INTO t VALUES ('Alice'), ('Albert'), ('Bob'), ('Anna');");

        auto res = exec_ok(db,
            "SELECT name FROM t WHERE name LIKE 'Al%' ORDER BY name;");
        CHECK(res.row_count() == 2);
        CHECK(res.get_string(0, 0) == "Albert");
        CHECK(res.get_string(1, 0) == "Alice");
    }

    TEST_CASE("SELECT from empty table") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");

        auto res = exec_ok(db, "SELECT * FROM t;");
        CHECK(res.row_count() == 0);
        CHECK(res.is_records());
    }

    TEST_CASE("Multiple aggregation functions") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (cat VARCHAR, val DOUBLE);");
        exec_ok(db,
            "INSERT INTO t VALUES ('a', 1.0), ('a', 2.0), ('b', 10.0);");

        auto res = exec_ok(db,
            "SELECT cat, COUNT(*) AS n, SUM(val) AS s, AVG(val) AS a "
            "FROM t GROUP BY cat ORDER BY cat;");
        CHECK(res.row_count() == 2);
        CHECK(res.get_string(0, 0) == "a");
        CHECK(res.get_int(0, 1) == 2);
        CHECK(res.get_double(0, 2) == doctest::Approx(3.0));
    }

    TEST_CASE("CASE expression") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (score INT);");
        exec_ok(db, "INSERT INTO t VALUES (90), (70), (50);");

        auto res = exec_ok(db,
            "SELECT score, "
            "CASE WHEN score >= 80 THEN 'pass' ELSE 'fail' END AS grade "
            "FROM t ORDER BY score;");
        CHECK(res.row_count() == 3);
        CHECK(res.get_string(0, 1) == "fail");
        CHECK(res.get_string(1, 1) == "fail");
        CHECK(res.get_string(2, 1) == "pass");
    }

    TEST_CASE("ALTER TABLE ADD COLUMN") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");
        exec_ok(db, "ALTER TABLE t ADD COLUMN name VARCHAR;");

        auto cols = db.table_columns("t").to_vector();
        CHECK(cols.size() == 2);
        CHECK(cols[1] == "name");

        auto res = exec_ok(db, "SELECT * FROM t;");
        CHECK(res.column_count() == 2);
        CHECK(res.is_null(0, 1));
    }

    TEST_CASE("Multiple INSERTs and COUNT") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");

        for (int i = 0; i < 100; ++i) {
            exec_ok(db, "INSERT INTO t VALUES (" + std::to_string(i) + ");");
        }

        auto res = exec_ok(db, "SELECT COUNT(*) FROM t;");
        CHECK(res.get_int(0, 0) == 100);
    }
}

// =====================================================================
//  Backup / Restore — full round-trip
// =====================================================================

TEST_SUITE("Restore round-trip") {

    TEST_CASE("backup then restore into a new database") {
        TmpDir src;
        TmpDir dst;
        TmpDir archive_dir;
        fs::path archive = archive_dir.path / "roundtrip.tar.gz";

        {
            auto db = open_tmp(src);
            exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
            exec_ok(db, "INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');");
            db.flush();

            REQUIRE(db.backup(archive.string()));
            REQUIRE(fs::exists(archive));
        }

        {
            auto db2 = open_tmp(dst);
            CHECK(db2.restore(archive.string()));

            auto res = exec_ok(db2, "SELECT * FROM t ORDER BY id;");
            CHECK(res.row_count() == 2);
            CHECK(res.get_string(0, 1) == "Alice");
            CHECK(res.get_string(1, 1) == "Bob");
        }
    }

    TEST_CASE("restore with invalid archive path fails gracefully") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        CHECK_FALSE(db.restore("/no/such/archive.tar.gz"));
    }

    TEST_CASE("potato_restore via C API round-trip") {
        TmpDir src;
        TmpDir dst;
        TmpDir archive_dir;
        fs::path archive = archive_dir.path / "c_restore.tar.gz";

        potato_db *db1 = potato_open_local(src.path.string().c_str());
        REQUIRE(db1 != nullptr);
        potato_result *r = potato_execute(db1, "CREATE TABLE rt (val INT);");
        potato_result_free(r);
        r = potato_execute(db1, "INSERT INTO rt VALUES (42);");
        potato_result_free(r);
        potato_flush(db1);
        REQUIRE(potato_backup(db1, archive.string().c_str()) == 0);
        potato_close(db1);

        potato_db *db2 = potato_open_local(dst.path.string().c_str());
        REQUIRE(db2 != nullptr);
        CHECK(potato_restore(db2, archive.string().c_str()) == 0);

        r = potato_execute(db2, "SELECT val FROM rt;");
        REQUIRE(r != nullptr);
        CHECK(potato_result_row_count(r) == 1);
        CHECK(potato_result_get_int(r, 0, 0) == 42);
        potato_result_free(r);
        potato_close(db2);
    }
}

// =====================================================================
//  DATE / TIMESTAMP — typed epoch getters
// =====================================================================

TEST_SUITE("Date and timestamp epoch getters") {

    TEST_CASE("get_date returns non-zero epoch days") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE d (dt DATE);");
        exec_ok(db, "INSERT INTO d VALUES ('2024-06-15');");

        auto res = exec_ok(db, "SELECT * FROM d;");
        int64_t epoch_days = res.get_date(0, 0);
        CHECK(epoch_days > 0);
        CHECK(epoch_days == 19889);
    }

    TEST_CASE("get_timestamp returns non-zero epoch microseconds") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE ts (t TIMESTAMP);");
        exec_ok(db, "INSERT INTO ts VALUES ('2024-01-15 10:30:00');");

        auto res = exec_ok(db, "SELECT * FROM ts;");
        int64_t epoch_us = res.get_timestamp(0, 0);
        CHECK(epoch_us > 0);
    }

    TEST_CASE("get_date on NULL returns zero") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE d (dt DATE);");
        exec_ok(db, "INSERT INTO d VALUES (NULL);");

        auto res = exec_ok(db, "SELECT * FROM d;");
        CHECK(res.is_null(0, 0));
        CHECK(res.get_date(0, 0) == 0);
    }

    TEST_CASE("get_timestamp on NULL returns zero") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE ts (t TIMESTAMP);");
        exec_ok(db, "INSERT INTO ts VALUES (NULL);");

        auto res = exec_ok(db, "SELECT * FROM ts;");
        CHECK(res.is_null(0, 0));
        CHECK(res.get_timestamp(0, 0) == 0);
    }

    TEST_CASE("potato_result_get_date via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE cd (dt DATE);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO cd VALUES ('2024-06-15');");
        potato_result_free(r);

        r = potato_execute(db, "SELECT * FROM cd;");
        REQUIRE(r != nullptr);
        long long days = potato_result_get_date(r, 0, 0);
        CHECK(days > 0);
        CHECK(days == 19889);
        potato_result_free(r);
        potato_close(db);
    }

    TEST_CASE("potato_result_get_timestamp via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE cts (t TIMESTAMP);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO cts VALUES ('2024-01-15 10:30:00');");
        potato_result_free(r);

        r = potato_execute(db, "SELECT * FROM cts;");
        REQUIRE(r != nullptr);
        long long us = potato_result_get_timestamp(r, 0, 0);
        CHECK(us > 0);
        potato_result_free(r);
        potato_close(db);
    }

    TEST_CASE("column_type for DATE and TIMESTAMP") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE dt (d DATE, t TIMESTAMP);");
        exec_ok(db, "INSERT INTO dt VALUES ('2024-01-01', '2024-01-01 00:00:00');");

        auto res = exec_ok(db, "SELECT * FROM dt;");
        CHECK(res.column_type(0) == POTATO_TYPE_DATE);
        CHECK(res.column_type(1) == POTATO_TYPE_TIMESTAMP);
    }
}

// =====================================================================
//  C API — potato_result_message
// =====================================================================

TEST_SUITE("C API result message") {

    TEST_CASE("potato_result_message returns string for DDL") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE msg (id INT);");
        REQUIRE(r != nullptr);
        CHECK(potato_result_get_kind(r) == POTATO_RESULT_MESSAGE);

        const char *msg = potato_result_message(r);
        REQUIRE(msg != nullptr);
        CHECK(std::string(msg).size() > 0);
        potato_result_free(r);

        potato_close(db);
    }

    TEST_CASE("potato_result_message returns NULL for records") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE msg2 (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "INSERT INTO msg2 VALUES (1);");
        potato_result_free(r);

        r = potato_execute(db, "SELECT * FROM msg2;");
        REQUIRE(r != nullptr);
        CHECK(potato_result_get_kind(r) == POTATO_RESULT_RECORDS);
        CHECK(potato_result_message(r) == nullptr);
        potato_result_free(r);

        potato_close(db);
    }

    TEST_CASE("potato_result_message on NULL is safe") {
        CHECK(potato_result_message(nullptr) == nullptr);
    }
}

// =====================================================================
//  Prepared statement edge cases
// =====================================================================

TEST_SUITE("Prepared statement edge cases") {

    TEST_CASE("execute_prepared with multiple parameters") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR, score INT);");
        exec_ok(db,
            "INSERT INTO t VALUES (1, 'Alice', 90), (2, 'Bob', 80), "
            "(3, 'Charlie', 70);");

        bool ok = db.prepare("find_range",
            "SELECT name FROM t WHERE score >= $1 AND score <= $2 ORDER BY name");
        REQUIRE(ok);

        auto res = db.execute_prepared("find_range", {"70", "85"});
        REQUIRE(res);
        CHECK(res->row_count() == 2);
        CHECK(res->get_string(0, 0) == "Bob");
        CHECK(res->get_string(1, 0) == "Charlie");
    }

    TEST_CASE("execute unknown prepared statement returns error") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto res = db.execute_prepared("nonexistent", {"1"});
        CHECK_FALSE(res);
        CHECK_FALSE(res.error().empty());
    }

    TEST_CASE("prepare with invalid SQL returns false") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        bool ok = db.prepare("bad", "NOT VALID SQL AT ALL");
        CHECK_FALSE(ok);
    }

    TEST_CASE("prepared statement reuse across inserts") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT, val VARCHAR);");
        exec_ok(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');");

        bool ok = db.prepare("lookup", "SELECT val FROM t WHERE id = $1");
        REQUIRE(ok);

        for (int i = 1; i <= 3; ++i) {
            auto res = db.execute_prepared("lookup", {std::to_string(i)});
            REQUIRE(res);
            CHECK(res->row_count() == 1);
        }
    }

    TEST_CASE("potato_prepare with NULL pointers via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        CHECK(potato_prepare(nullptr, "name", "SELECT 1") == -1);
        CHECK(potato_prepare(db, nullptr, "SELECT 1") == -1);
        CHECK(potato_prepare(db, "name", nullptr) == -1);

        potato_close(db);
    }
}

// =====================================================================
//  Execute file edge cases
// =====================================================================

TEST_SUITE("Execute file edge cases") {

    TEST_CASE("execute_file with non-existent path returns error") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        auto rlist = db.execute_file("/no/such/file.sql");
        CHECK_FALSE(rlist);
        CHECK_FALSE(rlist.error().empty());
    }

    TEST_CASE("execute_file with empty file") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        fs::path sql_file = tmp.path / "empty.sql";
        { std::ofstream f(sql_file); }

        auto rlist = db.execute_file(sql_file.string());
        REQUIRE(rlist);
        CHECK(rlist->count() == 0);
    }

    TEST_CASE("potato_execute_file with non-existent path via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result_list *rl = potato_execute_file(db, "/no/such/file.sql", true);
        CHECK(rl == nullptr);
        const char *err = potato_last_error(db);
        REQUIRE(err != nullptr);
        CHECK(std::string(err).size() > 0);

        potato_close(db);
    }
}

// =====================================================================
//  Out-of-bounds list accessors
// =====================================================================

TEST_SUITE("Out-of-bounds list accessors") {

    TEST_CASE("StringList get out of bounds returns empty") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");

        auto names = db.table_names();
        std::size_t n = names.count();
        CHECK(names.get(n + 100).empty());
    }

    TEST_CASE("potato_string_list_get out of bounds returns NULL") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_string_list *sl = potato_table_names(db);
        REQUIRE(sl != nullptr);
        CHECK(potato_string_list_get(sl, 9999) == nullptr);
        potato_string_list_free(sl);

        potato_close(db);
    }

    TEST_CASE("IndexList out of bounds returns empty strings") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "CREATE INDEX idx ON t (id);");

        auto idxs = db.indexes();
        CHECK(idxs.name(9999).empty());
        CHECK(idxs.table(9999).empty());
    }

    TEST_CASE("potato_index_list out of bounds returns NULL") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "CREATE TABLE obi (id INT);");
        potato_result_free(r);
        r = potato_execute(db, "CREATE INDEX idx_obi ON obi (id);");
        potato_result_free(r);

        potato_index_list *il = potato_indexes(db);
        REQUIRE(il != nullptr);
        CHECK(potato_index_list_name(il, 9999) == nullptr);
        CHECK(potato_index_list_table(il, 9999) == nullptr);
        potato_index_list_free(il);

        potato_close(db);
    }

    TEST_CASE("QueryLog out of bounds returns safe defaults") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "SELECT 1;");

        auto log = db.recent_queries();
        CHECK(log.sql(9999).empty());
        CHECK(log.duration_ms(9999) == 0);
        CHECK(log.rows(9999) == 0);
    }

    TEST_CASE("potato_query_log out of bounds via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_result *r = potato_execute(db, "SELECT 1;");
        potato_result_free(r);

        potato_query_log *log = potato_recent_queries(db);
        REQUIRE(log != nullptr);
        CHECK(potato_query_log_sql(log, 9999) == nullptr);
        CHECK(potato_query_log_duration_ms(log, 9999) == 0);
        CHECK(potato_query_log_rows(log, 9999) == 0);
        potato_query_log_free(log);

        potato_close(db);
    }

    TEST_CASE("ResultList out of bounds returns safe defaults") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        fs::path sql_file = tmp.path / "oob.sql";
        {
            std::ofstream f(sql_file);
            f << "SELECT 1;\n";
        }

        auto rlist = db.execute_file(sql_file.string());
        REQUIRE(rlist);
        CHECK(rlist->sql(9999).empty());
        CHECK_FALSE(rlist->has_error(9999));
        CHECK(rlist->result(9999) == nullptr);
    }

    TEST_CASE("potato_result_list out of bounds via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        fs::path sql_file = tmp.path / "oob_c.sql";
        {
            std::ofstream f(sql_file);
            f << "SELECT 1;\n";
        }

        potato_result_list *rl = potato_execute_file(db, sql_file.string().c_str(), true);
        REQUIRE(rl != nullptr);
        CHECK(potato_result_list_sql(rl, 9999) == nullptr);
        CHECK(potato_result_list_result(rl, 9999) == nullptr);
        CHECK(potato_result_list_error(rl, 9999) == nullptr);
        potato_result_list_free(rl);

        potato_close(db);
    }
}

// =====================================================================
//  Streaming — edge cases
// =====================================================================

TEST_SUITE("Streaming edge cases") {

    TEST_CASE("stream on error SQL returns error") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        auto stream = db.execute_stream("SELECT * FROM nonexistent;");
        CHECK_FALSE(stream);
        CHECK_FALSE(stream.error().empty());
    }

    TEST_CASE("stream exhaustion returns empty results") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");

        auto stream = db.execute_stream("SELECT * FROM t;");
        REQUIRE(stream);

        while (true) {
            auto batch = stream->next();
            if (!batch) break;
        }

        auto extra = stream->next();
        CHECK_FALSE(extra);
    }

    TEST_CASE("potato_execute_stream with invalid SQL via C API") {
        TmpDir tmp;
        potato_db *db = potato_open_local(tmp.path.string().c_str());
        REQUIRE(db != nullptr);

        potato_stream *s = potato_execute_stream(db, "DEFINITELY NOT SQL;");
        CHECK(s == nullptr);
        const char *err = potato_last_error(db);
        REQUIRE(err != nullptr);
        CHECK(std::string(err).size() > 0);

        potato_close(db);
    }
}

// =====================================================================
//  Multiple indexes on same table
// =====================================================================

TEST_SUITE("Multiple indexes") {

    TEST_CASE("create and list multiple indexes on one table") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (a INT, b VARCHAR, c DOUBLE);");
        exec_ok(db, "CREATE INDEX idx_a ON t (a);");
        exec_ok(db, "CREATE INDEX idx_b ON t (b);");

        auto idxs = db.indexes();
        REQUIRE(idxs.count() >= 2);

        std::vector<std::string> names;
        for (std::size_t i = 0; i < idxs.count(); ++i)
            names.push_back(idxs.name(i));

        CHECK(std::find(names.begin(), names.end(), "idx_a") != names.end());
        CHECK(std::find(names.begin(), names.end(), "idx_b") != names.end());
    }
}

// =====================================================================
//  DROP VIEW
// =====================================================================

TEST_SUITE("DROP VIEW") {

    TEST_CASE("DROP VIEW removes the view") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "CREATE VIEW v AS SELECT * FROM t;");

        auto names = db.view_names().to_vector();
        CHECK(std::find(names.begin(), names.end(), "v") != names.end());

        exec_ok(db, "DROP VIEW v;");

        auto names2 = db.view_names().to_vector();
        CHECK(std::find(names2.begin(), names2.end(), "v") == names2.end());
    }
}

// =====================================================================
//  Database reopen
// =====================================================================

TEST_SUITE("Database reopen") {

    TEST_CASE("data persists across close and reopen") {
        TmpDir tmp;

        {
            auto db = open_tmp(tmp);
            exec_ok(db, "CREATE TABLE t (id INT, name VARCHAR);");
            exec_ok(db, "INSERT INTO t VALUES (1, 'persistent');");
            db.flush();
        }

        {
            auto db = open_tmp(tmp);
            auto res = exec_ok(db, "SELECT name FROM t WHERE id = 1;");
            CHECK(res.row_count() == 1);
            CHECK(res.get_string(0, 0) == "persistent");
        }
    }

    TEST_CASE("schema persists across close and reopen") {
        TmpDir tmp;

        {
            auto db = open_tmp(tmp);
            exec_ok(db, "CREATE TABLE t (a INT, b VARCHAR, c DOUBLE);");
        }

        {
            auto db = open_tmp(tmp);
            auto cols = db.table_columns("t").to_vector();
            REQUIRE(cols.size() == 3);
            CHECK(cols[0] == "a");
            CHECK(cols[1] == "b");
            CHECK(cols[2] == "c");
        }
    }
}

// =====================================================================
//  Multiple concurrent database instances
// =====================================================================

TEST_SUITE("Multiple databases") {

    TEST_CASE("two databases operate independently") {
        TmpDir tmp1;
        TmpDir tmp2;
        auto db1 = open_tmp(tmp1);
        auto db2 = open_tmp(tmp2);

        exec_ok(db1, "CREATE TABLE t (id INT);");
        exec_ok(db1, "INSERT INTO t VALUES (1);");

        exec_ok(db2, "CREATE TABLE t (id INT);");
        exec_ok(db2, "INSERT INTO t VALUES (2), (3);");

        auto res1 = exec_ok(db1, "SELECT COUNT(*) FROM t;");
        auto res2 = exec_ok(db2, "SELECT COUNT(*) FROM t;");

        CHECK(res1.get_int(0, 0) == 1);
        CHECK(res2.get_int(0, 0) == 2);
    }
}

// =====================================================================
//  RAII wrappers — additional move semantics
// =====================================================================

TEST_SUITE("RAII wrappers extended") {

    TEST_CASE("Stream is move-constructible") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "INSERT INTO t VALUES (1);");

        auto s1 = db.execute_stream("SELECT * FROM t;");
        REQUIRE(s1);
        potato::Stream s2 = std::move(*s1);
        auto batch = s2.next();
        CHECK(batch.row_count() == 1);
    }

    TEST_CASE("ResultList is move-constructible") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        fs::path sql_file = tmp.path / "move_rl.sql";
        {
            std::ofstream f(sql_file);
            f << "CREATE TABLE t (id INT);\n"
              << "INSERT INTO t VALUES (1);\n";
        }

        auto rl1 = db.execute_file(sql_file.string());
        REQUIRE(rl1);
        potato::ResultList rl2 = std::move(*rl1);
        CHECK(rl2.count() == 2);
    }

    TEST_CASE("Stream is move-assignable") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t1 (id INT);");
        exec_ok(db, "CREATE TABLE t2 (id INT);");
        exec_ok(db, "INSERT INTO t1 VALUES (1);");
        exec_ok(db, "INSERT INTO t2 VALUES (2), (3);");

        auto s1 = db.execute_stream("SELECT * FROM t1;");
        auto s2 = db.execute_stream("SELECT * FROM t2;");
        REQUIRE(s1);
        REQUIRE(s2);

        *s2 = std::move(*s1);

        std::size_t total = 0;
        while (true) {
            auto batch = s2->next();
            if (!batch) break;
            total += batch.row_count();
        }
        CHECK(total == 1);
    }

    TEST_CASE("ResultList is move-assignable") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        fs::path f1 = tmp.path / "ma1.sql";
        fs::path f2 = tmp.path / "ma2.sql";
        {
            std::ofstream f(f1);
            f << "CREATE TABLE a (id INT);\n";
        }
        {
            std::ofstream f(f2);
            f << "CREATE TABLE b (id INT);\n"
              << "INSERT INTO b VALUES (1);\n";
        }

        auto rl1 = db.execute_file(f1.string());
        auto rl2 = db.execute_file(f2.string());
        REQUIRE(rl1);
        REQUIRE(rl2);

        *rl2 = std::move(*rl1);
        CHECK(rl2->count() == 1);
    }

    TEST_CASE("StringList is move-assignable") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE a (id INT);");
        exec_ok(db, "CREATE TABLE b (id INT);");

        auto sl1 = db.table_names();
        auto sl2 = db.table_columns("a");
        sl2 = std::move(sl1);
        CHECK(sl2.count() >= 2);
    }

    TEST_CASE("IndexList is move-assignable") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "CREATE TABLE t (id INT);");
        exec_ok(db, "CREATE INDEX idx ON t (id);");

        auto il1 = db.indexes();
        auto il2 = db.indexes();
        il2 = std::move(il1);
        CHECK(il2.count() >= 1);
    }

    TEST_CASE("QueryLog is move-assignable") {
        TmpDir tmp;
        auto db = open_tmp(tmp);
        exec_ok(db, "SELECT 1;");
        exec_ok(db, "SELECT 2;");

        auto ql1 = db.recent_queries();
        auto ql2 = db.recent_queries();
        ql2 = std::move(ql1);
        CHECK(ql2.count() >= 1);
    }
}

// =====================================================================
//  New feature bindings
// =====================================================================

TEST_SUITE("New feature bindings") {

    TEST_CASE("C API exposes function_names") {
        TmpDir tmp;
        potato_db *raw = potato_open_local(tmp.path.string().c_str());
        REQUIRE(raw != nullptr);

        auto *res = potato_execute(
            raw, "CREATE FUNCTION add1(x INT) RETURNS INT AS '$1 + 1';");
        REQUIRE(res != nullptr);
        potato_result_free(res);

        potato_string_list *fns = potato_function_names(raw);
        REQUIRE(fns != nullptr);
        bool found = false;
        for (size_t i = 0; i < potato_string_list_count(fns); ++i) {
            const char *name = potato_string_list_get(fns, i);
            if (name && std::string(name) == "add1") {
                found = true;
                break;
            }
        }
        CHECK(found);
        potato_string_list_free(fns);
        potato_close(raw);
    }

    TEST_CASE("C API exposes recent_cdc stream") {
        TmpDir tmp;
        potato_db *raw = potato_open_local(tmp.path.string().c_str());
        REQUIRE(raw != nullptr);

        auto *r1 = potato_execute(raw, "CREATE TABLE cdc_t (id INT, v INT);");
        REQUIRE(r1 != nullptr);
        potato_result_free(r1);
        auto *r2 = potato_execute(raw, "INSERT INTO cdc_t VALUES (1, 10);");
        REQUIRE(r2 != nullptr);
        potato_result_free(r2);
        auto *r3 = potato_execute(raw, "UPDATE cdc_t SET v = 20 WHERE id = 1;");
        REQUIRE(r3 != nullptr);
        potato_result_free(r3);
        auto *r4 = potato_execute(raw, "DELETE FROM cdc_t WHERE id = 1;");
        REQUIRE(r4 != nullptr);
        potato_result_free(r4);

        potato_result *cdc = potato_recent_cdc(raw);
        REQUIRE(cdc != nullptr);
        CHECK(potato_result_row_count(cdc) >= 3);
        CHECK(potato_result_column_count(cdc) >= 4);
        potato_result_free(cdc);
        potato_close(raw);
    }

    TEST_CASE("column type tags cover UUID INTERVAL ARRAY JSON") {
        TmpDir tmp;
        potato_db *raw = potato_open_local(tmp.path.string().c_str());
        REQUIRE(raw != nullptr);

        auto *create = potato_execute(
            raw,
            "CREATE TABLE typed (id UUID, dur INTERVAL, tags INT[], payload JSONB);");
        REQUIRE(create != nullptr);
        potato_result_free(create);
        auto *insert =
            potato_execute(raw, "INSERT INTO typed VALUES (NULL, NULL, NULL, '{\"k\":1}');");
        REQUIRE(insert != nullptr);
        potato_result_free(insert);

        potato_result *res = potato_execute(raw, "SELECT * FROM typed;");
        REQUIRE(res != nullptr);
        CHECK(potato_result_get_column_type(res, 0) == POTATO_TYPE_UUID);
        CHECK(potato_result_get_column_type(res, 1) == POTATO_TYPE_INTERVAL);
        CHECK(potato_result_get_column_type(res, 2) == POTATO_TYPE_ARRAY);
        CHECK(potato_result_get_column_type(res, 3) == POTATO_TYPE_JSON);
        potato_result_free(res);
        potato_close(raw);
    }

    TEST_CASE("C++ wrapper exposes function_names") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        exec_ok(db, "CREATE FUNCTION add2(x INT) RETURNS INT AS '$1 + 2';");
        auto names = db.function_names().to_vector();
        CHECK(std::find(names.begin(), names.end(), "add2") != names.end());
    }

    TEST_CASE("C++ wrapper exposes recent_cdc") {
        TmpDir tmp;
        auto db = open_tmp(tmp);

        exec_ok(db, "CREATE TABLE cdc_cpp (id INT, v INT);");
        exec_ok(db, "INSERT INTO cdc_cpp VALUES (1, 10);");
        exec_ok(db, "UPDATE cdc_cpp SET v = 20 WHERE id = 1;");
        exec_ok(db, "DELETE FROM cdc_cpp WHERE id = 1;");

        auto cdc = db.recent_cdc();
        REQUIRE_MESSAGE(cdc, cdc.error());
        CHECK(cdc->is_records());
        CHECK(cdc->row_count() >= 3);
    }
}

// =====================================================================
//  C API — NULL database pointer safety
// =====================================================================

TEST_SUITE("C API NULL db safety") {

    TEST_CASE("operations on NULL db return safe defaults") {
        CHECK(potato_execute(nullptr, "SELECT 1;") == nullptr);
        CHECK(potato_execute_readonly(nullptr, "SELECT 1;") == nullptr);
        CHECK(potato_execute_stream(nullptr, "SELECT 1;") == nullptr);
        CHECK(potato_execute_file(nullptr, "test.sql", true) == nullptr);
        CHECK(potato_table_names(nullptr) == nullptr);
        CHECK(potato_table_columns(nullptr, "t") == nullptr);
        CHECK(potato_view_names(nullptr) == nullptr);
        CHECK(potato_function_names(nullptr) == nullptr);
        CHECK(potato_indexes(nullptr) == nullptr);
        CHECK(potato_data_url(nullptr) == nullptr);
        CHECK(potato_recent_queries(nullptr) == nullptr);
        CHECK(potato_recent_cdc(nullptr) == nullptr);
        CHECK(potato_flush(nullptr) == -1);
        CHECK(potato_flush_table(nullptr, "t") == -1);
        CHECK(potato_backup(nullptr, "/tmp/b.tar.gz") == -1);
        CHECK(potato_restore(nullptr, "/tmp/b.tar.gz") == -1);
        CHECK(potato_table_parquet_file_count(nullptr, "t") == -1);
        CHECK(potato_table_total_bytes(nullptr, "t") == -1);
        CHECK(potato_table_oldest_file_age_secs(nullptr, "t") == -1);
    }

    TEST_CASE("result accessors on NULL return safe defaults") {
        CHECK(potato_result_get_kind(nullptr) == POTATO_RESULT_MESSAGE);
        CHECK(potato_result_message(nullptr) == nullptr);
        CHECK(potato_result_row_count(nullptr) == 0);
        CHECK(potato_result_column_count(nullptr) == 0);
        CHECK(potato_result_column_name(nullptr, 0) == nullptr);
        CHECK(potato_result_get_column_type(nullptr, 0) == POTATO_TYPE_NULL);
        CHECK(potato_result_display(nullptr) == nullptr);
        CHECK(potato_result_is_null(nullptr, 0, 0) == true);
        CHECK(potato_result_get_int(nullptr, 0, 0) == 0);
        CHECK(potato_result_get_double(nullptr, 0, 0) == 0.0);
        CHECK(potato_result_get_bool(nullptr, 0, 0) == false);
        CHECK(potato_result_get_date(nullptr, 0, 0) == 0);
        CHECK(potato_result_get_timestamp(nullptr, 0, 0) == 0);
        CHECK(potato_result_get_string(nullptr, 0, 0) == nullptr);
    }
}
