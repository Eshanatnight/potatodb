/**
 * Basic CRUD operations via the PotatoDB C++ API.
 *
 * Demonstrates: open, CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE.
 *
 * Build:
 *   cargo build --release -p potatodb-ffi
 *   g++ -std=c++17 -fno-exceptions -I../include basic.cpp \
 *       -L../../../target/release -lpotatodb_ffi \
 *       -lpthread -ldl -lm -o basic
 */

#include "potatodb.hpp"

#include <cstdlib>
#include <iostream>

/// Helper: execute SQL, print result or abort on error.
static bool run(potato::Database &db, const char *label, const std::string &sql) {
    std::cout << "── " << label << " ──\n";
    auto res = db.execute(sql);
    if (!res) {
        std::cerr << "  error: " << res.error() << "\n";
        return false;
    }
    if (res->is_message())
        std::cout << "  " << res->message() << "\n\n";
    else
        std::cout << res->display() << "\n\n";
    return true;
}

int main() {
    // ── Open database ─────────────────────────────────────────
    auto db = potato::Database::open("./ffi_basic_data");
    if (!db) {
        std::cerr << "open failed: " << db.error() << "\n";
        return EXIT_FAILURE;
    }

    // ── Create table ──────────────────────────────────────────
    if (!run(*db, "CREATE TABLE",
             "CREATE TABLE IF NOT EXISTS users "
             "(id INT, name VARCHAR, email VARCHAR, age INT);"))
        return EXIT_FAILURE;

    // ── Insert rows ───────────────────────────────────────────
    if (!run(*db, "INSERT single row",
             "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 32);"))
        return EXIT_FAILURE;

    if (!run(*db, "INSERT multiple rows",
             "INSERT INTO users VALUES "
             "(2, 'Bob',     'bob@example.com',     28), "
             "(3, 'Charlie', 'charlie@example.com', 45), "
             "(4, 'Diana',   'diana@example.com',   36), "
             "(5, 'Eve',     'eve@example.com',     24);"))
        return EXIT_FAILURE;

    // ── Select all ────────────────────────────────────────────
    if (!run(*db, "SELECT *", "SELECT * FROM users ORDER BY id;"))
        return EXIT_FAILURE;

    // ── Filtering ─────────────────────────────────────────────
    if (!run(*db, "WHERE + ORDER BY",
             "SELECT name, age FROM users WHERE age > 30 ORDER BY age DESC;"))
        return EXIT_FAILURE;

    if (!run(*db, "LIMIT + OFFSET",
             "SELECT name FROM users ORDER BY id LIMIT 2 OFFSET 1;"))
        return EXIT_FAILURE;

    // ── Aggregations ──────────────────────────────────────────
    if (!run(*db, "Aggregations",
             "SELECT COUNT(*) AS total, "
             "MIN(age) AS youngest, MAX(age) AS oldest, AVG(age) AS avg_age "
             "FROM users;"))
        return EXIT_FAILURE;

    // ── Update ────────────────────────────────────────────────
    if (!run(*db, "UPDATE",
             "UPDATE users SET email = 'alice@newdomain.com' WHERE name = 'Alice';"))
        return EXIT_FAILURE;

    if (!run(*db, "Verify UPDATE",
             "SELECT name, email FROM users WHERE name = 'Alice';"))
        return EXIT_FAILURE;

    // ── Delete ────────────────────────────────────────────────
    if (!run(*db, "DELETE", "DELETE FROM users WHERE name = 'Eve';"))
        return EXIT_FAILURE;

    if (!run(*db, "After DELETE", "SELECT * FROM users ORDER BY id;"))
        return EXIT_FAILURE;

    // ── Column-level access ───────────────────────────────────
    {
        auto res = db->execute("SELECT id, name, age FROM users ORDER BY id;");
        if (!res) {
            std::cerr << "query error: " << res.error() << "\n";
            return EXIT_FAILURE;
        }
        std::cout << "── Column-level access ──\n";
        std::cout << "Columns:";
        for (size_t c = 0; c < res->column_count(); ++c)
            std::cout << " " << res->column_name(c);
        std::cout << "\n";
        for (size_t r = 0; r < res->row_count(); ++r)
            std::cout << "  id=" << res->get_int(r, 0)
                      << "  name=" << res->get_string(r, 1)
                      << "  age=" << res->get_int(r, 2) << "\n";
        std::cout << "\n";
    }

    // ── Data URL ──────────────────────────────────────────────
    std::cout << "── Data URL ──\n"
              << "  " << db->data_url() << "\n\n";

    // ── Metadata introspection ────────────────────────────────
    {
        std::cout << "── Table names ──\n";
        auto tables = db->table_names();
        for (size_t i = 0; i < tables.count(); ++i)
            std::cout << "  " << tables.get(i) << "\n";
        std::cout << "\n";

        std::cout << "── Columns of 'users' ──\n";
        auto cols = db->table_columns("users");
        for (size_t i = 0; i < cols.count(); ++i)
            std::cout << "  " << cols.get(i) << "\n";
        std::cout << "\n";

        std::cout << "── View names ──\n";
        auto views = db->view_names();
        for (size_t i = 0; i < views.count(); ++i)
            std::cout << "  " << views.get(i) << "\n";
        std::cout << "(total: " << views.count() << ")\n\n";

        std::cout << "── Indexes ──\n";
        auto idxs = db->indexes();
        for (size_t i = 0; i < idxs.count(); ++i)
            std::cout << "  " << idxs.name(i) << " on " << idxs.table(i) << "\n";
        std::cout << "(total: " << idxs.count() << ")\n\n";

        std::cout << "── In transaction? ──\n  "
                  << (db->in_transaction() ? "yes" : "no") << "\n\n";
    }

    // ── Date / timestamp columns ─────────────────────────────
    run(*db, "CREATE events table",
        "CREATE TABLE IF NOT EXISTS events "
        "(id INT, name VARCHAR, event_date DATE, event_ts TIMESTAMP);");
    run(*db, "INSERT events",
        "INSERT INTO events VALUES "
        "(1, 'Launch', '2025-01-15', '2025-01-15 09:30:00'), "
        "(2, 'Update', '2025-06-01', '2025-06-01 14:00:00');");
    {
        auto res = db->execute("SELECT * FROM events ORDER BY id;");
        if (res) {
            std::cout << "── Date/timestamp via get_string ──\n";
            for (size_t r = 0; r < res->row_count(); ++r)
                std::cout << "  " << res->get_string(r, 1)
                          << " date=" << res->get_string(r, 2)
                          << " ts=" << res->get_string(r, 3) << "\n";

            std::cout << "── Date/timestamp raw values ──\n";
            for (size_t r = 0; r < res->row_count(); ++r)
                std::cout << "  date_epoch=" << res->get_date(r, 2)
                          << " ts_us=" << res->get_timestamp(r, 3) << "\n";
            std::cout << "\n";
        }
    }
    run(*db, "DROP events", "DROP TABLE events;");

    // ── Streaming results ─────────────────────────────────────
    {
        std::cout << "── Streaming query ──\n";
        auto stream = db->execute_stream("SELECT * FROM users ORDER BY id;");
        if (!stream) {
            std::cerr << "  stream error: " << stream.error() << "\n";
        } else if (stream->is_message()) {
            std::cout << "  message: " << stream->message() << "\n";
        } else {
            size_t batch_num = 0;
            while (true) {
                auto batch = stream->next();
                if (!batch) break;
                ++batch_num;
                std::cout << "  batch " << batch_num
                          << ": " << batch.row_count() << " rows\n";
            }
            std::cout << "  total batches: " << batch_num << "\n\n";
        }
    }

    // ── Recent queries ────────────────────────────────────────
    {
        std::cout << "── Recent queries ──\n";
        auto log = db->recent_queries();
        size_t n = log.count();
        size_t show = n > 5 ? 5 : n;
        for (size_t i = 0; i < show; ++i)
            std::cout << "  [" << log.duration_ms(i) << "ms, "
                      << log.rows(i) << " rows] "
                      << log.sql(i) << "\n";
        if (n > show) std::cout << "  ... and " << (n - show) << " more\n";
        std::cout << "\n";
    }

    // ── Cleanup ───────────────────────────────────────────────
    if (!run(*db, "DROP TABLE", "DROP TABLE users;"))
        return EXIT_FAILURE;

    std::cout << "All basic operations completed successfully.\n";
    return EXIT_SUCCESS;
}
