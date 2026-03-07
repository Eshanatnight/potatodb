/**
 * Example: using the PotatoDB C++ API (error-as-values, no exceptions).
 *
 * Build with CMake (see CMakeLists.txt) or manually:
 *
 *   cargo build --release -p potatodb-ffi
 *   g++ -std=c++17 -fno-exceptions -I../include main.cpp \
 *       -L../../../target/release -lpotatodb_ffi \
 *       -lpthread -ldl -lm -o example
 */

#include "potatodb.hpp"

#include <cstdlib>
#include <iostream>

#define TRY(expr)                                        \
    do {                                                 \
        auto _r = (expr);                                \
        if (!_r) {                                       \
            std::cerr << "error: " << _r.error() << "\n";\
            return EXIT_FAILURE;                         \
        }                                                \
    } while (0)

int main() {
    auto db = potato::Database::open("./example_data");
    if (!db) {
        std::cerr << "open failed: " << db.error() << "\n";
        return EXIT_FAILURE;
    }

    TRY(db->execute("CREATE TABLE IF NOT EXISTS users "
                     "(id INT, name VARCHAR, email VARCHAR);"));

    if (!db->prepare("ins_user", "INSERT INTO users VALUES ($1, $2, $3);")) {
        std::cerr << "prepare failed: " << db->last_error() << "\n";
        return EXIT_FAILURE;
    }
    TRY(db->execute_prepared("ins_user", {"1", "'Alice'", "'alice@example.com'"}));
    TRY(db->execute_prepared("ins_user", {"2", "'Bob'", "'bob@example.com'"}));
    TRY(db->execute_prepared("ins_user", {"3", "'Carol'", "'carol@example.com'"}));

    if (!db->flush_table("users")) {
        std::cerr << "flush failed: " << db->last_error() << "\n";
        return EXIT_FAILURE;
    }

    auto file_count = db->parquet_file_count("users");
    auto total_bytes = db->table_total_bytes("users");
    if (!file_count || !total_bytes) {
        std::cerr << "table stats failed: "
                  << (!file_count ? file_count.error() : total_bytes.error()) << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "users parquet files=" << *file_count
              << " bytes=" << *total_bytes << "\n";

    auto res = db->execute_readonly("SELECT * FROM users ORDER BY id;");
    if (!res) {
        std::cerr << "query failed: " << res.error() << "\n";
        return EXIT_FAILURE;
    }

    std::cout << "--- Formatted table ---\n";
    std::cout << res->display() << "\n\n";

    std::cout << "--- Column-level access ---\n";
    std::cout << "Columns: " << res->column_count() << "\n";
    for (size_t c = 0; c < res->column_count(); ++c)
        std::cout << "  [" << c << "] " << res->column_name(c) << "\n";

    std::cout << "\nRows: " << res->row_count() << "\n";
    for (size_t r = 0; r < res->row_count(); ++r) {
        std::cout << "  id=" << res->get_int(r, 0)
                  << "  name=" << res->get_string(r, 1)
                  << "  email=" << res->get_string(r, 2) << "\n";
    }

    auto agg = db->execute("SELECT COUNT(*) AS cnt, "
                            "MIN(id) AS min_id, MAX(id) AS max_id "
                            "FROM users;");
    if (!agg) {
        std::cerr << "aggregation failed: " << agg.error() << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "\n--- Aggregation ---\n";
    std::cout << agg->display() << "\n";

    if (!db->backup("./example_data_backup.tar.gz")) {
        std::cerr << "backup failed: " << db->last_error() << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "Backup written to ./example_data_backup.tar.gz\n";

    TRY(db->execute("DROP TABLE users;"));
    std::cout << "Table dropped.\n";

    return EXIT_SUCCESS;
}
