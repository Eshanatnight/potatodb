/**
 * Prepared statements via the PotatoDB C++ API.
 *
 * Demonstrates: potato_prepare, potato_execute_prepared, parameterised
 * INSERTs and SELECTs using $1, $2, ... placeholders.
 *
 * Build:
 *   cargo build --release -p potatodb-ffi
 *   g++ -std=c++17 -fno-exceptions -I../include prepared.cpp \
 *       -L../../../target/release -lpotatodb_ffi \
 *       -lpthread -ldl -lm -o prepared
 */

#include "potatodb.hpp"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

int main() {
    auto db = potato::Database::open("./ffi_prepared_data");
    if (!db) {
        std::cerr << "open failed: " << db.error() << "\n";
        return EXIT_FAILURE;
    }

    // ── Create table ──────────────────────────────────────────
    auto cr = db->execute(
        "CREATE TABLE IF NOT EXISTS products "
        "(id INT, name VARCHAR, price DOUBLE, in_stock BOOLEAN);");
    if (!cr) {
        std::cerr << "create table: " << cr.error() << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "Table created.\n\n";

    // ── Prepare an INSERT statement ───────────────────────────
    if (!db->prepare("ins_product",
                     "INSERT INTO products VALUES ($1, $2, $3, $4);")) {
        std::cerr << "prepare failed: " << db->last_error() << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "Prepared statement 'ins_product'.\n";

    // ── Execute the prepared INSERT with different parameters ─
    struct Product {
        std::string id, name, price, in_stock;
    };
    std::vector<Product> items = {
        {"1", "'Widget'",   "9.99",  "true"},
        {"2", "'Gadget'",   "24.50", "true"},
        {"3", "'Doohickey'","3.75",  "false"},
        {"4", "'Thingamajig'","15.00","true"},
        {"5", "'Whatsit'",  "7.25",  "false"},
    };

    for (auto &p : items) {
        auto res = db->execute_prepared("ins_product",
                                        {p.id, p.name, p.price, p.in_stock});
        if (!res) {
            std::cerr << "insert " << p.name << ": " << res.error() << "\n";
            return EXIT_FAILURE;
        }
    }
    std::cout << "Inserted " << items.size() << " products.\n\n";

    // ── Verify ────────────────────────────────────────────────
    {
        auto res = db->execute("SELECT * FROM products ORDER BY id;");
        if (!res) {
            std::cerr << "select: " << res.error() << "\n";
            return EXIT_FAILURE;
        }
        std::cout << "── All products ──\n" << res->display() << "\n\n";
    }

    // ── Prepare a parameterised SELECT ────────────────────────
    if (!db->prepare("find_cheap",
                     "SELECT name, price FROM products "
                     "WHERE price < $1 ORDER BY price;")) {
        std::cerr << "prepare select: " << db->last_error() << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "Prepared statement 'find_cheap'.\n";

    {
        auto res = db->execute_prepared("find_cheap", {"10.00"});
        if (!res) {
            std::cerr << "query: " << res.error() << "\n";
            return EXIT_FAILURE;
        }
        std::cout << "── Products under $10 ──\n" << res->display() << "\n\n";

        std::cout << "Row-by-row:\n";
        for (size_t r = 0; r < res->row_count(); ++r) {
            std::cout << "  " << res->get_string(r, 0)
                      << " — $" << res->get_double(r, 1) << "\n";
        }
        std::cout << "\n";
    }

    // ── Cleanup ───────────────────────────────────────────────
    auto drop = db->execute("DROP TABLE products;");
    if (!drop) {
        std::cerr << "drop: " << drop.error() << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "Table dropped. Done.\n";
    return EXIT_SUCCESS;
}
