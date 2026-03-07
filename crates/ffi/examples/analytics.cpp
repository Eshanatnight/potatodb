/**
 * Analytical queries via the PotatoDB C++ API.
 *
 * Demonstrates: JOINs, GROUP BY + HAVING, CTEs, window functions,
 * aggregations, CASE expressions, and subqueries.
 *
 * Build:
 *   cargo build --release -p potatodb-ffi
 *   g++ -std=c++17 -fno-exceptions -I../include analytics.cpp \
 *       -L../../../target/release -lpotatodb_ffi \
 *       -lpthread -ldl -lm -o analytics
 */

#include "potatodb.hpp"

#include <cstdlib>
#include <iostream>
#include <string>

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
    auto db = potato::Database::open("./ffi_analytics_data");
    if (!db) {
        std::cerr << "open: " << db.error() << "\n";
        return EXIT_FAILURE;
    }

    // ── Schema setup ──────────────────────────────────────────
    run(*db, "Create departments",
        "CREATE TABLE IF NOT EXISTS departments "
        "(dept_id INT, dept_name VARCHAR);");

    run(*db, "Create employees",
        "CREATE TABLE IF NOT EXISTS employees "
        "(emp_id INT, name VARCHAR, dept_id INT, salary DOUBLE, hire_year INT);");

    // ── Seed data ─────────────────────────────────────────────
    run(*db, "Insert departments",
        "INSERT INTO departments VALUES "
        "(1, 'Engineering'), (2, 'Marketing'), (3, 'Sales');");

    run(*db, "Insert employees",
        "INSERT INTO employees VALUES "
        "(101, 'Alice',   1, 120000, 2019), "
        "(102, 'Bob',     1, 110000, 2020), "
        "(103, 'Charlie', 2,  90000, 2018), "
        "(104, 'Diana',   2,  95000, 2021), "
        "(105, 'Eve',     3,  85000, 2020), "
        "(106, 'Frank',   3,  88000, 2022), "
        "(107, 'Grace',   1, 130000, 2017), "
        "(108, 'Hank',    3,  92000, 2019);");

    // ── INNER JOIN ────────────────────────────────────────────
    run(*db, "INNER JOIN",
        "SELECT e.name, d.dept_name, e.salary "
        "FROM employees e "
        "INNER JOIN departments d ON e.dept_id = d.dept_id "
        "ORDER BY e.salary DESC;");

    // ── GROUP BY + HAVING ─────────────────────────────────────
    run(*db, "GROUP BY + HAVING",
        "SELECT d.dept_name, "
        "  COUNT(*) AS headcount, "
        "  ROUND(AVG(e.salary), 2) AS avg_salary "
        "FROM employees e "
        "INNER JOIN departments d ON e.dept_id = d.dept_id "
        "GROUP BY d.dept_name "
        "HAVING COUNT(*) >= 2 "
        "ORDER BY avg_salary DESC;");

    // ── CTE (WITH) ────────────────────────────────────────────
    run(*db, "CTE — department stats",
        "WITH dept_stats AS ( "
        "  SELECT dept_id, "
        "    AVG(salary) AS avg_sal, "
        "    MAX(salary) AS max_sal "
        "  FROM employees "
        "  GROUP BY dept_id "
        ") "
        "SELECT d.dept_name, "
        "  ROUND(s.avg_sal, 2) AS avg_salary, "
        "  s.max_sal AS top_salary "
        "FROM dept_stats s "
        "INNER JOIN departments d ON s.dept_id = d.dept_id "
        "ORDER BY avg_salary DESC;");

    // ── CASE expression ───────────────────────────────────────
    run(*db, "CASE expression",
        "SELECT name, salary, "
        "  CASE "
        "    WHEN salary >= 120000 THEN 'Senior' "
        "    WHEN salary >= 100000 THEN 'Mid' "
        "    ELSE 'Junior' "
        "  END AS band "
        "FROM employees ORDER BY salary DESC;");

    // ── Scalar subquery ───────────────────────────────────────
    run(*db, "Scalar subquery — above average",
        "SELECT name, salary "
        "FROM employees "
        "WHERE salary > (SELECT AVG(salary) FROM employees) "
        "ORDER BY salary DESC;");

    // ── Column-level iteration ────────────────────────────────
    {
        auto res = db->execute(
            "SELECT d.dept_name, SUM(e.salary) AS total "
            "FROM employees e "
            "INNER JOIN departments d ON e.dept_id = d.dept_id "
            "GROUP BY d.dept_name "
            "ORDER BY total DESC;");
        if (!res) {
            std::cerr << "dept totals: " << res.error() << "\n";
            return EXIT_FAILURE;
        }
        std::cout << "── Department salary totals (column access) ──\n";
        for (size_t r = 0; r < res->row_count(); ++r) {
            std::cout << "  " << res->get_string(r, 0)
                      << ": $" << res->get_double(r, 1) << "\n";
        }
        std::cout << "\n";
    }

    // ── Cleanup ───────────────────────────────────────────────
    run(*db, "Drop employees",  "DROP TABLE employees;");
    run(*db, "Drop departments", "DROP TABLE departments;");

    std::cout << "All analytics examples completed successfully.\n";
    return EXIT_SUCCESS;
}
