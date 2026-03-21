/// Analytical queries: JOINs, CTEs, window functions, and subqueries.
///
/// Run with:
///   cargo run --example analytics
use potatodb_engine::PotatoDB;
use potatodb_examples::{BoxError, print_result, section};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let tmp = tempfile::tempdir()?;
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None).await?;

    // ── Seed data ─────────────────────────────────────────────
    section("Setting up sample data");

    db.execute("CREATE TABLE departments (id INT, name VARCHAR, budget DOUBLE);")
        .await?;
    db.execute(
        "INSERT INTO departments VALUES \
         (1, 'Engineering', 500000), \
         (2, 'Marketing',   200000), \
         (3, 'Sales',       300000);",
    )
    .await?;

    db.execute(
        "CREATE TABLE employees (id INT, name VARCHAR, dept_id INT, salary INT, hire_date DATE);",
    )
    .await?;
    db.execute(
        "INSERT INTO employees VALUES \
         (1,  'Alice',   1, 120000, '2020-03-15'), \
         (2,  'Bob',     1,  95000, '2021-07-01'), \
         (3,  'Charlie', 2,  85000, '2019-11-20'), \
         (4,  'Diana',   2,  90000, '2022-01-10'), \
         (5,  'Eve',     3, 110000, '2020-06-30'), \
         (6,  'Frank',   3,  75000, '2023-04-05'), \
         (7,  'Grace',   1, 130000, '2018-09-12'), \
         (8,  'Hank',    NULL, 60000, '2024-01-01');",
    )
    .await?;

    db.execute("CREATE TABLE orders (id INT, employee_id INT, amount DOUBLE, order_date DATE);")
        .await?;
    db.execute(
        "INSERT INTO orders VALUES \
         (100, 5, 15000.00, '2025-01-15'), \
         (101, 5, 22000.00, '2025-02-20'), \
         (102, 6,  8500.00, '2025-01-28'), \
         (103, 3, 12000.00, '2025-03-01'), \
         (104, 5, 31000.00, '2025-03-15'), \
         (105, 6,  4200.00, '2025-02-10');",
    )
    .await?;

    println!("Created departments (3), employees (8), orders (6).");

    // ── INNER JOIN ────────────────────────────────────────────
    section("INNER JOIN");
    let res = db
        .execute(
            "SELECT e.name, d.name AS department, e.salary \
             FROM employees e \
             INNER JOIN departments d ON e.dept_id = d.id \
             ORDER BY e.salary DESC;",
        )
        .await?;
    print_result("Employees with their departments", &res);

    // ── LEFT JOIN ─────────────────────────────────────────────
    section("LEFT JOIN");
    let res = db
        .execute(
            "SELECT e.name, COALESCE(d.name, 'Unassigned') AS department \
             FROM employees e \
             LEFT JOIN departments d ON e.dept_id = d.id \
             ORDER BY e.name;",
        )
        .await?;
    print_result("All employees (including unassigned)", &res);

    // ── GROUP BY with HAVING ──────────────────────────────────
    section("GROUP BY + HAVING");
    let res = db
        .execute(
            "SELECT d.name AS department, \
                    COUNT(*) AS headcount, \
                    AVG(e.salary) AS avg_salary \
             FROM employees e \
             JOIN departments d ON e.dept_id = d.id \
             GROUP BY d.name \
             HAVING COUNT(*) >= 2 \
             ORDER BY avg_salary DESC;",
        )
        .await?;
    print_result("Departments with 2+ employees", &res);

    // ── CTE (Common Table Expression) ─────────────────────────
    section("CTE (WITH clause)");
    let res = db
        .execute(
            "WITH dept_costs AS ( \
                 SELECT d.name AS dept, SUM(e.salary) AS total_salary \
                 FROM employees e \
                 JOIN departments d ON e.dept_id = d.id \
                 GROUP BY d.name \
             ) \
             SELECT dept, total_salary, \
                    ROUND(total_salary * 100.0 / (SELECT SUM(total_salary) FROM dept_costs), 1) \
                        AS pct_of_total \
             FROM dept_costs \
             ORDER BY total_salary DESC;",
        )
        .await?;
    print_result("Salary distribution by department", &res);

    // ── Window functions ──────────────────────────────────────
    section("WINDOW FUNCTIONS");
    let res = db
        .execute(
            "SELECT e.name, d.name AS dept, e.salary, \
                    RANK() OVER (PARTITION BY d.name ORDER BY e.salary DESC) AS dept_rank, \
                    ROW_NUMBER() OVER (ORDER BY e.salary DESC) AS overall_rank \
             FROM employees e \
             JOIN departments d ON e.dept_id = d.id \
             ORDER BY d.name, dept_rank;",
        )
        .await?;
    print_result("Employee rankings by salary", &res);

    let res = db
        .execute(
            "SELECT name, salary, \
                    salary - LAG(salary) OVER (ORDER BY salary) AS gap_to_prev, \
                    SUM(salary) OVER (ORDER BY salary ROWS UNBOUNDED PRECEDING) AS running_total \
             FROM employees \
             ORDER BY salary;",
        )
        .await?;
    print_result("Salary gaps and running totals", &res);

    // ── Subqueries ────────────────────────────────────────────
    section("SUBQUERIES");
    let res = db
        .execute(
            "SELECT name, salary, \
                    salary - (SELECT AVG(salary) FROM employees) AS diff_from_avg \
             FROM employees \
             ORDER BY diff_from_avg DESC;",
        )
        .await?;
    print_result("Scalar subquery: difference from average salary", &res);

    let res = db
        .execute(
            "SELECT name FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.employee_id = e.id) \
             ORDER BY name;",
        )
        .await?;
    print_result("EXISTS: employees who have placed orders", &res);

    let res = db
        .execute(
            "SELECT name, salary FROM employees \
             WHERE salary > (SELECT AVG(salary) FROM employees) \
             ORDER BY salary DESC;",
        )
        .await?;
    print_result("Above-average earners", &res);

    // ── CASE expression ───────────────────────────────────────
    section("CASE EXPRESSION");
    let res = db
        .execute(
            "SELECT name, salary, \
                    CASE \
                        WHEN salary >= 120000 THEN 'Senior' \
                        WHEN salary >=  90000 THEN 'Mid' \
                        ELSE 'Junior' \
                    END AS level \
             FROM employees \
             ORDER BY salary DESC;",
        )
        .await?;
    print_result("Employee levels by salary band", &res);

    // ── Multi-table aggregation with JOIN ─────────────────────
    section("SALES REPORT (multi-table join + aggregation)");
    let res = db
        .execute(
            "SELECT e.name AS salesperson, \
                    COUNT(o.id) AS num_orders, \
                    SUM(o.amount) AS total_revenue, \
                    ROUND(AVG(o.amount), 2) AS avg_order \
             FROM orders o \
             JOIN employees e ON o.employee_id = e.id \
             GROUP BY e.name \
             ORDER BY total_revenue DESC;",
        )
        .await?;
    print_result("Sales performance report", &res);

    // ── Cleanup ───────────────────────────────────────────────
    db.execute("DROP TABLE orders;").await?;
    db.execute("DROP TABLE employees;").await?;
    db.execute("DROP TABLE departments;").await?;

    println!("\nDone!");
    Ok(())
}
