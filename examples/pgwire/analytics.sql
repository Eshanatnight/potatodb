-- =========================================================
--  analytics.sql
--  Analytical queries you can run through psql connected
--  to the pgwire_server example.
--
--  Connect first:
--    psql -h 127.0.0.1 -p 5433 -U potatodb
-- =========================================================

-- ── Schema ────────────────────────────────────────────────
CREATE TABLE departments (
    dept_id   INT,
    dept_name VARCHAR
);

CREATE TABLE employees (
    emp_id    INT,
    name      VARCHAR,
    dept_id   INT,
    salary    DOUBLE,
    hire_year INT
);

-- ── Seed data ─────────────────────────────────────────────
INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales');

INSERT INTO employees VALUES
    (101, 'Alice',   1, 120000, 2019),
    (102, 'Bob',     1, 110000, 2020),
    (103, 'Charlie', 2,  90000, 2018),
    (104, 'Diana',   2,  95000, 2021),
    (105, 'Eve',     3,  85000, 2020),
    (106, 'Frank',   3,  88000, 2022),
    (107, 'Grace',   1, 130000, 2017),
    (108, 'Hank',    3,  92000, 2019);

-- ── INNER JOIN ────────────────────────────────────────────
SELECT e.name, d.dept_name, e.salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id
ORDER BY e.salary DESC;

-- ── GROUP BY + HAVING ─────────────────────────────────────
SELECT d.dept_name,
       COUNT(*)            AS headcount,
       ROUND(AVG(e.salary), 2) AS avg_salary
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id
GROUP BY d.dept_name
HAVING COUNT(*) >= 2
ORDER BY avg_salary DESC;

-- ── CTE (WITH) ────────────────────────────────────────────
WITH dept_stats AS (
    SELECT dept_id,
           AVG(salary) AS avg_sal,
           MAX(salary) AS max_sal
    FROM employees
    GROUP BY dept_id
)
SELECT d.dept_name,
       ROUND(s.avg_sal, 2) AS avg_salary,
       s.max_sal            AS top_salary
FROM dept_stats s
INNER JOIN departments d ON s.dept_id = d.dept_id
ORDER BY avg_salary DESC;

-- ── CASE expression ───────────────────────────────────────
SELECT name, salary,
       CASE
           WHEN salary >= 120000 THEN 'Senior'
           WHEN salary >= 100000 THEN 'Mid'
           ELSE 'Junior'
       END AS band
FROM employees
ORDER BY salary DESC;

-- ── Scalar subquery — above average ───────────────────────
SELECT name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees)
ORDER BY salary DESC;

-- ── Cleanup ───────────────────────────────────────────────
DROP TABLE employees;
DROP TABLE departments;
