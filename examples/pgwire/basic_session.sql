-- =========================================================
--  basic_session.sql
--  A quick-start session you can paste into psql after
--  connecting to the pgwire_server example.
--
--  Connect first:
--    psql -h 127.0.0.1 -p 5433 -U potatodb
-- =========================================================

-- ── Create a table ────────────────────────────────────────
CREATE TABLE users (
    id    INT,
    name  VARCHAR,
    email VARCHAR,
    age   INT
);

-- ── Insert rows ───────────────────────────────────────────
INSERT INTO users VALUES (1, 'Alice',   'alice@example.com',   32);
INSERT INTO users VALUES (2, 'Bob',     'bob@example.com',     28);
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 45);
INSERT INTO users VALUES (4, 'Diana',   'diana@example.com',   36);
INSERT INTO users VALUES (5, 'Eve',     'eve@example.com',     24);

-- ── Query all rows ────────────────────────────────────────
SELECT * FROM users ORDER BY id;

-- ── Filter + sort ─────────────────────────────────────────
SELECT name, age FROM users WHERE age > 30 ORDER BY age DESC;

-- ── Limit + offset ────────────────────────────────────────
SELECT name FROM users ORDER BY id LIMIT 3 OFFSET 1;

-- ── Aggregations ──────────────────────────────────────────
SELECT
    COUNT(*) AS total,
    MIN(age) AS youngest,
    MAX(age) AS oldest,
    AVG(age) AS avg_age
FROM users;

-- ── Update ────────────────────────────────────────────────
UPDATE users SET email = 'alice@newdomain.com' WHERE name = 'Alice';
SELECT name, email FROM users WHERE name = 'Alice';

-- ── Delete ────────────────────────────────────────────────
DELETE FROM users WHERE name = 'Eve';
SELECT * FROM users ORDER BY id;

-- ── Cleanup ───────────────────────────────────────────────
DROP TABLE users;
