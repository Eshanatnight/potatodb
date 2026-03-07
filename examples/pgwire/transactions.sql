-- =========================================================
--  transactions.sql
--  Demonstrates BEGIN / COMMIT / ROLLBACK through psql
--  connected to the pgwire_server example.
--
--  Connect first:
--    psql -h 127.0.0.1 -p 5433 -U potatodb
-- =========================================================

-- ── Setup ─────────────────────────────────────────────────
CREATE TABLE accounts (
    id      INT,
    owner   VARCHAR,
    balance DOUBLE
);

INSERT INTO accounts VALUES (1, 'Alice', 1000.00);
INSERT INTO accounts VALUES (2, 'Bob',   500.00);

SELECT * FROM accounts ORDER BY id;

-- ── Committed transaction ─────────────────────────────────
BEGIN;
UPDATE accounts SET balance = balance - 200 WHERE id = 1;
UPDATE accounts SET balance = balance + 200 WHERE id = 2;
COMMIT;

SELECT * FROM accounts ORDER BY id;
-- Alice = 800, Bob = 700

-- ── Rolled-back transaction ───────────────────────────────
BEGIN;
UPDATE accounts SET balance = 0 WHERE id = 1;
UPDATE accounts SET balance = 0 WHERE id = 2;
-- Oops — undo everything
ROLLBACK;

SELECT * FROM accounts ORDER BY id;
-- Still Alice = 800, Bob = 700

-- ── DDL inside a transaction ──────────────────────────────
BEGIN;
CREATE TABLE temp_data (x INT);
INSERT INTO temp_data VALUES (42);
SELECT * FROM temp_data;
ROLLBACK;
-- temp_data table is gone after rollback

-- ── Cleanup ───────────────────────────────────────────────
DROP TABLE accounts;
