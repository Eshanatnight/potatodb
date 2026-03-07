-- =========================================================
--  maintenance.sql
--  Database maintenance operations through psql connected
--  to the pgwire_server example.
--
--  Connect first:
--    psql -h 127.0.0.1 -p 5433 -U potatodb
-- =========================================================

-- ── Setup ─────────────────────────────────────────────────
CREATE TABLE logs (
    id      INT,
    level   VARCHAR,
    message VARCHAR
);

INSERT INTO logs VALUES
    (1, 'INFO',  'Server started'),
    (2, 'WARN',  'Disk usage high'),
    (3, 'ERROR', 'Connection timeout'),
    (4, 'INFO',  'Request processed'),
    (5, 'DEBUG', 'Cache miss');

-- ── FLUSH — write buffered data to Parquet ────────────────
FLUSH TABLE logs;

-- ── CREATE INDEX — physically sort data ───────────────────
CREATE INDEX idx_logs_level ON logs (level);

-- ── VACUUM — compact Parquet files ────────────────────────
VACUUM logs;

-- ── ANALYZE — refresh table statistics ────────────────────
ANALYZE logs;

-- ── Views ─────────────────────────────────────────────────
CREATE VIEW error_logs AS
    SELECT * FROM logs WHERE level = 'ERROR';

SELECT * FROM error_logs;

-- ── CREATE TABLE AS SELECT ────────────────────────────────
CREATE TABLE warnings AS
    SELECT id, message FROM logs WHERE level = 'WARN';

SELECT * FROM warnings;

-- ── Cleanup ───────────────────────────────────────────────
DROP VIEW error_logs;
DROP TABLE warnings;
DROP TABLE logs;
