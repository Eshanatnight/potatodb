# PotatoDB

A Parquet-backed SQL database written in Rust, powered by [Apache DataFusion](https://datafusion.apache.org/).

![Rust](https://img.shields.io/badge/Rust-2021-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

## Features

- **Parquet-native SQL engine** -- DataFusion-powered query execution over Zstd-compressed Parquet with bloom/page pruning
- **Local + S3 durability** -- local binary WAL (buffered with fdatasync) and S3-backed WAL metadata replay for crash recovery
- **Transactions with destructive rewrites** -- `UPDATE`, `DELETE`, `CREATE INDEX`, and `VACUUM` now work inside `BEGIN` / `COMMIT` / `ROLLBACK`
- **Time travel queries** -- `AS OF TIMESTAMP` reads from captured snapshots
- **Indexing options** -- multiple index metadata per table (primary + logical), plus `CREATE FULLTEXT INDEX` and `fts_match(...)`
- **Rich types** -- `UUID`, `INTERVAL`, `ARRAY`, `JSON/JSONB`, decimals, timestamps, binary
- **Programmability** -- lightweight `CREATE FUNCTION`, `CREATE PROCEDURE`, `CALL`, `DO $$ ... $$`, triggers, and `MERGE`
- **Integrity features** -- `PRIMARY KEY`, `UNIQUE`, `CHECK`, and `FOREIGN KEY` with `RESTRICT` / `CASCADE` / `SET NULL`
- **Operational tooling** -- CDC stream (`potatodb_cdc`) with durable persistence, `LISTEN`/`NOTIFY`, auto-vacuum, compaction, retention policies, plan cache
- **Observability** -- Prometheus metrics, per-query `QueryMetrics`, `\timing` command, and `EXPLAIN ANALYZE` passthrough
- **Security & multi-tenancy** -- persisted RBAC (roles/grants in the catalog) and connection pooling support
- **Multi-interface access** -- TUI, REPL, pgwire server (TLS-capable), C/C++ FFI, Python bindings, HTTP API, and WebSocket support

## Quick start

```bash
# Full-screen TUI (default)
cargo run

# Line-mode REPL
cargo run -- --repl

# Custom data directory
cargo run -- --data-dir /path/to/data

# Execute SQL file(s) and exit
cargo run -- -f setup.sql -f queries.sql

# Execute SQL files with per-statement timing
cargo run -- -f workload.sql --timing

# Start HTTP API server
cargo run -- --http-addr 127.0.0.1:8080

# Release build (recommended for real workloads)
cargo run --release
```

## S3 storage

Point `--data-dir` at an S3 URL. Access credentials are read from the standard
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.

```bash
# AWS S3
cargo run -- --data-dir s3://mybucket/potatodb

# MinIO / S3-compatible
cargo run -- \
  --data-dir s3://mybucket/potatodb \
  --s3-endpoint http://localhost:9000 \
  --s3-region us-east-1 \
  --s3-allow-http
```

## SQL reference

### Tables

```sql
CREATE TABLE users (id INT, name VARCHAR, email VARCHAR);
CREATE TABLE IF NOT EXISTS logs (ts TIMESTAMP, level VARCHAR, msg TEXT);

INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');

SELECT * FROM users WHERE id = 1;
SELECT name, COUNT(*) AS cnt FROM users GROUP BY name ORDER BY cnt DESC;

DROP TABLE users;
DROP TABLE IF EXISTS logs;
```

### Indexes

Indexes define the physical sort order of a table's Parquet files. This lets
DataFusion skip row groups via min/max statistics, avoid re-sorting for
`ORDER BY`, and stop early on `LIMIT` queries.

```sql
CREATE INDEX idx_users_email ON users (email);
CREATE INDEX idx_events_ts ON events (ts DESC);

DROP INDEX idx_users_email;
```

### Transactions

PotatoDB supports MVCC-style transactions with file-level snapshot isolation.
By default every statement auto-commits. Wrap statements in `BEGIN` / `COMMIT`
to group them atomically, or `ROLLBACK` to discard all changes since `BEGIN`.

```sql
BEGIN;
INSERT INTO orders VALUES (1, 99.99);
INSERT INTO orders VALUES (2, 149.50);
COMMIT;   -- both rows become durable atomically
```

```sql
BEGIN;
INSERT INTO orders VALUES (3, 200.00);
DROP TABLE orders;
ROLLBACK;  -- table and all data restored to pre-BEGIN state
```

**How it works:**

- `BEGIN` snapshots the catalog and records every `.parquet` file that exists per table.
- During the transaction, writes go to disk normally but the catalog is held in memory (not persisted).
- `COMMIT` flushes the catalog and executes any deferred file deletions (from `DROP TABLE`).
- `ROLLBACK` deletes Parquet files written since `BEGIN`, removes tables created during the transaction, restores the catalog from the snapshot, and re-registers all tables with DataFusion.

**Restrictions:**

- Nested `BEGIN` is not supported (returns an error).

### Updates and deletes

```sql
UPDATE users SET email = 'alice@newdomain.com' WHERE name = 'Alice';
UPDATE users SET name = UPPER(name) WHERE id > 5;

DELETE FROM users WHERE id = 3;
DELETE FROM orders WHERE total < 1.00;
```

Both support `RETURNING` to get affected rows back:

```sql
DELETE FROM users WHERE id = 42 RETURNING id, name;
UPDATE users SET email = 'new@example.com' WHERE id = 1 RETURNING *;
```

### Constraints

```sql
CREATE TABLE accounts (
    id    INT PRIMARY KEY,
    email VARCHAR UNIQUE,
    name  VARCHAR NOT NULL,
    age   INT,
    CHECK (age >= 0)
);

INSERT INTO accounts VALUES (1, 'alice@co.com', 'Alice', 30);
INSERT INTO accounts VALUES (1, 'bob@co.com', 'Bob', 25);    -- ERROR: PK violation
INSERT INTO accounts VALUES (2, 'alice@co.com', 'Ali', 28);  -- ERROR: UNIQUE violation
INSERT INTO accounts VALUES (3, 'carol@co.com', NULL, 22);   -- ERROR: NOT NULL violation
INSERT INTO accounts VALUES (4, 'dave@co.com', 'Dave', -5);  -- ERROR: CHECK violation
```

### Upserts (ON CONFLICT)

Silently skip conflicting rows:

```sql
INSERT INTO accounts VALUES (1, 'dupe@co.com', 'Dupe', 99)
  ON CONFLICT (id) DO NOTHING;
```

Or merge updates into existing rows:

```sql
INSERT INTO accounts VALUES (1, 'alice-updated@co.com', 'Alice U.', 31)
  ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, name = EXCLUDED.name;
```

### Views

```sql
CREATE VIEW active_users AS
  SELECT * FROM users WHERE last_login > '2025-01-01';

SELECT * FROM active_users ORDER BY name;

DROP VIEW active_users;
```

### Materialized views

Materialized views store their results as Parquet, so reads are fast even
when the underlying query is expensive:

```sql
CREATE MATERIALIZED VIEW monthly_totals AS
  SELECT DATE_TRUNC('month', order_date) AS month,
         SUM(total) AS revenue
  FROM orders
  GROUP BY 1;

SELECT * FROM monthly_totals ORDER BY month;

-- After new data arrives, refresh to pick up changes
REFRESH MATERIALIZED VIEW monthly_totals;
```

### Sequences

Auto-incrementing IDs without a dedicated serial type:

```sql
CREATE SEQUENCE user_ids;

INSERT INTO users VALUES (nextval('user_ids'), 'Alice', 'alice@example.com');
INSERT INTO users VALUES (nextval('user_ids'), 'Bob',   'bob@example.com');

SELECT * FROM users ORDER BY id;
-- id=1, id=2, ...

DROP SEQUENCE user_ids;
```

### Joins

```sql
-- Inner join
SELECT u.name, o.total
  FROM users u
  INNER JOIN orders o ON u.id = o.user_id;

-- Left join (all users, even those without orders)
SELECT u.name, COALESCE(SUM(o.total), 0) AS lifetime_spend
  FROM users u
  LEFT JOIN orders o ON u.id = o.user_id
  GROUP BY u.name;

-- Self-join (employees and managers)
SELECT e.name AS employee, m.name AS manager
  FROM employees e
  LEFT JOIN employees m ON e.manager_id = m.id;
```

### CTEs (Common Table Expressions)

```sql
WITH top_spenders AS (
    SELECT user_id, SUM(total) AS spend
      FROM orders
      GROUP BY user_id
      HAVING SUM(total) > 500
)
SELECT u.name, ts.spend
  FROM top_spenders ts
  JOIN users u ON u.id = ts.user_id
  ORDER BY ts.spend DESC;
```

### Window functions

```sql
SELECT name, department, salary,
       RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank,
       salary - LAG(salary) OVER (ORDER BY salary) AS gap_to_prev
  FROM employees;
```

### Subqueries

```sql
-- Scalar subquery
SELECT name, salary,
       salary - (SELECT AVG(salary) FROM employees) AS diff_from_avg
  FROM employees;

-- EXISTS
SELECT name FROM users u
  WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

-- IN
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100);
```

### Set operations

```sql
SELECT name FROM employees
UNION
SELECT name FROM contractors;

SELECT email FROM newsletter_subscribers
EXCEPT
SELECT email FROM unsubscribed;
```

### ALTER TABLE

```sql
ALTER TABLE users ADD COLUMN phone VARCHAR;
ALTER TABLE users DROP COLUMN phone;
ALTER TABLE users RENAME COLUMN email TO email_address;
ALTER TABLE users RENAME TO customers;
```

### TRUNCATE

Remove all rows from a table without dropping it:

```sql
TRUNCATE TABLE staging_imports;
```

### CREATE TABLE AS SELECT (CTAS)

Create a new table from a query:

```sql
CREATE TABLE high_value_orders AS
  SELECT * FROM orders WHERE total > 1000;
```

### Import and export (COPY)

```sql
COPY users FROM '/path/to/users.csv';
COPY users FROM '/path/to/users.json';
COPY users FROM '/path/to/users.parquet';

COPY orders TO '/tmp/orders_backup.csv';
COPY orders TO '/tmp/orders_backup.parquet';
```

The REPL also offers shorthand commands:

```
.import csv users /path/to/users.csv
.export parquet orders /tmp/orders.parquet
```

### Prepared statements

```sql
PREPARE find_user AS SELECT * FROM users WHERE id = $1;
EXECUTE find_user(42);

PREPARE insert_order AS INSERT INTO orders VALUES ($1, $2, $3);
EXECUTE insert_order(1, 99.99, '2025-06-15');
```

### EXPLAIN

Inspect the query plan before running:

```sql
EXPLAIN SELECT * FROM users WHERE email = 'alice@example.com';
EXPLAIN (FORMAT JSON) SELECT u.name, COUNT(o.id)
  FROM users u JOIN orders o ON u.id = o.user_id
  GROUP BY u.name;
```

### Maintenance commands

```sql
VACUUM users;      -- compacts fragmented parquet files
ANALYZE users;     -- refreshes optimizer statistics
FLUSH;             -- flushes all buffered inserts
FLUSH TABLE users; -- flushes buffered inserts for one table
```

### User-defined functions

```sql
CREATE FUNCTION add1(x INT) RETURNS INT AS '$1 + 1';
SELECT add1(41); -- 42
DROP FUNCTION add1;
```

### Time travel

```sql
SELECT * FROM orders AS OF TIMESTAMP '2026-03-04T09:00:00Z';
-- or epoch millis
SELECT * FROM orders AS OF TIMESTAMP 1772614800000;
```

### Foreign keys

```sql
CREATE TABLE parent (id INT PRIMARY KEY);
CREATE TABLE child (
  pid INT,
  FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
);
```

### Procedures and DO blocks

```sql
CREATE PROCEDURE seed_orders() AS $$
  INSERT INTO orders VALUES (1, 10.0);
  INSERT INTO orders VALUES (2, 20.0);
$$;
CALL seed_orders();

DO $$ INSERT INTO orders VALUES (3, 30.0); $$;
```

### Full-text search

```sql
CREATE FULLTEXT INDEX idx_docs_body ON docs(body);
SELECT id FROM docs WHERE fts_match('rust database');
```

### CDC and notifications

```sql
SELECT * FROM potatodb_cdc WHERE table = 'orders';

LISTEN jobs;
NOTIFY jobs, 'new-order';
```

### RBAC statements

```sql
CREATE USER app_user WITH PASSWORD 'secret';
CREATE ROLE analyst;
GRANT SELECT ON orders TO analyst;
REVOKE SELECT ON orders FROM analyst;
```

### Supported SQL (via DataFusion)

| Category          | Examples                                                                                                                                    |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Queries           | `SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `DISTINCT`                                                                                |
| Joins             | `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`                                                                                                   |
| Aggregations      | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV`, `APPROX_MEDIAN`                                                                              |
| Window functions  | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, `NTILE`                                                                                                |
| Set operations    | `UNION`, `EXCEPT`, `INTERSECT`                                                                                                              |
| Subqueries & CTEs | `WITH ... AS`, correlated and uncorrelated subqueries                                                                                       |
| Transactions      | `BEGIN`, `COMMIT`, `ROLLBACK`                                                                                                               |
| Metadata          | `SHOW TABLES`, `DESCRIBE table`                                                                                                             |
| Types             | `INT`, `BIGINT`, `REAL`, `DOUBLE`, `VARCHAR`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `DECIMAL`, `BYTEA`, `UUID`, `INTERVAL`, `ARRAY`, `JSON/JSONB` |

### REPL special commands

| Command             | Description                          |
| ------------------- | ------------------------------------ |
| `\q` / `quit` / `exit` | Quit                              |
| `\dt`               | List all tables                      |
| `\d <table>`        | Describe table schema                |
| `\di`               | List indexes                         |
| `\dv`               | List views                           |
| `\timing`           | Toggle per-query timing display      |
| `\i <file>` / `.source <file>` | Execute a SQL file          |
| `\backup <path>`    | Create local archive                 |
| `\restore <path>`   | Restore local archive                |
| `.import`           | Shorthand for COPY FROM              |
| `.export`           | Shorthand for COPY TO                |

## Architecture

PotatoDB is organized as a Cargo workspace with engine, clients, server, and API crates:

```
potatodb/
  Cargo.toml                  workspace root
  crates/
    catalog/                  potatodb-catalog
    display/                  potatodb-display
    engine/                   potatodb-engine
    ffi/                      potatodb-ffi (C/C++ bindings)
    wal/                      potatodb-wal
    repl/                     potatodb-repl
    tui/                      potatodb-tui
    server/                   potatodb-server (pgwire)
    python/                   potatodb-python
    http/                     potatodb-http (REST API)
    nodejs/                   potatodb-nodejs (Node.js integration)
    potatodb/                 potatodb (binary)
  examples/                   perftest and other example binaries
```

### Crate dependency graph

```
potatodb (binary)               potatodb-ffi (cdylib + staticlib)
  ├── potatodb-engine             ├── potatodb-engine
  │     ├── potatodb-catalog      └── potatodb-display
  │     └── potatodb-wal
  ├── potatodb-repl
  │     ├── potatodb-engine
  │     └── potatodb-display
  └── potatodb-tui
        ├── potatodb-engine
        └── potatodb-display

potatodb-server ── potatodb-engine
potatodb-http   ── potatodb-engine
potatodb-python ── potatodb-engine
```

### Crate responsibilities

| Crate              | Purpose                                                                         |
| ------------------ | ------------------------------------------------------------------------------- |
| `potatodb-catalog` | Persistent JSON catalog with snapshot/restore for MVCC transactions             |
| `potatodb-display` | Formats Arrow `RecordBatch` results as ASCII tables                             |
| `potatodb-engine`  | Core engine: DDL, DML, transactions, DataFusion SessionContext, Parquet I/O, S3 |
| `potatodb-ffi`     | C/C++ FFI bindings (static + shared library, C header, C++ RAII wrapper)        |
| `potatodb-repl`    | Line-mode interactive SQL shell with readline and history                       |
| `potatodb-tui`     | Full-screen ratatui terminal UI with scrollable results and sidebar             |
| `potatodb-wal`     | Local binary WAL + replay/checkpoint support                                    |
| `potatodb-server`  | PostgreSQL wire protocol server                                                 |
| `potatodb-python`  | PyO3 Python bindings                                                            |
| `potatodb-http`    | HTTP API server (`/health`, `/tables`, `/query`, ...)                           |
| `potatodb`         | Binary entry point, CLI argument parsing                                        |

### How it works

1. **`CREATE TABLE`** creates a storage directory (local) or prefix (S3) and registers a DataFusion `ListingTable` that reads/writes Parquet files from that location.
2. **`INSERT INTO`** is either buffered in-memory (flush by thresholds) or written directly for constrained / returning paths.
3. **`SELECT`** queries are planned and executed by DataFusion with full predicate pushdown, bloom filter pruning, and page-index pruning against the Parquet files.
4. **`CREATE INDEX`** reads all data sorted by the indexed columns, rewrites the Parquet files in sorted order, and registers a sort-order hint with DataFusion so it can skip row groups and avoid redundant sorts.
5. **`DROP TABLE`** deregisters from DataFusion, deletes all Parquet files (via `ObjectStore`), and removes the table and its indexes from the catalog.
6. **`BEGIN` / `COMMIT` / `ROLLBACK`** provide atomic multi-statement transactions via file-level MVCC: the catalog is snapshotted at `BEGIN`, mutations are held in memory, and `ROLLBACK` deletes new files and restores the snapshot.
7. The **catalog** (`catalog.json`) is persisted through the `ObjectStore` trait, making it work identically for local filesystems and S3.
8. For local storage, the **WAL** (`wal.log`) is replayed on startup to recover committed statements after crashes.

### Performance

The DataFusion session is configured with:

- **Predicate pushdown** and **filter reordering** -- filters pushed into the Parquet reader and ordered by selectivity
- **Row-group and page-index pruning** -- min/max statistics skip data before it's read
- **Bloom filters** -- written on every INSERT, checked on equality predicates
- **Zstd(3) compression** by default (configurable via `POTATODB_PARQUET_COMPRESSION`) with dictionary encoding -- smaller files, less I/O
- **Parallel scans** across all CPU cores
- **Deferred auto-analyze** -- `ANALYZE` runs between queries instead of blocking inserts
- **Approximate distinct counts** -- `ANALYZE` uses HyperLogLog (`APPROX_DISTINCT`) instead of exact `COUNT(DISTINCT)`
- **Buffered WAL writes** -- Arrow IPC WAL uses 256 KB buffered I/O with explicit `fdatasync`
- **Tuned allocator** -- mimalloc with a 10-second purge delay to reduce `madvise` overhead

The release profile uses single codegen unit. For AVX2/AVX-512 SIMD vectorization in Arrow and Parquet codecs, set `RUSTFLAGS="-C target-cpu=native"` when building locally (not used in CI to avoid SIGILL on GitHub Actions runners).

## Runtime tuning

Common environment variables:

- `POTATODB_WAL_CHECKPOINT_BYTES` -- WAL checkpoint threshold for autocommit mode (default: 4 GiB)
- `POTATODB_WRITE_BUFFER_ROWS` -- buffered insert row threshold (default: 10,000)
- `POTATODB_WRITE_BUFFER_BYTES` -- buffered insert byte threshold (default: 64 MiB)
- `POTATODB_WRITE_BUFFER_MS` -- buffered insert age threshold (default: 360000 ms / 6 min)
- `POTATODB_AUTO_ANALYZE_ROWS` -- rows written before automatic `ANALYZE` (default: 10,000)
- `POTATODB_AUTO_VACUUM_INTERVAL_SECS` -- background compaction interval (0 disables)
- `POTATODB_AUTO_VACUUM_FILE_THRESHOLD` -- file-count score component for compaction
- `POTATODB_AUTO_VACUUM_BYTES_THRESHOLD` -- byte-size score component for compaction
- `POTATODB_AUTO_VACUUM_AGE_SECS` -- compact very old tables even with lower score
- `POTATODB_PARQUET_COMPRESSION` -- Parquet compression algorithm (default: `zstd(3)`). Supported values: `zstd(N)`, `snappy`, `gzip(N)`, `brotli(N)`, `lz4`, `lz4_raw`, `lzo`, `uncompressed`
- `POTATODB_SLOW_QUERY_MS` / `POTATODB_QUERY_LOG_MAX` -- slow-query threshold and in-memory query log size
- `POTATODB_CDC_CAPACITY` -- max in-memory CDC event buffer length
- `POTATODB_SNAPSHOT_RETENTION_MS` -- retention window for `AS OF TIMESTAMP` snapshots
- `POTATODB_TLS_CERT` / `POTATODB_TLS_KEY` -- enable TLS in pgwire server
- `MIMALLOC_PURGE_DELAY` -- seconds before mimalloc returns memory to the OS (default: 10, set automatically at startup)

## C / C++ FFI

The `potatodb-ffi` crate builds a static library (`libpotatodb_ffi.a`) and a
shared library (`libpotatodb_ffi.so` / `.dylib`) that expose the PotatoDB engine
to C and C++ programs.

### Building the library

```bash
cargo build --release -p potatodb-ffi
# produces target/release/libpotatodb_ffi.a  (static)
# produces target/release/libpotatodb_ffi.so (shared, Linux)
```

### Headers

Both headers live in `crates/ffi/include/`:

| Header         | Language | Description                                                      |
| -------------- | -------- | ---------------------------------------------------------------- |
| `potatodb.h`   | C11+     | Opaque handles, `extern "C"` functions, enum type tags           |
| `potatodb.hpp` | C++17    | Header-only RAII wrapper (no exceptions, `-fno-exceptions` safe) |

### C++ API overview

All fallible operations return `potato::Expected<T>` -- a lightweight
result type that holds either a value or an error string. Check success
with `if (!result)` and read the error with `.error()`. No exceptions
are thrown anywhere.

#### Opening a database

```cpp
#include "potatodb.hpp"

// Local storage
auto db = potato::Database::open("./my_data");
if (!db) {
    std::cerr << db.error() << "\n";
    return 1;
}

// S3 storage
auto db = potato::Database::open_s3(
    "s3://mybucket/prefix",
    "http://localhost:9000",  // endpoint (empty string for AWS default)
    "us-east-1",             // region
    true                     // allow_http
);
```

#### Executing SQL

```cpp
// DDL / DML -- check for errors, discard the result
auto ok = db->execute("CREATE TABLE t (id INT, name VARCHAR);");
if (!ok) { std::cerr << ok.error() << "\n"; return 1; }

// Query -- use the result
auto res = db->execute("SELECT * FROM t ORDER BY id;");
if (!res) { std::cerr << res.error() << "\n"; return 1; }

// Read-only query fast path
auto ro = db->execute_readonly("SELECT COUNT(*) FROM t;");
if (!ro) { std::cerr << ro.error() << "\n"; return 1; }
```

#### Prepared statements and backup/restore

```cpp
if (!db->prepare("ins", "INSERT INTO t VALUES ($1, $2);")) {
    std::cerr << db->last_error() << "\n";
    return 1;
}
auto ins = db->execute_prepared("ins", {"1", "'Alice'"});
if (!ins) { std::cerr << ins.error() << "\n"; return 1; }

if (!db->backup("./snapshot.tar.gz")) {
    std::cerr << db->last_error() << "\n";
    return 1;
}
```

#### Reading results

```cpp
// Formatted ASCII table
std::cout << res->display() << "\n";

// Metadata
res->row_count();            // total rows
res->column_count();         // number of columns
res->column_name(0);         // column name by index
res->column_type(0);         // POTATO_TYPE_INT32, _STRING, etc.
res->is_records();           // true for SELECT results
res->is_message();           // true for DDL/DML confirmations
res->message();              // status string for DDL/DML

// Row-level access (0-indexed)
res->is_null(row, col);
res->get_int(row, col);      // int64_t (0 if NULL)
res->get_double(row, col);   // double  (0.0 if NULL)
res->get_bool(row, col);     // bool    (false if NULL)
res->get_string(row, col);   // std::string ("" if NULL)
```

#### Iterating over rows

```cpp
for (size_t r = 0; r < res->row_count(); ++r) {
    int64_t     id    = res->get_int(r, 0);
    std::string name  = res->get_string(r, 1);
    std::string email = res->get_string(r, 2);
    std::cout << id << " " << name << " " << email << "\n";
}
```

### Compiling a C++ program

```bash
g++ -std=c++17 -fno-exceptions -O2 \
    -Icrates/ffi/include \
    my_app.cpp \
    -Ltarget/release -lpotatodb_ffi \
    -lpthread -ldl -lm \
    -o my_app
```

A `CMakeLists.txt` is provided in `crates/ffi/` for CMake-based projects.
A complete example is in `crates/ffi/examples/main.cpp`.

### C API

The raw C API (`potatodb.h`) can be used directly from C or any language
with C FFI support. It uses opaque `potato_db*` and `potato_result*`
handles with manual lifetime management:

```c
#include "potatodb.h"

potato_db *db = potato_open_local("./data");
potato_result *res = potato_execute(db, "SELECT 1 + 1 AS answer;");

printf("answer = %lld\n", potato_result_get_int(res, 0, 0));

potato_result_free(res);
potato_close(db);
```

## Python API

The `potatodb-python` crate provides [PyO3](https://pyo3.rs/) bindings.
Build with [maturin](https://www.maturin.rs/):

```bash
cd crates/python
maturin develop --release
```

Then use from Python:

```python
from potatodb_python import PotatoDB

db = PotatoDB.open("./my_data")

db.execute("CREATE TABLE products (id INT, name VARCHAR, price DOUBLE);")
db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99);")
db.execute("INSERT INTO products VALUES (2, 'Gadget', 24.95);")
db.execute("INSERT INTO products VALUES (3, 'Gizmo', 14.50);")

rows = db.execute("SELECT * FROM products WHERE price > 10 ORDER BY price DESC;")
for row in rows:
    print(f"{row['id']}: {row['name']} - ${row['price']:.2f}")
# 2: Gadget - $24.95
# 3: Gizmo - $14.50

result = db.execute("DROP TABLE products;")
print(result)  # "Table 'products' dropped."

db.close()
```

DDL/DML statements return a status string; queries return a list of dicts.

## pgwire server

The `potatodb-server` crate exposes PotatoDB over the PostgreSQL wire
protocol. Any PostgreSQL-compatible client can connect -- `psql`, DBeaver,
language drivers, etc.

```rust
use potatodb_server::start_server;

#[tokio::main]
async fn main() {
    start_server("./data", "127.0.0.1:5432").await.unwrap();
}
```

Connect with `psql`:

```bash
PGPASSWORD=potatodb psql -h 127.0.0.1 -p 5432 -U potatodb -d potatodb
```

Default credentials are controlled by `POTATODB_USER` and
`POTATODB_PASSWORD` environment variables (both default to `potatodb`).
Set `POTATODB_TLS_CERT` and `POTATODB_TLS_KEY` to enable TLS.

## HTTP API

Start via CLI:

```bash
cargo run -- --http-addr 127.0.0.1:8080
```

Endpoints:

- `GET /health`
- `GET /metrics` (Prometheus metrics)
- `GET /tables`
- `GET /tables/{name}/stats`
- `POST /query` with JSON body `{"sql":"SELECT ..."}`
- `GET /ws` (WebSocket for streaming queries)

## Development

```bash
# Run all targets: format, lint, test, build
make

# Run tests across all crates
make test

# Build optimized release binary
make release

# Launch the REPL / TUI
make run
make run-tui

# Build the C/C++ FFI library and example
make ffi
make ffi-example

# Format / lint
make fmt
make fmt-check
make clippy

# Generate and open documentation
make doc
```

### Performance testing

An end-to-end performance test suite is available as an example binary.
It runs a representative workload (bulk inserts, queries, joins,
aggregations, window functions, DML, DDL, transactions) and produces
a JSON report with timing statistics.

```bash
# Run performance test
make perftest

# Save a baseline for later comparison
make perf-save

# Run and compare against a saved baseline
make perf-compare

# Tune scale factor and iterations
make perftest PERF_SCALE=2 PERF_ITERS=5
```

See [DOCS.md](DOCS.md) for detailed technical documentation covering
internals, the transaction model, Parquet tuning knobs, the FFI memory
ownership model, and more.

## License

MIT
