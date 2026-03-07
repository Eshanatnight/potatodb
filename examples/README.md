# PotatoDB Examples

Self-contained examples demonstrating different PotatoDB use cases. Each
example creates a temporary database, runs through its scenario, and cleans
up automatically.

## Rust examples

Build and run any example from the **repository root**:

```bash
# Basic CRUD (create, insert, select, update, delete)
cargo run --example basic

# Analytical queries (joins, CTEs, window functions, aggregations)
cargo run --example analytics

# Data integrity (constraints, upserts, error handling)
cargo run --example constraints

# Transactions (BEGIN / COMMIT / ROLLBACK, persistence)
cargo run --example transactions

# Maintenance (indexes, VACUUM, backup/restore, views, sequences)
cargo run --example maintenance
```

Add `--release` for optimised builds:

```bash
cargo run --release --example analytics
```

### What each example covers

| Example | Topics |
|---------|--------|
| `basic` | `CREATE TABLE`, single & multi-row `INSERT`, `SELECT` with `WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`, `UPDATE`, `DELETE`, `DISTINCT`, `DROP TABLE`, aggregations (`COUNT`, `MIN`, `MAX`, `AVG`) |
| `analytics` | `INNER JOIN`, `LEFT JOIN` with `COALESCE`, `GROUP BY` + `HAVING`, CTEs (`WITH`), window functions (`RANK`, `ROW_NUMBER`, `LAG`, running totals), scalar subqueries, `EXISTS`, `CASE` expressions, multi-table reporting |
| `constraints` | `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, `CHECK` constraints, constraint violation handling, `ON CONFLICT DO NOTHING`, `ON CONFLICT DO UPDATE` (upsert) |
| `transactions` | Auto-commit mode, `BEGIN` / `COMMIT` (atomic batches), `BEGIN` / `ROLLBACK` (discard changes), rollback reverting `CREATE TABLE` and `DROP TABLE`, cross-restart persistence |
| `maintenance` | `CREATE INDEX` (physical sort), `VACUUM` (compaction), `ANALYZE` (statistics), `FLUSH` (write buffer), views, materialized views + `REFRESH`, sequences (`nextval`), `CREATE TABLE AS SELECT`, `COPY TO` (CSV export), `BACKUP` / `RESTORE`, `DROP INDEX` |

## pgwire server example

Start a PostgreSQL-compatible server and connect with `psql` or any
PostgreSQL client:

```bash
# Terminal 1 — start the server (port 5433)
cargo run --example pgwire_server

# Terminal 2 — connect with psql
psql -h 127.0.0.1 -p 5433 -U potatodb
# Password: potatodb
```

Ready-made SQL scripts are provided in `examples/pgwire/`:

```bash
# Run a script directly through psql
psql -h 127.0.0.1 -p 5433 -U potatodb -f examples/pgwire/basic_session.sql
psql -h 127.0.0.1 -p 5433 -U potatodb -f examples/pgwire/analytics.sql
psql -h 127.0.0.1 -p 5433 -U potatodb -f examples/pgwire/transactions.sql
psql -h 127.0.0.1 -p 5433 -U potatodb -f examples/pgwire/maintenance.sql
```

| Script | Topics |
|--------|--------|
| `basic_session.sql` | `CREATE TABLE`, `INSERT`, `SELECT` with `WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`, aggregations, `UPDATE`, `DELETE`, `DROP TABLE` |
| `analytics.sql` | `INNER JOIN`, `GROUP BY` + `HAVING`, CTEs (`WITH`), `CASE` expressions, scalar subqueries |
| `transactions.sql` | `BEGIN` / `COMMIT`, `BEGIN` / `ROLLBACK`, DDL inside transactions |
| `maintenance.sql` | `FLUSH`, `CREATE INDEX`, `VACUUM`, `ANALYZE`, views, `CREATE TABLE AS SELECT` |

Environment variables for the server:

| Variable | Default | Description |
|----------|---------|-------------|
| `POTATODB_DATA_DIR` | `./pgwire_example_data` | Database storage path |
| `POTATODB_BIND` | `127.0.0.1:5433` | Listen address |
| `POTATODB_USER` | `potatodb` | Login username |
| `POTATODB_PASSWORD` | `potatodb` | Login password |
| `POTATODB_AUTO_VACUUM_INTERVAL_SECS` | `0` (off) | Auto-vacuum check interval |

## Python example

Build the Python bindings first, then run the script:

```bash
cd crates/python
maturin develop --release
cd ../..

python examples/python/analytics.py
```

## C++ / C examples

Examples live in `crates/ffi/examples/`. Build the FFI library first, then
compile with CMake or manually:

```bash
# Build the Rust FFI library
cargo build --release -p potatodb-ffi

# Option A: CMake
cd crates/ffi
cmake -B build
cmake --build build

# Option B: Manual (Linux/macOS)
cd crates/ffi/examples
g++ -std=c++17 -fno-exceptions -I../include basic.cpp \
    -L../../../target/release -lpotatodb_ffi \
    -lpthread -ldl -lm -o basic
```

| Example | File | Topics |
|---------|------|--------|
| `main` | `main.cpp` | Combined overview — create, insert, prepared statements, flush, stats, query, backup, drop |
| `basic` | `basic.cpp` | CRUD — `CREATE TABLE`, single & multi-row `INSERT`, `SELECT` with `WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`, `UPDATE`, `DELETE`, aggregations, column-level access, `DROP TABLE` |
| `prepared` | `prepared.cpp` | Prepared statements — `prepare`, `execute_prepared` with `$1`..`$N` placeholders for INSERTs and SELECTs |
| `analytics` | `analytics.cpp` | Analytical queries — `INNER JOIN`, `GROUP BY` + `HAVING`, CTEs (`WITH`), `CASE` expressions, scalar subqueries |
| `c_api` | `c_api.cpp` | Plain C API — uses only `potatodb.h` (no C++ wrapper); demonstrates `potato_open_local`, `potato_execute`, column metadata, row-level access, NULL handling, flush & storage stats |
