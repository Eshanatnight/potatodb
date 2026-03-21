# PotatoDB -- Technical Documentation

This document covers the internal design, data flow, storage format,
configuration knobs, and integration APIs in detail. For a quick-start
guide and feature overview, see [README.md](README.md).

## Current capability snapshot

Recent engine extensions include:

- additional SQL types (`UUID`, `INTERVAL`, `ARRAY`, `JSON/JSONB`)
- user-defined SQL functions (`CREATE FUNCTION` / `DROP FUNCTION`)
- transactions (`BEGIN`, `COMMIT`, `ROLLBACK`) with destructive rewrites, plus `SAVEPOINT` / `ROLLBACK TO` / `RELEASE SAVEPOINT`
- snapshot-based time-travel reads (`AS OF TIMESTAMP`)
- foreign-key constraints with `RESTRICT` / `CASCADE` / `SET NULL`
- CDC virtual table (`potatodb_cdc`) with durable disk persistence
- `LISTEN` / `NOTIFY`
- procedure support (`CREATE PROCEDURE`, `CALL`, `DO $$...$$`)
- full-text index metadata with `fts_match(...)` rewrite
- HTTP API crate (`potatodb-http`) and TLS-enabled pgwire server
- WebSocket support and `crates/nodejs` integration crate
- deletion vectors and partitioning (improved storage layouts)
- S3-backed WAL support and durable CDC improvements
- plan cache and query metrics (`QueryMetrics`) with `EXPLAIN ANALYZE` passthrough
- compaction and background maintenance (automatic file rewrites)
- PL/pgSQL compatibility, triggers, and `MERGE` support; built-ins like `generate_series`
- Prometheus metrics for observability and connection pooling support
- persisted RBAC (roles/privileges) stored in the catalog
- in-memory storage (`:memory:` / `memory://...`) via `object_store::memory::InMemory` (no WAL; ephemeral)
- TUI and REPL meta-commands (`\dt`, `\di`, `\dv`, `\d`, `\ds`, `\df`, `\du`, `\timing`)
- `\timing` toggle for detailed per-query timing and I/O metrics in REPL and TUI
- deferred auto-analyze (moved off the insert hot path)
- buffered Arrow WAL writes with fdatasync and directory caching
- approximate distinct counts (`APPROX_DISTINCT`) in `ANALYZE`
- mimalloc purge-delay tuning for reduced `madvise` overhead
- end-to-end performance test suite (`perftest`) with JSON reporting and baseline comparison

---

## Table of contents

1. [Storage model](#storage-model)
2. [Catalog](#catalog)
3. [Engine internals](#engine-internals)
4. [WAL and durability](#wal-and-durability)
5. [SQL processing pipeline](#sql-processing-pipeline)
6. [Indexes](#indexes)
7. [Parquet tuning](#parquet-tuning)
8. [S3 integration](#s3-integration)
9. [In-memory storage](#in-memory-storage)
10. [REPL](#repl)
11. [TUI](#tui)
12. [CLI arguments](#cli-arguments)
13. [CLI file execution](#cli-file-execution)
14. [C / C++ FFI](#c--c-ffi)
15. [Runtime environment variables](#runtime-environment-variables)
16. [Workspace layout](#workspace-layout)
17. [Build & release profiles](#build--release-profiles)
18. [Formatting conventions](#formatting-conventions)
19. [Testing](#testing)

---

## Storage model

Every table is backed by a directory (local filesystem), an object key
prefix (S3), or the same prefix layout on an in-memory `ObjectStore`.
PotatoDB writes Parquet files through DataFusion `ListingTable`.
For S3 and in-memory backends, table listing URLs use a trailing `/` so
DataFusion treats each table location as a directory rather than a single file.
For local writes, the engine can buffer inserts in memory and flush by
row/byte/time thresholds, which reduces tiny-file churn on write-heavy
workloads. `SELECT` queries transparently read across all files in the
directory and merge schemas if needed.

```
potatodb_data/
  catalog.json            # persistent metadata
  users/
    a3f1...e7.parquet     # first INSERT batch
    c8b2...d4.parquet     # second INSERT batch
  orders/
    19fa...b1.parquet
```

Parquet files are written with:

- **Zstd level-3 compression** -- good ratio-to-speed trade-off.
- **Dictionary encoding** on all columns.
- **Page-level statistics** (min/max per data page).
- **Bloom filters** on every column.
- **Row groups up to 1,048,576 rows**, data pages up to 20,000 rows.

These settings are configured in `build_session_config()` inside the
engine crate and apply to every `INSERT INTO` and `CREATE INDEX` write.

---

## Catalog

The catalog is the single source of truth for what tables and indexes
exist. It is serialized as JSON and persisted through the `ObjectStore`
trait, so the same code path works for local files, S3, and in-memory storage.

### On-disk format

```json
{
  "tables": {
    "users": {
      "name": "users",
      "columns": [
        { "name": "id",    "data_type": "INT",     "nullable": true },
        { "name": "name",  "data_type": "VARCHAR", "nullable": true },
        { "name": "email", "data_type": "VARCHAR", "nullable": true }
      ],
      "path": "/abs/path/to/potatodb_data/users"
    }
  },
  "indexes": {
    "idx_users_id": {
      "name": "idx_users_id",
      "table_name": "users",
      "columns": [
        { "name": "id", "ascending": true }
      ],
      "logical_only": false,
      "primary": true
    }
  },
  "udfs": {
    "add1": {
      "name": "add1",
      "args": ["x"],
      "return_type": "INT",
      "body": "$1 + 1"
    }
  }
}
```

The catalog also stores RBAC role definitions and grants so role state (roles, memberships,
and object privileges) is persisted and reloaded on startup.

### Legacy migration

Catalogs written before index support stored only a bare
`HashMap<String, TableMeta>` at the JSON root. The loader tries the
new `CatalogData` format first, falls back to the legacy format, and
upgrades silently on the next save.

### Persistence guarantees

Catalog mutations (`add_table`, `remove_table`, `add_index`, `remove_index`,
etc.) serialize the full catalog through `ObjectStore::put`. For local
databases, mutating SQL statements are written to `wal.log` before execution
and replayed on restart. For S3 databases, WAL entries are persisted as JSON
under `_wal/entries.json` in the configured prefix and replayed similarly.
In-memory databases skip WAL entirely; catalog state exists only in RAM.

---

## Engine internals

`PotatoDB` is the central struct. It owns:

| Field         | Type                      | Purpose                                |
|---------------|---------------------------|----------------------------------------|
| `ctx`         | `SessionContext`          | DataFusion query engine                |
| `catalog`     | `Catalog`                 | Persistent table/index metadata        |
| `data_url`    | `String`                  | Canonical base path, `s3://`, or `memory://` URL |
| `store`       | `Arc<dyn ObjectStore>`    | Parquet + catalog I/O                  |
| `is_s3`       | `bool`                    | Quick backend check                    |
| `is_memory`   | `bool`                    | `true` for in-memory `InMemory` store  |
| `s3_prefix`   | `String`                  | Key prefix (S3 bucket or `memory://` path) |
| `wal`         | `Option<Wal>`             | Local write-ahead log handle           |
| `write_buffer`| `HashMap<...>`            | Per-table buffered INSERT batches      |

### Deferred auto-analyze

The engine tracks rows written per table and triggers `ANALYZE` after a
configurable threshold (`POTATODB_AUTO_ANALYZE_ROWS`). Instead of
running `ANALYZE` synchronously on the insert path, table names are
pushed to a `pending_analyze_tables` queue. The pending analyses are
drained at the start of the next `execute()` call, keeping inserts fast.

`ANALYZE` itself uses `APPROX_DISTINCT` (HyperLogLog) instead of exact
`COUNT(DISTINCT)` for column-level distinct-count statistics, which is
significantly cheaper on large tables.

### Initialization

On construction (`PotatoDB::new`), the engine:

1. Configures a `SessionConfig` with tuned Parquet options.
2. Sets up the appropriate `ObjectStore` (local `LocalFileSystem`,
   `AmazonS3`, or `InMemory`).
3. Registers the store with DataFusion (S3 and in-memory).
4. Loads the catalog from `catalog.json`.
5. Opens/replays `wal.log` for local storage (and S3 WAL when configured), then
   checkpoints as appropriate. Skips WAL setup entirely for in-memory mode.
6. Re-registers persisted tables as `ListingTable`s (concurrently), including
   sort-order hints from any indexes.

---

## WAL and durability

For local backends, PotatoDB uses `crates/wal` as an append-only journal:

- Mutating statements append `Pending` entries before execution.
- Auto-commit statements append a commit marker and checkpoint
  opportunistically (size-threshold based).
- Explicit transactions commit/abort with transaction ids.
- Startup recovery replays committed pending entries.

`Wal::append()` flushes and `sync_data()`s writes. In S3 mode, WAL entries are
stored in object storage (`_wal/entries.json`) and replayed on startup.

In-memory mode does not open or replay any WAL (including Arrow IPC WAL).

### Arrow IPC WAL

The `ArrowWal` (used for Arrow IPC-encoded row batches) wraps file
writes in a 256 KB `BufWriter` and explicitly calls `sync_data()` after
each append for durability without flushing filesystem metadata. It also
maintains a `HashSet<String>` of known table directories to skip
redundant `create_dir_all` syscalls on repeated inserts to the same
table. The directory cache is cleared on checkpoint.

---

## SQL processing pipeline

```
SQL string
  │
  ▼
sqlparser::Parser  (PostgreSQL dialect)
  │
  ├─ CreateTable  ──► handle_create_table()  ──► mkdir (local only) + register ListingTable + save catalog
  ├─ Insert       ──► handle_insert()        ──► buffer or immediate write + constraints
  ├─ CreateIndex  ──► handle_create_index()  ──► sort data + rewrite Parquet + save catalog
  ├─ Drop(Table)  ──► handle_drop_table()    ──► deregister + delete files + save catalog
  ├─ Drop(Index)  ──► handle_drop_index()    ──► deregister + re-register without sort hint
  ├─ FLUSH        ──► flush_table/flush_all  ──► drain buffered rows to Parquet
  │
  └─ everything else  ──► ctx.sql()  ──► DataFusion planning + execution
                                            │
                                            ▼
                                       Vec<RecordBatch>
```

The write path includes optional buffering for insert-heavy workloads.
Buffered rows flush when row/byte/time thresholds are reached, when reads
need fresh visibility, or when explicit `FLUSH` is invoked.

If `sqlparser` cannot parse the statement (e.g. `SHOW TABLES`,
`DESCRIBE`), the raw SQL is forwarded to DataFusion which handles
these as built-in statements when `information_schema` is enabled.

### Type mapping

SQL types in `CREATE TABLE` are parsed by sqlparser and mapped to Arrow
`DataType` values:

| SQL type                | Arrow type                            |
|-------------------------|---------------------------------------|
| `BOOLEAN` / `BOOL`      | `Boolean`                             |
| `TINYINT`               | `Int8`                                |
| `SMALLINT` / `INT2`     | `Int16`                               |
| `INT` / `INTEGER`       | `Int32`                               |
| `BIGINT` / `INT8`       | `Int64`                               |
| `REAL`                  | `Float32`                             |
| `FLOAT` / `DOUBLE`      | `Float64`                             |
| `VARCHAR` / `TEXT`       | `Utf8`                                |
| `DATE`                  | `Date32`                              |
| `TIMESTAMP`             | `Timestamp(Microsecond, None)`        |
| `TIMESTAMP WITH TZ`     | `Timestamp(Microsecond, Some("UTC"))` |
| `DECIMAL(p,s)`          | `Decimal128(p, s)`                    |
| `BYTEA` / `BLOB`        | `Binary`                              |
| `UUID`                  | `FixedSizeBinary(16)`                 |
| `INTERVAL`              | `Duration(Microsecond)`               |
| `ARRAY<T>`              | `List(T)`                             |
| `JSON` / `JSONB`        | `Utf8`                                |

The mapping lives in `sqlparser_type_to_arrow()`.

---

## Indexes

An index in PotatoDB defines the **physical sort order** of a table's
Parquet files. It is not a separate B-tree structure.

### CREATE INDEX flow

1. Read all data: `SELECT * FROM "table" ORDER BY col1 ASC, col2 DESC`.
2. Deregister the table from DataFusion.
3. Delete all existing `.parquet` files via the `ObjectStore`.
4. Save the `IndexDef` to the catalog.
5. Re-register the table as a `ListingTable` with
   `ListingOptions::with_file_sort_order(...)`.
6. Write the sorted data back via a temporary `MemTable` and
   `INSERT INTO "table" SELECT * FROM __potato_idx_tmp`.

### Optimizer benefits

When DataFusion knows files are sorted, it can:

- **Skip redundant sorts** -- `ORDER BY` on the indexed columns becomes
  a no-op streaming merge.
- **Prune row groups more aggressively** -- sorted data produces tighter
  min/max statistics per row group.
- **Terminate early on LIMIT** -- `SELECT ... ORDER BY indexed_col LIMIT 10`
  reads only the first rows.

### Notes

- Multiple index definitions can co-exist in the catalog.
- One index is marked `primary` (physical ordering), others may be
  `logical_only` hints.
- New inserts may still append unsorted files; periodic maintenance
  (`VACUUM` / index rebuild) improves ordering quality.

---

## Parquet tuning

All tuning knobs are set in `build_session_config()`:

| Setting                       | Value       | Effect                                       |
|-------------------------------|-------------|----------------------------------------------|
| `pushdown_filters`            | `true`      | Push `WHERE` predicates into the Parquet reader |
| `reorder_filters`             | `true`      | Reorder predicates by selectivity             |
| `pruning`                     | `true`      | Skip row groups via min/max statistics        |
| `enable_page_index`           | `true`      | Skip individual pages within row groups       |
| `bloom_filter_on_read`        | `true`      | Check bloom filters for equality predicates   |
| `bloom_filter_on_write`       | `true`      | Embed bloom filters in every Parquet file      |
| `compression`                 | `zstd(3)`   | Zstandard level 3 compression                 |
| `dictionary_enabled`          | `true`      | Dictionary-encode all columns                 |
| `statistics_enabled`          | `page`      | Write min/max statistics per page             |
| `max_row_group_size`          | `1,048,576` | Rows per row group                            |
| `write_batch_size`            | `16,384`    | Parquet write buffer size (env: `POTATODB_PARQUET_WRITE_BATCH_SIZE`) |
| `data_page_row_count_limit`   | `20,000`    | Max rows per data page                        |
| `batch_size`                  | `32,768`    | Arrow batch size (env: `POTATODB_BATCH_SIZE`) |
| `target_partitions`           | CPU cores   | Parallel partitions (env: `POTATODB_TARGET_PARTITIONS`) |

---

## Profiling (macOS)

On macOS, use `scripts/profile_sample.sh` to profile potatodb with the `sample` tool
while running a SQL workload:

```bash
./scripts/profile_sample.sh sample_data.sql
```

Environment variables control sampling:

| Variable             | Default | Purpose                              |
|----------------------|---------|--------------------------------------|
| `SAMPLE_INTERVAL_MS` | `1`     | Sampling interval in milliseconds    |
| `SAMPLE_DURATION_SEC`| `10`    | Total sampling duration in seconds   |
| `SAMPLE_OUTPUT`      | auto    | Output report file path              |
| `DATA_DIR`           | `./potatodb_profile_data` | PotatoDB data directory     |

Example with longer sampling:

```bash
SAMPLE_DURATION_SEC=30 SAMPLE_INTERVAL_MS=2 ./scripts/profile_sample.sh sample_data.sql
```

---

## S3 integration

When `data_url` starts with `s3://`, the engine:

1. Parses bucket name and key prefix from the URL.
2. Builds an `AmazonS3` store via `AmazonS3Builder::from_env()`, which
   reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and
   `AWS_ENDPOINT_URL` from the environment.
3. Applies any explicit overrides from `S3Config` (endpoint, region,
   allow_http).
4. Registers the store with DataFusion:
   `ctx.register_object_store(&Url::parse("s3://bucket")?, store)`.

After registration, all `ListingTableUrl::parse("s3://bucket/prefix/table")`
calls route I/O through the registered store automatically. The catalog
is also stored in S3 at `<prefix>/catalog.json`.

### DROP TABLE on S3

S3 has no directory concept. `handle_drop_table` lists all objects under
the table's key prefix and deletes them one by one via `ObjectStore::delete`.

---

## In-memory storage

When `data_url` is `:memory:`, `memory` (case-insensitive), or starts with
`memory://`, the engine:

1. Parses an optional host (default `potatodb`) and optional path prefix,
   mirroring S3 `s3://bucket/prefix` layout.
2. Builds an [`InMemory`](https://docs.rs/object_store/latest/object_store/memory/struct.InMemory.html)
   store and registers it with DataFusion:
   `ctx.register_object_store(&Url::parse("memory://host/")?, store)`.
3. Sets `is_memory` and keeps `wal` / `ArrowWal` as `None`; no CDC log path on
   disk.
4. Stores catalog and Parquet objects under the same key-prefix rules as S3
   (`catalog.json` or `{prefix}/catalog.json`).

`PotatoDB::is_in_memory()` exposes the mode to callers. `backup` and `restore`
return an error (as with S3): there is no local directory tree to archive.

---

## REPL

The `potatodb-repl` crate provides a line-mode SQL shell using `rustyline`.

- **Multi-line input** -- lines are buffered until a `;` terminator.
- **History** -- saved to `~/.potatodb_history` between sessions.
- **Special commands** -- `\q` (also `quit`, `exit`), `\dt`, `\d <table>`,
  `\di`, `\dv`, `\timing`, `\i <file>` (also `.source <file>`),
  `\backup <archive>`, `\restore <archive>`, `.import`, `.export`.
- **Timing** -- each query prints row count and wall-clock duration.
  The `\timing` command toggles detailed per-statement timing with
  I/O metrics (rows read, bytes scanned, elapsed breakdown).

---

## TUI

The `potatodb-tui` crate provides a full-screen terminal UI using
`ratatui`. It supports a subset of meta-commands: `\dt`, `\di`, `\dv`,
`\d <table>`, `\ds`, `\df`, `\du`, and `\timing`. File-oriented
commands (`\i`, `\backup`, `\restore`, `.import`, `.export`) are only
available in the REPL. The `\timing` command toggles detailed I/O
metrics in the status bar.

### Layout

```
┌─ PotatoDB ── TUI Mode ──────────────────────────────────────┐
│ ┌─ Tables ─┐ ┌─ Results ──────────────────────────────────┐ │
│ │ users    │ │ +----+-------+-----------+                 │ │
│ │ orders   │ │ | id | name  | email     |                 │ │
│ │          │ │ +----+-------+-----------+                 │ │
│ │          │ │ | 1  | Alice | alice@... |                 │ │
│ │          │ │ +----+-------+-----------+                 │ │
│ └──────────┘ └────────────────────────────────────────────┘ │
│ ┌─ Query ─────────────────────────────────────────────────┐ │
│ │ potatodb> SELECT * FROM users;█                         │ │
│ └─────────────────────────────────────────────────────────┘ │
│  3 row(s) | PT0.005S │ ↑↓ history  PgUp/Dn scroll  ...    │ │
└─────────────────────────────────────────────────────────────┘
```

### Keyboard shortcuts

| Key            | Action                        |
|----------------|-------------------------------|
| Enter          | Execute the current query     |
| Up / Down      | Browse query history          |
| PgUp / PgDn    | Scroll the results pane       |
| Tab            | Toggle the table sidebar      |
| Home / End     | Move cursor to start/end      |
| Ctrl+C         | Quit                          |

---

## CLI arguments

| Argument | Type | Default | Env | Description |
|----------|------|---------|-----|-------------|
| `--data-dir` | `String` | `./potatodb_data` | | Storage location (local path, `s3://` URL, `:memory:`, or `memory://...`) |
| `--s3-endpoint` | `String` | | | S3-compatible endpoint URL |
| `--s3-region` | `String` | `us-east-1` | | AWS/S3 region |
| `--s3-allow-http` | `bool` | `false` | | Allow plain HTTP to S3 (for MinIO, etc.) |
| `--wal-dir` | `String` | | | Separate directory for write-ahead logs |
| `--repl` | `bool` | `false` | | Use line-mode REPL instead of TUI |
| `--theme` | `String` | `potato` | `POTATODB_THEME` | TUI colour theme (`potato`, `catppuccin-mocha`) |
| `-f`, `--file` | `Vec<String>` | | | SQL file(s) to execute non-interactively |
| `--timing` | `bool` | `false` | | Print total execution time for file mode |
| `--http-addr` | `String` | | | Start the HTTP API server on this address |

---

## CLI file execution

The `potatodb` binary accepts one or more `--file` / `-f` arguments to
execute SQL files non-interactively. Output is formatted identically to
the REPL (ASCII tables via `format_batches_truncated`, row counts, and
simplified error messages).

```bash
# Execute a single file
cargo run --release -- -f schema.sql

# Execute multiple files in order
cargo run --release -- -f schema.sql -f seed.sql -f queries.sql

# Show total elapsed time at the end
cargo run --release -- -f workload.sql --timing
```

When `--timing` is enabled, the total wall-clock time for all file
execution is printed at the end.

---

## C / C++ FFI

The `potatodb-ffi` crate produces both `libpotatodb_ffi.a` (static) and
`libpotatodb_ffi.so` (shared). It provides three layers:

### Layer 1: Rust FFI bridge (`src/lib.rs`)

- `extern "C"` functions with `#[no_mangle]`.
- Opaque `potato_db` and `potato_result` handles passed as raw pointers.
- Each `potato_db` owns a `tokio::Runtime` so all async engine calls
  are driven with `rt.block_on()`.

### Layer 2: C header (`include/potatodb.h`)

Declares all symbols with `extern "C"` linkage. Key functions:

| Function                        | Purpose                                   |
|---------------------------------|-------------------------------------------|
| `potato_open` / `potato_open_local` | Open a database                       |
| `potato_close`                  | Close and free the database handle        |
| `potato_execute`                | Execute SQL, get a result handle          |
| `potato_execute_readonly`       | Execute read-only SQL                     |
| `potato_prepare`                | Prepare named statement                   |
| `potato_execute_prepared`       | Execute prepared statement with params    |
| `potato_backup` / `potato_restore` | Create and restore local archives      |
| `potato_last_error`             | Last error string (NULL if none)          |
| `potato_result_get_kind`        | Records vs. message discriminant          |
| `potato_result_row_count`       | Total rows                                |
| `potato_result_column_count`    | Number of columns                         |
| `potato_result_column_name`     | Column name by index                      |
| `potato_result_get_column_type` | Column type tag                           |
| `potato_result_display`         | Formatted ASCII table                     |
| `potato_result_is_null`         | NULL check at (row, col)                  |
| `potato_result_get_int`         | Read `int64_t`                            |
| `potato_result_get_double`      | Read `double`                             |
| `potato_result_get_bool`        | Read `bool`                               |
| `potato_result_get_string`      | Read `char*` (caller frees with `potato_string_free`) |
| `potato_result_free`            | Free result handle                        |

### Layer 3: C++ header (`include/potatodb.hpp`)

Header-only, C++17, **no exceptions** (`-fno-exceptions` safe).

- `potato::Expected<T>` -- result type holding a value or error string.
- `potato::Database` -- RAII handle; constructed via static `open()` /
  `open_s3()` factory methods that return `Expected<Database>`.
- `potato::Result` -- RAII handle with typed accessors (`get_int`,
  `get_string`, etc.).

All fallible methods return `Expected<T>`. Callers check with
`if (!result)` and read `.error()`.

### Memory ownership rules

| Returned pointer                        | Lifetime / ownership                                |
|-----------------------------------------|-----------------------------------------------------|
| `potato_last_error` return value        | Valid until next call on the same `potato_db`        |
| `potato_result_message` return value    | Valid for the lifetime of the `potato_result`        |
| `potato_result_column_name` return value| Valid for the lifetime of the `potato_result`        |
| `potato_result_display` return value    | Valid for the lifetime of the `potato_result`        |
| `potato_result_get_string` return value | Caller-owned; free with `potato_string_free`         |

### Linking

```bash
# Static linking (recommended)
g++ -std=c++17 -fno-exceptions -O2 \
    -Icrates/ffi/include my_app.cpp \
    -Ltarget/release -lpotatodb_ffi \
    -lpthread -ldl -lm -o my_app

# Dynamic linking
g++ -std=c++17 -fno-exceptions -O2 \
    -Icrates/ffi/include my_app.cpp \
    -Ltarget/release -lpotatodb_ffi \
    -o my_app
# then: LD_LIBRARY_PATH=target/release ./my_app
```

A `CMakeLists.txt` is provided in `crates/ffi/` for CMake integration.

---

## Runtime environment variables

### Engine

| Variable | Default | Purpose |
|----------|---------|---------|
| `POTATODB_WAL_CHECKPOINT_BYTES` | `4294967296` | Checkpoint WAL when it grows beyond this size (4 GB) |
| `POTATODB_WRITE_BUFFER_ROWS` | `10000` | Flush buffered inserts when row threshold is reached |
| `POTATODB_WRITE_BUFFER_BYTES` | `67108864` | Flush buffered inserts when byte threshold is reached |
| `POTATODB_WRITE_BUFFER_MS` | `360000` | Flush buffered inserts when oldest buffered rows age out (360 s) |
| `POTATODB_AUTO_ANALYZE_ROWS` | `10000` | Run `ANALYZE` automatically after this many written rows |
| `POTATODB_SLOW_QUERY_MS` | `500` | Slow-query warning threshold |
| `POTATODB_QUERY_LOG_MAX` | `200` | In-memory recent-query log capacity |
| `POTATODB_CDC_CAPACITY` | `2000` | Maximum CDC events kept in memory |
| `POTATODB_SNAPSHOT_RETENTION_MS` | `86400000` | Snapshot retention window for time-travel queries |
| `POTATODB_BATCH_SIZE` | `32768` | DataFusion Arrow batch size during execution |
| `POTATODB_PARQUET_WRITE_BATCH_SIZE` | `16384` | Parquet write buffer size per batch |
| `POTATODB_TARGET_PARTITIONS` | CPU cores | Number of parallel execution partitions |
| `POTATODB_ENFORCE_BATCH_SIZE_IN_JOINS` | `true` | Enforce batch size in hash joins (reduces memory churn) |
| `POTATODB_COALESCE_BATCHES` | `true` | Coalesce small batches between operators |
| `POTATODB_PARQUET_COMPRESSION` | `zstd(3)` | Parquet compression algorithm (`zstd(N)`, `snappy`, `gzip(N)`, `brotli(N)`, `lz4`, `lz4_raw`, `uncompressed`) |
| `POTATODB_THEME` | `potato` | TUI colour theme (`potato`, `catppuccin-mocha`) |

### Allocator

| Variable | Default | Purpose |
|----------|---------|---------|
| `MIMALLOC_PURGE_DELAY` | `10` | Seconds before mimalloc returns memory to the OS (set automatically at startup if unset) |

### Server

| Variable | Default | Purpose |
|----------|---------|---------|
| `POTATODB_USER` | `potatodb` | pgwire auth username |
| `POTATODB_PASSWORD` | `potatodb` | pgwire auth password |
| `POTATODB_AUTO_VACUUM_INTERVAL_SECS` | `0` | Background compaction interval (`0` disables) |
| `POTATODB_AUTO_VACUUM_FILE_THRESHOLD` | `25` | File-count score component for compaction |
| `POTATODB_AUTO_VACUUM_BYTES_THRESHOLD` | `268435456` | Byte-size score component for compaction |
| `POTATODB_AUTO_VACUUM_AGE_SECS` | `3600` | Age-trigger for compaction candidates |
| `POTATODB_TLS_CERT` | unset | Path to TLS certificate (enables pgwire TLS when set with key) |
| `POTATODB_TLS_KEY` | unset | Path to TLS private key |

---

## Workspace layout

```
potatodb/
  Cargo.toml                   workspace root + shared dependency versions
  Makefile                     build, test, format, perftest targets
  rustfmt.toml                 formatting rules (applied workspace-wide)
  .cargo/config.toml           (optional: RUSTFLAGS=-C target-cpu=native for local SIMD)
  README.md                    user-facing overview
  DOCS.md                      this file
  sample_data.sql              large sample dataset for profiling
  crates/
    catalog/                   potatodb-catalog
    display/                   potatodb-display
    engine/                    potatodb-engine
      tests/smoke.rs           integration tests
    wal/                       potatodb-wal
    server/                    potatodb-server (pgwire)
    ffi/                       potatodb-ffi
      include/potatodb.h       C header
      include/potatodb.hpp     C++ header
      examples/main.cpp        C++ example
      CMakeLists.txt           CMake build for C++ consumers
    python/                    potatodb-python bindings (PyO3)
    http/                      potatodb-http (REST API)
    nodejs/                    potatodb-nodejs (Node.js integration)
    repl/                      potatodb-repl
    tui/                       potatodb-tui
    potatodb/                  binary entry point
  examples/
    examples/perftest.rs       end-to-end performance test binary
```

All dependency versions are centralized under `[workspace.dependencies]`
in the root `Cargo.toml`. Individual crates reference them with
`dep.workspace = true`.

---

## Build & release profiles

### Debug (default)

```bash
cargo build           # unoptimized, debug symbols
cargo run             # run the REPL
cargo test --workspace
```

### Release

```bash
cargo build --release
cargo run --release
```

The release profile in the root `Cargo.toml` enables:

| Setting             | Value    | Effect                                              |
|---------------------|----------|-----------------------------------------------------|
| `opt-level`         | `3`      | Maximum compiler optimizations                      |
| `lto`               | `"fat"`  | Full cross-crate link-time optimization             |
| `codegen-units`     | `1`      | Single codegen unit for maximum inlining            |
| `strip`             | `true`   | Strip debug symbols from the binary                 |
| `panic`             | `"abort"`| Remove unwind tables for smaller, faster code       |

For local builds, you can set `RUSTFLAGS="-C target-cpu=native"` so the
compiler emits AVX2/AVX-512/NEON instructions available on your machine.
This significantly benefits Arrow's columnar operations and Parquet codec
hot paths. (Not enabled by default to avoid SIGILL on CI runners.)

---

## Formatting conventions

The workspace uses `rustfmt.toml` at the repository root. Key settings:

| Rule                        | Value    |
|-----------------------------|----------|
| `max_width`                 | `100`    |
| `tab_spaces`                | `4`      |
| `use_field_init_shorthand`  | `true`   |
| `use_try_shorthand`         | `true`   |
| `reorder_imports`           | `true`   |
| `reorder_modules`           | `true`   |
| `newline_style`             | `Unix`   |

Run the formatter workspace-wide:

```bash
cargo fmt --all
```

Check without modifying:

```bash
cargo fmt --all -- --check
```

---

## Testing

Integration tests live in `crates/engine/tests/smoke.rs` and now cover:

- core lifecycle (`CREATE TABLE`, `INSERT`, `SELECT`, `DROP`)
- transaction commit/rollback including destructive rewrite paths
- additional types, UDFs, procedures, full-text query rewrite
- time-travel (`AS OF TIMESTAMP`)
- foreign keys (`RESTRICT`, `CASCADE`, `SET NULL`)
- CDC virtual table, `LISTEN`/`NOTIFY`, plan cache behavior
- copy/import schema evolution paths

```bash
# Run all tests
cargo test --workspace

# Run only engine integration tests
cargo test -p potatodb-engine

# Run a single test by name
cargo test -p potatodb-engine test_create_index_sorts_data
```

### Performance testing

The `examples/examples/perftest.rs` binary runs an end-to-end workload
covering bulk inserts, point lookups, full scans, aggregations, joins,
subqueries, CTEs, window functions, sorting, DML, DDL, and transactions.
Each benchmark is repeated for a configurable number of iterations, and
the results include median, mean, min, max, and p95 timings.

```bash
# Run with defaults (scale=1, iterations=3)
make perftest

# Save a JSON baseline
make perf-save

# Run and compare against the saved baseline
make perf-compare

# Custom parameters
make perftest PERF_SCALE=2 PERF_ITERS=5
```

The JSON report structure:

```json
{
  "timestamp": 1710000000,
  "scale": 1,
  "iterations": 3,
  "seed_ms": 1234.5,
  "benchmarks": {
    "bulk_insert_1000": {
      "median_ms": 12.3,
      "mean_ms": 13.1,
      "min_ms": 11.0,
      "max_ms": 16.0,
      "p95_ms": 15.8,
      "iterations": 3
    }
  }
}
```

When `--baseline <path>` is provided, the test prints a formatted
comparison table showing the delta percentage for each benchmark
relative to the baseline.

### Generating rustdoc

```bash
cargo doc --workspace --no-deps --open
```

Every public struct, enum, function, and field has a `///` doc comment.
Module-level `//!` comments describe the purpose and design of each crate.
