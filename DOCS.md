# PotatoDB -- Technical Documentation

This document covers the internal design, data flow, storage format,
configuration knobs, and integration APIs in detail. For a quick-start
guide and feature overview, see [README.md](README.md).

## Current capability snapshot

Recent engine extensions include:

- additional SQL types (`UUID`, `INTERVAL`, `ARRAY`, `JSON/JSONB`)
- user-defined SQL functions (`CREATE FUNCTION` / `DROP FUNCTION`)
- destructive rewrite operations enabled inside transactions
- snapshot-based time-travel reads (`AS OF TIMESTAMP`)
- foreign-key constraints with `RESTRICT` / `CASCADE` / `SET NULL`
- CDC virtual table (`potatodb_cdc`)
- `LISTEN` / `NOTIFY`
- procedure support (`CREATE PROCEDURE`, `CALL`, `DO $$...$$`)
- full-text index metadata with `fts_match(...)` rewrite
- HTTP API crate (`potatodb-http`) and TLS-enabled pgwire server

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
9. [REPL](#repl)
10. [TUI](#tui)
11. [C / C++ FFI](#c--c-ffi)
12. [Runtime environment variables](#runtime-environment-variables)
13. [Workspace layout](#workspace-layout)
14. [Build & release profiles](#build--release-profiles)
15. [Formatting conventions](#formatting-conventions)
16. [Testing](#testing)

---

## Storage model

Every table is backed by a directory (local filesystem) or an object key
prefix (S3). PotatoDB writes Parquet files through DataFusion `ListingTable`.
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
trait, so the same code path works for both local files and S3.

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

---

## Engine internals

`PotatoDB` is the central struct. It owns:

| Field         | Type                      | Purpose                                |
|---------------|---------------------------|----------------------------------------|
| `ctx`         | `SessionContext`          | DataFusion query engine                |
| `catalog`     | `Catalog`                 | Persistent table/index metadata        |
| `data_url`    | `String`                  | Canonical base path or `s3://` URL     |
| `store`       | `Arc<dyn ObjectStore>`    | Parquet + catalog I/O                  |
| `is_s3`       | `bool`                    | Quick backend check                    |
| `s3_prefix`   | `String`                  | Key prefix within the S3 bucket        |
| `wal`         | `Option<Wal>`             | Local write-ahead log handle           |
| `write_buffer`| `HashMap<...>`            | Per-table buffered INSERT batches      |

On construction (`PotatoDB::new`), the engine:

1. Configures a `SessionConfig` with tuned Parquet options.
2. Sets up the appropriate `ObjectStore` (local `LocalFileSystem` or
   `AmazonS3`).
3. Registers the store with DataFusion (S3 only).
4. Loads the catalog from `catalog.json`.
5. Opens/replays `wal.log` for local storage, then checkpoints it.
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

---

## SQL processing pipeline

```
SQL string
  │
  ▼
sqlparser::Parser  (PostgreSQL dialect)
  │
  ├─ CreateTable  ──► handle_create_table()  ──► mkdir + register ListingTable + save catalog
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
| `write_batch_size`            | `8,192`     | Parquet write buffer size (env: `POTATODB_PARQUET_WRITE_BATCH_SIZE`) |
| `data_page_row_count_limit`   | `20,000`    | Max rows per data page                        |
| `batch_size`                  | `8,192`     | Arrow batch size (env: `POTATODB_BATCH_SIZE`) |
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

## REPL

The `potatodb-repl` crate provides a line-mode SQL shell using `rustyline`.

- **Multi-line input** -- lines are buffered until a `;` terminator.
- **History** -- saved to `~/.potatodb_history` between sessions.
- **Special commands** -- `\q`, `\dt`, `\d <table>`, `\di`, `\dv`,
  `\backup <archive>`, `\restore <archive>`, `.import`, `.export`.
- **Timing** -- each query prints row count and wall-clock duration
  (via `chrono::Utc`).

---

## TUI

The `potatodb-tui` crate provides a full-screen terminal UI using
`ratatui`.

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
| `POTATODB_WAL_CHECKPOINT_BYTES` | `4194304` | Checkpoint WAL when it grows beyond this size |
| `POTATODB_WRITE_BUFFER_ROWS` | `10000` | Flush buffered inserts when row threshold is reached |
| `POTATODB_WRITE_BUFFER_BYTES` | `67108864` | Flush buffered inserts when byte threshold is reached |
| `POTATODB_WRITE_BUFFER_MS` | `5000` | Flush buffered inserts when oldest buffered rows age out |
| `POTATODB_AUTO_ANALYZE_ROWS` | `10000` | Run `ANALYZE` automatically after this many written rows |
| `POTATODB_SLOW_QUERY_MS` | `500` | Slow-query warning threshold |
| `POTATODB_QUERY_LOG_MAX` | `200` | In-memory recent-query log capacity |
| `POTATODB_CDC_CAPACITY` | `2000` | Maximum CDC events kept in memory |
| `POTATODB_SNAPSHOT_RETENTION_MS` | `86400000` | Snapshot retention window for time-travel queries |
| `POTATODB_BATCH_SIZE` | `8192` | DataFusion Arrow batch size during execution |
| `POTATODB_PARQUET_WRITE_BATCH_SIZE` | `8192` | Parquet write buffer size per batch |
| `POTATODB_TARGET_PARTITIONS` | CPU cores | Number of parallel execution partitions |
| `POTATODB_ENFORCE_BATCH_SIZE_IN_JOINS` | `true` | Enforce batch size in hash joins (reduces memory churn) |
| `POTATODB_COALESCE_BATCHES` | `true` | Coalesce small batches between operators |

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
  rustfmt.toml                 formatting rules (applied workspace-wide)
  .cargo/config.toml           RUSTFLAGS=-C target-cpu=native
  README.md                    user-facing overview
  DOCS.md                      this file
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
    repl/                      potatodb-repl
    tui/                       potatodb-tui
    potatodb/                  binary entry point
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

Additionally, `.cargo/config.toml` sets `RUSTFLAGS = -C target-cpu=native`
so the compiler emits AVX2/AVX-512/NEON instructions available on the
build machine. This significantly benefits Arrow's columnar operations
and Parquet codec hot paths.

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

### Generating rustdoc

```bash
cargo doc --workspace --no-deps --open
```

Every public struct, enum, function, and field has a `///` doc comment.
Module-level `//!` comments describe the purpose and design of each crate.
