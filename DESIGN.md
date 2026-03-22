# PotatoDB -- Design Document

This document describes the internal architecture, data structures, and
design decisions behind PotatoDB. It is intended for contributors and
anyone curious about how a Parquet-backed SQL database works end to end.

For usage and SQL examples see [README.md](README.md). For configuration
knobs and API reference see [DOCS.md](DOCS.md).

---

## Table of contents

1. [Design philosophy](#design-philosophy)
2. [Architecture overview](#architecture-overview)
3. [Crate structure](#crate-structure)
4. [Data model](#data-model)
5. [Catalog](#catalog)
6. [SQL processing pipeline](#sql-processing-pipeline)
7. [Write path](#write-path)
8. [Read path](#read-path)
9. [Mutation path (UPDATE / DELETE)](#mutation-path)
10. [DDL operations](#ddl-operations)
11. [Indexes](#indexes)
12. [Transactions](#transactions)
13. [Write-ahead log](#write-ahead-log)
14. [Views and materialized views](#views-and-materialized-views)
15. [Sequences](#sequences)
16. [Maintenance](#maintenance)
17. [S3 backend](#s3-backend)
18. [Query execution and optimization](#query-execution-and-optimization)
19. [pgwire server](#pgwire-server)
20. [Client interfaces](#client-interfaces)
21. [Backup and restore](#backup-and-restore)
22. [Performance design](#performance-design)

---

## Design philosophy

PotatoDB sits at the intersection of a traditional SQL database and a
lakehouse query engine. The core idea:

- **Parquet as the storage layer.** Every table is a directory of
  Parquet files. No custom page format, no B-tree on disk. Parquet
  gives columnar compression, bloom filters, min/max statistics, and
  dictionary encoding for free.

- **DataFusion as the execution engine.** Rather than writing a custom
  query planner, PotatoDB delegates planning, optimization, and
  execution to Apache DataFusion. This provides a mature cost-based
  optimizer, vectorized execution, predicate pushdown, and parallelism
  across CPU cores.

- **Thin orchestration layer on top.** PotatoDB adds what DataFusion
  does not provide out of the box: a persistent catalog, a write-ahead
  log, buffered ingestion, constraint enforcement, MVCC transactions,
  maintenance commands (VACUUM, ANALYZE), and multiple client
  interfaces (REPL, TUI, pgwire, C/C++ FFI, Python).

The result is a lightweight database that gets the analytical
performance of Parquet with the ergonomics of PostgreSQL-compatible
SQL.

---

## Architecture overview

```
                    ┌────────────────────────────────────────────────┐
                    │                  Clients                       │
                    │  REPL  │  TUI  │  pgwire  │  FFI  │  Python    │
                    └────────┬───────┬──────────┬───────┬────────────┘
                             │       │          │       │
                             ▼       ▼          ▼       ▼
                    ┌────────────────────────────────────────────────┐
                    │              PotatoDB Engine                   │
                    │                                                │
                    │  SQL dispatch ──► Statement handlers           │
                    │  Write buffer ──► Flush to Parquet             │
                    │  Constraint validation                         │
                    │  Transaction coordinator (MVCC)                │
                    │  Query log / slow query detection              │
                    │  Auto-analyze trigger                          │
                    │                                                │
                    │  ┌──────────────────────────────────────────┐  │
                    │  │          Apache DataFusion               │  │
                    │  │  SQL parser ──► Logical plan             │  │
                    │  │  Cost-based optimizer                    │  │
                    │  │  Physical plan ──► Vectorized execution  │  │
                    │  │  ListingTable (Parquet reader/writer)    │  │
                    │  └──────────────────────────────────────────┘  │
                    └────────┬───────────────────────┬───────────────┘
                             │                       │
                    ┌────────▼────────┐    ┌─────────▼──────────┐
                    │    Catalog      │    │    WAL             │
                    │  catalog.json   │    │    wal.log         │
                    │  (JSON, via     │    │    (binary, local  │
                    │   ObjectStore)  │    │     filesystem)    │
                    └────────┬────────┘    └────────────────────┘
                             │
                    ┌────────▼────────────────────────────────────┐
                    │              ObjectStore                    │
                    │ LocalFileSystem │ AmazonS3 │ InMemory       │
                    └────────┬────────┴────┬─────┴────────┬───────┘
                             │             │              │
                    ┌────────▼────────┐   ┌─▼────────┐  ┌──▼──────────────┐
                    │  Local disk     │   │ S3/MinIO │  │ RAM (process)   │
                    │  potatodb_data/ │   │s3://…/   │  │ memory://…/     │
                    │   table_a/      │   │ *.parquet│  │ *.parquet       │
                    │     *.parquet   │   │catalog   │  │ catalog.json    │
                    │   catalog.json  │   │ .json    │  │ (ephemeral)     │
                    └─────────────────┘   └──────────┘  └─────────────────┘
```

---

## Crate structure

The workspace is organized as nine crates. Each has a focused
responsibility and a narrow public API.

```
potatodb/
  Cargo.toml                    workspace root
  crates/
    catalog/                    potatodb-catalog
    display/                    potatodb-display
    engine/                     potatodb-engine
    wal/                        potatodb-wal
    server/                     potatodb-server
    ffi/                        potatodb-ffi
    python/                     potatodb-python
    repl/                       potatodb-repl
    tui/                        potatodb-tui
    potatodb/                   binary entry point
```

### Dependency graph

```
potatodb (binary)                 potatodb-server
  ├── potatodb-engine               ├── potatodb-engine
  │     ├── potatodb-catalog        └── potatodb-display
  │     └── potatodb-wal
  ├── potatodb-repl               potatodb-ffi
  │     ├── potatodb-engine         ├── potatodb-engine
  │     └── potatodb-display        └── potatodb-display
  └── potatodb-tui
        ├── potatodb-engine       potatodb-python
        └── potatodb-display        └── potatodb-engine
```

### Responsibilities

| Crate | Role |
|-------|------|
| `potatodb-catalog` | JSON-serialized metadata for tables, indexes, views, sequences. Snapshot/restore for transactions. Persisted through the ObjectStore trait. |
| `potatodb-wal` | Binary append-only write-ahead log. Entry-level CRC-32C. Replay, checkpoint, per-entry fsync. |
| `potatodb-engine` | Central `PotatoDB` struct. SQL dispatch, statement handlers, write buffering, constraint validation, transaction coordination, DataFusion session management. |
| `potatodb-display` | Thin wrapper around Arrow's pretty printer. Formats `RecordBatch` as ASCII tables. |
| `potatodb-server` | pgwire protocol server. MD5 auth, simple and extended query handlers, auto-vacuum background task. |
| `potatodb-ffi` | C/C++ foreign function interface. Opaque handles, `extern "C"` bridge, header-only C++17 RAII wrapper. |
| `potatodb-python` | PyO3 bindings. `PotatoDB.open()`, `execute()`, `close()`. |
| `potatodb-repl` | Interactive SQL shell. Rustyline, multi-line input, history, tab completion, special commands. |
| `potatodb-tui` | Full-screen ratatui terminal UI. Table sidebar, scrollable results, query history. |
| `potatodb` | Binary entry point. CLI argument parsing via clap. |

---

## Data model

### One table = one directory of Parquet files

Every table maps to a filesystem directory (local), an object key
prefix (S3), or the same prefix layout on an in-memory `ObjectStore`.
Each `INSERT` produces one Parquet file named with a UUID.
`SELECT` queries read across all files in the directory (or listing prefix).

```
potatodb_data/
  catalog.json
  wal.log
  users/
    a3f1...e7.parquet       ← first INSERT batch
    c8b2...d4.parquet       ← second INSERT batch
  orders/
    19fa...b1.parquet
```

This design has a deliberate trade-off: writes are append-only and fast,
but many small files accumulate over time. `VACUUM` merges them back
into one large file. Auto-vacuum (in server mode) runs this
automatically.

### Parquet file properties

Every Parquet file written by PotatoDB uses these settings:

| Property | Value | Rationale |
|----------|-------|-----------|
| Compression | Zstd level 3 | Good ratio-to-speed trade-off for analytical data |
| Dictionary encoding | All columns | Reduces size for low-cardinality columns |
| Statistics | Page-level min/max | Enables fine-grained predicate pruning |
| Bloom filters | Every column | Speeds up equality predicates |
| Row group size | 1,048,576 rows | Large enough for efficient columnar scans |
| Data page size | 20,000 rows | Fine-grained page-index pruning within row groups |
| Write batch size | 1,024 rows | Arrow batch size during Parquet writes |

### Schema ownership

The schema of a table is always defined by the catalog, not inferred
from Parquet file headers. When a table is registered with DataFusion
as a `ListingTable`, the schema is passed explicitly from
`catalog.tables[name].columns`. DataFusion uses this schema to read
all Parquet files in the directory.

This means `ALTER TABLE ADD COLUMN` only updates the catalog. Old
Parquet files that lack the new column still read correctly because
DataFusion fills missing columns with NULLs during schema merging.

---

## Catalog

The catalog is the single source of truth for all database metadata.

### Data structures

```
CatalogData
  ├── tables:    HashMap<String, TableMeta>
  ├── indexes:   HashMap<String, IndexDef>
  ├── views:     HashMap<String, ViewDef>
  └── sequences: HashMap<String, SequenceDef>
```

**TableMeta** stores everything about a table:

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `String` | Table name |
| `columns` | `Vec<ColumnDef>` | Column name, data type, nullability |
| `path` | `String` | Absolute path or S3 URL to the table directory |
| `partition_columns` | `Vec<String>` | Hive-style partition columns |
| `statistics` | `Option<TableStatistics>` | Row count, per-column null/distinct/min/max |
| `retention_seconds` | `Option<u64>` | File age limit for retention-based cleanup |
| `constraints` | `Vec<TableConstraint>` | PrimaryKey, Unique, Check constraints |
| `file_stats` | `Vec<FileStats>` | Per-file row count, size, min/max, creation time |

**TableConstraint** is an enum with three variants:

- `PrimaryKey { columns }` -- enforces uniqueness and non-null.
- `Unique { name, columns }` -- enforces uniqueness, allows NULLs.
- `Check { name, expr }` -- arbitrary boolean expression.

**IndexDef** stores index metadata:

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `String` | Index name |
| `table_name` | `String` | Owning table |
| `columns` | `Vec<IndexColumn>` | Column name + ascending/descending |

**ViewDef** represents both regular and materialized views:

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `String` | View name |
| `sql` | `String` | Defining query |
| `materialized` | `bool` | Whether this is a materialized view |
| `backing_table` | `Option<String>` | Hidden table name for materialized views |

**SequenceDef** tracks auto-increment state:

| Field | Type | Purpose |
|-------|------|---------|
| `name` | `String` | Sequence name |
| `current_value` | `i64` | Next value to return |
| `increment` | `i64` | Step size |
| `min_value` | `Option<i64>` | Lower bound |
| `max_value` | `Option<i64>` | Upper bound |

### Persistence

The catalog is serialized as pretty-printed JSON via serde and written
through the `ObjectStore` trait. This means the same code path works
for local files (`potatodb_data/catalog.json`), S3
(`s3://bucket/prefix/catalog.json`), and in-memory storage (`catalog.json`
or `{prefix}/catalog.json` keys in `InMemory`).

On every mutation (`add_table`, `remove_table`, `add_index`, etc.), the
full catalog is re-serialized and written atomically. During an explicit
transaction, `save()` is suppressed; only `force_save()` (called at
`COMMIT`) persists changes.

### Legacy migration

Catalogs created before index/view/sequence support stored only a bare
`HashMap<String, TableMeta>` at the JSON root. The loader tries
`CatalogData` first; on failure, it falls back to the legacy format
and fills empty maps for indexes, views, and sequences. The next
`save()` upgrades the file silently.

### Snapshot and restore

The transaction system depends on the catalog being cheaply
snapshotable:

- `snapshot()` clones all four `HashMap`s into a tuple.
- `restore(snap)` replaces the four maps from the tuple.

This gives `ROLLBACK` the ability to undo all catalog mutations made
since `BEGIN`, including tables created, indexes added, and sequences
advanced.

---

## SQL processing pipeline

### Dispatch overview

Every SQL string enters through `PotatoDB::execute()`. The dispatch
is a two-stage process: token-based fast paths, then full parsing.

```
execute(sql)
  │
  ├─ expand_nextval_calls(sql)     ← replace nextval('seq') with literals
  ├─ strip_as_of_timestamp(sql)    ← compatibility shim
  │
  ├─ Token-level dispatch (before parsing):
  │    FLUSH / FLUSH TABLE        → flush_all() or flush_table()
  │    TRUNCATE                   → handle_truncate()
  │    REFRESH MATERIALIZED VIEW  → handle_refresh_materialized_view()
  │    VACUUM / ANALYZE           → handle_vacuum() / handle_analyze()
  │
  ├─ If SQL does not start with INSERT:
  │    flush_all()                ← make buffered inserts visible
  │
  ├─ Parse with sqlparser (PostgreSQL dialect)
  │
  ├─ Statement dispatch:
  │    StartTransaction           → handle_begin()
  │    Commit                     → handle_commit()
  │    Rollback                   → handle_rollback()
  │    CreateTable                → handle_create_table()
  │    CreateIndex                → handle_create_index()
  │    CreateSequence             → handle_create_sequence()
  │    CreateView                 → handle_create_view()
  │    Drop(Table)                → handle_drop_table()
  │    Drop(Index)                → handle_drop_index()
  │    Drop(View)                 → handle_drop_view()
  │    Drop(Sequence)             → handle_drop_sequence()
  │    Insert                     → handle_insert()
  │    Delete                     → handle_delete()
  │    Update                     → handle_update()
  │    AlterTable                 → handle_alter_table()
  │    Prepare                    → handle_prepare()
  │    Execute                    → handle_execute_prepared()
  │    Copy                       → handle_copy_from() or handle_copy_to()
  │    Explain                    → ctx.sql(normalize_explain_sql())
  │    Everything else            → ctx.sql(sql)  (DataFusion)
  │
  └─ Post-execution:
       wal_finish_autocommit()    ← commit WAL entry, maybe checkpoint
       finalize_query()           ← log query, detect slow queries
```

### Why two-stage dispatch?

Some statements (`FLUSH`, `TRUNCATE`, `VACUUM`, `ANALYZE`) use
keywords or syntax that sqlparser does not recognize natively. The
token-level check handles these before attempting a parse. If parsing
fails for everything else, the raw SQL is forwarded to DataFusion,
which handles `SHOW TABLES`, `DESCRIBE`, and other built-in statements.

### Flush-before-read guarantee

Before executing any non-INSERT statement, `flush_all()` drains every
table's write buffer to disk. This ensures that a `SELECT` immediately
after `INSERT` always sees the inserted rows, even if the buffer
thresholds have not been reached.

### nextval expansion

Sequences are implemented as a pre-processing step: before parsing,
`expand_nextval_calls()` scans the SQL string for `nextval('name')`
calls, retrieves the next value from the catalog, and replaces the
call with a literal integer. This avoids needing a custom DataFusion
UDF.

---

## Write path

### INSERT flow

```
handle_insert(sql, stmt)
  │
  ├─ ON CONFLICT present?
  │    yes → handle_upsert()      ← see upsert section below
  │
  ├─ wal_append_pending(sql)
  ├─ Resolve target columns
  ├─ Run source query via DataFusion → batches
  │
  ├─ Needs immediate write?
  │    yes if: replaying WAL, RETURNING clause, table has constraints,
  │            or any NOT NULL column
  │    │
  │    ├─ yes → Direct path:
  │    │    INSERT via DataFusion → validate_constraints_batch()
  │    │    → maybe_auto_analyze() → refresh_table_file_stats()
  │    │    → return Records (if RETURNING) or Message
  │    │
  │    └─ no → Buffered path:
  │         validate_not_null_batches() → buffer_insert_batches()
  │         → return Message("X row(s) inserted")
  │
  └─ wal_finish_autocommit()
```

### Buffered ingestion

For INSERT-heavy workloads without constraints, PotatoDB buffers rows
in memory instead of writing a Parquet file per statement. The buffer
is stored per table:

```
write_buffer: HashMap<String, BufferedInsert>

BufferedInsert {
    columns: Vec<String>,       // column ordering of buffered data
    batches: Vec<RecordBatch>,  // accumulated Arrow batches
    row_count: usize,           // total rows in buffer
    approx_bytes: usize,        // estimated memory usage
    first_buffered_at: Instant, // when the first row was buffered
}
```

The buffer flushes to Parquet when any threshold is reached:

| Threshold | Default | Environment variable |
|-----------|---------|---------------------|
| Row count | 10,000 | `POTATODB_WRITE_BUFFER_ROWS` |
| Byte size | 64 MiB | `POTATODB_WRITE_BUFFER_BYTES` |
| Time age | 5,000 ms | `POTATODB_WRITE_BUFFER_MS` |

Flush also triggers when:

- A non-INSERT statement executes (flush-before-read guarantee).
- The column list for a new INSERT differs from the buffered columns.
- `FLUSH` or `FLUSH TABLE` is invoked explicitly.

The flush path creates a temporary `MemTable` from the buffered
batches, registers it as `__potato_flush_tmp`, and runs
`INSERT INTO "table" SELECT * FROM __potato_flush_tmp` through
DataFusion. This reuses the existing Parquet write path.

### Constraint validation

Constraints are checked after data lands in Parquet files (for direct
inserts) or during the flush path:

**NOT NULL:** Scans each column's null bitmap. Rejects if any NULL
is found in a non-nullable column.

**PRIMARY KEY:** Checks three conditions per batch:
1. No NULLs in the PK columns.
2. No duplicate key combinations within the batch.
3. No existing rows in the table share the same key (via
   `SELECT COUNT(*) ... WHERE pk_col = value`).

**UNIQUE:** Same as PRIMARY KEY, but NULLs are allowed.

**CHECK:** For each CHECK constraint, creates a temporary `MemTable`
from the batch and runs `SELECT COUNT(*) FROM tmp WHERE NOT (expr)`.
If the count is non-zero, the constraint is violated.

### Upserts (ON CONFLICT)

The upsert handler (`handle_upsert`) implements both `DO NOTHING` and
`DO UPDATE`:

1. Flush the target table so all data is on disk.
2. Resolve conflict columns from the table's PRIMARY KEY or UNIQUE
   constraint.
3. Build a `UpsertRow` per source row containing the values and the
   conflict key.
4. Query existing rows matching any conflict key.
5. Split source rows into `insert_rows` (no conflict) and
   `update_rows` (conflict found).
6. Insert new rows via a regular `INSERT INTO ... VALUES`.
7. For `DO UPDATE`: build a `SELECT` with `CASE WHEN` expressions
   that apply `EXCLUDED` values where the conflict key matches,
   then `rewrite_table()` with the modified data.
8. Run full constraint validation on the resulting table.

---

## Read path

### SELECT flow

```
execute("SELECT ...")
  │
  ├─ flush_all()                  ← drain write buffers
  ├─ Parse → Statement (not matched by custom handlers)
  ├─ ctx.sql(sql)                 ← DataFusion
  │    │
  │    ├─ Parse SQL → LogicalPlan
  │    ├─ Optimize (predicate pushdown, filter reorder, join reorder)
  │    ├─ Create PhysicalPlan
  │    ├─ Execute across target_partitions threads
  │    │    ├─ Open Parquet files via ListingTable
  │    │    ├─ Skip row groups (min/max stats)
  │    │    ├─ Skip pages (page-index pruning)
  │    │    ├─ Check bloom filters (equality predicates)
  │    │    ├─ Apply remaining predicates
  │    │    └─ Assemble RecordBatches
  │    └─ Collect → Vec<RecordBatch>
  │
  └─ finalize_query()
```

PotatoDB delegates the entire read path to DataFusion. The engine's
role is limited to:

1. Flushing write buffers before reads.
2. Registering tables with the correct schema and sort order hints.
3. Providing optimizer statistics from ANALYZE.

DataFusion handles everything else: predicate pushdown into the
Parquet reader, bloom filter checks, page-level pruning via min/max
statistics, parallel scanning across files, join strategies, and
result materialization.

### Statistics-aware table provider

When registering a table, the engine wraps the `ListingTable` in a
`StatsAwareTableProvider` that returns the statistics collected by
`ANALYZE`. This gives the DataFusion optimizer better cardinality
estimates for join ordering and filter selectivity.

---

## Mutation path

PotatoDB implements UPDATE and DELETE as **read-modify-write**
operations on Parquet files. This is a consequence of Parquet being
an immutable columnar format.

### UPDATE flow

```
handle_update(sql, stmt)
  │
  ├─ Reject if in_transaction()   ← destructive rewrite
  ├─ wal_append_pending(sql)
  │
  ├─ Build projection:
  │    For each column in the table:
  │      If the column is in SET clause:
  │        CASE WHEN (where_clause) THEN (new_value) ELSE col END
  │      Else:
  │        col
  │
  ├─ Run: SELECT <projections> FROM table → modified batches
  ├─ rewrite_table(table, schema, modified)
  │    ├─ Deregister table from DataFusion
  │    ├─ Delete all existing Parquet files
  │    ├─ Re-register empty table
  │    ├─ MemTable from modified → INSERT INTO table SELECT * FROM tmp
  │    └─ refresh_table_file_stats()
  │
  ├─ validate_table_constraints()
  ├─ validate_check_constraints()
  │
  └─ If RETURNING: SELECT returning_cols FROM table WHERE where_clause
```

The `CASE WHEN` projection approach means the entire table is read
once, the matching rows are modified in memory, and the full table
is rewritten as new Parquet files. This is efficient for bulk updates
but not optimal for single-row changes on large tables.

### DELETE flow

```
handle_delete(sql, stmt)
  │
  ├─ Reject if in_transaction()
  ├─ wal_append_pending(sql)
  │
  ├─ If RETURNING:
  │    SELECT returning_cols FROM table WHERE selection → returning_batches
  │
  ├─ Build: SELECT * FROM table WHERE NOT (selection) → surviving rows
  ├─ rewrite_table(table, schema, surviving)
  │
  └─ Return Records (if RETURNING) or Message("N row(s) deleted")
```

DELETE is implemented by selecting only the rows that do *not* match
the WHERE clause, then rewriting the table with those surviving rows.
`DELETE FROM table` (no WHERE) produces an empty result, effectively
truncating all data.

### rewrite_table utility

Both UPDATE and DELETE funnel through `rewrite_table()`:

1. Deregister the table from DataFusion.
2. Delete all existing `.parquet` files via the ObjectStore.
3. Re-register an empty `ListingTable`.
4. Create a `MemTable` from the new batches.
5. Run `INSERT INTO "table" SELECT * FROM __potato_rewrite_tmp`.
6. Refresh file stats in the catalog.

This is the same pattern used by `CREATE INDEX` and `VACUUM`.

---

## DDL operations

### CREATE TABLE

1. Check `IF NOT EXISTS`. Return early if the table already exists.
2. If the statement has a `query` (CTAS), delegate to `handle_ctas()`.
3. Append a WAL entry.
4. Parse columns via `sql_column_to_catalog()` and constraints via
   `sql_constraints_to_catalog()`.
5. Compute `table_url = data_url / table_name`.
6. Create the directory (local) or assume the prefix exists (S3).
7. Build an Arrow schema from the column definitions.
8. Register a `ListingTable` with DataFusion.
9. Build `TableMeta` and add it to the catalog.
10. Refresh file stats.

### CREATE TABLE AS SELECT (CTAS)

1. Run the query via DataFusion to get result batches.
2. Infer column definitions from the Arrow schema of the results.
3. Create the directory and register an empty `ListingTable`.
4. If the query returned data, create a `MemTable` and INSERT into
   the new table.
5. Add the table to the catalog with empty constraints.

### ALTER TABLE

All ALTER TABLE operations update the catalog and re-register the
table with DataFusion. No Parquet files are rewritten.

| Operation | What changes |
|-----------|-------------|
| ADD COLUMN | New `ColumnDef` appended to `meta.columns`. Old files lack the column; DataFusion fills NULLs on read. |
| DROP COLUMN | `ColumnDef` removed from `meta.columns`. Old files still contain the column; DataFusion ignores it based on the catalog schema. |
| RENAME COLUMN | `ColumnDef.name` updated in place. |
| RENAME TABLE | `meta.name` updated, all index `table_name` fields updated, table deregistered and re-registered under the new name. The storage path does not change. |

### DROP TABLE

1. Check `IF EXISTS`.
2. Append a WAL entry.
3. Remove the table (and its indexes) from the catalog.
4. Deregister from DataFusion.
5. If inside a transaction: defer file deletion to `COMMIT`.
6. Otherwise: delete all Parquet files immediately.
   - Local: `remove_dir_all(table_dir)`.
   - S3: list all objects under the prefix, delete each one.

### TRUNCATE

1. Append a WAL entry.
2. Deregister the table from DataFusion.
3. Delete all Parquet files (same as DROP TABLE file cleanup).
4. Re-register an empty `ListingTable`.

Unlike DROP TABLE, the catalog entry (schema, constraints, indexes)
is preserved.

---

## Indexes

### What an index means in PotatoDB

An index is not a separate B-tree or hash structure. It declares the
**physical sort order** of the table's Parquet files. When DataFusion
knows files are sorted, it can:

- **Skip redundant sorts** -- `ORDER BY` on indexed columns becomes
  a streaming merge.
- **Prune row groups aggressively** -- sorted data produces tighter
  min/max statistics.
- **Terminate early on LIMIT** -- `SELECT ... ORDER BY col LIMIT 10`
  reads only the first rows.

### CREATE INDEX flow

```
handle_create_index(sql, stmt)
  │
  ├─ Reject if in_transaction()
  ├─ Validate columns exist on the table
  ├─ wal_append_pending(sql)
  │
  ├─ SELECT * FROM table ORDER BY col1 ASC, col2 DESC → sorted_batches
  │
  ├─ Deregister table from DataFusion
  ├─ Delete all existing Parquet files
  ├─ Save IndexDef to catalog
  ├─ Re-register ListingTable with file_sort_order hint
  │
  ├─ If sorted_batches is non-empty:
  │    MemTable → INSERT INTO table SELECT * FROM __potato_idx_tmp
  │
  └─ Return "Index 'name' created"
```

The entire table is read, sorted, the old files are deleted, and the
sorted data is written back. This is a full table rewrite.

### DROP INDEX

Dropping an index removes the `IndexDef` from the catalog and
re-registers the table without a sort-order hint. No Parquet files
are rewritten. The data remains physically sorted but DataFusion no
longer knows about it.

### Limitations

- Only one sort order is communicated to DataFusion at a time (the
  first index in the catalog for that table).
- New inserts after `CREATE INDEX` append unsorted Parquet files.
  Periodically rebuild the index to maintain full sort order.
- `CREATE INDEX` is forbidden inside explicit transactions because it
  destructively rewrites files.

---

## Transactions

PotatoDB provides MVCC-style transactions with file-level snapshot
isolation.

### State

```
Transaction {
    catalog_snapshot: CatalogSnapshot,  // cloned catalog state at BEGIN
    file_snapshot: HashMap<String, Vec<String>>,  // table → [parquet files]
    deferred_deletes: Vec<TableMeta>,   // tables to delete at COMMIT
    wal_txn_id: u64,                    // WAL transaction identifier
}
```

### BEGIN

1. Reject if a transaction is already active (no nesting).
2. `catalog.snapshot()` -- clone all four catalog maps.
3. For each table, list all Parquet files and store the paths.
4. Mark the catalog as in-transaction (`save()` becomes a no-op).
5. Assign a WAL transaction ID.

### During the transaction

- Writes go to disk normally (new Parquet files are created).
- DDL mutations update the in-memory catalog but are not persisted.
- The WAL records all statements under the transaction ID.

### COMMIT

1. Flush all write buffers.
2. Set the catalog to not-in-transaction.
3. `catalog.force_save()` -- persist the catalog.
4. For each `deferred_deletes` (from DROP TABLE during the txn):
   delete the table's storage.
5. `wal.commit(txn_id)` -- append a commit marker.
6. `wal.checkpoint()` -- truncate the WAL.

### ROLLBACK

1. Set the catalog to not-in-transaction.
2. For each table: compare current Parquet files against the
   snapshot. Delete files that were not present at BEGIN.
3. For tables created during the transaction: delete their storage
   and deregister from DataFusion.
4. `catalog.restore(snapshot)` -- revert to the BEGIN state.
5. `catalog.force_save()` -- persist the restored catalog.
6. Deregister all tables from DataFusion.
7. `reload_tables()` -- re-register everything from the catalog.
8. `wal.abort(txn_id)` -- append an abort marker.
9. `wal.checkpoint()` -- truncate the WAL.

### Restrictions

| Operation | Allowed in transaction? | Reason |
|-----------|------------------------|--------|
| INSERT | Yes | Append-only; new files tracked for rollback |
| SELECT | Yes | Read-only |
| CREATE TABLE | Yes | Catalog change can be reverted |
| DROP TABLE | Yes | Files deferred until COMMIT |
| UPDATE | No | Destructive table rewrite |
| DELETE | No | Destructive table rewrite |
| CREATE INDEX | No | Destructive table rewrite |
| VACUUM | No | Destructive table rewrite |

The common thread is that operations which rewrite Parquet files in
place are forbidden because the file-level snapshot mechanism can only
track file additions, not in-place modifications.

---

## Write-ahead log

### Purpose

The WAL provides crash recovery for local databases. If the process
crashes after a mutation is acknowledged but before the Parquet file
is fully written, the WAL replays the statement on restart.

### On-disk format

Each entry is a fixed-header binary record:

```
┌──────────────┬──────────────┬──────────────┬──────────┬──────────────┐
│ len: u32 LE  │ crc: u32 LE  │ txn_id: u64  │ status:  │ sql: [u8;N]  │
│ (4 bytes)    │ (4 bytes)    │  LE (8 bytes)│ u8 (1B)  │ (variable)   │
└──────────────┴──────────────┴──────────────┴──────────┴──────────────┘
```

- `len` is the byte count of everything after itself (crc + txn_id +
  status + sql).
- `crc` is CRC-32C over txn_id, status, and sql bytes.
- `status`: 0 = Pending, 1 = Committed, 2 = Aborted.
- `sql`: the UTF-8 SQL statement (empty for commit/abort markers).

### Entry lifecycle

**Auto-commit statement:**

1. `wal_append_pending()` writes a Pending entry with `txn_id = 0`.
2. The statement executes.
3. `wal_finish_autocommit()` writes a Committed marker for `txn_id = 0`.
4. Optionally `wal.maybe_checkpoint(threshold)` truncates the WAL if
   it exceeds `POTATODB_WAL_CHECKPOINT_BYTES` (default 4 MiB).

**Explicit transaction:**

1. `BEGIN` assigns a non-zero `txn_id`.
2. Each statement in the transaction appends a Pending entry with
   that `txn_id`.
3. `COMMIT` appends a Committed marker. `ROLLBACK` appends an
   Aborted marker.
4. Both COMMIT and ROLLBACK checkpoint (truncate) the WAL.

### Recovery on startup

`Wal::recover()` reads all entries:

1. Collect sets of committed and aborted transaction IDs from marker
   entries.
2. Return only Pending entries where:
   - The SQL is non-empty, AND
   - `txn_id == 0` (auto-commit, always replay), OR
   - `txn_id` is in the committed set, AND
   - `txn_id` is NOT in the aborted set.

If a record is corrupted (bad length or CRC mismatch), recovery stops
at that point and returns only the entries read before it.

The engine replays these entries by calling `execute()` with the
`replaying_wal` flag set, which forces direct (non-buffered) inserts
and suppresses new WAL writes.

### Durability guarantees

Every `append()` call does `writer.flush()` + `writer.sync_data()`.
This ensures the entry is on stable storage before the call returns.
The WAL is the durability backstop for local databases.

### S3 and in-memory modes

The WAL is disabled when the data URL is not a local filesystem path:
`PotatoDB::new` skips `wal.log` and Arrow IPC WAL for `s3://` and for
in-memory URLs (`:memory:`, `memory://...`). S3 writes are durable once
acknowledged by the service; in-memory state is ephemeral and lost on exit.

---

## Views and materialized views

### Regular views

A regular view is a named query stored in the catalog. It is
registered with DataFusion via `CREATE OR REPLACE VIEW name AS query`.
DataFusion treats it as a logical alias that is expanded during
planning.

On restart, all views are re-registered from the catalog.

### Materialized views

A materialized view stores its query results as a hidden backing
table (named `__mv_<view_name>`). The visible view is defined as
`SELECT * FROM __mv_<view_name>`, so reads go directly to Parquet
files.

**CREATE MATERIALIZED VIEW:**

1. Create the backing table via CTAS (run the query, write results
   to Parquet).
2. Create a regular view pointing at the backing table.
3. Store `ViewDef { materialized: true, backing_table: Some("__mv_...") }`.

**REFRESH MATERIALIZED VIEW:**

1. Drop the backing table.
2. Re-parse the original query from `view.sql`.
3. Re-run CTAS to create a fresh backing table.
4. Recreate the view over the new backing table.

**DROP VIEW (materialized):**

1. Remove the view from the catalog and deregister from DataFusion.
2. Drop the backing table and delete its Parquet files.

---

## Sequences

Sequences provide auto-incrementing values without a dedicated serial
type.

### Storage

Sequences are stored in the catalog under `sequences: HashMap<String, SequenceDef>`. Each call to `next_sequence_value()` increments
`current_value` by `increment`, enforces `min_value`/`max_value`
bounds, and persists the catalog.

### SQL integration

`nextval('seq_name')` calls are resolved at the SQL text level before
parsing. The `expand_nextval_calls()` function scans the SQL string
with a regex, calls `catalog.next_sequence_value()` for each match,
and replaces the function call with a literal integer. This means
sequences work in any SQL context where a literal would be valid.

---

## Maintenance

### VACUUM

VACUUM merges all Parquet files for a table into one optimally
encoded file.

```
handle_vacuum(table_name)
  │
  ├─ flush_all(), reject if in_transaction()
  ├─ apply_retention_policy()   ← delete files older than retention_seconds
  │
  ├─ Build ORDER BY from first index (if any)
  ├─ SELECT * FROM table [ORDER BY ...] → all_batches
  ├─ rewrite_table(table, schema, all_batches)
  │    ├─ Delete all old Parquet files
  │    ├─ Write one new file from all_batches
  │    └─ Refresh file stats
  │
  ├─ handle_analyze(table)      ← refresh statistics
  └─ Return message with file count and row count
```

If an index exists, the data is re-sorted during VACUUM, maintaining
the physical sort order.

### ANALYZE

ANALYZE collects optimizer statistics for a table:

1. `SELECT COUNT(*)` for the total row count.
2. For each column: `SELECT COUNT(*) - COUNT(col)` for null count,
   `COUNT(DISTINCT col)` for distinct count, `MIN(col)` and
   `MAX(col)` for range.
3. Store `TableStatistics` in the catalog.
4. DataFusion uses these statistics for cost-based optimization.

### Auto-analyze

After every flush or direct INSERT, the engine increments a per-table
counter (`rows_since_analyze`). When the counter reaches
`POTATODB_AUTO_ANALYZE_ROWS` (default 10,000), ANALYZE runs
automatically and the counter resets.

### Auto-vacuum (server mode)

When `POTATODB_AUTO_VACUUM_INTERVAL_SECS > 0`, the pgwire server
spawns a background task that periodically scores each table:

```
file_score = parquet_file_count / POTATODB_AUTO_VACUUM_FILE_THRESHOLD
bytes_score = total_bytes / POTATODB_AUTO_VACUUM_BYTES_THRESHOLD
score = file_score + bytes_score
```

Tables with `score >= 1.0` or whose oldest file exceeds
`POTATODB_AUTO_VACUUM_AGE_SECS` are vacuumed. Candidates are sorted
by score (descending), then age (descending), then name.

---

## S3 backend

### Initialization

When `data_url` starts with `s3://`:

1. Parse the bucket name and key prefix.
2. Build an `AmazonS3` store via `AmazonS3Builder::from_env()`,
   reading `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_REGION`, and `AWS_ENDPOINT_URL`.
3. Apply overrides from `S3Config` (endpoint, region, allow_http).
4. Register the store with DataFusion:
   `ctx.register_object_store(&Url::parse("s3://bucket")?, store)`.

### Table URLs

Table storage paths become `s3://bucket/prefix/table_name/`. The
same `ListingTableUrl::parse()` and `ObjectStore` abstraction used
for local files works transparently.

### Catalog on S3

The catalog is stored at `s3://bucket/prefix/catalog.json`. The
`Catalog` struct writes through the same `ObjectStore::put()` call
used for local files.

### DROP TABLE on S3

S3 has no directory concept. `handle_drop_table` lists all objects
under the table's key prefix (`ObjectStore::list(prefix)`) and
deletes them one by one via `ObjectStore::delete`.

### What S3 mode disables

- **WAL** -- no local filesystem journal; S3 writes are durable once
  acknowledged.
- **Backup/restore** -- the tar-based backup only works with local
  files.
- **Retention policy** -- file age queries use local filesystem
  metadata, which is unavailable on S3.

---

## In-memory backend

When `data_url` is `:memory:`, `memory` (case-insensitive), or starts with
`memory://`:

1. The engine builds an [`InMemory`](https://docs.rs/object_store/latest/object_store/memory/struct.InMemory.html)
   `ObjectStore` and registers it with DataFusion under `memory://<host>/`.
2. Catalog and Parquet objects use the same key-prefix layout as S3 (optional
   path prefix after the host). Table listing URLs end with `/` so DataFusion
   treats each table as a directory listing.
3. No WAL, no Arrow IPC WAL, no on-disk CDC log. `is_memory` is set on
   `PotatoDB`; `PotatoDB::is_in_memory()` exposes this to embeddings.
4. **Backup/restore** (tar) is unsupported, same as S3. **Retention policy**
   applies only to local disk (object-store backends return early).

This mode is intended for tests, scratch sessions, and tooling — not for
durable production data.

---

## Query execution and optimization

### DataFusion session configuration

The engine configures DataFusion's `SessionConfig` with settings
tuned for Parquet workloads:

| Setting | Value | Effect |
|---------|-------|--------|
| `pushdown_filters` | true | Push WHERE predicates into the Parquet reader |
| `reorder_filters` | true | Reorder predicates by selectivity |
| `pruning` | true | Skip row groups via min/max statistics |
| `enable_page_index` | true | Skip individual pages within row groups |
| `bloom_filter_on_read` | true | Check bloom filters for equality predicates |
| `bloom_filter_on_write` | true | Embed bloom filters in every Parquet file |
| `compression` | zstd(3) | Zstandard level 3 compression |
| `dictionary_enabled` | true | Dictionary-encode all columns |
| `statistics_enabled` | page | Write min/max statistics per page |
| `max_row_group_size` | 1,048,576 | Rows per row group |
| `data_page_row_count_limit` | 20,000 | Max rows per data page |
| `batch_size` | 8,192 | Arrow batch size during execution |
| `target_partitions` | CPU cores | Parallel scan / aggregation partitions |
| `information_schema` | true | Enable SHOW TABLES, DESCRIBE, etc. |

### Query optimization layers

A query goes through these optimization stages inside DataFusion:

1. **Parsing** -- SQL → unresolved logical plan.
2. **Analysis** -- resolve table/column references, type coercion.
3. **Logical optimization** -- predicate pushdown, projection
   pushdown, common subexpression elimination, join reordering.
4. **Physical planning** -- choose join algorithms (hash, sort-merge,
   nested loop), scan strategies, sort implementations.
5. **Execution** -- vectorized execution across `target_partitions`
   threads, with pipeline-breaking operators (sort, hash aggregate)
   and streaming operators (filter, projection).

### How PotatoDB helps the optimizer

- **Sort-order hints** from indexes let DataFusion skip redundant
  sorts and terminate early on LIMIT.
- **Table statistics** from ANALYZE provide cardinality estimates for
  join ordering and cost-based decisions.
- **Bloom filters** enable fast equality checks without reading
  column data.
- **Page-level statistics** enable skipping individual data pages
  within row groups, not just entire row groups.

---

## pgwire server

The `potatodb-server` crate exposes PotatoDB over the PostgreSQL wire
protocol using the `pgwire` crate.

### Authentication

MD5 password authentication with credentials from environment
variables:

- `POTATODB_USER` (default `potatodb`)
- `POTATODB_PASSWORD` (default `potatodb`)

The server uses a fixed salt `[1, 2, 3, 4]` and computes the MD5
hash via `pgwire::api::auth::md5pass::hash_md5_password`.

### Connection lifecycle

```
TcpListener::accept()
  │
  ├─ tokio::spawn per connection
  │
  ├─ pgwire::tokio::process_socket(socket, None, handler_factory)
  │    │
  │    ├─ SSL negotiation (optional)
  │    ├─ Startup → MD5 authentication
  │    ├─ ReadyForQuery
  │    │
  │    └─ Message loop:
  │         Query (simple)   → Processor::do_query()
  │         Parse/Bind/Exec  → Processor::do_query() (extended)
  │         Terminate        → close connection
  │
  └─ Connection closed
```

### Simple query handler

`do_query()` receives the raw SQL string. It detects read-only
queries by checking the first token (SELECT, WITH, SHOW, DESCRIBE,
EXPLAIN, VALUES) and routes to `execute_readonly()` or `execute()`.

Results are converted from Arrow `RecordBatch` to pgwire `Response`:

- `QueryResult::Records` → build `FieldInfo` from schema, encode
  each row with `DataRowEncoder`, wrap in `QueryResponse`.
- `QueryResult::Message` → `Response::Execution` with a command tag.

### Extended query handler (prepared statements)

The extended query protocol uses `Portal<String>`:

1. **Parse** -- store the SQL template.
2. **Bind** -- substitute `$1`, `$2`, ... with parameter values from
   the portal. Parameters are substituted in reverse order to avoid
   `$10` vs `$1` collisions.
3. **Execute** -- run the substituted SQL.

Parameter formatting: numeric and boolean types are unquoted; text
is quoted with `'` and internal quotes escaped as `''`; NULL parameters
become the literal `NULL`.

### Arrow to PostgreSQL type mapping

| Arrow type | PostgreSQL type |
|------------|----------------|
| Boolean | BOOL |
| Int8, Int16 | INT2 |
| Int32, UInt8, UInt16 | INT4 |
| Int64, UInt32, UInt64 | INT8 |
| Float32 | FLOAT4 |
| Float64 | FLOAT8 |
| Utf8, LargeUtf8 | VARCHAR |
| Date32, Date64 | DATE |
| Timestamp | TIMESTAMP |
| Decimal128 | NUMERIC |
| Binary, LargeBinary | BYTEA |
| Everything else | TEXT |

### Auto-vacuum background task

When `POTATODB_AUTO_VACUUM_INTERVAL_SECS > 0`, `start_server()`
spawns a `tokio::spawn` task that:

1. Sleeps for the configured interval.
2. Acquires a write lock on the shared `PotatoDB` instance.
3. Scores each table and vacuums candidates (see [Maintenance](#maintenance)).

The shared state is `Arc<RwLock<PotatoDB>>`. Read-only queries
acquire a read lock; mutations acquire a write lock. The auto-vacuum
task acquires a write lock for each VACUUM operation.

---

## Client interfaces

### REPL

The REPL uses `rustyline` for line editing, history, and tab
completion.

**Multi-line input:** Lines accumulate in a buffer. The prompt
changes from `potatodb> ` to `       -> ` while accumulating. When a
line ends with `;`, the buffer is executed and cleared.

**Tab completion:** `ReplHelper` implements `rustyline::Completer`
with a word list built from SQL keywords plus table and column names.

**History:** Persisted to `~/.potatodb_history` between sessions.

**Special commands:** Dispatched before SQL parsing.

| Command | Implementation |
|---------|---------------|
| `\dt` | `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'` |
| `\d table` | `DESCRIBE table` |
| `\di` | Calls `db.indexes()` directly |
| `\dv` | Calls `db.view_names()` directly |
| `\backup path` | `db.backup(path)` |
| `\restore path` | `db.restore(path)` |
| `.import fmt table path` | `COPY table FROM 'path'` |
| `.export fmt table path` | `COPY table TO 'path'` |

### TUI

The TUI uses `ratatui` with `crossterm` as the backend.

**Layout:**

```
┌─ Title bar ───────────────────────────────────────────────┐
│ ┌─ Tables ─┐ ┌─ Results ─────────────────────────────────┐│
│ │ table_a  │ │                                           ││
│ │ table_b  │ │  (query results as ASCII table)           ││
│ │          │ │                                           ││
│ └──────────┘ └───────────────────────────────────────────┘│
│ ┌─ Query ─────────────────────────────────────────────────┐│
│ │ potatodb> SELECT * FROM table_a;                       ││
│ └─────────────────────────────────────────────────────────┘│
│ Status: 3 row(s) | 0.005s | ↑↓ history  Tab sidebar ...  │
└───────────────────────────────────────────────────────────┘
```

**State:** The `App` struct holds input text, cursor position,
query history, result lines, scroll offset, sidebar visibility,
and table list.

**Event loop:** Poll for key events every 50 ms. Key presses are
mapped to `Action` variants: `Execute(sql)`, `Quit`, or `None`.
After execution, the sidebar refreshes the table list.

### C / C++ FFI

The FFI crate produces `libpotatodb_ffi.a` (static) and
`libpotatodb_ffi.so` (shared).

**Rust bridge:** `extern "C"` functions with `#[no_mangle]`. Opaque
handles (`potato_db`, `potato_result`) hide the Rust types. Each
`potato_db` owns a `tokio::Runtime` to drive async calls with
`rt.block_on()`.

**C++ wrapper:** Header-only C++17, no exceptions. `potato::Database`
and `potato::Result` are RAII types with move semantics. All fallible
operations return `potato::Expected<T>`.

**Memory ownership:**

| Handle | Created by | Freed by |
|--------|-----------|----------|
| `potato_db*` | `potato_open` / `potato_open_local` | `potato_close` |
| `potato_result*` | `potato_execute` and friends | `potato_result_free` |
| `char*` from `get_string` | `potato_result_get_string` | `potato_string_free` |
| Other `const char*` | Various accessors | Valid for handle lifetime; do not free |

### Python

PyO3 bindings expose a `PotatoDB` class with three methods:

- `PotatoDB.open(path)` -- opens a local database.
- `execute(sql)` -- returns a status string (DDL/DML) or a list of
  dicts (queries). Arrow arrays are mapped to Python types: Int32 →
  int, Float64 → float, Boolean → bool, Utf8 → str, NULL → None.
- `close()` -- releases the database handle.

---

## Backup and restore

Backup and restore are local-only operations using tar + gzip.

**Backup:**

```
tar -czf <archive_path> -C <data_url> .
```

Creates a compressed archive of the entire data directory, including
`catalog.json`, `wal.log`, and all table directories with their
Parquet files.

**Restore:**

1. Deregister all tables from DataFusion.
2. Delete the data directory.
3. Extract the archive: `tar -xzf <archive_path> -C <data_url>`.
4. Reload the catalog from `catalog.json`.
5. Re-register all tables.

These operations are not available in S3 mode because they depend on
local filesystem tar operations.

---

## Performance design

### Compile-time optimizations

The release profile maximizes single-threaded performance:

| Setting | Value | Effect |
|---------|-------|--------|
| `opt-level` | 3 | Maximum compiler optimizations |
| `lto` | `"fat"` | Full cross-crate link-time optimization |
| `codegen-units` | 1 | Single codegen unit for maximum inlining |
| `strip` | true | Strip debug symbols |
| `panic` | `"abort"` | Remove unwind tables |

For local builds, `RUSTFLAGS="-C target-cpu=native"` enables AVX2,
AVX-512, or NEON SIMD instructions depending on the build machine.
This significantly benefits Arrow's columnar operations and Parquet
codec paths. (Not set in config to avoid SIGILL on CI.)

### Runtime optimizations

- **Buffered ingestion** avoids creating one Parquet file per INSERT
  statement, reducing metadata overhead and improving scan performance.
- **Auto-ANALYZE** keeps optimizer statistics fresh without manual
  intervention.
- **Auto-VACUUM** (server mode) prevents file count from growing
  unboundedly.
- **Parallel scans** across `num_cpus` threads for all queries.
- **Write buffer flush-before-read** guarantees read-your-writes
  without forcing every INSERT to disk.

### I/O optimizations

- **Predicate pushdown** into the Parquet reader means only relevant
  row groups and pages are read from disk.
- **Bloom filters** provide O(1) membership checks for equality
  predicates, avoiding full column scans.
- **Dictionary encoding** reduces I/O for low-cardinality columns.
- **Zstd compression** reduces file sizes by 3-10x depending on data,
  reducing disk reads.
- **Page-level statistics** enable finer-grained pruning than
  row-group-level statistics alone.
