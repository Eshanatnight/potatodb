#![allow(
    clippy::too_many_lines,
    clippy::option_if_let_else,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::option_option
)]
//! Core database engine that wraps a `DataFusion` [`SessionContext`].
//!
//! The engine intercepts DDL statements (`CREATE TABLE`, `DROP TABLE`,
//! `CREATE INDEX`, `DROP INDEX`, `ALTER TABLE`), DML statements
//! (`DELETE`, `UPDATE`), views (`CREATE VIEW`, `DROP VIEW`),
//! transaction-control statements (`BEGIN`, `COMMIT`, `ROLLBACK`),
//! prepared statements (`PREPARE`, `EXECUTE`), and maintenance commands
//! (`VACUUM`, `ANALYZE`) to manage Parquet-backed storage and the
//! persistent catalog, while delegating queries and other SQL to
//! `DataFusion`.
//!
//! ## MVCC / transaction model
//!
//! By default every statement runs in **auto-commit** mode.  Wrapping
//! statements in `BEGIN` / `COMMIT` groups them into an atomic
//! transaction.  On `ROLLBACK` the engine:
//!
//! 1. Deletes any Parquet files written during the transaction.
//! 2. Restores the catalog to its pre-`BEGIN` snapshot.
//! 3. Re-registers all tables with `DataFusion`.
//!
//! `CREATE INDEX`, `DELETE`, `UPDATE`, and `VACUUM` are not allowed
//! inside an explicit transaction because they destructively rewrite
//! Parquet files and cannot be rolled back.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, StringArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::{csv, json};
use async_trait::async_trait;
use chrono::Utc;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::common::{
    stats::Precision, ColumnStatistics as DfColumnStatistics, ScalarValue,
    Statistics as DfStatistics,
};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, TableProviderFilterPushDown, TableType,
};
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::*;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use sqlparser::ast::{
    AlterTableOperation, ColumnDef as SqlColumnDef, ColumnOption, DataType as SqlDataType,
    ExactNumberInfo, MergeAction, MergeClauseKind, MergeInsertKind, ObjectType, OnConflict,
    OnConflictAction, OnInsert, ReferentialAction, SequenceOptions, SetExpr, Statement,
    TableConstraint as SqlTableConstraint, TableFactor, TableWithJoins, TimezoneInfo,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use url::Url;

mod checkpoint_policy;
mod sql_helpers;

use checkpoint_policy::should_checkpoint_autocommit;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use potatodb_catalog::{
    Catalog, CatalogSnapshot, ColumnDef, ColumnStatistics, FileStats, IndexColumn, IndexDef,
    MigrationRecord, SequenceDef, TableConstraint as CatalogTableConstraint, TableMeta,
    TableStatistics, TriggerDef, UdfDef, ViewDef,
};
use potatodb_wal::{ArrowWal, EntryStatus, Wal, WalEntry};
use sql_helpers::{
    is_read_only_sql, normalize_explain_sql, parse_as_of_timestamp, strip_as_of_timestamp,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The result of executing a SQL statement.
pub enum QueryResult {
    /// Row data returned by a SELECT or similar query.
    Records(Vec<RecordBatch>),
    /// A human-readable status message (e.g. "Table created.").
    Message(String),
}

/// Streaming variant of [`QueryResult`] for large result sets.
pub enum QueryResultStream {
    /// A streaming sequence of record batches.
    Stream(SendableRecordBatchStream),
    /// A human-readable status message.
    Message(String),
}

/// Configuration for connecting to an S3-compatible object store.
pub struct S3Config {
    /// Endpoint URL (e.g. `http://localhost:9000` for `MinIO`).
    pub endpoint: Option<String>,
    /// AWS region (e.g. `us-east-1`).
    pub region: Option<String>,
    /// Whether to allow plain HTTP (non-TLS) connections.
    pub allow_http: bool,
    /// Local directory for WAL file when using S3 storage.
    /// Defaults to `./potatodb_s3_wal` when not specified.
    pub wal_dir: Option<String>,
}

/// A prepared statement stored for later execution with parameters.
#[derive(Clone)]
struct PreparedStatement {
    sql_template: String,
    logical_plan: Option<LogicalPlan>,
}

/// In-memory query log entry.
#[derive(Debug, Clone)]
pub struct QueryLogEntry {
    pub sql: String,
    pub duration: Duration,
    pub rows: usize,
}

/// Per-query I/O metrics collected during execution.
#[derive(Debug, Clone, Default)]
pub struct QueryMetrics {
    /// Number of Parquet files opened during the query.
    pub parquet_files_read: usize,
    /// Approximate bytes scanned from Parquet.
    pub bytes_scanned: u64,
}

#[derive(Debug, Clone)]
struct CdcEvent {
    table: String,
    op: String,
    timestamp_ms: i64,
    rows: usize,
}

#[derive(Debug, Clone)]
struct FulltextIndexDef {
    table_name: String,
    columns: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct InvertedIndex {
    postings: HashMap<String, Vec<(String, usize)>>,
}

impl InvertedIndex {
    fn add_document(&mut self, doc_id: &str, row_idx: usize, text: &str) {
        for token in tokenize(text) {
            self.postings
                .entry(token)
                .or_default()
                .push((doc_id.to_string(), row_idx));
        }
    }

    #[allow(dead_code)]
    fn search(&self, query: &str) -> Vec<(String, usize)> {
        let tokens = tokenize(query);
        if tokens.is_empty() {
            return Vec::new();
        }
        let mut result: Option<HashSet<(String, usize)>> = None;
        for token in &tokens {
            let matches: HashSet<(String, usize)> = self
                .postings
                .get(token)
                .map(|v| v.iter().cloned().collect())
                .unwrap_or_default();
            result = Some(match result {
                Some(prev) => prev.intersection(&matches).cloned().collect(),
                None => matches,
            });
        }
        result.unwrap_or_default().into_iter().collect()
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty() && s.len() > 1)
        .map(String::from)
        .collect()
}

#[derive(Debug, Clone)]
struct DbSnapshot {
    timestamp_ms: i64,
    tables: HashMap<String, Vec<RecordBatch>>,
}

#[derive(Debug)]
struct StatsAwareTableProvider {
    inner: Arc<dyn TableProvider>,
    stats: Option<DfStatistics>,
}

#[async_trait]
impl TableProvider for StatsAwareTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<DfStatistics> {
        self.stats.clone().or_else(|| self.inner.statistics())
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}

/// Splits Projection nodes that contain more than one scalar subquery into
/// nested Projections with at most one each.  This prevents `DataFusion`'s
/// `scalar_subquery_to_join` rule from producing duplicate `__always_true`
/// columns that cause an "Ambiguous reference" error.
#[derive(Debug)]
struct SplitScalarSubqueries;

impl OptimizerRule for SplitScalarSubqueries {
    fn name(&self) -> &'static str {
        "split_scalar_subqueries"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<datafusion::optimizer::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::optimizer::ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Projection(ref proj) = plan else {
            return Ok(Transformed::no(plan));
        };

        let sq_indices: Vec<usize> = proj
            .expr
            .iter()
            .enumerate()
            .filter(|(_, e)| {
                e.exists(|inner| Ok(matches!(inner, Expr::ScalarSubquery(_))))
                    .unwrap_or(false)
            })
            .map(|(i, _)| i)
            .collect();

        if sq_indices.len() <= 1 {
            return Ok(Transformed::no(plan));
        }

        let input = proj.input.as_ref().clone();
        let first_sq_idx = sq_indices[0];
        let first_sq_expr = proj.expr[first_sq_idx].clone();

        let sq_alias = format!("__potato_sq_{first_sq_idx}");

        let (base_expr, orig_alias) = match first_sq_expr {
            Expr::Alias(ref a) => (a.expr.as_ref().clone(), Some(a.name.clone())),
            _ => (first_sq_expr, None),
        };

        let mut inner_exprs: Vec<Expr> = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::from)
            .collect();
        inner_exprs.push(base_expr.alias(&sq_alias));

        let inner_plan = LogicalPlanBuilder::from(input)
            .project(inner_exprs)?
            .build()?;

        let col_ref = Expr::Column(Column::new_unqualified(&sq_alias));
        let outer_ref = match orig_alias {
            Some(name) => col_ref.alias(name),
            None => col_ref,
        };

        let mut outer_exprs = Vec::with_capacity(proj.expr.len());
        for (i, expr) in proj.expr.iter().enumerate() {
            if i == first_sq_idx {
                outer_exprs.push(outer_ref.clone());
            } else {
                outer_exprs.push(expr.clone());
            }
        }

        let new_plan = LogicalPlanBuilder::from(inner_plan)
            .project(outer_exprs)?
            .build()?;

        Ok(Transformed::yes(new_plan))
    }
}

/// State for an in-flight explicit transaction started with `BEGIN`.
struct Transaction {
    /// Catalog state captured at `BEGIN` for rollback.
    catalog_snapshot: CatalogSnapshot,
    /// Per-table set of `.parquet` file paths that existed at `BEGIN`.
    /// Files not in this set were created during the transaction.
    file_snapshot: HashMap<String, HashSet<ObjPath>>,
    /// Tables whose physical files should be deleted on `COMMIT`
    /// (populated by `DROP TABLE` inside a transaction).
    deferred_deletes: Vec<TableMeta>,
    /// WAL transaction id for this explicit transaction.
    wal_txn_id: u64,
    /// In-memory backups for tables rewritten inside this transaction.
    rewrite_backups: HashMap<String, Vec<RecordBatch>>,
    /// Named savepoints for partial rollback.
    savepoints: Vec<Savepoint>,
}

/// State captured at a SAVEPOINT for ROLLBACK TO SAVEPOINT.
#[derive(Clone)]
struct Savepoint {
    name: String,
    catalog_snapshot: CatalogSnapshot,
    file_snapshot: HashMap<String, HashSet<ObjPath>>,
    rewrite_backups: HashMap<String, Vec<RecordBatch>>,
}

/// Buffered INSERT payload for a single table/column-shape.
struct BufferedInsert {
    columns: Option<Vec<String>>,
    batches: Vec<RecordBatch>,
    row_count: usize,
    approx_bytes: usize,
    first_buffered_at: Instant,
}

/// The `PotatoDB` database engine.
///
/// Wraps a `DataFusion` [`SessionContext`] with a persistent [`Catalog`] and
/// an [`ObjectStore`] for Parquet I/O. Supports both local filesystem and
/// S3-compatible storage backends.
pub struct PotatoDB {
    ctx: SessionContext,
    catalog: Catalog,
    /// Canonical data location (local absolute path or `s3://bucket/prefix`).
    data_url: String,
    store: Arc<dyn ObjectStore>,
    is_s3: bool,
    /// Object key prefix within the S3 bucket (empty for local storage).
    s3_prefix: String,
    /// Active explicit transaction, if any.
    active_txn: Option<Transaction>,
    /// Named prepared statements.
    prepared_statements: HashMap<String, PreparedStatement>,
    /// Local write-ahead log (disabled for S3-backed databases).
    wal: Option<Wal>,
    /// Arrow IPC data WAL for durable INSERT buffering (local only).
    arrow_wal: Option<ArrowWal>,
    /// Monotonic transaction id generator for WAL entries.
    txn_counter: u64,
    /// Internal guard used while replaying WAL during startup.
    replaying_wal: bool,
    /// Recent query log entries.
    query_log: VecDeque<QueryLogEntry>,
    /// Slow query threshold in milliseconds.
    slow_query_threshold_ms: u64,
    /// Max number of query log entries kept in memory.
    max_query_log_entries: usize,
    /// WAL size threshold for periodic autocommit checkpoints.
    wal_checkpoint_threshold_bytes: u64,
    /// Additional checkpoint trigger by committed autocommit operations.
    wal_checkpoint_every_commits: u64,
    /// Additional checkpoint trigger by elapsed time since last checkpoint.
    wal_checkpoint_interval: Duration,
    /// Number of autocommit writes since the last WAL checkpoint.
    wal_commits_since_checkpoint: u64,
    /// Instant when the last WAL checkpoint finished.
    last_wal_checkpoint_at: Instant,
    /// Per-table in-memory buffered INSERT data.
    write_buffer: HashMap<String, BufferedInsert>,
    /// Flush threshold by buffered row count.
    write_buffer_row_threshold: usize,
    /// Flush threshold by buffered bytes.
    write_buffer_byte_threshold: usize,
    /// Flush threshold by buffered age.
    write_buffer_time_threshold: Duration,
    /// Monotonic suffix for temporary table names.
    temp_table_counter: u64,
    /// Rows written since last ANALYZE, tracked per table.
    rows_since_analyze: HashMap<String, usize>,
    /// Threshold that triggers automatic ANALYZE.
    auto_analyze_threshold_rows: usize,
    /// Tables that have hit the auto-analyze threshold but haven't been
    /// analyzed yet.  Drained lazily at the start of the next `execute`
    /// call so that INSERT / COPY / UPSERT callers are not blocked.
    pending_analyze_tables: Vec<String>,
    /// Logical-plan cache for repeated read-only SQL text.
    plan_cache: HashMap<String, LogicalPlan>,
    /// Number of times a cached plan was reused (plan cache hits).
    plan_cache_hits: u64,
    procedures: HashMap<String, String>,
    fulltext_indexes: HashMap<String, FulltextIndexDef>,
    /// In-memory inverted index for full-text search, keyed by FTS index name.
    fts_inverted_index: HashMap<String, InvertedIndex>,
    notification_queues: HashMap<String, VecDeque<String>>,
    cdc_log: VecDeque<CdcEvent>,
    cdc_capacity: usize,
    cdc_log_path: Option<PathBuf>,
    current_user: String,
    snapshots: VecDeque<DbSnapshot>,
    snapshot_retention_ms: i64,
    /// Whether time-travel snapshots are being captured.  Activated on
    /// the first `AS OF TIMESTAMP` query to avoid the overhead of
    /// snapshotting every table after every mutation when the feature
    /// is unused.
    snapshots_enabled: bool,
    /// When a table accumulates more Parquet files than this, an
    /// automatic compaction (similar to VACUUM) is triggered after
    /// flush.  Set to 0 to disable.
    auto_compact_file_threshold: usize,
    /// I/O metrics from the last executed query.
    last_query_metrics: QueryMetrics,
    /// Replica URLs for WAL-based read replication (metadata only).
    replicas: Vec<String>,
}

/// Returns the Parquet compression setting from `POTATODB_PARQUET_COMPRESSION`,
/// defaulting to `"zstd(3)"`.
fn parquet_compression_str() -> String {
    std::env::var("POTATODB_PARQUET_COMPRESSION").unwrap_or_else(|_| "zstd(3)".into())
}

/// Parses a compression string like `"zstd(3)"`, `"snappy"`, or `"gzip(6)"`
/// into a [`parquet::basic::Compression`] value.
fn parse_parquet_compression(s: &str) -> parquet::basic::Compression {
    use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};

    fn extract_level(s: &str, prefix: &str) -> Option<i32> {
        let rest = s.strip_prefix(prefix)?.trim();
        rest.strip_prefix('(')?
            .strip_suffix(')')?
            .trim()
            .parse()
            .ok()
    }

    let lower = s.trim().to_ascii_lowercase();
    match lower.as_str() {
        "uncompressed" | "none" => Compression::UNCOMPRESSED,
        "snappy" => Compression::SNAPPY,
        "lzo" => Compression::LZO,
        "lz4" => Compression::LZ4,
        "lz4_raw" => Compression::LZ4_RAW,
        "gzip" => Compression::GZIP(GzipLevel::default()),
        "brotli" => Compression::BROTLI(BrotliLevel::default()),
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        other => {
            if let Some(lvl) = extract_level(other, "zstd") {
                Compression::ZSTD(ZstdLevel::try_new(lvl).unwrap_or_default())
            } else if let Some(lvl) = extract_level(other, "gzip") {
                #[allow(clippy::cast_sign_loss)]
                Compression::GZIP(GzipLevel::try_new(lvl as u32).unwrap_or_default())
            } else if let Some(lvl) = extract_level(other, "brotli") {
                #[allow(clippy::cast_sign_loss)]
                Compression::BROTLI(BrotliLevel::try_new(lvl as u32).unwrap_or_default())
            } else {
                Compression::ZSTD(ZstdLevel::try_new(3).unwrap_or_default())
            }
        }
    }
}

/// Builds a [`SessionConfig`] tuned for Parquet performance.
///
/// Key settings can be overridden via environment variables:
/// - `POTATODB_BATCH_SIZE` — Arrow batch size during execution (default: 32768)
/// - `POTATODB_PARQUET_WRITE_BATCH_SIZE` — Parquet write buffer size (default: 16384)
/// - `POTATODB_TARGET_PARTITIONS` — Number of parallel partitions (default: CPU cores)
/// - `POTATODB_ENFORCE_BATCH_SIZE_IN_JOINS` — Enforce batch size in hash joins (default: true)
/// - `POTATODB_COALESCE_BATCHES` — Coalesce small batches between operators (default: true)
fn build_session_config() -> SessionConfig {
    let parallelism = std::env::var("POTATODB_TARGET_PARTITIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(std::num::NonZero::get)
                .unwrap_or(4)
        });

    let batch_size = std::env::var("POTATODB_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(32768);

    let write_batch_size = std::env::var("POTATODB_PARQUET_WRITE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(16384);

    let enforce_batch_size_in_joins = std::env::var("POTATODB_ENFORCE_BATCH_SIZE_IN_JOINS")
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => v.parse::<bool>().ok(),
        })
        .unwrap_or(true);

    let coalesce_batches = std::env::var("POTATODB_COALESCE_BATCHES")
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => v.parse::<bool>().ok(),
        })
        .unwrap_or(true);

    let mut config = SessionConfig::new()
        .with_information_schema(true)
        .with_batch_size(batch_size)
        .with_target_partitions(parallelism)
        .with_enforce_batch_size_in_joins(enforce_batch_size_in_joins)
        .with_coalesce_batches(coalesce_batches);

    config.options_mut().optimizer.skip_failed_rules = true;

    let parquet = &mut config.options_mut().execution.parquet;

    parquet.pushdown_filters = true;
    parquet.reorder_filters = true;
    parquet.pruning = true;
    parquet.enable_page_index = true;
    parquet.bloom_filter_on_read = true;

    parquet.compression = Some(parquet_compression_str());
    parquet.dictionary_enabled = Some(true);
    parquet.statistics_enabled = Some("page".to_string());
    parquet.bloom_filter_on_write = true;
    parquet.max_row_group_size = 1_048_576;
    parquet.write_batch_size = write_batch_size;
    parquet.data_page_row_count_limit = 20_000;

    config
}

impl PotatoDB {
    /// Creates a new `PotatoDB` instance.
    ///
    /// `data_url` may be a local path (`./data`) or an S3 URL
    /// (`s3://bucket/prefix`).  When S3 is used, `s3_config` supplies
    /// endpoint, region, and TLS settings; access credentials are read
    /// from the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
    /// environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error if the data directory cannot be created, the S3
    /// connection fails, or the catalog cannot be loaded.
    pub async fn new(data_url: String, s3_config: Option<S3Config>) -> Result<Self, BoxError> {
        let config = build_session_config();
        let ctx = SessionContext::new_with_config(config);
        ctx.add_optimizer_rule(Arc::new(SplitScalarSubqueries));

        // Ensure generate_series table function is available (PostgreSQL-compatible)
        let gs = datafusion::functions_table::generate_series();
        ctx.register_udtf("generate_series", Arc::clone(gs.function()));

        let (store, is_s3, s3_prefix, data_url_normalized) = if data_url.starts_with("s3://") {
            let parsed = Url::parse(&data_url)?;
            let bucket = parsed
                .host_str()
                .ok_or("Missing bucket name in S3 URL")?
                .to_string();
            let prefix = parsed
                .path()
                .trim_start_matches('/')
                .trim_end_matches('/')
                .to_string();

            let mut builder = AmazonS3Builder::from_env().with_bucket_name(&bucket);

            if let Some(ref cfg) = s3_config {
                if let Some(ref endpoint) = cfg.endpoint {
                    builder = builder.with_endpoint(endpoint);
                }
                if let Some(ref region) = cfg.region {
                    builder = builder.with_region(region);
                }
                if cfg.allow_http {
                    builder = builder.with_allow_http(true);
                }
            }

            let s3: Arc<dyn ObjectStore> = Arc::new(builder.build()?);
            let bucket_url = Url::parse(&format!("s3://{bucket}"))?;
            ctx.register_object_store(&bucket_url, s3.clone());

            let normalized =
                format!("s3://{bucket}") + if prefix.is_empty() { "" } else { "/" } + &prefix;
            (s3, true, prefix, normalized)
        } else {
            let abs_path = PathBuf::from(&data_url);
            std::fs::create_dir_all(&abs_path)?;
            let abs_path = abs_path.canonicalize()?;
            // On Windows, canonicalize() returns paths with the \\?\ extended-length
            // prefix which disables path normalization (forward slashes are not
            // treated as separators).  Strip the prefix so that the rest of the
            // engine can safely join paths with `/`.
            #[cfg(windows)]
            let abs_path = {
                let s = abs_path.to_string_lossy();
                if let Some(stripped) = s.strip_prefix(r"\\?\") {
                    PathBuf::from(stripped)
                } else {
                    abs_path
                }
            };
            let local: Arc<dyn ObjectStore> =
                Arc::new(LocalFileSystem::new_with_prefix(&abs_path)?);
            let normalized = abs_path.to_string_lossy().to_string();
            (local, false, String::new(), normalized)
        };

        let catalog_obj_path = if is_s3 {
            if s3_prefix.is_empty() {
                ObjPath::from("catalog.json")
            } else {
                ObjPath::from(format!("{s3_prefix}/catalog.json"))
            }
        } else {
            ObjPath::from("catalog.json")
        };

        let mut catalog = Catalog::load(store.clone(), catalog_obj_path).await?;

        let wal_base_dir = if is_s3 {
            let dir = s3_config
                .as_ref()
                .and_then(|c| c.wal_dir.clone())
                .unwrap_or_else(|| ".potatodb_s3_wal".to_string());
            PathBuf::from(dir)
        } else {
            PathBuf::from(&data_url_normalized)
        };

        std::fs::create_dir_all(&wal_base_dir)?;

        let wal_path = wal_base_dir.join("wal.log");
        let replay_entries = Wal::recover(&wal_path)?;
        let wal = Some(Wal::open(&wal_path)?);
        let arrow_wal_dir = wal_base_dir.join("_arrow_wal");
        let arrow_wal_sync = std::env::var("POTATODB_ARROW_WAL_SYNC")
            .ok()
            .unwrap_or_else(|| "always".to_string())
            .to_ascii_lowercase();
        let arrow_wal_sync_every_n = std::env::var("POTATODB_ARROW_WAL_SYNC_EVERY_N")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());
        let arrow_wal_sync_ms = std::env::var("POTATODB_ARROW_WAL_SYNC_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());
        let arrow_wal_scratch_bytes = std::env::var("POTATODB_ARROW_WAL_SCRATCH_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(4 * 1024 * 1024);

        let arrow_wal_sync_policy = match arrow_wal_sync.as_str() {
            "never" | "none" | "off" => potatodb_wal::ArrowWalSyncPolicy::Never,
            "always" => potatodb_wal::ArrowWalSyncPolicy::Always,
            "every_n" | "everyn" => potatodb_wal::ArrowWalSyncPolicy::EveryNAppends(
                arrow_wal_sync_every_n.unwrap_or(10),
            ),
            "every_ms" | "everyms" | "interval" => potatodb_wal::ArrowWalSyncPolicy::EveryInterval(
                Duration::from_millis(arrow_wal_sync_ms.unwrap_or(10)),
            ),
            _ => {
                if let Some(n) = arrow_wal_sync_every_n {
                    potatodb_wal::ArrowWalSyncPolicy::EveryNAppends(n)
                } else if let Some(ms) = arrow_wal_sync_ms {
                    potatodb_wal::ArrowWalSyncPolicy::EveryInterval(Duration::from_millis(ms))
                } else {
                    potatodb_wal::ArrowWalSyncPolicy::Always
                }
            }
        };

        let arrow_wal_cfg = potatodb_wal::ArrowWalConfig {
            sync_policy: arrow_wal_sync_policy,
            scratch_capacity_bytes: arrow_wal_scratch_bytes,
        };

        let arrow_wal = Some(ArrowWal::open_with_config(&arrow_wal_dir, arrow_wal_cfg)?);

        let slow_query_threshold_ms = std::env::var("POTATODB_SLOW_QUERY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(500);
        let max_query_log_entries = std::env::var("POTATODB_QUERY_LOG_MAX")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(200);
        let wal_checkpoint_threshold_bytes = std::env::var("POTATODB_WAL_CHECKPOINT_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(4 * 1024 * 1024 * 1024);
        let wal_checkpoint_every_commits = std::env::var("POTATODB_WAL_CHECKPOINT_COMMITS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let wal_checkpoint_interval = std::env::var("POTATODB_WAL_CHECKPOINT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map_or(Duration::from_secs(0), Duration::from_millis);
        let write_buffer_row_threshold = std::env::var("POTATODB_WRITE_BUFFER_ROWS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10_000);
        let write_buffer_byte_threshold = std::env::var("POTATODB_WRITE_BUFFER_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(64 * 1024 * 1024);
        let write_buffer_time_threshold = std::env::var("POTATODB_WRITE_BUFFER_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map_or(Duration::from_secs(360), Duration::from_millis);
        let auto_analyze_threshold_rows = std::env::var("POTATODB_AUTO_ANALYZE_ROWS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(10_000);
        let cdc_capacity = std::env::var("POTATODB_CDC_CAPACITY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(2_000);
        let current_user = std::env::var("POTATODB_USER").unwrap_or_else(|_| "potatodb".into());
        let snapshot_retention_ms = std::env::var("POTATODB_SNAPSHOT_RETENTION_MS")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(24 * 60 * 60 * 1000);

        if catalog.users.is_empty() {
            catalog.users.insert(
                current_user.clone(),
                potatodb_catalog::UserDef {
                    name: current_user.clone(),
                    password: String::new(),
                },
            );
            catalog.roles.insert(
                "admin".to_string(),
                potatodb_catalog::RoleDef {
                    name: "admin".to_string(),
                    privileges: vec![potatodb_catalog::Privilege {
                        kind: "ALL".to_string(),
                        table: None,
                    }],
                },
            );
            catalog
                .user_roles
                .insert(current_user.clone(), vec!["admin".to_string()]);
        }

        let cdc_log_path = if is_s3 {
            None
        } else {
            Some(PathBuf::from(&data_url_normalized).join("_cdc_log.jsonl"))
        };
        let mut db = Self {
            ctx,
            catalog,
            data_url: data_url_normalized,
            store,
            is_s3,
            s3_prefix,
            active_txn: None,
            prepared_statements: HashMap::new(),
            wal,
            arrow_wal,
            txn_counter: 1,
            replaying_wal: false,
            query_log: VecDeque::new(),
            slow_query_threshold_ms,
            max_query_log_entries,
            wal_checkpoint_threshold_bytes,
            wal_checkpoint_every_commits,
            wal_checkpoint_interval,
            wal_commits_since_checkpoint: 0,
            last_wal_checkpoint_at: Instant::now(),
            write_buffer: HashMap::new(),
            write_buffer_row_threshold,
            write_buffer_byte_threshold,
            write_buffer_time_threshold,
            temp_table_counter: 0,
            rows_since_analyze: HashMap::new(),
            auto_analyze_threshold_rows,
            pending_analyze_tables: Vec::new(),
            plan_cache: HashMap::new(),
            plan_cache_hits: 0,
            procedures: HashMap::new(),
            fulltext_indexes: HashMap::new(),
            fts_inverted_index: HashMap::new(),
            notification_queues: HashMap::new(),
            cdc_log: VecDeque::new(),
            cdc_capacity,
            cdc_log_path,
            current_user,
            snapshots: VecDeque::new(),
            snapshot_retention_ms,
            snapshots_enabled: false,
            auto_compact_file_threshold: 20,
            last_query_metrics: QueryMetrics::default(),
            replicas: Vec::new(),
        };
        db.reload_tables().await?;
        if let Some(path) = &db.cdc_log_path {
            if let Ok(contents) = std::fs::read_to_string(path) {
                for line in contents.lines() {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(line) {
                        db.cdc_log.push_back(CdcEvent {
                            table: val["table"].as_str().unwrap_or("").to_string(),
                            op: val["op"].as_str().unwrap_or("").to_string(),
                            timestamp_ms: val["timestamp_ms"].as_i64().unwrap_or(0),
                            rows: val["rows"].as_u64().unwrap_or(0) as usize,
                        });
                    }
                }
                while db.cdc_log.len() > db.cdc_capacity {
                    let _ = db.cdc_log.pop_front();
                }
            }
        }
        let _ = db.capture_snapshot().await;

        {
            let arrow_wal_dir = wal_base_dir.join("_arrow_wal");
            let recovered_batches = ArrowWal::recover(&arrow_wal_dir)?;
            if !recovered_batches.is_empty() {
                db.replaying_wal = true;
                for (table_name, batches) in recovered_batches {
                    if db.catalog.tables.contains_key(&table_name) {
                        db.buffer_insert_batches(&table_name, None, batches).await?;
                        db.flush_table(&table_name).await?;
                    }
                }
                db.replaying_wal = false;
            }
        }

        if !replay_entries.is_empty() {
            db.replaying_wal = true;
            for entry in replay_entries {
                db.execute(&entry.sql).await?;
            }
            db.replaying_wal = false;
            if let Some(wal) = db.wal.as_mut() {
                wal.checkpoint()?;
            }
        }

        Ok(db)
    }

    /// Returns `true` when inside an explicit `BEGIN` transaction.
    #[must_use]
    pub const fn in_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    /// Returns the data URL / directory path for this database.
    #[must_use]
    pub fn data_url(&self) -> &str {
        &self.data_url
    }

    /// Returns all known table names from the catalog.
    #[must_use]
    pub fn table_names(&self) -> Vec<String> {
        self.catalog.tables.keys().cloned().collect()
    }

    /// Returns column names for a table, or empty when table is unknown.
    #[must_use]
    pub fn table_columns(&self, table_name: &str) -> Vec<String> {
        self.catalog
            .tables
            .get(table_name)
            .map(|m| m.columns.iter().map(|c| c.name.clone()).collect())
            .unwrap_or_default()
    }

    /// Returns `(index_name, table_name)` for all indexes.
    #[must_use]
    pub fn indexes(&self) -> Vec<(String, String)> {
        self.catalog
            .indexes
            .iter()
            .map(|(name, def)| (name.clone(), def.table_name.clone()))
            .collect()
    }

    /// Returns all view names.
    #[must_use]
    pub fn view_names(&self) -> Vec<String> {
        self.catalog.views.keys().cloned().collect()
    }

    /// Returns all user-defined SQL function names.
    #[must_use]
    pub fn function_names(&self) -> Vec<String> {
        self.catalog.udfs.keys().cloned().collect()
    }

    /// Returns all sequence names.
    #[must_use]
    pub fn sequence_names(&self) -> Vec<String> {
        self.catalog.sequences.keys().cloned().collect()
    }

    /// Returns `(username, roles)` for every known user.
    #[must_use]
    pub fn user_info(&self) -> Vec<(String, Vec<String>)> {
        self.catalog
            .users
            .keys()
            .map(|u| {
                let roles: Vec<String> =
                    self.catalog.user_roles.get(u).cloned().unwrap_or_default();
                (u.clone(), roles)
            })
            .collect()
    }

    /// Returns the I/O metrics from the last executed query.
    #[must_use]
    pub const fn last_query_metrics(&self) -> &QueryMetrics {
        &self.last_query_metrics
    }

    /// Returns plan cache statistics: `(cached_plan_count, cache_hit_count)`.
    #[must_use]
    pub fn plan_cache_stats(&self) -> (usize, u64) {
        (self.plan_cache.len(), self.plan_cache_hits)
    }

    /// Returns the number of deletion vectors for a table.
    ///
    /// Used to determine if compaction is needed for future incremental DML.
    #[must_use]
    pub fn deletion_vector_count(&self, table: &str) -> usize {
        self.catalog
            .tables
            .get(table)
            .map_or(0, |m| m.deletion_vectors.len())
    }

    #[must_use]
    pub fn replica_urls(&self) -> &[String] {
        &self.replicas
    }

    /// Returns the number of parquet files currently backing a table.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot be listed.
    pub async fn parquet_file_count(
        &self,
        table_name: &str,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.list_parquet_files(table_name).await?.len())
    }

    /// Returns total bytes across all parquet files currently backing a table.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot be listed.
    pub async fn table_total_bytes(
        &self,
        table_name: &str,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let prefix = self.table_obj_prefix(table_name);
        let entries: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
        Ok(entries
            .into_iter()
            .filter(|e| e.location.as_ref().ends_with(".parquet"))
            .map(|e| e.size)
            .sum())
    }

    /// Returns age in seconds of the oldest parquet file for a table.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot be listed.
    pub async fn table_oldest_file_age_secs(
        &self,
        table_name: &str,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let prefix = self.table_obj_prefix(table_name);
        let entries: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
        let now = Utc::now();
        let max_age = entries
            .into_iter()
            .filter(|e| e.location.as_ref().ends_with(".parquet"))
            .map(|e| {
                now.signed_duration_since(e.last_modified)
                    .num_seconds()
                    .max(0) as u64
            })
            .max()
            .unwrap_or(0);
        Ok(max_age)
    }

    /// Lightweight file-stats update: only records the file count and
    /// total size from the object-store listing without opening each
    /// Parquet file for metadata. Used on the hot path (flush, rewrite).
    async fn refresh_table_file_stats_light(&mut self, table_name: &str) -> Result<(), BoxError> {
        let prefix = self.table_obj_prefix(table_name);
        let entries: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;

        let file_stats: Vec<FileStats> = entries
            .into_iter()
            .filter(|e| e.location.as_ref().ends_with(".parquet"))
            .map(|entry| FileStats {
                path: entry.location.as_ref().to_string(),
                row_count: None,
                min_values: HashMap::new(),
                max_values: HashMap::new(),
                size_bytes: entry.size,
                created_at: Some(entry.last_modified.timestamp()),
            })
            .collect();

        self.catalog.set_file_stats(table_name, file_stats).await?;
        Ok(())
    }

    /// Full file-stats refresh: lists all Parquet files and opens each
    /// one to read row-group metadata (row counts, min/max stats).
    /// Used by VACUUM and ANALYZE where accuracy matters.
    async fn refresh_table_file_stats(&mut self, table_name: &str) -> Result<(), BoxError> {
        let prefix = self.table_obj_prefix(table_name);
        let entries: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
        let table_meta = self.catalog.tables.get(table_name).cloned();

        let mut file_stats = Vec::new();
        for entry in entries
            .into_iter()
            .filter(|e| e.location.as_ref().ends_with(".parquet"))
        {
            let mut row_count = None;
            let mut min_values = HashMap::new();
            let mut max_values = HashMap::new();
            if !self.is_s3 {
                if let Some(meta) = table_meta.as_ref() {
                    if let Some(filename) = entry.location.as_ref().rsplit('/').next() {
                        let local_path = PathBuf::from(&meta.path).join(filename);
                        if local_path.exists() {
                            if let Ok(file) = File::open(&local_path) {
                                if let Ok(reader) = SerializedFileReader::new(file) {
                                    let pq_meta = reader.metadata();
                                    row_count = Some(pq_meta.file_metadata().num_rows() as u64);
                                    collect_minmax_from_parquet(
                                        pq_meta,
                                        &mut min_values,
                                        &mut max_values,
                                    );
                                }
                            }
                        }
                    }
                }
            }

            file_stats.push(FileStats {
                path: entry.location.as_ref().to_string(),
                row_count,
                min_values,
                max_values,
                size_bytes: entry.size,
                created_at: Some(entry.last_modified.timestamp()),
            });
        }

        self.catalog.set_file_stats(table_name, file_stats).await?;
        Ok(())
    }

    const fn next_txn_id(&mut self) -> u64 {
        let id = self.txn_counter;
        self.txn_counter = self.txn_counter.saturating_add(1);
        id
    }

    fn current_wal_txn_id(&self) -> u64 {
        self.active_txn.as_ref().map_or(0, |txn| txn.wal_txn_id)
    }

    fn wal_append_pending(&mut self, sql: &str) -> Result<(), BoxError> {
        if self.replaying_wal {
            return Ok(());
        }
        let txn_id = self.current_wal_txn_id();
        if let Some(wal) = self.wal.as_mut() {
            wal.append_no_sync(&WalEntry {
                txn_id,
                status: EntryStatus::Pending,
                sql: sql.to_string(),
            })?;
        }
        Ok(())
    }

    fn wal_finish_autocommit(&mut self, force_checkpoint: bool) -> Result<(), BoxError> {
        if self.replaying_wal || self.in_transaction() {
            return Ok(());
        }
        if let Some(wal) = self.wal.as_mut() {
            wal.commit_no_checkpoint(0)?;
            self.wal_commits_since_checkpoint = self.wal_commits_since_checkpoint.saturating_add(1);
            let elapsed = self.last_wal_checkpoint_at.elapsed();
            if should_checkpoint_autocommit(
                force_checkpoint,
                self.wal_commits_since_checkpoint,
                self.wal_checkpoint_every_commits,
                elapsed,
                self.wal_checkpoint_interval,
            ) {
                wal.checkpoint()?;
                self.wal_commits_since_checkpoint = 0;
                self.last_wal_checkpoint_at = Instant::now();
            } else {
                wal.maybe_checkpoint(self.wal_checkpoint_threshold_bytes)?;
            }
        }
        Ok(())
    }

    fn handle_checkpoint(&mut self) -> Result<QueryResult, BoxError> {
        if self.in_transaction() {
            return Err("CHECKPOINT is not allowed in an explicit transaction".into());
        }
        if let Some(wal) = self.wal.as_mut() {
            wal.checkpoint()?;
            self.wal_commits_since_checkpoint = 0;
            self.last_wal_checkpoint_at = Instant::now();
        }
        if let Some(awal) = self.arrow_wal.as_mut() {
            awal.checkpoint_all()?;
        }
        Ok(QueryResult::Message(
            "Checkpoint completed for WAL and Arrow WAL.".to_string(),
        ))
    }

    fn next_temp_table_name(&mut self, tag: &str) -> String {
        let id = self.temp_table_counter;
        self.temp_table_counter = self.temp_table_counter.saturating_add(1);
        format!("__potato_{tag}_tmp_{id}")
    }

    async fn buffer_insert_batches(
        &mut self,
        table_name: &str,
        columns: Option<Vec<String>>,
        batches: Vec<RecordBatch>,
    ) -> Result<(usize, bool), BoxError> {
        let incoming_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        if incoming_rows == 0 {
            return Ok((0, false));
        }
        let incoming_bytes = estimate_batch_bytes(&batches);

        if let Some(existing) = self.write_buffer.get(table_name) {
            if existing.columns != columns {
                self.flush_table(table_name).await?;
            }
        }

        let entry = self
            .write_buffer
            .entry(table_name.to_string())
            .or_insert_with(|| BufferedInsert {
                columns: columns.clone(),
                batches: Vec::new(),
                row_count: 0,
                approx_bytes: 0,
                first_buffered_at: Instant::now(),
            });

        if entry.batches.is_empty() {
            entry.columns = columns;
            entry.first_buffered_at = Instant::now();
        }

        if !self.replaying_wal {
            if let Some(ref mut awal) = self.arrow_wal {
                awal.append(table_name, &batches)?;
            }
        }

        entry.batches.extend(batches);
        entry.row_count = entry.row_count.saturating_add(incoming_rows);
        entry.approx_bytes = entry.approx_bytes.saturating_add(incoming_bytes);

        let should_flush = entry.row_count >= self.write_buffer_row_threshold
            || entry.approx_bytes >= self.write_buffer_byte_threshold
            || entry.first_buffered_at.elapsed() >= self.write_buffer_time_threshold;

        if should_flush {
            self.flush_table(table_name).await?;
        }

        Ok((incoming_rows, should_flush))
    }

    async fn flush_table(&mut self, table_name: &str) -> Result<usize, BoxError> {
        let Some(buffered) = self.write_buffer.remove(table_name) else {
            return Ok(0);
        };
        if buffered.row_count == 0 || buffered.batches.is_empty() {
            return Ok(0);
        }

        let schema = buffered
            .batches
            .first()
            .map(arrow::array::RecordBatch::schema)
            .ok_or("Buffered INSERT had no schema")?;
        let target_columns = if let Some(cols) = buffered.columns.clone() {
            cols
        } else {
            self.catalog
                .tables
                .get(table_name)
                .map(|m| m.columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>())
                .unwrap_or_default()
        };

        self.validate_constraints_batch(table_name, &target_columns, &buffered.batches)
            .await?;

        let row_count = buffered.row_count;
        let tmp_name = self.next_temp_table_name("flush");
        let mem = MemTable::try_new(schema, vec![buffered.batches])?;
        self.ctx.register_table(&tmp_name, Arc::new(mem))?;

        let insert_sql = if let Some(cols) = buffered.columns.as_ref() {
            if cols.is_empty() {
                format!("INSERT INTO \"{table_name}\" SELECT * FROM \"{tmp_name}\"")
            } else {
                let cols_sql = cols
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("INSERT INTO \"{table_name}\" ({cols_sql}) SELECT * FROM \"{tmp_name}\"")
            }
        } else {
            format!("INSERT INTO \"{table_name}\" SELECT * FROM \"{tmp_name}\"")
        };

        let write_result = self.ctx.sql(&insert_sql).await?.collect().await.map(|_| ());
        let _ = self.ctx.deregister_table(&tmp_name);
        write_result?;

        if let Some(ref mut awal) = self.arrow_wal {
            awal.checkpoint_table(table_name)?;
        }

        self.maybe_auto_analyze(table_name, row_count);
        self.refresh_table_file_stats_light(table_name).await?;
        self.maybe_auto_compact(table_name).await?;

        Ok(row_count)
    }

    /// Compacts a table's Parquet files when the file count exceeds
    /// `auto_compact_file_threshold`.  Re-reads all data, deletes
    /// fragments, and writes a single optimized file (respecting index
    /// sort order).
    async fn maybe_auto_compact(&mut self, table_name: &str) -> Result<(), BoxError> {
        if self.auto_compact_file_threshold == 0 || self.in_transaction() {
            return Ok(());
        }
        let files = self.list_parquet_files(table_name).await?;
        if files.len() < self.auto_compact_file_threshold {
            return Ok(());
        }
        let indexes = self.catalog.indexes_for_table(table_name);
        let order_clause = if let Some(idx) = indexes.first() {
            let parts: Vec<String> = idx
                .columns
                .iter()
                .map(|c| {
                    format!(
                        "\"{}\" {}",
                        c.name,
                        if c.ascending { "ASC" } else { "DESC" }
                    )
                })
                .collect();
            format!(" ORDER BY {}", parts.join(", "))
        } else {
            String::new()
        };
        let select_sql = format!("SELECT * FROM \"{table_name}\"{order_clause}");
        let df = self.ctx.sql(&select_sql).await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await?;
        self.rewrite_table(table_name, schema, batches).await?;
        Ok(())
    }

    async fn flush_all(&mut self) -> Result<usize, BoxError> {
        let mut total = 0usize;
        let table_names: Vec<String> = self.write_buffer.keys().cloned().collect();
        for table_name in table_names {
            total = total.saturating_add(self.flush_table(&table_name).await?);
        }
        Ok(total)
    }

    /// Flushes only the write-buffered tables whose names appear in
    /// `sql_upper` (the uppercased SQL text).  Falls back to
    /// [`flush_all`] when no buffered tables are found in the SQL or
    /// when the buffer is small.
    async fn flush_buffered_for_sql(&mut self, sql_upper: &str) -> Result<usize, BoxError> {
        if self.write_buffer.is_empty() {
            return Ok(0);
        }
        let to_flush: Vec<String> = self
            .write_buffer
            .keys()
            .filter(|name| {
                let upper_name = name.to_uppercase();
                sql_upper.contains(&upper_name)
            })
            .cloned()
            .collect();
        if to_flush.is_empty() {
            return self.flush_all().await;
        }
        let mut total = 0usize;
        for name in to_flush {
            total = total.saturating_add(self.flush_table(&name).await?);
        }
        Ok(total)
    }

    fn maybe_auto_analyze(&mut self, table_name: &str, rows_written: usize) {
        if rows_written == 0 || self.auto_analyze_threshold_rows == 0 {
            return;
        }
        let entry = self
            .rows_since_analyze
            .entry(table_name.to_string())
            .or_insert(0);
        *entry = entry.saturating_add(rows_written);
        if *entry >= self.auto_analyze_threshold_rows {
            *entry = 0;
            self.pending_analyze_tables.push(table_name.to_string());
        }
    }

    async fn drain_pending_analyze(&mut self) {
        if self.pending_analyze_tables.is_empty() {
            return;
        }
        let tables = std::mem::take(&mut self.pending_analyze_tables);
        for table in tables {
            let _ = self.handle_analyze(&table).await;
        }
    }

    async fn expand_nextval_calls(&mut self, sql: &str) -> Result<String, BoxError> {
        let lowered = sql.to_lowercase();
        if !lowered.contains("nextval('") {
            return Ok(sql.to_string());
        }
        let mut out = sql.to_string();
        let mut search_from = 0usize;
        loop {
            let haystack = out[search_from..].to_lowercase();
            let Some(rel_start) = haystack.find("nextval('") else {
                break;
            };
            let start = search_from + rel_start;
            let name_start = start + "nextval('".len();
            let Some(name_end_rel) = out[name_start..].to_lowercase().find("')") else {
                break;
            };
            let name_end = name_start + name_end_rel;
            let seq_name = &out[name_start..name_end];
            let value = self.catalog.next_sequence_value(seq_name).await?;
            let replacement = value.to_string();
            let replace_end = name_end + 2;
            out.replace_range(start..replace_end, &replacement);
            search_from = start + replacement.len();
        }
        Ok(out)
    }

    fn table_url(&self, table_name: &str) -> String {
        format!("{}/{table_name}", self.data_url.trim_end_matches('/'))
    }

    fn table_obj_prefix(&self, table_name: &str) -> ObjPath {
        if self.s3_prefix.is_empty() {
            ObjPath::from(table_name)
        } else {
            ObjPath::from(format!("{}/{table_name}", self.s3_prefix))
        }
    }

    /// Lists all `.parquet` object paths under a table's prefix.
    async fn list_parquet_files(&self, table_name: &str) -> Result<HashSet<ObjPath>, BoxError> {
        let prefix = self.table_obj_prefix(table_name);
        let entries: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
        Ok(entries
            .into_iter()
            .filter(|e| e.location.as_ref().ends_with(".parquet"))
            .map(|e| e.location)
            .collect())
    }

    /// Re-registers every table and view in the catalog with `DataFusion`,
    /// including sort-order hints from indexes.
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn reload_tables(&mut self) -> Result<(), BoxError> {
        let tables: Vec<TableMeta> = self.catalog.tables.values().cloned().collect();
        let mut table_specs = Vec::with_capacity(tables.len());
        for meta in &tables {
            if !self.is_s3 {
                let table_dir = PathBuf::from(&meta.path);
                if !table_dir.exists() {
                    std::fs::create_dir_all(&table_dir)?;
                }
            }
            let schema = columns_to_schema(&meta.columns)?;
            table_specs.push((
                meta.name.clone(),
                schema,
                meta.path.clone(),
                meta.partition_columns.clone(),
            ));
        }
        let db_ref: &Self = &*self;
        let register_futs =
            table_specs
                .into_iter()
                .map(|(name, schema, path, partitions)| async move {
                    db_ref
                        .register_listing_table(&name, schema, &path, &partitions)
                        .await
                });
        futures::future::try_join_all(register_futs).await?;

        let views: Vec<ViewDef> = self.catalog.views.values().cloned().collect();
        for view in &views {
            let _ = self
                .ctx
                .sql(&format!(
                    "CREATE OR REPLACE VIEW \"{}\" AS {}",
                    view.name, view.sql
                ))
                .await;
        }

        self.rebuild_fts_indexes().await?;

        Ok(())
    }

    /// Rebuilds all inverted indexes from `fulltext_indexes` metadata and table data.
    async fn rebuild_fts_indexes(&mut self) -> Result<(), BoxError> {
        for (idx_name, def) in &self.fulltext_indexes.clone() {
            let table_name = &def.table_name;
            let columns = &def.columns;
            let cols_sql = columns
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let select_sql = format!("SELECT {cols_sql} FROM \"{table_name}\"");
            let batches = self.collect_with_plan_cache(&select_sql).await?;
            let mut idx = InvertedIndex::default();
            let mut row_offset = 0;
            for batch in &batches {
                let num_rows = batch.num_rows();
                for row in 0..num_rows {
                    let mut text_parts = Vec::new();
                    for col_name in columns {
                        if let Some(col) = batch.column_by_name(col_name) {
                            let s = array_value_to_string(col.as_ref(), row);
                            text_parts.push(s);
                        }
                    }
                    let text = text_parts.join(" ");
                    idx.add_document(table_name, row_offset + row, &text);
                }
                row_offset += num_rows;
            }
            self.fts_inverted_index.insert(idx_name.clone(), idx);
        }
        Ok(())
    }

    /// Re-registers a single table with `DataFusion`, creating its
    /// storage directory if needed.  Used after operations that change
    /// a table's schema or index sort order.
    #[allow(dead_code, clippy::needless_pass_by_ref_mut)]
    async fn reload_single_table(&mut self, table_name: &str) -> Result<(), BoxError> {
        let Some(meta) = self.catalog.tables.get(table_name).cloned() else {
            return Ok(());
        };
        if !self.is_s3 {
            let table_dir = PathBuf::from(&meta.path);
            if !table_dir.exists() {
                std::fs::create_dir_all(&table_dir)?;
            }
        }
        let schema = columns_to_schema(&meta.columns)?;
        let _ = self.ctx.deregister_table(table_name);
        self.register_listing_table(table_name, schema, &meta.path, &meta.partition_columns)
            .await
    }

    /// Registers a Parquet-backed [`ListingTable`] with `DataFusion`.
    #[allow(clippy::unused_async)]
    async fn register_listing_table(
        &self,
        name: &str,
        schema: SchemaRef,
        table_url_str: &str,
        partition_columns: &[String],
    ) -> Result<(), BoxError> {
        let table_url = ListingTableUrl::parse(table_url_str)?;
        let parallelism = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(4);

        let mut listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true)
            .with_target_partitions(parallelism);

        let mut indexes = self.catalog.indexes_for_table(name);
        indexes.sort_by(|a, b| b.primary.cmp(&a.primary).then_with(|| a.name.cmp(&b.name)));
        let sort_orders: Vec<Vec<_>> = indexes
            .into_iter()
            .filter(|idx| !idx.logical_only)
            .map(|idx| {
                idx.columns
                    .iter()
                    .map(|c| col(&c.name).sort(c.ascending, !c.ascending))
                    .collect()
            })
            .collect();
        if !sort_orders.is_empty() {
            listing_options = listing_options.with_file_sort_order(sort_orders);
        }

        if !partition_columns.is_empty() {
            let partition_types: Vec<(String, DataType)> = partition_columns
                .iter()
                .filter_map(|pc| {
                    schema
                        .field_with_name(pc)
                        .ok()
                        .map(|f| (pc.clone(), f.data_type().clone()))
                })
                .collect();
            if !partition_types.is_empty() {
                listing_options = listing_options.with_table_partition_cols(partition_types);
            }
        }

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table: Arc<dyn TableProvider> = Arc::new(ListingTable::try_new(config)?);
        let stats = self.catalog.tables.get(name).and_then(|m| {
            m.statistics
                .as_ref()
                .map(|s| catalog_stats_to_df(s, &table.schema()))
                .or_else(|| file_stats_to_df(&m.file_stats, &table.schema()))
        });
        let provider: Arc<dyn TableProvider> = Arc::new(StatsAwareTableProvider {
            inner: table,
            stats,
        });
        self.ctx.register_table(name, provider)?;
        Ok(())
    }

    /// Deletes all `.parquet` files under a table's storage prefix.
    async fn delete_parquet_files(&self, table_name: &str) -> Result<(), BoxError> {
        let prefix = self.table_obj_prefix(table_name);
        let entries: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
        for entry in entries {
            if entry.location.as_ref().ends_with(".parquet") {
                self.store.delete(&entry.location).await?;
            }
        }
        Ok(())
    }

    #[allow(clippy::unused_async)]
    async fn apply_retention_policy(&self, table_name: &str) -> Result<usize, BoxError> {
        let Some(meta) = self.catalog.tables.get(table_name) else {
            return Ok(0);
        };
        let Some(retention_secs) = meta.retention_seconds else {
            return Ok(0);
        };
        if self.is_s3 {
            return Ok(0);
        }

        let cutoff = SystemTime::now()
            .checked_sub(Duration::from_secs(retention_secs))
            .ok_or("Invalid retention cutoff")?;

        let mut deleted = 0usize;
        let table_dir = PathBuf::from(&meta.path);
        if !table_dir.exists() {
            return Ok(0);
        }
        for entry in std::fs::read_dir(table_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                continue;
            }
            let modified = entry.metadata()?.modified()?;
            if modified < cutoff {
                std::fs::remove_file(path)?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    /// Deletes a table's storage directory (local) or all objects (S3).
    async fn delete_table_storage(&self, meta: &TableMeta) -> Result<(), BoxError> {
        if self.is_s3 {
            let prefix = self.table_obj_prefix(&meta.name);
            let entries: Vec<_> = self.store.list(Some(&prefix)).try_collect().await?;
            for entry in entries {
                self.store.delete(&entry.location).await?;
            }
        } else {
            let table_dir = PathBuf::from(&meta.path);
            if table_dir.exists() {
                std::fs::remove_dir_all(&table_dir)?;
            }
        }
        Ok(())
    }

    /// Replaces a table's Parquet files with the given `batches`.
    ///
    /// Before touching any files, a full in-memory snapshot of the current
    /// table data is captured.  If the write of new data fails (e.g. disk
    /// full, schema mismatch), the snapshot is written back so no data is
    /// lost.
    ///
    /// This is the core "copy-on-write rewrite" used by `DELETE`,
    /// `UPDATE`, `VACUUM`, and `CREATE INDEX`.
    async fn rewrite_table(
        &mut self,
        table_name: &str,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<(), BoxError> {
        let table_meta = self
            .catalog
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{table_name}' does not exist"))?
            .clone();

        let original_batches: Vec<RecordBatch> = self
            .ctx
            .sql(&format!("SELECT * FROM \"{table_name}\""))
            .await?
            .collect()
            .await?;
        let original_schema = if original_batches.is_empty() {
            None
        } else {
            Some(original_batches[0].schema())
        };

        self.ctx.deregister_table(table_name)?;
        self.delete_parquet_files(table_name).await?;

        let table_schema = columns_to_schema(&table_meta.columns)?;
        if !self.is_s3 {
            std::fs::create_dir_all(&table_meta.path)?;
        }
        self.register_listing_table(
            table_name,
            table_schema,
            &table_meta.path,
            &table_meta.partition_columns,
        )
        .await?;

        let has_data = !batches.is_empty() && batches.iter().any(|b| b.num_rows() > 0);
        if has_data {
            let prep = MemTable::try_new(schema, vec![batches]).and_then(|mem| {
                self.ctx
                    .register_table("__potato_rewrite_tmp", Arc::new(mem))?;
                Ok(())
            });

            let write_result: Result<(), BoxError> = if let Err(e) = prep {
                Err(e.into())
            } else {
                let r = match self
                    .ctx
                    .sql(&format!(
                        "INSERT INTO \"{table_name}\" SELECT * FROM __potato_rewrite_tmp"
                    ))
                    .await
                {
                    Ok(df) => df.collect().await,
                    Err(e) => Err(e),
                };
                let _ = self.ctx.deregister_table("__potato_rewrite_tmp");
                r.map(|_| ()).map_err(Into::into)
            };

            if let Err(e) = write_result {
                self.restore_table_from_snapshot(table_name, original_schema, original_batches)
                    .await;
                return Err(e);
            }
        }

        self.refresh_table_file_stats_light(table_name).await?;

        Ok(())
    }

    /// Best-effort restore of table data from an in-memory snapshot.
    async fn restore_table_from_snapshot(
        &self,
        table_name: &str,
        original_schema: Option<SchemaRef>,
        original_batches: Vec<RecordBatch>,
    ) {
        let Some(schema) = original_schema else {
            return;
        };
        let has_orig =
            !original_batches.is_empty() && original_batches.iter().any(|b| b.num_rows() > 0);
        if !has_orig {
            return;
        }
        let Ok(mem) = MemTable::try_new(schema, vec![original_batches]) else {
            return;
        };
        if self
            .ctx
            .register_table("__potato_restore_tmp", Arc::new(mem))
            .is_err()
        {
            return;
        }
        let _ = async {
            self.ctx
                .sql(&format!(
                    "INSERT INTO \"{table_name}\" SELECT * FROM __potato_restore_tmp"
                ))
                .await?
                .collect()
                .await
        }
        .await;
        let _ = self.ctx.deregister_table("__potato_restore_tmp");
    }

    /// Captures a full-table backup before a destructive rewrite in a txn.
    async fn backup_table_for_txn(&mut self, table_name: &str) -> Result<(), BoxError> {
        let Some(txn) = self.active_txn.as_mut() else {
            return Ok(());
        };
        if txn.rewrite_backups.contains_key(table_name) {
            return Ok(());
        }
        let df = self
            .ctx
            .sql(&format!("SELECT * FROM \"{table_name}\""))
            .await?;
        let batches = df.collect().await?;
        txn.rewrite_backups.insert(table_name.to_string(), batches);
        Ok(())
    }

    // ── Transaction control ────────────────────────────────────

    /// `BEGIN` -- starts an explicit transaction.
    async fn handle_begin(&mut self) -> Result<QueryResult, BoxError> {
        if self.active_txn.is_some() {
            return Err("Already inside a transaction (nested BEGIN not supported)".into());
        }

        let catalog_snapshot = self.catalog.snapshot();

        let mut file_snapshot = HashMap::new();
        for name in self.catalog.tables.keys() {
            file_snapshot.insert(name.clone(), self.list_parquet_files(name).await?);
        }

        self.catalog.set_in_transaction(true);
        let wal_txn_id = self.next_txn_id();
        self.active_txn = Some(Transaction {
            catalog_snapshot,
            file_snapshot,
            deferred_deletes: Vec::new(),
            wal_txn_id,
            rewrite_backups: HashMap::new(),
            savepoints: Vec::new(),
        });

        Ok(QueryResult::Message("BEGIN".into()))
    }

    /// `COMMIT` -- persists all mutations and executes deferred deletes.
    async fn handle_commit(&mut self) -> Result<QueryResult, BoxError> {
        let _ = self.flush_all().await?;
        let txn = self
            .active_txn
            .take()
            .ok_or("No active transaction to COMMIT")?;

        self.catalog.set_in_transaction(false);
        self.catalog.force_save().await?;

        for meta in &txn.deferred_deletes {
            self.delete_table_storage(meta).await?;
        }

        if !self.replaying_wal {
            if let Some(wal) = self.wal.as_mut() {
                wal.commit(txn.wal_txn_id)?;
                wal.checkpoint()?;
            }
        }
        if self.snapshots_enabled {
            let _ = self.capture_snapshot().await;
        }

        Ok(QueryResult::Message("COMMIT".into()))
    }

    /// `ROLLBACK` -- reverts the catalog and deletes files written
    /// since `BEGIN`.
    async fn handle_rollback(&mut self) -> Result<QueryResult, BoxError> {
        let txn = self
            .active_txn
            .take()
            .ok_or("No active transaction to ROLLBACK")?;

        self.catalog.set_in_transaction(false);

        let current_table_names: Vec<String> = self.catalog.tables.keys().cloned().collect();
        let snapshot_table_names: HashSet<&String> = txn.file_snapshot.keys().collect();

        for (table_name, old_files) in &txn.file_snapshot {
            if let Ok(current_files) = self.list_parquet_files(table_name).await {
                for f in current_files {
                    if !old_files.contains(&f) {
                        let _ = self.store.delete(&f).await;
                    }
                }
            }
        }

        for name in &current_table_names {
            if !snapshot_table_names.contains(name) {
                if let Some(meta) = self.catalog.tables.get(name) {
                    let _ = self.delete_table_storage(meta).await;
                }
                let _ = self.ctx.deregister_table(name.as_str());
            }
        }

        self.catalog.restore(txn.catalog_snapshot);
        self.catalog.force_save().await?;

        for name in &current_table_names {
            let _ = self.ctx.deregister_table(name.as_str());
        }
        self.reload_tables().await?;
        for (table, batches) in txn.rewrite_backups {
            if let Some(meta) = self.catalog.tables.get(&table) {
                let schema = columns_to_schema(&meta.columns)?;
                self.rewrite_table(&table, schema, batches).await?;
            }
        }

        if !self.replaying_wal {
            if let Some(wal) = self.wal.as_mut() {
                wal.abort(txn.wal_txn_id)?;
                wal.checkpoint()?;
            }
        }

        Ok(QueryResult::Message("ROLLBACK".into()))
    }

    /// `SAVEPOINT name` -- captures current state for partial rollback.
    async fn handle_savepoint(&mut self, name: &str) -> Result<QueryResult, BoxError> {
        let catalog_snapshot = self.catalog.snapshot();

        let table_names: Vec<String> = self.catalog.tables.keys().cloned().collect();
        let mut file_snapshot = HashMap::new();
        for table_name in &table_names {
            file_snapshot.insert(
                table_name.clone(),
                self.list_parquet_files(table_name).await?,
            );
        }

        let rewrite_backups = self
            .active_txn
            .as_ref()
            .map(|txn| txn.rewrite_backups.clone())
            .unwrap_or_default();

        let txn = self
            .active_txn
            .as_mut()
            .ok_or("SAVEPOINT requires an active transaction")?;

        txn.savepoints.push(Savepoint {
            name: name.to_string(),
            catalog_snapshot,
            file_snapshot,
            rewrite_backups,
        });

        Ok(QueryResult::Message(format!("SAVEPOINT {name}")))
    }

    /// `ROLLBACK TO [SAVEPOINT] name` -- restores state to the named savepoint.
    async fn handle_rollback_to(&mut self, name: &str) -> Result<QueryResult, BoxError> {
        let (catalog_snapshot, file_snapshot, rewrite_backups) = {
            let txn = self
                .active_txn
                .as_mut()
                .ok_or("ROLLBACK TO SAVEPOINT requires an active transaction")?;

            let idx = txn
                .savepoints
                .iter()
                .rposition(|sp| sp.name == name)
                .ok_or_else(|| format!("Savepoint '{name}' does not exist"))?;

            let catalog_snapshot = txn.savepoints[idx].catalog_snapshot.clone();
            let file_snapshot = txn.savepoints[idx].file_snapshot.clone();
            let rewrite_backups = txn.savepoints[idx].rewrite_backups.clone();
            txn.savepoints.truncate(idx + 1);

            (catalog_snapshot, file_snapshot, rewrite_backups)
        };

        let current_table_names: Vec<String> = self.catalog.tables.keys().cloned().collect();
        let snapshot_table_names: HashSet<&String> = file_snapshot.keys().collect();

        for (table_name, old_files) in &file_snapshot {
            if let Ok(current_files) = self.list_parquet_files(table_name).await {
                for f in &current_files {
                    if !old_files.contains(f) {
                        let _ = self.store.delete(f).await;
                    }
                }
            }
        }

        for table_name in &current_table_names {
            if !snapshot_table_names.contains(table_name) {
                if let Some(meta) = self.catalog.tables.get(table_name) {
                    let _ = self.delete_table_storage(meta).await;
                }
                let _ = self.ctx.deregister_table(table_name.as_str());
            }
        }

        self.catalog.restore(catalog_snapshot);

        for table_name in &current_table_names {
            let _ = self.ctx.deregister_table(table_name.as_str());
        }
        self.reload_tables().await?;

        for (table, batches) in rewrite_backups {
            if let Some(meta) = self.catalog.tables.get(&table) {
                let schema = columns_to_schema(&meta.columns)?;
                self.rewrite_table(&table, schema, batches).await?;
            }
        }

        let catalog_snapshot = self.catalog.snapshot();
        let table_names: Vec<String> = self.catalog.tables.keys().cloned().collect();
        let mut file_snapshot = HashMap::new();
        for table_name in &table_names {
            file_snapshot.insert(
                table_name.clone(),
                self.list_parquet_files(table_name).await?,
            );
        }

        let txn = self.active_txn.as_mut().unwrap();
        txn.catalog_snapshot = catalog_snapshot;
        txn.file_snapshot = file_snapshot;
        txn.rewrite_backups = HashMap::new();

        Ok(QueryResult::Message(format!(
            "ROLLBACK TO SAVEPOINT {name}"
        )))
    }

    /// `RELEASE [SAVEPOINT] name` -- discards the named savepoint without restoring.
    fn handle_release_savepoint(&mut self, name: &str) -> Result<QueryResult, BoxError> {
        let txn = self
            .active_txn
            .as_mut()
            .ok_or("RELEASE SAVEPOINT requires an active transaction")?;

        let idx = txn
            .savepoints
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| format!("Savepoint '{name}' does not exist"))?;

        txn.savepoints.truncate(idx);

        Ok(QueryResult::Message(format!("RELEASE SAVEPOINT {name}")))
    }

    // ── SQL file execution ─────────────────────────────────────

    /// Reads a `.sql` file, splits it into individual statements on
    /// semicolons (respecting `--` line comments, `/* */` block comments,
    /// and single-quoted string literals), and executes each statement in
    /// order.
    ///
    /// Returns a `Vec` of `(statement, result)` pairs. Execution stops
    /// at the first error unless `continue_on_error` is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL file cannot be read.
    pub async fn execute_file(
        &mut self,
        path: &str,
        continue_on_error: bool,
    ) -> Result<Vec<(String, Result<QueryResult, BoxError>)>, BoxError> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read SQL file '{path}': {e}"))?;
        let statements = split_sql_statements(&contents);
        let mut results = Vec::new();
        for stmt in statements {
            let result = self.execute(&stmt).await;
            let is_err = result.is_err();
            results.push((stmt, result));
            if is_err && !continue_on_error {
                break;
            }
        }
        Ok(results)
    }

    // ── SQL dispatch ───────────────────────────────────────────

    /// Parses and executes a single SQL statement.
    ///
    /// DDL, DML, transaction-control, and maintenance statements are
    /// intercepted. Everything else is forwarded to `DataFusion`.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL is invalid or execution fails.
    #[async_recursion::async_recursion]
    pub async fn execute(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        self.drain_pending_analyze().await;
        self.last_query_metrics = QueryMetrics::default();
        let started_at = Instant::now();
        let mut effective_sql = self.expand_nextval_calls(sql).await?;
        if let Some(ts) = parse_as_of_timestamp(&effective_sql) {
            let result = self.execute_as_of_timestamp(&effective_sql, ts).await;
            return self.finalize_query(sql, started_at, result);
        }
        if !effective_sql
            .trim_start()
            .to_uppercase()
            .starts_with("CREATE FUNCTION ")
            && !effective_sql
                .trim_start()
                .to_uppercase()
                .starts_with("DROP FUNCTION ")
        {
            effective_sql = expand_user_defined_functions(&effective_sql, &self.catalog.udfs)?;
        }
        effective_sql = rewrite_fulltext_match_sql(&effective_sql, &self.fulltext_indexes);
        let sql = effective_sql.as_str();

        let trimmed = sql.trim().trim_end_matches(';').trim();
        let upper = trimmed.to_uppercase();
        if upper == "FLUSH" || upper.starts_with("FLUSH ") {
            let tokens: Vec<&str> = trimmed.split_whitespace().collect();
            let flushed = if tokens.len() == 1 {
                self.flush_all().await?
            } else {
                let table_name = if tokens
                    .get(1)
                    .is_some_and(|t| t.eq_ignore_ascii_case("TABLE"))
                {
                    tokens
                        .get(2)
                        .copied()
                        .ok_or("FLUSH TABLE requires a table name")?
                } else {
                    tokens
                        .get(1)
                        .copied()
                        .ok_or("FLUSH requires an optional table name")?
                };
                self.flush_table(table_name.trim_matches('"')).await?
            };
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(
                sql,
                started_at,
                Ok(QueryResult::Message(format!("{flushed} row(s) flushed."))),
            );
        }

        if !upper.starts_with("INSERT ") {
            let needs_no_flush = upper.starts_with("SHOW ")
                || upper.starts_with("PREPARE ")
                || upper.starts_with("LISTEN ")
                || upper.starts_with("NOTIFY ")
                || upper.starts_with("CREATE USER ")
                || upper.starts_with("CREATE ROLE ")
                || upper.starts_with("GRANT ")
                || upper.starts_with("REVOKE ");
            if !needs_no_flush {
                self.flush_buffered_for_sql(&upper).await?;
            }
        }

        if upper.starts_with("SELECT ") && upper.contains("FROM POTATODB_CDC") {
            let result = self.handle_select_cdc(trimmed);
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("SELECT ") && upper.contains("FROM POTATODB_SYSTEM_STATUS") {
            let result = self.handle_select_system_status();
            return self.finalize_query(sql, started_at, result);
        }

        if upper.contains("PG_CATALOG.")
            || upper.contains("PG_TYPE")
            || upper.contains("PG_CLASS")
            || upper.contains("PG_NAMESPACE")
            || upper.contains("PG_ATTRIBUTE")
        {
            let result = self.handle_pg_catalog_query(trimmed);
            return self.finalize_query(sql, started_at, result);
        }

        if upper.starts_with("CREATE FUNCTION ") {
            let result = self.handle_create_function(trimmed).await;
            if result.is_ok() {
                self.wal_finish_autocommit(true)?;
            }
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("DROP FUNCTION ") {
            let result = self.handle_drop_function(trimmed).await;
            if result.is_ok() {
                self.wal_finish_autocommit(true)?;
            }
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }

        if upper.starts_with("CREATE USER ") {
            let result = self.handle_create_user(trimmed).await;
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("CREATE ROLE ") {
            let result = self.handle_create_role(trimmed).await;
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("GRANT ") {
            let result = self.handle_grant(trimmed).await;
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("REVOKE ") {
            let result = self.handle_revoke(trimmed).await;
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("LISTEN ") {
            let result = self.handle_listen(trimmed);
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("NOTIFY ") {
            let result = self.handle_notify(trimmed);
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("CREATE PROCEDURE ") {
            let result = self.handle_create_procedure(trimmed);
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("CALL ") {
            let result = self.handle_call_procedure(trimmed).await;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("DO $$") {
            let result = self.handle_do_block(trimmed).await;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("CREATE FULLTEXT INDEX ") {
            let result = self.handle_create_fulltext_index(trimmed).await;
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("CREATE TRIGGER ") {
            let result = self.handle_create_trigger(trimmed).await;
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("DROP TRIGGER ") {
            let result = self.handle_drop_trigger(trimmed).await;
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("SAVEPOINT ") {
            let name = trimmed["SAVEPOINT ".len()..].trim().trim_matches('"');
            let result = self.handle_savepoint(name).await;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("ROLLBACK TO ") {
            let rest = trimmed["ROLLBACK TO ".len()..].trim();
            let name = rest
                .strip_prefix("SAVEPOINT ")
                .unwrap_or(rest)
                .trim()
                .trim_matches('"');
            let result = self.handle_rollback_to(name).await;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("RELEASE SAVEPOINT ") || upper.starts_with("RELEASE ") {
            let name = if upper.starts_with("RELEASE SAVEPOINT ") {
                trimmed["RELEASE SAVEPOINT ".len()..]
                    .trim()
                    .trim_matches('"')
            } else {
                trimmed["RELEASE ".len()..].trim().trim_matches('"')
            };
            let result = self.handle_release_savepoint(name);
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("CREATE MIGRATION ") {
            let result = self.handle_create_migration(trimmed).await;
            if result.is_ok() {
                self.wal_finish_autocommit(true)?;
            }
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper == "MIGRATE" || upper.starts_with("MIGRATE ") {
            let result = self.handle_migrate(trimmed).await;
            if result.is_ok() {
                self.wal_finish_autocommit(true)?;
            }
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("ADD REPLICA ") {
            let result = self.handle_add_replica(trimmed);
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("REMOVE REPLICA ") {
            let result = self.handle_remove_replica(trimmed);
            return self.finalize_query(sql, started_at, result);
        }

        self.enforce_access(trimmed)?;

        if upper.starts_with("TRUNCATE ") {
            let tokens: Vec<&str> = trimmed.split_whitespace().collect();
            let table_name = if tokens
                .get(1)
                .is_some_and(|t| t.eq_ignore_ascii_case("TABLE"))
            {
                tokens
                    .get(2)
                    .copied()
                    .ok_or("TRUNCATE requires a table name")?
            } else {
                tokens
                    .get(1)
                    .copied()
                    .ok_or("TRUNCATE requires a table name")?
            };
            let result = self.handle_truncate(table_name.trim_matches('"')).await;
            if result.is_ok() {
                self.wal_finish_autocommit(true)?;
            }
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper.starts_with("REFRESH MATERIALIZED VIEW ") {
            let view_name = trimmed
                .split_whitespace()
                .nth(3)
                .ok_or("REFRESH MATERIALIZED VIEW requires a view name")?
                .trim_matches('"');
            let result = self.handle_refresh_materialized_view(view_name).await;
            if result.is_ok() {
                self.wal_finish_autocommit(true)?;
            }
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }
        if upper == "CHECKPOINT" || upper == "CHECKPOINT;" {
            let result = self.handle_checkpoint();
            self.catalog.flush_if_dirty().await?;
            return self.finalize_query(sql, started_at, result);
        }

        let dialect = PostgreSqlDialect {};
        let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
            if upper.starts_with("VACUUM ") || upper == "VACUUM" {
                let parts: Vec<&str> = trimmed.split_whitespace().collect();
                let (table_name, do_analyze) = if parts.len() >= 3
                    && parts.get(1).map(|s| s.to_uppercase()) == Some("ANALYZE".into())
                {
                    (parts.get(2).map_or("", |s| s.trim_matches('"')), true)
                } else if parts.len() >= 2 {
                    (parts.get(1).map_or("", |s| s.trim_matches('"')), false)
                } else {
                    return self.finalize_query(
                        sql,
                        started_at,
                        Err("VACUUM requires a table name".into()),
                    );
                };
                if table_name.is_empty() {
                    return self.finalize_query(
                        sql,
                        started_at,
                        Err("VACUUM requires a table name".into()),
                    );
                }
                let vac_result = self.handle_vacuum(table_name).await;
                let result = if do_analyze {
                    match vac_result {
                        Ok(QueryResult::Message(vac_msg)) => {
                            match self.handle_analyze(table_name).await {
                                Ok(QueryResult::Message(analyze_msg)) => {
                                    Ok(QueryResult::Message(format!("{vac_msg} {analyze_msg}")))
                                }
                                other => other,
                            }
                        }
                        other => other,
                    }
                } else {
                    vac_result
                };
                self.catalog.flush_if_dirty().await?;
                return self.finalize_query(sql, started_at, result);
            }
            if upper.starts_with("ANALYZE ") || upper == "ANALYZE" {
                let table_name = trimmed
                    .split_whitespace()
                    .nth(1)
                    .ok_or("ANALYZE requires a table name")?;
                let result = self.handle_analyze(table_name).await;
                self.catalog.flush_if_dirty().await?;
                return self.finalize_query(sql, started_at, result);
            }
            let batches = self.collect_with_plan_cache(sql).await?;
            return self.finalize_query(sql, started_at, Ok(QueryResult::Records(batches)));
        };

        if statements.len() != 1 {
            return self.finalize_query(
                sql,
                started_at,
                Err("Expected exactly one SQL statement".into()),
            );
        }

        let result = match &statements[0] {
            Statement::StartTransaction { .. } => self.handle_begin().await,
            Statement::Commit { .. } => self.handle_commit().await,
            Statement::Rollback { .. } => self.handle_rollback().await,
            Statement::CreateTable(create) => self.handle_create_table(create).await,
            Statement::CreateIndex(create_idx) => self.handle_create_index(create_idx).await,
            Statement::CreateSequence {
                ref name,
                if_not_exists,
                ref sequence_options,
                ..
            } => {
                self.handle_create_sequence(&name.to_string(), *if_not_exists, sequence_options)
                    .await
            }
            Statement::Drop {
                object_type: ObjectType::Table,
                names,
                if_exists,
                ..
            } => {
                let name = names
                    .first()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default();
                self.handle_drop_table(&name, *if_exists).await
            }
            Statement::Drop {
                object_type: ObjectType::Index,
                names,
                if_exists,
                ..
            } => {
                let name = names
                    .first()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default();
                self.handle_drop_index(&name, *if_exists).await
            }
            Statement::Drop {
                object_type: ObjectType::View,
                names,
                if_exists,
                ..
            } => {
                let name = names
                    .first()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default();
                self.handle_drop_view(&name, *if_exists).await
            }
            Statement::Drop {
                object_type: ObjectType::Sequence,
                names,
                if_exists,
                ..
            } => {
                let name = names
                    .first()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default();
                self.handle_drop_sequence(&name, *if_exists).await
            }
            Statement::Delete(ref delete) => self.handle_delete(sql, delete).await,
            Statement::Update {
                ref table,
                ref assignments,
                ref selection,
                ..
            } => {
                self.handle_update(sql, table, assignments, selection.as_ref())
                    .await
            }
            Statement::Insert(insert) => self.handle_insert(sql, insert).await,
            Statement::Merge {
                ref table,
                ref source,
                ref on,
                ref clauses,
                ..
            } => self.handle_merge(table, source, on, clauses).await,
            Statement::AlterTable {
                ref name,
                ref operations,
                if_exists,
                ..
            } => {
                self.handle_alter_table(sql, &name.to_string(), operations, *if_exists)
                    .await
            }
            Statement::CreateView {
                ref name,
                ref query,
                materialized,
                or_replace,
                ..
            } => {
                self.handle_create_view(&name.to_string(), query, *or_replace, *materialized)
                    .await
            }
            Statement::Prepare {
                ref name,
                ref statement,
                ..
            } => {
                self.handle_prepare(&name.to_string(), &statement.to_string())
                    .await
            }
            Statement::Execute {
                ref name,
                ref parameters,
                ..
            } => {
                let stmt_name = name
                    .as_ref()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default();
                self.handle_execute_prepared(&stmt_name, parameters).await
            }
            Statement::Copy { .. } => {
                let upper = sql.to_uppercase();
                if upper.contains(" FROM ") {
                    self.handle_copy_from(sql).await
                } else {
                    self.handle_copy_to(sql).await
                }
            }
            Statement::Explain { .. } => {
                let normalized = normalize_explain_sql(sql);
                let batches = self.collect_with_plan_cache(&normalized).await?;
                Ok(QueryResult::Records(batches))
            }
            Statement::Analyze { ref table_name, .. } => {
                self.handle_analyze(&table_name.to_string()).await
            }
            Statement::Vacuum(ref v) => {
                let tbl = v
                    .table_name
                    .as_ref()
                    .map(std::string::ToString::to_string)
                    .ok_or("VACUUM requires a table name")?;
                self.handle_vacuum(&tbl).await
            }
            _ => {
                let batches = self.collect_with_plan_cache(sql).await?;
                Ok(QueryResult::Records(batches))
            }
        };

        let is_mutating = matches!(
            &statements[0],
            Statement::CreateTable(_)
                | Statement::CreateIndex(_)
                | Statement::CreateSequence { .. }
                | Statement::Insert(_)
                | Statement::Delete(_)
                | Statement::Update { .. }
                | Statement::Merge { .. }
                | Statement::AlterTable { .. }
                | Statement::CreateView { .. }
                | Statement::Drop {
                    object_type: ObjectType::Table
                        | ObjectType::Index
                        | ObjectType::View
                        | ObjectType::Sequence,
                    ..
                }
                | Statement::Copy { .. }
        );

        let force_wal_checkpoint = matches!(
            &statements[0],
            Statement::CreateTable(_)
                | Statement::CreateIndex(_)
                | Statement::CreateSequence { .. }
                | Statement::Merge { .. }
                | Statement::AlterTable { .. }
                | Statement::CreateView { .. }
                | Statement::Drop {
                    object_type: ObjectType::Table
                        | ObjectType::Index
                        | ObjectType::View
                        | ObjectType::Sequence,
                    ..
                }
        );

        if is_mutating && result.is_ok() {
            let mutated_table = extract_mutated_table(&statements[0]);
            self.evict_plan_cache(mutated_table.as_deref());
            self.wal_finish_autocommit(force_wal_checkpoint)?;
            if self.snapshots_enabled {
                let _ = self.capture_snapshot().await;
            }
        }

        self.catalog.flush_if_dirty().await?;
        self.finalize_query(sql, started_at, result)
    }

    /// Streaming variant of [`execute`](Self::execute).
    ///
    /// For read-only queries (`SELECT`, `EXPLAIN`, `SHOW`, etc.) the
    /// result is streamed without materializing all batches in memory.
    /// DDL and DML statements are delegated to [`execute`](Self::execute).
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL is invalid or execution fails.
    pub async fn execute_stream(&mut self, sql: &str) -> Result<QueryResultStream, BoxError> {
        let first_word = sql.split_whitespace().next().unwrap_or("").to_uppercase();

        if matches!(
            first_word.as_str(),
            "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "EXPLAIN"
        ) {
            let _ = self.flush_all().await?;
        }

        match first_word.as_str() {
            "SELECT" | "EXPLAIN" | "SHOW" | "DESCRIBE" | "WITH" | "VALUES" => {
                let df = self.ctx.sql(sql).await?;
                let stream = df.execute_stream().await?;
                Ok(QueryResultStream::Stream(stream))
            }
            _ => match self.execute(sql).await? {
                QueryResult::Message(msg) => Ok(QueryResultStream::Message(msg)),
                QueryResult::Records(batches) => {
                    let schema = batches.first().map_or_else(
                        || Arc::new(Schema::empty()),
                        arrow::array::RecordBatch::schema,
                    );
                    let iter = batches
                        .into_iter()
                        .map(Ok::<_, datafusion::error::DataFusionError>);
                    let stream = RecordBatchStreamAdapter::new(schema, futures::stream::iter(iter));
                    Ok(QueryResultStream::Stream(Box::pin(stream)))
                }
            },
        }
    }

    /// Executes read-only SQL, rejecting any mutating statements.
    ///
    /// Only `SELECT`, `WITH`, `SHOW`, `DESCRIBE`, and `EXPLAIN`
    /// statements are permitted. All other SQL is rejected with an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the SQL is mutating, invalid, or execution fails.
    pub async fn execute_readonly(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        if !is_read_only_sql(sql) {
            return Err("execute_readonly rejects mutating SQL".into());
        }
        let _ = self.flush_all().await?;
        let batches = self.collect_with_plan_cache(sql).await?;
        Ok(QueryResult::Records(batches))
    }

    /// Returns recent query log entries, oldest-first.
    #[must_use]
    pub fn recent_queries(&self) -> Vec<QueryLogEntry> {
        self.query_log.iter().cloned().collect()
    }

    /// Creates a compressed backup archive containing catalog and table files.
    ///
    /// # Errors
    ///
    /// Returns an error if backup is attempted on S3 or the tar command fails.
    #[allow(clippy::unused_async)]
    pub async fn backup(&self, archive_path: &str) -> Result<(), BoxError> {
        if self.is_s3 {
            return Err(
                "Backup is only supported for local data directories. For S3 databases, recovery is supported via WAL replay on startup.".into(),
            );
        }
        let status = Command::new("tar")
            .arg("-czf")
            .arg(archive_path)
            .arg("-C")
            .arg(&self.data_url)
            .arg(".")
            .status()?;
        if !status.success() {
            return Err("Backup command failed".into());
        }
        Ok(())
    }

    /// Restores a compressed backup archive into the current data directory.
    ///
    /// # Errors
    ///
    /// Returns an error if restore is attempted on S3 or the tar command fails.
    pub async fn restore(&mut self, archive_path: &str) -> Result<(), BoxError> {
        if self.is_s3 {
            return Err(
                "Restore is only supported for local data directories. For S3 databases, recovery is supported via WAL replay on startup.".into(),
            );
        }

        let current_table_names: Vec<String> = self.catalog.tables.keys().cloned().collect();
        for name in &current_table_names {
            let _ = self.ctx.deregister_table(name);
        }

        let data_dir = PathBuf::from(&self.data_url);
        if data_dir.exists() {
            std::fs::remove_dir_all(&data_dir)?;
        }
        std::fs::create_dir_all(&data_dir)?;

        let status = Command::new("tar")
            .arg("-xzf")
            .arg(archive_path)
            .arg("-C")
            .arg(&self.data_url)
            .status()?;
        if !status.success() {
            return Err("Restore command failed".into());
        }

        self.catalog = Catalog::load(self.store.clone(), ObjPath::from("catalog.json")).await?;
        self.reload_tables().await?;
        Ok(())
    }

    fn finalize_query(
        &mut self,
        sql: &str,
        started_at: Instant,
        result: Result<QueryResult, BoxError>,
    ) -> Result<QueryResult, BoxError> {
        let duration = started_at.elapsed();
        let rows = match &result {
            Ok(QueryResult::Records(batches)) => batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum(),
            _ => 0,
        };
        self.query_log.push_back(QueryLogEntry {
            sql: sql.to_string(),
            duration,
            rows,
        });
        while self.query_log.len() > self.max_query_log_entries {
            let _ = self.query_log.pop_front();
        }
        if duration.as_millis() >= u128::from(self.slow_query_threshold_ms) {
            eprintln!("slow query ({} ms): {}", duration.as_millis(), sql.trim());
        }
        result
    }

    fn evict_plan_cache(&mut self, table_name: Option<&str>) {
        if let Some(name) = table_name {
            self.plan_cache.retain(|sql, _| !sql.contains(name));
        } else {
            self.plan_cache.clear();
        }
    }

    async fn collect_with_plan_cache(&mut self, sql: &str) -> Result<Vec<RecordBatch>, BoxError> {
        if is_read_only_sql(sql) {
            for table_name in extract_table_names_from_readonly_sql(sql) {
                if let Some(meta) = self.catalog.tables.get(&table_name) {
                    if !meta.partition_columns.is_empty() {
                        eprintln!(
                            "potatodb: query touches partitioned table '{}' (partition cols: {:?}). \
                             Partition pruning not yet implemented.",
                            table_name,
                            meta.partition_columns
                        );
                    }
                }
            }
            if let Some(plan) = self.plan_cache.get(sql).cloned() {
                if let Ok(df) = self.ctx.execute_logical_plan(plan).await {
                    if let Ok(batches) = df.collect().await {
                        self.plan_cache_hits = self.plan_cache_hits.saturating_add(1);
                        return Ok(batches);
                    }
                }
                self.plan_cache.remove(sql);
            }
            match self.ctx.sql(sql).await {
                Ok(df) => {
                    let plan = df.logical_plan().clone();
                    let batches = df.collect().await?;
                    self.plan_cache.insert(sql.to_string(), plan);
                    return Ok(batches);
                }
                Err(e) if e.to_string().contains("coerce types Duration") => {
                    if let Some(rewritten) = rewrite_date_subtraction_sql(sql) {
                        let df = self.ctx.sql(&rewritten).await?;
                        let plan = df.logical_plan().clone();
                        let batches = df.collect().await?;
                        self.plan_cache.insert(sql.to_string(), plan);
                        return Ok(batches);
                    }
                    return Err(e.into());
                }
                Err(e) => return Err(e.into()),
            }
        }
        match self.ctx.sql(sql).await {
            Ok(df) => Ok(df.collect().await?),
            Err(e) if e.to_string().contains("coerce types Duration") => {
                if let Some(rewritten) = rewrite_date_subtraction_sql(sql) {
                    let df = self.ctx.sql(&rewritten).await?;
                    return Ok(df.collect().await?);
                }
                Err(e.into())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn capture_snapshot(&mut self) -> Result<(), BoxError> {
        let mut tables = HashMap::new();
        for table in self.table_names() {
            let df = self.ctx.sql(&format!("SELECT * FROM \"{table}\"")).await?;
            let batches = df.collect().await?;
            tables.insert(table, batches);
        }
        let now = Utc::now().timestamp_millis();
        self.snapshots.push_back(DbSnapshot {
            timestamp_ms: now,
            tables,
        });
        while self
            .snapshots
            .front()
            .is_some_and(|s| now.saturating_sub(s.timestamp_ms) > self.snapshot_retention_ms)
        {
            let _ = self.snapshots.pop_front();
        }
        Ok(())
    }

    async fn execute_as_of_timestamp(
        &mut self,
        sql: &str,
        timestamp_ms: i64,
    ) -> Result<QueryResult, BoxError> {
        if !self.snapshots_enabled {
            self.snapshots_enabled = true;
            let _ = self.capture_snapshot().await;
        }

        let bare_sql = strip_as_of_timestamp(sql);
        let snapshot = self
            .snapshots
            .iter()
            .filter(|s| s.timestamp_ms <= timestamp_ms)
            .max_by_key(|s| s.timestamp_ms)
            .ok_or("No snapshot available for requested AS OF timestamp")?
            .clone();

        let mut rewritten = bare_sql.clone();
        let mut registered: Vec<String> = Vec::new();
        for (table, batches) in &snapshot.tables {
            let temp_name = format!("__potato_asof_{table}");
            let schema = if let Some(first) = batches.first() {
                first.schema()
            } else if let Some(meta) = self.catalog.tables.get(table) {
                columns_to_schema(&meta.columns)?
            } else {
                continue;
            };
            let mem = MemTable::try_new(schema, vec![batches.clone()])?;
            self.ctx.register_table(&temp_name, Arc::new(mem))?;
            registered.push(temp_name.clone());
            rewritten = rewritten
                .replace(
                    &format!("FROM \"{table}\""),
                    &format!("FROM \"{temp_name}\""),
                )
                .replace(
                    &format!("JOIN \"{table}\""),
                    &format!("JOIN \"{temp_name}\""),
                )
                .replace(&format!("FROM {table}"), &format!("FROM \"{temp_name}\""))
                .replace(&format!("JOIN {table}"), &format!("JOIN \"{temp_name}\""));
        }

        let result = self
            .collect_with_plan_cache(&rewritten)
            .await
            .map(QueryResult::Records);

        for t in registered {
            let _ = self.ctx.deregister_table(&t);
        }
        result
    }

    fn record_cdc_event(&mut self, table: &str, op: &str, rows: usize) {
        let timestamp_ms = Utc::now().timestamp_millis();
        self.cdc_log.push_back(CdcEvent {
            table: table.to_string(),
            op: op.to_string(),
            timestamp_ms,
            rows,
        });
        while self.cdc_log.len() > self.cdc_capacity {
            let _ = self.cdc_log.pop_front();
        }
        if let Some(path) = &self.cdc_log_path {
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                let line = serde_json::json!({
                    "table": table,
                    "op": op,
                    "timestamp_ms": timestamp_ms,
                    "rows": rows
                });
                let _ = writeln!(f, "{line}");
            }
        }
    }

    fn current_user_is_admin(&self) -> bool {
        let Some(role_names) = self.catalog.user_roles.get(&self.current_user) else {
            return false;
        };
        role_names.iter().any(|r| {
            self.catalog.roles.get(r).is_some_and(|role_def| {
                role_def
                    .privileges
                    .iter()
                    .any(|p| p.kind == "ALL" || p.kind == "*")
            })
        })
    }

    fn user_has_privilege(&self, action: &str, table: Option<&str>) -> bool {
        if self.current_user_is_admin() {
            return true;
        }
        let Some(role_names) = self.catalog.user_roles.get(&self.current_user) else {
            return false;
        };
        for rn in role_names {
            if let Some(role_def) = self.catalog.roles.get(rn) {
                for priv_entry in &role_def.privileges {
                    if (priv_entry.kind == "ALL" || priv_entry.kind == action)
                        && (priv_entry.table.is_none() || priv_entry.table.as_deref() == table)
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn enforce_access(&self, sql: &str) -> Result<(), BoxError> {
        if self.current_user_is_admin() {
            return Ok(());
        }
        let action = sql.split_whitespace().next().unwrap_or("").to_uppercase();
        if matches!(
            action.as_str(),
            "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "EXPLAIN"
        ) {
            if !self.user_has_privilege("SELECT", None) {
                return Err(format!(
                    "permission denied for user '{}' on {}",
                    self.current_user, action
                )
                .into());
            }
            return Ok(());
        }
        if !self.user_has_privilege(&action, None) {
            return Err(format!(
                "permission denied for user '{}' on statement '{}'",
                self.current_user, action
            )
            .into());
        }
        Ok(())
    }

    fn handle_select_cdc(&self, sql: &str) -> Result<QueryResult, BoxError> {
        let mut rows: Vec<&CdcEvent> = self.cdc_log.iter().collect();
        let upper = sql.to_uppercase();
        if let Some(idx) = upper.find("WHERE") {
            let where_clause = &sql[idx + "WHERE".len()..];
            if let Some(eq_idx) = where_clause.to_uppercase().find("TABLE") {
                let rest = &where_clause[eq_idx + "TABLE".len()..];
                if let Some(pos) = rest.find('=') {
                    let table = rest[pos + 1..]
                        .trim()
                        .trim_end_matches(';')
                        .trim_matches('\'')
                        .trim_matches('"')
                        .to_string();
                    rows.retain(|e| e.table.eq_ignore_ascii_case(&table));
                }
            }
        }
        let table_arr = StringArray::from(
            rows.iter()
                .map(|e| e.table.clone())
                .collect::<Vec<String>>(),
        );
        let op_arr = StringArray::from(rows.iter().map(|e| e.op.clone()).collect::<Vec<String>>());
        let ts_arr = Int64Array::from(rows.iter().map(|e| e.timestamp_ms).collect::<Vec<i64>>());
        #[allow(clippy::cast_possible_wrap)]
        let row_arr = Int64Array::from(rows.iter().map(|e| e.rows as i64).collect::<Vec<i64>>());
        let schema = Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("op", DataType::Utf8, false),
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("rows", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(table_arr),
                Arc::new(op_arr),
                Arc::new(ts_arr),
                Arc::new(row_arr),
            ],
        )?;
        Ok(QueryResult::Records(vec![batch]))
    }

    #[allow(clippy::cast_possible_wrap)]
    fn handle_select_system_status(&self) -> Result<QueryResult, BoxError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_cache_entries", DataType::Int64, false),
            Field::new("plan_cache_hits", DataType::Int64, false),
            Field::new("query_log_entries", DataType::Int64, false),
            Field::new("snapshots", DataType::Int64, false),
            Field::new("wal_commits_since_checkpoint", DataType::Int64, false),
            Field::new("wal_checkpoint_threshold_bytes", DataType::Int64, false),
            Field::new("last_query_parquet_files_read", DataType::Int64, false),
            Field::new("last_query_bytes_scanned", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![self.plan_cache.len() as i64])),
                Arc::new(Int64Array::from(vec![self.plan_cache_hits as i64])),
                Arc::new(Int64Array::from(vec![self.query_log.len() as i64])),
                Arc::new(Int64Array::from(vec![self.snapshots.len() as i64])),
                Arc::new(Int64Array::from(vec![
                    self.wal_commits_since_checkpoint as i64,
                ])),
                Arc::new(Int64Array::from(vec![
                    self.wal_checkpoint_threshold_bytes as i64,
                ])),
                Arc::new(Int64Array::from(vec![
                    self.last_query_metrics.parquet_files_read as i64,
                ])),
                Arc::new(Int64Array::from(vec![
                    self.last_query_metrics.bytes_scanned as i64,
                ])),
            ],
        )?;
        Ok(QueryResult::Records(vec![batch]))
    }

    #[allow(clippy::cast_possible_wrap)]
    fn handle_pg_catalog_query(&self, sql: &str) -> Result<QueryResult, BoxError> {
        let upper = sql.to_uppercase();
        if upper.contains("PG_TYPE") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("oid", DataType::Int32, false),
                Field::new("typname", DataType::Utf8, false),
                Field::new("typlen", DataType::Int16, false),
            ]));
            let oid_arr = Int32Array::from(vec![23, 1043, 16, 701, 25, 1114, 1082]);
            let name_arr = StringArray::from(vec![
                "int4",
                "varchar",
                "bool",
                "float8",
                "text",
                "timestamp",
                "date",
            ]);
            let len_arr = Int16Array::from(vec![4, -1, 1, 8, -1, 8, 4]);
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(oid_arr), Arc::new(name_arr), Arc::new(len_arr)],
            )?;
            return Ok(QueryResult::Records(vec![batch]));
        }
        if upper.contains("PG_CLASS") {
            let table_names: Vec<String> = self.catalog.tables.keys().cloned().collect();
            let schema = Arc::new(Schema::new(vec![
                Field::new("oid", DataType::Int32, false),
                Field::new("relname", DataType::Utf8, false),
            ]));
            let oids: Vec<i32> = (1..=table_names.len() as i32).collect();
            let oid_arr = Int32Array::from(oids);
            let name_arr = StringArray::from(table_names);
            let batch = RecordBatch::try_new(schema, vec![Arc::new(oid_arr), Arc::new(name_arr)])?;
            return Ok(QueryResult::Records(vec![batch]));
        }
        if upper.contains("PG_NAMESPACE") {
            let schema = Arc::new(Schema::new(vec![
                Field::new("oid", DataType::Int32, false),
                Field::new("nspname", DataType::Utf8, false),
            ]));
            let oid_arr = Int32Array::from(vec![2200]);
            let name_arr = StringArray::from(vec!["public"]);
            let batch = RecordBatch::try_new(schema, vec![Arc::new(oid_arr), Arc::new(name_arr)])?;
            return Ok(QueryResult::Records(vec![batch]));
        }
        if upper.contains("PG_ATTRIBUTE") {
            let mut attrelids = Vec::new();
            let mut attnames = Vec::new();
            let mut attnums = Vec::new();
            let mut atttypids = Vec::new();
            let mut rel_oid: i32 = 1;
            for meta in self.catalog.tables.values() {
                for (idx, col) in meta.columns.iter().enumerate() {
                    attrelids.push(rel_oid);
                    attnames.push(col.name.clone());
                    attnums.push((idx + 1) as i32);
                    atttypids.push(sql_type_to_pg_oid(&col.data_type));
                }
                rel_oid += 1;
            }
            let schema = Arc::new(Schema::new(vec![
                Field::new("attrelid", DataType::Int32, false),
                Field::new("attname", DataType::Utf8, false),
                Field::new("attnum", DataType::Int32, false),
                Field::new("atttypid", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(attrelids)),
                    Arc::new(StringArray::from(attnames)),
                    Arc::new(Int32Array::from(attnums)),
                    Arc::new(Int32Array::from(atttypids)),
                ],
            )?;
            return Ok(QueryResult::Records(vec![batch]));
        }
        Ok(QueryResult::Records(vec![]))
    }

    fn handle_listen(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let channel = sql
            .split_whitespace()
            .nth(1)
            .ok_or("LISTEN requires channel name")?
            .trim_end_matches(';')
            .trim_matches('"')
            .to_string();
        self.notification_queues.entry(channel.clone()).or_default();
        Ok(QueryResult::Message(format!(
            "LISTEN '{channel}' registered."
        )))
    }

    fn handle_notify(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let payload_sql = sql.trim().trim_end_matches(';');
        let after = payload_sql
            .split_once(char::is_whitespace)
            .map(|(_, rest)| rest.trim())
            .ok_or("NOTIFY requires channel name")?;
        let (channel, payload) = if let Some((ch, pl)) = after.split_once(',') {
            (
                ch.trim().trim_matches('"').to_string(),
                pl.trim().trim_matches('\'').to_string(),
            )
        } else {
            (after.trim().trim_matches('"').to_string(), String::new())
        };
        self.notification_queues
            .entry(channel.clone())
            .or_default()
            .push_back(payload.clone());
        Ok(QueryResult::Message(format!(
            "NOTIFY sent on '{channel}' (payload {} byte(s)).",
            payload.len()
        )))
    }

    fn handle_create_procedure(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("CREATE PROCEDURE ") {
            return Err("Expected CREATE PROCEDURE".into());
        }
        let after = trimmed["CREATE PROCEDURE ".len()..].trim();
        let name_end = after
            .find(|c: char| c == '(' || c.is_whitespace())
            .ok_or("CREATE PROCEDURE requires a name")?;
        let name = after[..name_end].trim_matches('"').to_string();
        let body =
            extract_dollar_quoted_body(trimmed).ok_or("CREATE PROCEDURE requires $$...$$")?;
        self.procedures.insert(name.clone(), body);
        Ok(QueryResult::Message(format!("Procedure '{name}' created.")))
    }

    async fn handle_create_trigger(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("CREATE TRIGGER ") {
            return Err("Expected CREATE TRIGGER".into());
        }
        let after = trimmed["CREATE TRIGGER ".len()..].trim();
        let name_end = after
            .find(|c: char| c.is_whitespace())
            .ok_or("CREATE TRIGGER requires trigger name")?;
        let name = after[..name_end].trim_matches('"').to_string();
        let rest = after[name_end..].trim();
        let timing = if rest.to_uppercase().starts_with("BEFORE ") {
            "BEFORE"
        } else if rest.to_uppercase().starts_with("AFTER ") {
            "AFTER"
        } else {
            return Err("CREATE TRIGGER requires BEFORE or AFTER".into());
        };
        let after_timing = rest[6..].trim();
        let event = if after_timing.to_uppercase().starts_with("INSERT ") {
            "INSERT"
        } else if after_timing.to_uppercase().starts_with("UPDATE ") {
            "UPDATE"
        } else if after_timing.to_uppercase().starts_with("DELETE ") {
            "DELETE"
        } else {
            return Err("CREATE TRIGGER requires INSERT, UPDATE, or DELETE".into());
        };
        let after_event = &after_timing[event.len()..];
        let on_idx = find_ci(after_event, " ON ").ok_or("CREATE TRIGGER requires ON")?;
        let table_part = after_event[on_idx + 4..].trim();
        let exec_idx = find_ci(table_part, " EXECUTE ").ok_or("CREATE TRIGGER requires EXECUTE")?;
        let table = table_part[..exec_idx].trim().trim_matches('"').to_string();
        let body =
            extract_dollar_quoted_body(trimmed).ok_or("CREATE TRIGGER requires $$...$$ body")?;
        let def = TriggerDef {
            name: name.clone(),
            table,
            event: event.to_string(),
            timing: timing.to_string(),
            body,
        };
        self.catalog.triggers.insert(name.clone(), def);
        self.catalog.save().await?;
        Ok(QueryResult::Message(format!("Trigger '{name}' created.")))
    }

    async fn handle_drop_trigger(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("DROP TRIGGER ") {
            return Err("Expected DROP TRIGGER".into());
        }
        let name = trimmed["DROP TRIGGER ".len()..]
            .split_whitespace()
            .next()
            .ok_or("DROP TRIGGER requires trigger name")?
            .trim_matches('"')
            .to_string();
        self.catalog.triggers.remove(&name);
        self.catalog.save().await?;
        Ok(QueryResult::Message(format!("Trigger '{name}' dropped.")))
    }

    fn handle_call_procedure<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<QueryResult, BoxError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let trimmed = sql.trim().trim_end_matches(';');
            let after = trimmed
                .split_once(char::is_whitespace)
                .map(|(_, rest)| rest.trim())
                .ok_or("CALL requires a procedure name")?;
            let name = after
                .split('(')
                .next()
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .to_string();
            let body = self
                .procedures
                .get(&name)
                .cloned()
                .ok_or_else(|| format!("Procedure '{name}' does not exist"))?;
            for stmt in split_sql_statements(&body) {
                let _ = self.execute(&stmt).await?;
            }
            Ok(QueryResult::Message(format!("CALL '{name}' completed.")))
        })
    }

    fn handle_do_block<'a>(
        &'a mut self,
        sql: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<QueryResult, BoxError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let body = extract_dollar_quoted_body(sql).ok_or("DO requires $$...$$ body")?;
            let mut vars: HashMap<String, String> = HashMap::new();

            // Parse DECLARE/BEGIN/END structure
            let (declare_section, exec_section) = if let Some(begin_idx) = find_ci(&body, "BEGIN") {
                let declare = body[..begin_idx].trim();
                let mut exec = body[begin_idx + 5..].trim();
                // Strip trailing END if present (END; or END $$)
                if let Some(end_idx) = exec.to_uppercase().rfind("END") {
                    exec = exec[..end_idx].trim();
                }
                (declare, exec)
            } else {
                ("", body.as_str())
            };

            // Parse DECLARE section: var_name TYPE := value; or var_name TYPE;
            if !declare_section.is_empty() {
                let declare_body = if declare_section.to_uppercase().starts_with("DECLARE") {
                    declare_section["DECLARE".len()..].trim()
                } else {
                    declare_section
                };
                for line in declare_body.split(';') {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    let parts: Vec<&str> = line.splitn(2, ":=").collect();
                    let name = parts[0]
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .to_lowercase();
                    let val = if parts.len() > 1 {
                        parts[1].trim().to_string()
                    } else {
                        "NULL".into()
                    };
                    if !name.is_empty() {
                        vars.insert(name, val);
                    }
                }
            }

            // Execute statements with variable substitution.
            // Process vars by descending name length so "myvar" is replaced before "var".
            let mut var_order: Vec<_> = vars.keys().collect();
            var_order.sort_by_key(|b| std::cmp::Reverse(b.len()));

            for stmt in split_sql_statements(exec_section) {
                let mut resolved = stmt.clone();
                for k in &var_order {
                    let v = &vars[*k];
                    resolved = resolved.replace(&format!("${k}"), v);
                    // Replace whole-word variable references (standalone identifier)
                    resolved = substitute_plpgsql_var(&resolved, k, v);
                }
                let upper = resolved.trim().to_uppercase();
                if upper.starts_with("RAISE ") {
                    // RAISE NOTICE 'msg' - treat as no-op (or could collect for messages)
                    continue;
                }
                let _ = self.execute(&resolved).await?;
            }
            Ok(QueryResult::Message("DO block completed.".into()))
        })
    }

    async fn handle_create_fulltext_index(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let trimmed = sql.trim().trim_end_matches(';');
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("CREATE FULLTEXT INDEX ") {
            return Err("Expected CREATE FULLTEXT INDEX".into());
        }
        let after = trimmed["CREATE FULLTEXT INDEX ".len()..].trim();
        let on_idx = find_ci(after, " ON ").ok_or("CREATE FULLTEXT INDEX requires ON")?;
        let idx_name = after[..on_idx].trim().trim_matches('"').to_string();
        let on_rest = after[on_idx + " ON ".len()..].trim();
        let open = on_rest
            .find('(')
            .ok_or("FULLTEXT INDEX requires column list")?;
        let close = on_rest
            .rfind(')')
            .ok_or("FULLTEXT INDEX requires column list")?;
        let table_name = on_rest[..open].trim().trim_matches('"').to_string();
        let columns = split_top_level_csv(&on_rest[open + 1..close])
            .into_iter()
            .map(|c| c.trim().trim_matches('"').to_string())
            .filter(|c| !c.is_empty())
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Err("FULLTEXT INDEX requires at least one column".into());
        }
        self.wal_append_pending(sql)?;
        self.fulltext_indexes.insert(
            idx_name.clone(),
            FulltextIndexDef {
                table_name: table_name.clone(),
                columns: columns.clone(),
            },
        );

        // Build inverted index from table data
        let cols_sql = columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let select_sql = format!("SELECT {cols_sql} FROM \"{table_name}\"");
        let batches = self.collect_with_plan_cache(&select_sql).await?;
        let mut idx = InvertedIndex::default();
        let mut row_offset = 0;
        for batch in &batches {
            let num_rows = batch.num_rows();
            for row in 0..num_rows {
                let mut text_parts = Vec::new();
                for col_name in &columns {
                    if let Some(col) = batch.column_by_name(col_name) {
                        let s = array_value_to_string(col.as_ref(), row);
                        text_parts.push(s);
                    }
                }
                let text = text_parts.join(" ");
                idx.add_document(&table_name, row_offset + row, &text);
            }
            row_offset += num_rows;
        }
        self.fts_inverted_index.insert(idx_name.clone(), idx);

        Ok(QueryResult::Message(format!(
            "Fulltext index '{idx_name}' created on '{table_name}'."
        )))
    }

    async fn handle_create_user(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let tokens: Vec<&str> = sql
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .collect();
        if tokens.len() < 3 {
            return Err("CREATE USER requires a name".into());
        }
        let name = tokens[2].trim_matches('"').to_string();
        let password = find_ci(sql, "PASSWORD")
            .map(|idx| {
                sql[idx + "PASSWORD".len()..]
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .trim_end_matches(';')
                    .to_string()
            })
            .unwrap_or_default();
        self.catalog.users.insert(
            name.clone(),
            potatodb_catalog::UserDef {
                name: name.clone(),
                password,
            },
        );
        self.catalog.user_roles.entry(name.clone()).or_default();
        self.catalog.save().await?;
        Ok(QueryResult::Message(format!("User '{name}' created.")))
    }

    fn handle_add_replica(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        // ADD REPLICA 'url' or ADD REPLICA "url"
        let rest = sql["ADD REPLICA ".len()..].trim().trim_end_matches(';');
        let url = rest.trim().trim_matches('\'').trim_matches('"').to_string();
        if url.is_empty() {
            return Err("ADD REPLICA requires a URL".into());
        }
        if !self.replicas.contains(&url) {
            self.replicas.push(url.clone());
        }
        Ok(QueryResult::Message(format!("Replica '{url}' added.")))
    }

    fn handle_remove_replica(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let rest = sql["REMOVE REPLICA ".len()..].trim().trim_end_matches(';');
        let url = rest.trim().trim_matches('\'').trim_matches('"').to_string();
        if url.is_empty() {
            return Err("REMOVE REPLICA requires a URL".into());
        }
        if self.replicas.iter().any(|r| r == &url) {
            self.replicas.retain(|r| r != &url);
        }
        Ok(QueryResult::Message(format!("Replica '{url}' removed.")))
    }

    async fn handle_create_migration(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        // CREATE MIGRATION <version> <description> AS $$ sql $$
        let rest = sql["CREATE MIGRATION ".len()..]
            .trim()
            .trim_end_matches(';');
        let mut tokens = rest.splitn(2, |c: char| c.is_ascii_whitespace());
        let version_str = tokens
            .next()
            .ok_or("CREATE MIGRATION requires a version number")?;
        let version: u64 = version_str
            .parse()
            .map_err(|_| "CREATE MIGRATION version must be a non-negative integer")?;
        let after_version = tokens
            .next()
            .ok_or("CREATE MIGRATION requires description and AS $$ sql $$")?;
        let as_delim = "AS $$";
        let upper_rest = after_version.to_uppercase();
        let as_idx = upper_rest
            .find(as_delim)
            .ok_or("CREATE MIGRATION requires AS $$ ... $$")?;
        let description = after_version[..as_idx].trim().to_string();
        let sql_body = after_version[as_idx + as_delim.len()..]
            .trim_end()
            .strip_suffix("$$")
            .ok_or("CREATE MIGRATION requires closing $$")?
            .trim()
            .to_string();
        if self.catalog.migrations.iter().any(|m| m.version == version) {
            return Err(format!("Migration version {version} already exists").into());
        }
        let record = MigrationRecord {
            version,
            description: description.clone(),
            sql: sql_body.clone(),
            applied_at_ms: 0, // Not applied yet
        };
        self.catalog.migrations.push(record);
        self.catalog.migrations.sort_by_key(|m| m.version);
        self.catalog.save().await?;
        Ok(QueryResult::Message(format!(
            "Migration {version} '{description}' registered."
        )))
    }

    async fn handle_migrate(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        // MIGRATE or MIGRATE TO <version>
        let rest = sql["MIGRATE".len()..].trim().trim_end_matches(';');
        let target_version = if rest.to_uppercase().starts_with("TO ") {
            let v_str = rest["TO ".len()..].trim();
            Some(
                v_str
                    .parse::<u64>()
                    .map_err(|_| "MIGRATE TO requires a valid version number")?,
            )
        } else if rest.is_empty() {
            None
        } else {
            return Err("MIGRATE expects optional TO <version>".into());
        };
        let current = self.catalog.schema_version;
        let pending: Vec<_> = self
            .catalog
            .migrations
            .iter()
            .filter(|m| m.version > current)
            .cloned()
            .collect();
        let to_run: Vec<_> = if let Some(target) = target_version {
            pending
                .into_iter()
                .filter(|m| m.version <= target)
                .collect()
        } else {
            pending
        };
        let mut applied = 0;
        for m in to_run {
            self.execute(&m.sql).await?;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            if let Some(rec) = self
                .catalog
                .migrations
                .iter_mut()
                .find(|r| r.version == m.version)
            {
                rec.applied_at_ms = now_ms;
            }
            self.catalog.schema_version = m.version;
            applied += 1;
        }
        self.catalog.save().await?;
        Ok(QueryResult::Message(format!(
            "Applied {applied} migration(s). Schema version: {}.",
            self.catalog.schema_version
        )))
    }

    async fn handle_create_role(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let tokens: Vec<&str> = sql
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .collect();
        if tokens.len() < 3 {
            return Err("CREATE ROLE requires a name".into());
        }
        let role = tokens[2].trim_matches('"').to_string();
        self.catalog
            .roles
            .entry(role.clone())
            .or_insert_with(|| potatodb_catalog::RoleDef {
                name: role.clone(),
                privileges: Vec::new(),
            });
        self.catalog.save().await?;
        Ok(QueryResult::Message(format!("Role '{role}' created.")))
    }

    async fn handle_grant(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let tokens: Vec<&str> = sql
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .collect();
        if tokens.len() < 6 {
            return Err("GRANT syntax: GRANT <PRIV> ON <TABLE> TO <ROLE|USER>".into());
        }
        let privilege = tokens[1].to_uppercase();
        let table_name = tokens[3].trim_matches('"').to_string();
        let target = tokens
            .last()
            .copied()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        let priv_entry = potatodb_catalog::Privilege {
            kind: privilege.clone(),
            table: Some(table_name),
        };
        if self.catalog.users.contains_key(&target) {
            let role_name = format!("__user_{target}");
            let role_def = self
                .catalog
                .roles
                .entry(role_name.clone())
                .or_insert_with(|| potatodb_catalog::RoleDef {
                    name: role_name.clone(),
                    privileges: Vec::new(),
                });
            if !role_def.privileges.contains(&priv_entry) {
                role_def.privileges.push(priv_entry);
            }
            let user_role_list = self.catalog.user_roles.entry(target.clone()).or_default();
            if !user_role_list.contains(&role_name) {
                user_role_list.push(role_name);
            }
        } else {
            let role_def = self.catalog.roles.entry(target.clone()).or_insert_with(|| {
                potatodb_catalog::RoleDef {
                    name: target.clone(),
                    privileges: Vec::new(),
                }
            });
            if !role_def.privileges.contains(&priv_entry) {
                role_def.privileges.push(priv_entry);
            }
        }
        self.catalog.save().await?;
        Ok(QueryResult::Message(format!(
            "Granted '{privilege}' to '{target}'."
        )))
    }

    async fn handle_revoke(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let tokens: Vec<&str> = sql
            .trim()
            .trim_end_matches(';')
            .split_whitespace()
            .collect();
        if tokens.len() < 6 {
            return Err("REVOKE syntax: REVOKE <PRIV> ON <TABLE> FROM <ROLE|USER>".into());
        }
        let privilege = tokens[1].to_uppercase();
        let table_name = tokens[3].trim_matches('"').to_string();
        let target = tokens
            .last()
            .copied()
            .unwrap_or_default()
            .trim_matches('"')
            .to_string();
        if self.catalog.users.contains_key(&target) {
            let role_name = format!("__user_{target}");
            if let Some(role_def) = self.catalog.roles.get_mut(&role_name) {
                role_def
                    .privileges
                    .retain(|p| !(p.kind == privilege && p.table.as_deref() == Some(&table_name)));
            }
        } else if let Some(role_def) = self.catalog.roles.get_mut(&target) {
            role_def
                .privileges
                .retain(|p| !(p.kind == privilege && p.table.as_deref() == Some(&table_name)));
        }
        self.catalog.save().await?;
        Ok(QueryResult::Message(format!(
            "Revoked '{privilege}' from '{target}'."
        )))
    }

    // ── DDL handlers ───────────────────────────────────────────

    /// Handles `CREATE TABLE` (including `CREATE TABLE ... AS SELECT`).
    async fn handle_create_table(
        &mut self,
        create: &sqlparser::ast::CreateTable,
    ) -> Result<QueryResult, BoxError> {
        let table_name = create.name.to_string();

        if self.catalog.tables.contains_key(&table_name) {
            if create.if_not_exists {
                return Ok(QueryResult::Message(format!(
                    "Table '{table_name}' already exists, skipping."
                )));
            }
            return Err(format!("Table '{table_name}' already exists").into());
        }

        self.wal_append_pending(&create.to_string())?;

        let partition_columns: Vec<String> = create
            .partition_by
            .as_ref()
            .map(|pb| {
                let s = pb.to_string();
                let inner = if let (Some(open), Some(close)) = (s.find('('), s.rfind(')')) {
                    if close > open {
                        &s[open + 1..close]
                    } else {
                        s.as_str()
                    }
                } else {
                    s.as_str()
                };
                inner
                    .trim_matches(|c: char| c == '(' || c == ')' || c.is_whitespace())
                    .split(',')
                    .map(|p| p.trim().trim_matches('"').to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        if let Some(ref query) = create.query {
            return self
                .handle_ctas(&table_name, query, create.if_not_exists, &partition_columns)
                .await;
        }

        let columns: Vec<ColumnDef> = create.columns.iter().map(sql_column_to_catalog).collect();
        let constraints = sql_constraints_to_catalog(&create.constraints);

        if columns.is_empty() {
            return Err("CREATE TABLE requires at least one column".into());
        }

        let table_url_str = self.table_url(&table_name);

        if !self.is_s3 {
            std::fs::create_dir_all(&table_url_str)?;
        }

        let schema = columns_to_schema(&columns)?;
        self.register_listing_table(&table_name, schema, &table_url_str, &partition_columns)
            .await?;

        let meta = TableMeta {
            name: table_name.clone(),
            columns,
            path: table_url_str,
            partition_columns,
            statistics: None,
            retention_seconds: None,
            constraints,
            file_stats: Vec::new(),
            deletion_vectors: Vec::new(),
        };
        self.catalog.add_table(meta).await?;
        self.refresh_table_file_stats(&table_name).await?;

        Ok(QueryResult::Message(format!(
            "Table '{table_name}' created."
        )))
    }

    /// Handles `CREATE TABLE ... AS SELECT`.
    async fn handle_ctas(
        &mut self,
        table_name: &str,
        query: &sqlparser::ast::Query,
        _if_not_exists: bool,
        partition_columns: &[String],
    ) -> Result<QueryResult, BoxError> {
        let select_sql = query.to_string();
        let df = self.ctx.sql(&select_sql).await?;
        let arrow_schema = df.schema().as_arrow().clone();
        let batches = df.collect().await?;

        let columns: Vec<ColumnDef> = arrow_schema
            .fields()
            .iter()
            .map(|f| ColumnDef {
                name: f.name().clone(),
                data_type: arrow_type_to_sql_string(f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();

        let table_url_str = self.table_url(table_name);
        if !self.is_s3 {
            std::fs::create_dir_all(&table_url_str)?;
        }

        let schema = columns_to_schema(&columns)?;
        self.register_listing_table(
            table_name,
            schema.clone(),
            &table_url_str,
            partition_columns,
        )
        .await?;

        let has_data = !batches.is_empty() && batches.iter().any(|b| b.num_rows() > 0);
        if has_data {
            let mem_schema = Arc::new(arrow_schema);
            let mem = MemTable::try_new(mem_schema, vec![batches])?;
            self.ctx
                .register_table("__potato_ctas_tmp", Arc::new(mem))?;
            self.ctx
                .sql(&format!(
                    "INSERT INTO \"{table_name}\" SELECT * FROM __potato_ctas_tmp"
                ))
                .await?
                .collect()
                .await?;
            self.ctx.deregister_table("__potato_ctas_tmp")?;
        }

        let meta = TableMeta {
            name: table_name.to_string(),
            columns,
            path: table_url_str,
            partition_columns: partition_columns.to_vec(),
            statistics: None,
            retention_seconds: None,
            constraints: Vec::new(),
            file_stats: Vec::new(),
            deletion_vectors: Vec::new(),
        };
        self.catalog.add_table(meta).await?;
        self.refresh_table_file_stats(table_name).await?;

        Ok(QueryResult::Message(format!(
            "Table '{table_name}' created."
        )))
    }

    /// Handles `CREATE INDEX`.
    ///
    /// Rejected inside an explicit transaction because it destructively
    /// rewrites Parquet files.
    async fn handle_create_index(
        &mut self,
        create: &sqlparser::ast::CreateIndex,
    ) -> Result<QueryResult, BoxError> {
        let index_name = create
            .name
            .as_ref()
            .map(std::string::ToString::to_string)
            .ok_or("Index name is required")?;
        let table_name = create.table_name.to_string();

        if self.catalog.indexes.contains_key(&index_name) {
            if create.if_not_exists {
                return Ok(QueryResult::Message(format!(
                    "Index '{index_name}' already exists, skipping."
                )));
            }
            return Err(format!("Index '{index_name}' already exists").into());
        }

        let table_meta = self
            .catalog
            .tables
            .get(&table_name)
            .ok_or_else(|| format!("Table '{table_name}' does not exist"))?
            .clone();

        let index_columns: Vec<IndexColumn> = create
            .columns
            .iter()
            .map(|ob| {
                let col_name = ob.column.expr.to_string();
                if !table_meta.columns.iter().any(|c| c.name == col_name) {
                    return Err(
                        format!("Column '{col_name}' does not exist in '{table_name}'").into(),
                    );
                }
                Ok(IndexColumn {
                    name: col_name,
                    ascending: ob.column.options.asc.unwrap_or(true),
                })
            })
            .collect::<Result<Vec<_>, BoxError>>()?;

        if index_columns.is_empty() {
            return Err("Index requires at least one column".into());
        }

        self.wal_append_pending(&create.to_string())?;
        self.backup_table_for_txn(&table_name).await?;

        let order_clause = index_columns
            .iter()
            .map(|c| {
                format!(
                    "\"{}\" {}",
                    c.name,
                    if c.ascending { "ASC" } else { "DESC" }
                )
            })
            .collect::<Vec<_>>()
            .join(", ");

        let sort_sql = format!("SELECT * FROM \"{table_name}\" ORDER BY {order_clause}");
        let df = self.ctx.sql(&sort_sql).await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let sorted_batches = df.collect().await?;
        let has_data =
            !sorted_batches.is_empty() && sorted_batches.iter().any(|b| b.num_rows() > 0);

        self.ctx.deregister_table(&table_name)?;
        self.delete_parquet_files(&table_name).await?;

        for idx in self.catalog.indexes.values_mut() {
            if idx.table_name == table_name {
                idx.primary = false;
                idx.logical_only = true;
            }
        }
        let index_def = IndexDef {
            name: index_name.clone(),
            table_name: table_name.clone(),
            columns: index_columns,
            logical_only: false,
            primary: true,
        };
        self.catalog.add_index(index_def).await?;

        let table_schema = columns_to_schema(&table_meta.columns)?;
        if !self.is_s3 {
            std::fs::create_dir_all(&table_meta.path)?;
        }
        self.register_listing_table(
            &table_name,
            table_schema,
            &table_meta.path,
            &table_meta.partition_columns,
        )
        .await?;

        if has_data {
            let mem = MemTable::try_new(schema, vec![sorted_batches])?;
            self.ctx.register_table("__potato_idx_tmp", Arc::new(mem))?;
            self.ctx
                .sql(&format!(
                    "INSERT INTO \"{table_name}\" SELECT * FROM __potato_idx_tmp"
                ))
                .await?
                .collect()
                .await?;
            self.ctx.deregister_table("__potato_idx_tmp")?;
        }

        let col_list = create
            .columns
            .iter()
            .map(|c| c.column.expr.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        Ok(QueryResult::Message(format!(
            "Index '{index_name}' created on '{table_name}' ({col_list})."
        )))
    }

    /// Handles `DROP TABLE`.
    ///
    /// Inside an explicit transaction the file deletion is deferred
    /// until `COMMIT`; on `ROLLBACK` the table is restored.
    async fn handle_drop_table(
        &mut self,
        name: &str,
        if_exists: bool,
    ) -> Result<QueryResult, BoxError> {
        if !self.catalog.tables.contains_key(name) {
            return if if_exists {
                Ok(QueryResult::Message(format!(
                    "Table '{name}' does not exist, skipping."
                )))
            } else {
                Err(format!("Table '{name}' does not exist").into())
            };
        }

        let drop_sql = if if_exists {
            format!("DROP TABLE IF EXISTS \"{name}\"")
        } else {
            format!("DROP TABLE \"{name}\"")
        };
        self.wal_append_pending(&drop_sql)?;

        let meta = self
            .catalog
            .remove_table(name)
            .await?
            .ok_or_else(|| format!("Table '{name}' does not exist"))?;
        self.ctx.deregister_table(name)?;

        if self.in_transaction() {
            if let Some(txn) = self.active_txn.as_mut() {
                txn.deferred_deletes.push(meta);
            }
        } else {
            self.delete_table_storage(&meta).await?;
        }

        Ok(QueryResult::Message(format!("Table '{name}' dropped.")))
    }

    /// Handles `DROP INDEX`.
    async fn handle_drop_index(
        &mut self,
        name: &str,
        if_exists: bool,
    ) -> Result<QueryResult, BoxError> {
        if !self.catalog.indexes.contains_key(name) {
            return if if_exists {
                Ok(QueryResult::Message(format!(
                    "Index '{name}' does not exist, skipping."
                )))
            } else {
                Err(format!("Index '{name}' does not exist").into())
            };
        }

        let drop_sql = if if_exists {
            format!("DROP INDEX IF EXISTS \"{name}\"")
        } else {
            format!("DROP INDEX \"{name}\"")
        };
        self.wal_append_pending(&drop_sql)?;

        let index_def = self
            .catalog
            .remove_index(name)
            .await?
            .ok_or_else(|| format!("Index '{name}' does not exist"))?;
        let table_name = &index_def.table_name;
        let has_primary = self
            .catalog
            .indexes
            .values()
            .any(|idx| idx.table_name == *table_name && idx.primary);
        if !has_primary {
            if let Some((_, next_idx)) = self
                .catalog
                .indexes
                .iter_mut()
                .find(|(_, idx)| idx.table_name == *table_name)
            {
                next_idx.primary = true;
                next_idx.logical_only = false;
            }
        }
        if let Some(meta) = self.catalog.tables.get(table_name).cloned() {
            self.ctx.deregister_table(table_name)?;
            let schema = columns_to_schema(&meta.columns)?;
            self.register_listing_table(table_name, schema, &meta.path, &meta.partition_columns)
                .await?;
        }

        Ok(QueryResult::Message(format!("Index '{name}' dropped.")))
    }

    async fn handle_create_sequence(
        &mut self,
        name: &str,
        if_not_exists: bool,
        options: &[SequenceOptions],
    ) -> Result<QueryResult, BoxError> {
        if self.catalog.sequences.contains_key(name) {
            return if if_not_exists {
                Ok(QueryResult::Message(format!(
                    "Sequence '{name}' already exists, skipping."
                )))
            } else {
                Err(format!("Sequence '{name}' already exists").into())
            };
        }

        self.wal_append_pending(&format!("CREATE SEQUENCE \"{name}\""))?;

        let mut start: i64 = 1;
        let mut increment: i64 = 1;
        let mut min_value: Option<i64> = None;
        let mut max_value: Option<i64> = None;

        for opt in options {
            match opt {
                SequenceOptions::StartWith(expr, _) => {
                    start = expr_to_i64(expr)?;
                }
                SequenceOptions::IncrementBy(expr, _) => {
                    increment = expr_to_i64(expr)?;
                }
                SequenceOptions::MinValue(Some(expr)) => {
                    min_value = Some(expr_to_i64(expr)?);
                }
                SequenceOptions::MaxValue(Some(expr)) => {
                    max_value = Some(expr_to_i64(expr)?);
                }
                _ => {}
            }
        }

        self.catalog
            .add_sequence(SequenceDef {
                name: name.to_string(),
                current_value: start,
                increment,
                min_value,
                max_value,
            })
            .await?;

        Ok(QueryResult::Message(format!("Sequence '{name}' created.")))
    }

    async fn handle_drop_sequence(
        &mut self,
        name: &str,
        if_exists: bool,
    ) -> Result<QueryResult, BoxError> {
        if !self.catalog.sequences.contains_key(name) {
            return if if_exists {
                Ok(QueryResult::Message(format!(
                    "Sequence '{name}' does not exist, skipping."
                )))
            } else {
                Err(format!("Sequence '{name}' does not exist").into())
            };
        }

        self.wal_append_pending(&format!("DROP SEQUENCE \"{name}\""))?;
        self.catalog.remove_sequence(name).await?;
        Ok(QueryResult::Message(format!("Sequence '{name}' dropped.")))
    }

    /// Handles lightweight SQL macro functions:
    /// `CREATE FUNCTION name(arg TYPE, ...) RETURNS TYPE AS 'expr'`.
    async fn handle_create_function(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let def = parse_create_function(sql)?;
        self.wal_append_pending(sql)?;
        self.catalog.add_udf(def.clone()).await?;
        Ok(QueryResult::Message(format!(
            "Function '{}' created.",
            def.name
        )))
    }

    /// Handles `DROP FUNCTION [IF EXISTS] name`.
    async fn handle_drop_function(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let (if_exists, name) = parse_drop_function(sql)?;
        if !self.catalog.udfs.contains_key(&name) {
            return if if_exists {
                Ok(QueryResult::Message(format!(
                    "Function '{name}' does not exist, skipping."
                )))
            } else {
                Err(format!("Function '{name}' does not exist").into())
            };
        }
        self.wal_append_pending(sql)?;
        self.catalog.remove_udf(&name).await?;
        Ok(QueryResult::Message(format!("Function '{name}' dropped.")))
    }

    /// Handles `TRUNCATE TABLE`.
    async fn handle_truncate(&mut self, table_name: &str) -> Result<QueryResult, BoxError> {
        let meta = self
            .catalog
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{table_name}' does not exist"))?
            .clone();

        self.wal_append_pending(&format!("TRUNCATE TABLE \"{table_name}\""))?;

        self.ctx.deregister_table(table_name)?;
        self.delete_parquet_files(table_name).await?;

        let schema = columns_to_schema(&meta.columns)?;
        self.register_listing_table(table_name, schema, &meta.path, &meta.partition_columns)
            .await?;
        self.record_cdc_event(table_name, "TRUNCATE", 0);

        Ok(QueryResult::Message(format!(
            "Table '{table_name}' truncated."
        )))
    }

    // ── DML handlers ───────────────────────────────────────────

    /// Handles `DELETE FROM table WHERE condition`.
    ///
    /// Uses copy-on-write: selects surviving rows, rewrites the table.
    /// Not allowed inside an explicit transaction.
    async fn handle_delete(
        &mut self,
        sql: &str,
        delete: &sqlparser::ast::Delete,
    ) -> Result<QueryResult, BoxError> {
        let table_name = extract_delete_table_name(delete)?;
        let before_triggers: Vec<String> = self
            .catalog
            .triggers
            .values()
            .filter(|t| t.table == table_name && t.event == "DELETE" && t.timing == "BEFORE")
            .map(|t| t.body.clone())
            .collect();
        for body in before_triggers {
            let _ = self.execute(&body).await;
        }
        let returning_clause = extract_returning_clause(sql);

        if !self.catalog.tables.contains_key(&table_name) {
            return Err(format!("Table '{table_name}' does not exist").into());
        }

        self.wal_append_pending(&delete.to_string())?;
        self.backup_table_for_txn(&table_name).await?;

        let delete_filter = delete
            .selection
            .as_ref()
            .map_or_else(|| "TRUE".to_string(), std::string::ToString::to_string);
        let fk_actions: Vec<(String, String, String, String)> = self
            .catalog
            .tables
            .iter()
            .flat_map(|(child_table, meta)| {
                meta.constraints.iter().filter_map(|c| {
                    if let CatalogTableConstraint::ForeignKey {
                        columns,
                        ref_table,
                        ref_columns,
                        on_delete,
                        ..
                    } = c
                    {
                        if ref_table == &table_name && columns.len() == 1 && ref_columns.len() == 1
                        {
                            return Some((
                                child_table.clone(),
                                columns[0].clone(),
                                ref_columns[0].clone(),
                                on_delete.clone().unwrap_or_else(|| "RESTRICT".to_string()),
                            ));
                        }
                    }
                    None
                })
            })
            .collect();
        for (child_table, fk_col, ref_col, on_delete) in fk_actions {
            let key_sql = format!(
                "SELECT DISTINCT \"{ref_col}\" FROM \"{table_name}\" WHERE ({delete_filter}) AND \"{ref_col}\" IS NOT NULL"
            );
            let key_df = self.ctx.sql(&key_sql).await?;
            let key_batches = key_df.collect().await?;
            let mut key_values: Vec<String> = Vec::new();
            for batch in &key_batches {
                if batch.num_columns() == 0 {
                    continue;
                }
                for row in 0..batch.num_rows() {
                    key_values.push(array_value_to_sql_literal(batch.column(0).as_ref(), row));
                }
            }
            if key_values.is_empty() {
                continue;
            }
            let key_list = key_values.join(", ");
            let ref_sql = format!(
                "SELECT COUNT(*) AS c FROM \"{child_table}\" WHERE \"{fk_col}\" IN ({key_list})"
            );
            let ref_df = self.ctx.sql(&ref_sql).await?;
            let ref_batches = ref_df.collect().await?;
            let refs = scalar_count(&ref_batches);
            if refs <= 0 {
                continue;
            }
            match on_delete.to_uppercase().as_str() {
                "CASCADE" => {
                    let keep_sql = format!(
                        "SELECT * FROM \"{child_table}\" WHERE NOT (\"{fk_col}\" IN ({key_list}))"
                    );
                    let df = self.ctx.sql(&keep_sql).await?;
                    let schema = Arc::new(df.schema().as_arrow().clone());
                    let batches = df.collect().await?;
                    self.rewrite_table(&child_table, schema, batches).await?;
                }
                "SET NULL" => {
                    let child_meta = self
                        .catalog
                        .tables
                        .get(&child_table)
                        .ok_or_else(|| format!("Table '{child_table}' does not exist"))?
                        .clone();
                    let projections = child_meta
                        .columns
                        .iter()
                        .map(|c| {
                            if c.name == fk_col {
                                format!(
                                    "CASE WHEN \"{fk_col}\" IN ({key_list}) THEN NULL ELSE \"{fk_col}\" END AS \"{fk_col}\""
                                )
                            } else {
                                format!("\"{}\"", c.name)
                            }
                        })
                        .collect::<Vec<_>>();
                    let rewrite_sql =
                        format!("SELECT {} FROM \"{child_table}\"", projections.join(", "));
                    let df = self.ctx.sql(&rewrite_sql).await?;
                    let schema = Arc::new(df.schema().as_arrow().clone());
                    let batches = df.collect().await?;
                    self.rewrite_table(&child_table, schema, batches).await?;
                }
                _ => {
                    return Err(format!(
                        "FOREIGN KEY RESTRICT violation on '{table_name}' referenced by '{child_table}'"
                    )
                    .into());
                }
            }
        }

        let count_sql = format!("SELECT COUNT(*) AS c FROM \"{table_name}\"");
        let count_df = self.ctx.sql(&count_sql).await?;
        let count_batches = count_df.collect().await?;
        let old_count: i64 = count_batches
            .iter()
            .filter_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| (0..a.len()).map(move |i| a.value(i)))
            })
            .flatten()
            .sum();

        let where_clause = match &delete.selection {
            Some(expr) => format!("WHERE NOT ({expr})"),
            None => "WHERE FALSE".to_string(),
        };
        let select_sql = format!("SELECT * FROM \"{table_name}\" {where_clause}");
        let returning_batches = if let Some(returning) = &returning_clause {
            let returning_where = match &delete.selection {
                Some(expr) => format!("WHERE {expr}"),
                None => String::new(),
            };
            let returning_sql =
                format!("SELECT {returning} FROM \"{table_name}\" {returning_where}");
            Some(self.ctx.sql(&returning_sql).await?.collect().await?)
        } else {
            None
        };

        let df = self.ctx.sql(&select_sql).await?;
        let logical_schema = Arc::new(df.schema().as_arrow().clone());
        let surviving = df.collect().await?;
        let schema = if surviving.is_empty() {
            logical_schema
        } else {
            surviving[0].schema()
        };
        let new_count: usize = surviving
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        let deleted = old_count as usize - new_count;

        self.rewrite_table(&table_name, schema, surviving).await?;
        if let Some(meta) = self.catalog.tables.get_mut(&table_name) {
            meta.deletion_vectors.clear();
        }
        self.record_cdc_event(&table_name, "DELETE", deleted);

        let after_triggers: Vec<String> = self
            .catalog
            .triggers
            .values()
            .filter(|t| t.table == table_name && t.event == "DELETE" && t.timing == "AFTER")
            .map(|t| t.body.clone())
            .collect();
        for body in after_triggers {
            let _ = self.execute(&body).await;
        }

        if let Some(batches) = returning_batches {
            Ok(QueryResult::Records(batches))
        } else {
            Ok(QueryResult::Message(format!("{deleted} row(s) deleted.")))
        }
    }

    /// Handles `UPDATE table SET col=val WHERE condition`.
    ///
    /// Constructs a SELECT with CASE expressions for modified columns,
    /// then rewrites the table. Not allowed inside an explicit transaction.
    async fn handle_update(
        &mut self,
        sql: &str,
        table: &sqlparser::ast::TableWithJoins,
        assignments: &[sqlparser::ast::Assignment],
        selection: Option<&sqlparser::ast::Expr>,
    ) -> Result<QueryResult, BoxError> {
        let table_name = table.relation.to_string();
        let before_triggers: Vec<String> = self
            .catalog
            .triggers
            .values()
            .filter(|t| t.table == table_name && t.event == "UPDATE" && t.timing == "BEFORE")
            .map(|t| t.body.clone())
            .collect();
        for body in before_triggers {
            let _ = self.execute(&body).await;
        }
        let table_meta = self
            .catalog
            .tables
            .get(&table_name)
            .ok_or_else(|| format!("Table '{table_name}' does not exist"))?
            .clone();
        let returning_clause = extract_returning_clause(sql);

        self.wal_append_pending(sql)?;
        self.backup_table_for_txn(&table_name).await?;

        let where_expr =
            selection.map_or_else(|| "TRUE".to_string(), std::string::ToString::to_string);

        let count_sql = format!("SELECT COUNT(*) AS c FROM \"{table_name}\" WHERE {where_expr}");
        let count_df = self.ctx.sql(&count_sql).await?;
        let count_batches = count_df.collect().await?;
        let updated: i64 = count_batches
            .iter()
            .filter_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| (0..a.len()).map(move |i| a.value(i)))
            })
            .flatten()
            .sum();

        let projections: Vec<String> = table_meta
            .columns
            .iter()
            .map(|c| {
                let target_name = &c.name;
                if let Some(assignment) = assignments.iter().find(|a| {
                    let t = match &a.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                        sqlparser::ast::AssignmentTarget::Tuple(names) => names
                            .last()
                            .map(std::string::ToString::to_string)
                            .unwrap_or_default(),
                    };
                    t.trim_matches('"') == target_name
                }) {
                    format!(
                        "CASE WHEN ({where_expr}) THEN CAST(({val}) AS {ty}) ELSE \"{col}\" END AS \"{col}\"",
                        val = assignment.value,
                        ty = c.data_type,
                        col = target_name
                    )
                } else {
                    format!("\"{target_name}\"")
                }
            })
            .collect();

        let rewrite_sql = format!("SELECT {} FROM \"{table_name}\"", projections.join(", "));

        let df = self.ctx.sql(&rewrite_sql).await?;
        let logical_schema = Arc::new(df.schema().as_arrow().clone());
        let modified = df.collect().await?;
        let schema = if modified.is_empty() {
            logical_schema
        } else {
            modified[0].schema()
        };

        self.rewrite_table(&table_name, schema, modified).await?;
        if let Some(meta) = self.catalog.tables.get_mut(&table_name) {
            meta.deletion_vectors.clear();
        }
        self.validate_table_constraints(&table_name).await?;
        self.validate_check_constraints(&table_name).await?;
        self.record_cdc_event(&table_name, "UPDATE", updated as usize);

        if let Some(returning) = returning_clause {
            let returning_sql =
                format!("SELECT {returning} FROM \"{table_name}\" WHERE {where_expr}");
            let batches = self.ctx.sql(&returning_sql).await?.collect().await?;
            Ok(QueryResult::Records(batches))
        } else {
            Ok(QueryResult::Message(format!("{updated} row(s) updated.")))
        }
    }

    /// Handles `MERGE INTO target USING source ON condition WHEN MATCHED/NOT MATCHED ...`.
    ///
    /// Materialises the USING source into a temporary `__merge_src` table,
    /// then directly computes the merged result using `ctx.sql()` + LEFT JOIN
    /// (bypassing `execute()` so the temp table stays visible to `DataFusion`).
    async fn handle_merge(
        &mut self,
        table: &TableFactor,
        source: &TableFactor,
        on: &sqlparser::ast::Expr,
        clauses: &[sqlparser::ast::MergeClause],
    ) -> Result<QueryResult, BoxError> {
        let (target_name, target_alias) = match table {
            TableFactor::Table { name, alias, .. } => {
                let n = name.to_string().trim_matches('"').to_string();
                let a = alias.as_ref().map(|a| a.name.value.clone());
                (n, a)
            }
            _ => return Err("MERGE target must be a table".into()),
        };

        let table_meta = self
            .catalog
            .tables
            .get(&target_name)
            .ok_or_else(|| format!("Table '{target_name}' does not exist"))?
            .clone();

        let _ = self.flush_all().await?;

        let (source_sql, source_alias, source_table_name) = match source {
            TableFactor::Derived {
                subquery, alias, ..
            } => (
                subquery.to_string(),
                alias.as_ref().map(|a| a.name.value.clone()),
                None,
            ),
            TableFactor::Table { name, alias, .. } => {
                let sn = name.to_string().trim_matches('"').to_string();
                (
                    format!("SELECT * FROM {name}"),
                    alias.as_ref().map(|a| a.name.value.clone()),
                    Some(sn),
                )
            }
            _ => return Err("Unsupported MERGE source type".into()),
        };

        let rewrite = |s: &str| -> String {
            let mut r = s.to_string();
            if let Some(ref a) = target_alias {
                r = r.replace(&format!("{a}."), &format!("\"{target_name}\"."));
            }
            if let Some(ref a) = source_alias {
                r = r.replace(&format!("{a}."), "\"__merge_src\".");
            } else if let Some(ref sn) = source_table_name {
                r = r.replace(&format!("{sn}."), "\"__merge_src\".");
            }
            r
        };

        let src_df = self.ctx.sql(&source_sql).await?;
        let src_batches = src_df.collect().await?;
        if src_batches.is_empty() || src_batches.iter().all(|b| b.num_rows() == 0) {
            return Ok(QueryResult::Message(
                "0 row(s) updated, 0 row(s) inserted.".into(),
            ));
        }
        let src_schema = src_batches[0].schema();
        let mem_table = MemTable::try_new(src_schema, vec![src_batches])?;
        self.ctx
            .register_table("__merge_src", Arc::new(mem_table))?;

        let on_str = rewrite(&on.to_string());

        let mut total_updated = 0_i64;
        let mut total_inserted = 0_i64;

        let inner: Result<(), BoxError> = async {
            for clause in clauses {
                let predicate_str = clause
                    .predicate
                    .as_ref()
                    .map(|p| format!(" AND ({})", rewrite(&p.to_string())))
                    .unwrap_or_default();

                match (&clause.clause_kind, &clause.action) {
                    (MergeClauseKind::Matched, MergeAction::Update { assignments }) => {
                        let mut update_map: HashMap<String, String> = HashMap::new();
                        for a in assignments {
                            let col = a.target.to_string().trim_matches('"').to_string();
                            let val = rewrite(&a.value.to_string());
                            update_map.insert(col, val);
                        }

                        let projections: Vec<String> = table_meta
                            .columns
                            .iter()
                            .map(|c| {
                                if let Some(val) = update_map.get(&c.name) {
                                    format!(
                                        "CASE WHEN \"__merge_src_match\" IS NOT NULL \
                                         THEN CAST({val} AS {}) \
                                         ELSE \"{target_name}\".\"{col}\" \
                                         END AS \"{col}\"",
                                        c.data_type,
                                        col = c.name
                                    )
                                } else {
                                    format!("\"{target_name}\".\"{}\"", c.name)
                                }
                            })
                            .collect();

                        let count_sql = format!(
                            "SELECT COUNT(*) AS c \
                             FROM \"{target_name}\" \
                             INNER JOIN \"__merge_src\" ON ({on_str}){predicate_str}"
                        );
                        let count_batches = self.ctx.sql(&count_sql).await?.collect().await?;
                        total_updated = count_batches
                            .iter()
                            .find_map(|b| {
                                b.column(0)
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .map(|a| a.value(0))
                            })
                            .unwrap_or(0);

                        if total_updated > 0 {
                            let rewrite_sql = format!(
                                "SELECT {proj} \
                                 FROM \"{target_name}\" \
                                 LEFT JOIN (\
                                     SELECT *, TRUE AS \"__merge_src_match\" \
                                     FROM \"__merge_src\"\
                                 ) AS \"__merge_src\" ON ({on_str}){predicate_str}",
                                proj = projections.join(", ")
                            );
                            let df = self.ctx.sql(&rewrite_sql).await?;
                            let logical_schema = Arc::new(df.schema().as_arrow().clone());
                            let batches = df.collect().await?;
                            let schema = if batches.is_empty() {
                                logical_schema
                            } else {
                                batches[0].schema()
                            };
                            self.rewrite_table(&target_name, schema, batches).await?;
                        }
                    }
                    (MergeClauseKind::Matched, MergeAction::Delete) => {
                        let keep_sql = format!(
                            "SELECT \"{target_name}\".* \
                             FROM \"{target_name}\" \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM \"__merge_src\" \
                                 WHERE ({on_str}){predicate_str}\
                             )"
                        );
                        let df = self.ctx.sql(&keep_sql).await?;
                        let logical_schema = Arc::new(df.schema().as_arrow().clone());
                        let batches = df.collect().await?;
                        let schema = if batches.is_empty() {
                            logical_schema
                        } else {
                            batches[0].schema()
                        };
                        self.rewrite_table(&target_name, schema, batches).await?;
                    }
                    (
                        MergeClauseKind::NotMatched | MergeClauseKind::NotMatchedByTarget,
                        MergeAction::Insert(insert_expr),
                    ) => {
                        let (cols, select_list) = match &insert_expr.kind {
                            MergeInsertKind::Row => {
                                let cols = if insert_expr.columns.is_empty() {
                                    String::new()
                                } else {
                                    format!(
                                        " ({})",
                                        insert_expr
                                            .columns
                                            .iter()
                                            .map(std::string::ToString::to_string)
                                            .collect::<Vec<_>>()
                                            .join(", ")
                                    )
                                };
                                (cols, "*".to_string())
                            }
                            MergeInsertKind::Values(values) => {
                                let cols = if insert_expr.columns.is_empty() {
                                    String::new()
                                } else {
                                    format!(
                                        " ({})",
                                        insert_expr
                                            .columns
                                            .iter()
                                            .map(std::string::ToString::to_string)
                                            .collect::<Vec<_>>()
                                            .join(", ")
                                    )
                                };
                                let select_list = values.rows.first().map_or_else(
                                    || "*".to_string(),
                                    |row| {
                                        row.iter()
                                            .map(|v| rewrite(&v.to_string()))
                                            .collect::<Vec<_>>()
                                            .join(", ")
                                    },
                                );
                                (cols, select_list)
                            }
                        };
                        let insert_sql = format!(
                            "INSERT INTO \"{target_name}\"{cols} \
                             SELECT {select_list} FROM \"__merge_src\" \
                             WHERE NOT EXISTS (\
                                 SELECT 1 FROM \"{target_name}\" \
                                 WHERE ({on_str})\
                             ){predicate_str}"
                        );
                        let insert_batches = self.ctx.sql(&insert_sql).await?.collect().await?;
                        total_inserted = insert_batches
                            .iter()
                            .find_map(|b| {
                                b.column(0)
                                    .as_any()
                                    .downcast_ref::<arrow::array::UInt64Array>()
                                    .map(|a| a.value(0).cast_signed())
                            })
                            .unwrap_or(0);
                        self.refresh_table_file_stats_light(&target_name).await?;
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        .await;

        let _ = self.ctx.deregister_table("__merge_src");
        inner?;

        self.record_cdc_event(
            &target_name,
            "MERGE",
            (total_updated + total_inserted) as usize,
        );

        Ok(QueryResult::Message(format!(
            "{total_updated} row(s) updated, {total_inserted} row(s) inserted."
        )))
    }

    // ── ALTER TABLE ────────────────────────────────────────────

    /// Handles `ALTER TABLE` with ADD COLUMN, DROP COLUMN, and RENAME COLUMN.
    async fn handle_alter_table(
        &mut self,
        sql: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
        if_exists: bool,
    ) -> Result<QueryResult, BoxError> {
        if !self.catalog.tables.contains_key(table_name) {
            if if_exists {
                return Ok(QueryResult::Message(format!(
                    "Table '{table_name}' does not exist, skipping."
                )));
            }
            return Err(format!("Table '{table_name}' does not exist").into());
        }

        if let Some((old_name, new_name)) = parse_alter_table_rename(sql) {
            return self.handle_rename_table(&old_name, &new_name).await;
        }

        if let Some(retention) = parse_retention_setting(sql) {
            self.wal_append_pending(sql)?;
            let meta = self.catalog.tables.get_mut(table_name).unwrap();
            meta.retention_seconds = retention;
            self.catalog.save().await?;
            return Ok(QueryResult::Message(match retention {
                Some(secs) => {
                    format!("Retention policy set on '{table_name}' to {secs} second(s).")
                }
                None => format!("Retention policy cleared on '{table_name}'."),
            }));
        }

        self.wal_append_pending(sql)?;

        let mut messages = Vec::new();

        for op in operations {
            match op {
                AlterTableOperation::AddColumn {
                    column_def,
                    if_not_exists,
                    ..
                } => {
                    let col = sql_column_to_catalog(column_def);
                    let meta = self.catalog.tables.get_mut(table_name).unwrap();
                    if meta.columns.iter().any(|c| c.name == col.name) {
                        if *if_not_exists {
                            messages
                                .push(format!("Column '{}' already exists, skipping.", col.name));
                            continue;
                        }
                        return Err(format!(
                            "Column '{}' already exists in '{table_name}'",
                            col.name
                        )
                        .into());
                    }
                    messages.push(format!("Column '{}' added.", col.name));
                    meta.columns.push(col);
                }
                AlterTableOperation::DropColumn {
                    column_names,
                    if_exists: col_if_exists,
                    ..
                } => {
                    let col_name = column_names
                        .first()
                        .map(|c| c.value.clone())
                        .unwrap_or_default();
                    let meta = self.catalog.tables.get_mut(table_name).unwrap();
                    if !meta.columns.iter().any(|c| c.name == col_name) {
                        if *col_if_exists {
                            messages.push(format!("Column '{col_name}' does not exist, skipping."));
                            continue;
                        }
                        return Err(format!(
                            "Column '{col_name}' does not exist in '{table_name}'"
                        )
                        .into());
                    }
                    meta.columns.retain(|c| c.name != col_name);
                    messages.push(format!("Column '{col_name}' dropped."));
                }
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let old_name = old_column_name.value.clone();
                    let new_name = new_column_name.value.clone();
                    let meta = self.catalog.tables.get_mut(table_name).unwrap();
                    if let Some(col) = meta.columns.iter_mut().find(|c| c.name == old_name) {
                        col.name.clone_from(&new_name);
                        messages.push(format!("Column '{old_name}' renamed to '{new_name}'."));
                    } else {
                        return Err(format!(
                            "Column '{old_name}' does not exist in '{table_name}'"
                        )
                        .into());
                    }
                }
                other => {
                    return Err(format!("Unsupported ALTER TABLE operation: {other}").into());
                }
            }
        }

        self.catalog.save().await?;

        self.ctx.deregister_table(table_name)?;
        let meta = self.catalog.tables.get(table_name).unwrap();
        let schema = columns_to_schema(&meta.columns)?;
        let path = meta.path.clone();
        let pc = meta.partition_columns.clone();
        self.register_listing_table(table_name, schema, &path, &pc)
            .await?;

        Ok(QueryResult::Message(messages.join(" ")))
    }

    async fn handle_rename_table(
        &mut self,
        old_name: &str,
        new_name: &str,
    ) -> Result<QueryResult, BoxError> {
        if old_name == new_name {
            return Ok(QueryResult::Message(format!(
                "Table '{old_name}' already has that name."
            )));
        }
        if self.catalog.tables.contains_key(new_name) {
            return Err(format!("Table '{new_name}' already exists").into());
        }

        self.wal_append_pending(&format!(
            "ALTER TABLE \"{old_name}\" RENAME TO \"{new_name}\""
        ))?;

        let mut meta = self
            .catalog
            .tables
            .remove(old_name)
            .ok_or_else(|| format!("Table '{old_name}' does not exist"))?;

        meta.name = new_name.to_string();
        self.catalog
            .tables
            .insert(new_name.to_string(), meta.clone());
        for index in self.catalog.indexes.values_mut() {
            if index.table_name == old_name {
                index.table_name = new_name.to_string();
            }
        }
        self.catalog.save().await?;

        let _ = self.ctx.deregister_table(old_name);
        let schema = columns_to_schema(&meta.columns)?;
        self.register_listing_table(new_name, schema, &meta.path, &meta.partition_columns)
            .await?;

        Ok(QueryResult::Message(format!(
            "Table '{old_name}' renamed to '{new_name}'."
        )))
    }

    // ── Views ──────────────────────────────────────────────────

    /// Handles `CREATE VIEW`.
    async fn handle_create_view(
        &mut self,
        view_name: &str,
        query: &sqlparser::ast::Query,
        or_replace: bool,
        materialized: bool,
    ) -> Result<QueryResult, BoxError> {
        if self.catalog.views.contains_key(view_name) && !or_replace {
            return Err(format!("View '{view_name}' already exists").into());
        }

        let sql_text = query.to_string();
        let create_sql = if materialized {
            format!("CREATE MATERIALIZED VIEW \"{view_name}\" AS {sql_text}")
        } else if or_replace {
            format!("CREATE OR REPLACE VIEW \"{view_name}\" AS {sql_text}")
        } else {
            format!("CREATE VIEW \"{view_name}\" AS {sql_text}")
        };
        self.wal_append_pending(&create_sql)?;

        let backing_table = if materialized {
            let backing = format!("__mv_{view_name}");
            if self.catalog.tables.contains_key(&backing) {
                let _ = self.handle_drop_table(&backing, true).await;
            }
            self.handle_ctas(&backing, query, false, &[]).await?;
            self.ctx
                .sql(&format!(
                    "CREATE OR REPLACE VIEW \"{view_name}\" AS SELECT * FROM \"{backing}\""
                ))
                .await?;
            Some(backing)
        } else {
            self.ctx
                .sql(&format!(
                    "CREATE OR REPLACE VIEW \"{view_name}\" AS {sql_text}"
                ))
                .await?;
            None
        };

        self.catalog
            .add_view(ViewDef {
                name: view_name.to_string(),
                sql: sql_text,
                materialized,
                backing_table,
            })
            .await?;

        Ok(QueryResult::Message(format!(
            "{} view '{view_name}' created.",
            if materialized { "Materialized" } else { "View" }
        )))
    }

    /// Handles `DROP VIEW`.
    async fn handle_drop_view(
        &mut self,
        name: &str,
        if_exists: bool,
    ) -> Result<QueryResult, BoxError> {
        if !self.catalog.views.contains_key(name) {
            return if if_exists {
                Ok(QueryResult::Message(format!(
                    "View '{name}' does not exist, skipping."
                )))
            } else {
                Err(format!("View '{name}' does not exist").into())
            };
        }

        let drop_sql = if if_exists {
            format!("DROP VIEW IF EXISTS \"{name}\"")
        } else {
            format!("DROP VIEW \"{name}\"")
        };
        self.wal_append_pending(&drop_sql)?;

        let removed = self.catalog.remove_view(name).await?;
        let _ = self.ctx.deregister_table(name);
        if let Some(view) = removed {
            if view.materialized {
                if let Some(backing) = view.backing_table {
                    let _ = self.handle_drop_table(&backing, true).await;
                }
            }
        }
        Ok(QueryResult::Message(format!("View '{name}' dropped.")))
    }

    async fn handle_refresh_materialized_view(
        &mut self,
        view_name: &str,
    ) -> Result<QueryResult, BoxError> {
        let view = self
            .catalog
            .views
            .get(view_name)
            .cloned()
            .ok_or_else(|| format!("View '{view_name}' does not exist"))?;
        if !view.materialized {
            return Err(format!("View '{view_name}' is not materialized").into());
        }
        let backing = view
            .backing_table
            .clone()
            .ok_or("Materialized view is missing backing table metadata")?;

        self.wal_append_pending(&format!("REFRESH MATERIALIZED VIEW \"{view_name}\""))?;
        let _ = self.handle_drop_table(&backing, true).await;

        let dialect = PostgreSqlDialect {};
        let parsed = Parser::parse_sql(&dialect, &format!("SELECT * FROM ({}) t", view.sql))?;
        let query = match &parsed[0] {
            Statement::Query(q) => q.as_ref().clone(),
            _ => return Err("Stored materialized view SQL is invalid".into()),
        };
        self.handle_ctas(&backing, &query, false, &[]).await?;
        self.ctx
            .sql(&format!(
                "CREATE OR REPLACE VIEW \"{view_name}\" AS SELECT * FROM \"{backing}\""
            ))
            .await?;

        Ok(QueryResult::Message(format!(
            "Materialized view '{view_name}' refreshed."
        )))
    }

    /// Executes INSERT statements, including `ON CONFLICT` upserts.
    async fn handle_insert(
        &mut self,
        sql: &str,
        insert: &sqlparser::ast::Insert,
    ) -> Result<QueryResult, BoxError> {
        if let Some(OnInsert::OnConflict(on_conflict)) = &insert.on {
            return self.handle_upsert(insert, on_conflict).await;
        }

        let table_name = insert.table.to_string();
        let before_triggers: Vec<String> = self
            .catalog
            .triggers
            .values()
            .filter(|t| t.table == table_name && t.event == "INSERT" && t.timing == "BEFORE")
            .map(|t| t.body.clone())
            .collect();
        for body in before_triggers {
            let _ = self.execute(&body).await;
        }
        let table_meta = self
            .catalog
            .tables
            .get(&table_name)
            .cloned()
            .ok_or_else(|| format!("Table '{table_name}' does not exist"))?;
        let target_columns: Vec<String> = if insert.columns.is_empty() {
            table_meta.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            insert.columns.iter().map(|c| c.value.clone()).collect()
        };
        let source = insert
            .source
            .as_ref()
            .ok_or("INSERT requires a source query")?;
        let source_df = self.ctx.sql(&source.to_string()).await?;
        let batches = source_df.collect().await?;

        let returning_clause = extract_returning_clause(sql);
        let requires_immediate =
            self.replaying_wal || returning_clause.is_some() || !table_meta.constraints.is_empty();

        if requires_immediate {
            self.wal_append_pending(sql)?;
            let inserted_rows: usize = batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();

            if inserted_rows > 0 {
                let schema = batches[0].schema();
                let tmp_name = self.next_temp_table_name("ins");
                let mem = MemTable::try_new(schema.clone(), vec![batches.clone()])?;
                self.ctx.register_table(&tmp_name, Arc::new(mem))?;
                let cols_sql = target_columns
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                let insert_sql = format!(
                    "INSERT INTO \"{table_name}\" ({cols_sql}) SELECT * FROM \"{tmp_name}\""
                );
                self.ctx.sql(&insert_sql).await?.collect().await?;
                let _ = self.ctx.deregister_table(&tmp_name);

                // Update FTS inverted indexes for the new rows
                let row_offset: usize = match self
                    .collect_with_plan_cache(&format!("SELECT COUNT(*) FROM \"{table_name}\""))
                    .await
                {
                    Ok(count_batches) => count_batches
                        .first()
                        .and_then(|batch| {
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .map(|a| a.value(0) as usize)
                        })
                        .map_or(0, |total| total.saturating_sub(inserted_rows)),
                    Err(_) => 0,
                };
                for (idx_name, def) in &self.fulltext_indexes.clone() {
                    if def.table_name != table_name {
                        continue;
                    }
                    if let Some(idx) = self.fts_inverted_index.get_mut(idx_name) {
                        let mut row_idx = row_offset;
                        for batch in &batches {
                            for row in 0..batch.num_rows() {
                                let mut text_parts = Vec::new();
                                for col_name in &def.columns {
                                    let s = target_columns
                                        .iter()
                                        .position(|c| c == col_name)
                                        .map(|i| {
                                            array_value_to_string(batch.column(i).as_ref(), row)
                                        })
                                        .unwrap_or_default();
                                    text_parts.push(s);
                                }
                                let text = text_parts.join(" ");
                                idx.add_document(&table_name, row_idx, &text);
                                row_idx += 1;
                            }
                        }
                    }
                }
            }

            self.validate_constraints_batch(&table_name, &target_columns, &batches)
                .await?;
            self.maybe_auto_analyze(&table_name, inserted_rows);
            self.refresh_table_file_stats_light(&table_name).await?;
            self.record_cdc_event(&table_name, "INSERT", inserted_rows);

            let after_triggers: Vec<String> = self
                .catalog
                .triggers
                .values()
                .filter(|t| t.table == table_name && t.event == "INSERT" && t.timing == "AFTER")
                .map(|t| t.body.clone())
                .collect();
            for body in after_triggers {
                let _ = self.execute(&body).await;
            }

            let out = if let Some(ret) = &returning_clause {
                let ret_sql = format!("SELECT {ret} FROM \"{table_name}\"");
                self.ctx.sql(&ret_sql).await?.collect().await?
            } else {
                Vec::new()
            };
            return Ok(QueryResult::Records(out));
        }

        if self.arrow_wal.is_none() {
            self.wal_append_pending(sql)?;
        }

        // Eagerly validate NOT NULL on the provided columns so violations
        // are reported immediately rather than deferred to flush time.
        self.validate_not_null_positional(&table_name, &target_columns, &batches)?;

        let columns = if insert.columns.is_empty() {
            None
        } else {
            Some(target_columns)
        };
        let (rows, flushed) = self
            .buffer_insert_batches(&table_name, columns, batches)
            .await?;
        self.record_cdc_event(&table_name, "INSERT", rows);

        let after_triggers: Vec<String> = self
            .catalog
            .triggers
            .values()
            .filter(|t| t.table == table_name && t.event == "INSERT" && t.timing == "AFTER")
            .map(|t| t.body.clone())
            .collect();
        for body in after_triggers {
            let _ = self.execute(&body).await;
        }

        Ok(QueryResult::Message(if flushed {
            format!("{rows} row(s) inserted into '{table_name}' (buffer flushed).")
        } else {
            format!("{rows} row(s) buffered for '{table_name}'.")
        }))
    }

    async fn handle_upsert(
        &mut self,
        insert: &sqlparser::ast::Insert,
        on_conflict: &OnConflict,
    ) -> Result<QueryResult, BoxError> {
        let table_name = insert.table.to_string();
        let _ = self.flush_table(&table_name).await?;
        let table_meta = self
            .catalog
            .tables
            .get(&table_name)
            .ok_or_else(|| format!("Table '{table_name}' does not exist"))?
            .clone();
        let conflict_columns = resolve_conflict_columns(&table_meta, on_conflict)?;
        if conflict_columns.is_empty() {
            return Err("ON CONFLICT requires a conflict target".into());
        }

        let target_columns: Vec<String> = if insert.columns.is_empty() {
            table_meta.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            insert.columns.iter().map(|c| c.value.clone()).collect()
        };

        let source = insert
            .source
            .as_ref()
            .ok_or("UPSERT requires an INSERT source")?;
        let source_df = self.ctx.sql(&source.to_string()).await?;
        let source_batches = source_df.collect().await?;

        self.wal_append_pending(&insert.to_string())?;

        #[derive(Clone)]
        #[allow(clippy::items_after_statements)]
        struct UpsertRow {
            values: HashMap<String, String>,
            row_values_sql: Vec<String>,
            conflict_values: Vec<String>,
            conflict_key: String,
        }

        let mut source_rows = Vec::<UpsertRow>::new();
        for batch in &source_batches {
            for row in 0..batch.num_rows() {
                let mut value_map = HashMap::new();
                let mut row_values_sql = Vec::new();
                for (idx, col_name) in target_columns.iter().enumerate() {
                    let literal = array_value_to_sql_literal(batch.column(idx).as_ref(), row);
                    value_map.insert(col_name.clone(), literal.clone());
                    row_values_sql.push(literal);
                }
                let conflict_values: Vec<String> = conflict_columns
                    .iter()
                    .map(|c| {
                        value_map
                            .get(c)
                            .cloned()
                            .unwrap_or_else(|| "NULL".to_string())
                    })
                    .collect();
                let conflict_key = conflict_values.join("\x1f");
                source_rows.push(UpsertRow {
                    values: value_map,
                    row_values_sql,
                    conflict_values,
                    conflict_key,
                });
            }
        }

        if source_rows.is_empty() {
            return Ok(QueryResult::Message(
                "UPSERT complete: 0 inserted, 0 updated, 0 skipped.".into(),
            ));
        }

        let mut conflict_predicates = Vec::new();
        let mut seen_conflict_keys = HashSet::new();
        for row in &source_rows {
            if !seen_conflict_keys.insert(row.conflict_key.clone()) {
                continue;
            }
            let pred = conflict_columns
                .iter()
                .zip(row.conflict_values.iter())
                .map(|(c, v)| format!("\"{c}\" = {v}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            conflict_predicates.push(format!("({pred})"));
        }

        let mut existing_conflict_keys = HashSet::new();
        if !conflict_predicates.is_empty() {
            let conflict_sql = format!(
                "SELECT * FROM \"{table_name}\" WHERE {}",
                conflict_predicates.join(" OR ")
            );
            let conflict_df = self.ctx.sql(&conflict_sql).await?;
            let conflict_batches = conflict_df.collect().await?;
            for batch in &conflict_batches {
                for row in 0..batch.num_rows() {
                    let key = conflict_columns
                        .iter()
                        .map(|c| {
                            let idx = batch.schema().index_of(c).unwrap_or(usize::MAX);
                            if idx == usize::MAX {
                                "NULL".to_string()
                            } else {
                                array_value_to_sql_literal(batch.column(idx).as_ref(), row)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join("\x1f");
                    existing_conflict_keys.insert(key);
                }
            }
        }

        let mut insert_rows = Vec::<UpsertRow>::new();
        let mut update_rows = Vec::<UpsertRow>::new();
        for row in source_rows {
            if existing_conflict_keys.contains(&row.conflict_key) {
                update_rows.push(row);
            } else {
                insert_rows.push(row);
            }
        }

        let mut inserted = 0usize;
        let mut updated = 0usize;
        let mut skipped = 0usize;

        if !insert_rows.is_empty() {
            let values_sql = insert_rows
                .iter()
                .map(|r| format!("({})", r.row_values_sql.join(", ")))
                .collect::<Vec<_>>()
                .join(", ");
            let insert_sql = format!(
                "INSERT INTO \"{table_name}\" ({}) VALUES {values_sql}",
                target_columns
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            self.ctx.sql(&insert_sql).await?.collect().await?;
            inserted = insert_rows.len();
        }

        match &on_conflict.action {
            OnConflictAction::DoNothing => {
                skipped = update_rows.len();
            }
            OnConflictAction::DoUpdate(do_update) => {
                if !update_rows.is_empty() {
                    let assignment_exprs: HashMap<String, String> = do_update
                        .assignments
                        .iter()
                        .map(|a| {
                            let target = match &a.target {
                                sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                    name.to_string()
                                }
                                sqlparser::ast::AssignmentTarget::Tuple(names) => names
                                    .last()
                                    .map(std::string::ToString::to_string)
                                    .unwrap_or_default(),
                            }
                            .trim_matches('"')
                            .to_string();
                            (target, a.value.to_string())
                        })
                        .collect();

                    let projections: Vec<String> = table_meta
                        .columns
                        .iter()
                        .map(|c| {
                            if let Some(expr_template) = assignment_exprs.get(&c.name) {
                                let mut when_clauses = Vec::new();
                                for row in &update_rows {
                                    let conflict_predicate = conflict_columns
                                        .iter()
                                        .zip(row.conflict_values.iter())
                                        .map(|(col, v)| format!("\"{col}\" = {v}"))
                                        .collect::<Vec<_>>()
                                        .join(" AND ");

                                    let mut apply_predicate = conflict_predicate;
                                    if let Some(sel) = &do_update.selection {
                                        let mut sel_expr = sel.to_string();
                                        for (k, v) in &row.values {
                                            sel_expr =
                                                sel_expr.replace(&format!("EXCLUDED.{k}"), v);
                                            sel_expr =
                                                sel_expr.replace(&format!("excluded.{k}"), v);
                                        }
                                        apply_predicate =
                                            format!("({apply_predicate}) AND ({sel_expr})");
                                    }

                                    let mut expr = expr_template.clone();
                                    for (k, v) in &row.values {
                                        expr = expr.replace(&format!("EXCLUDED.{k}"), v);
                                        expr = expr.replace(&format!("excluded.{k}"), v);
                                    }
                                    when_clauses
                                        .push(format!("WHEN ({apply_predicate}) THEN ({expr})"));
                                }

                                if when_clauses.is_empty() {
                                    format!("\"{}\"", c.name)
                                } else {
                                    format!(
                                        "CASE {} ELSE \"{col}\" END AS \"{col}\"",
                                        when_clauses.join(" "),
                                        col = c.name
                                    )
                                }
                            } else {
                                format!("\"{}\"", c.name)
                            }
                        })
                        .collect();

                    let rewrite_sql =
                        format!("SELECT {} FROM \"{table_name}\"", projections.join(", "));
                    let df = self.ctx.sql(&rewrite_sql).await?;
                    let schema = Arc::new(df.schema().as_arrow().clone());
                    let modified = df.collect().await?;
                    self.rewrite_table(&table_name, schema, modified).await?;
                    updated = update_rows.len();
                }
            }
        }

        self.validate_not_null_table(&table_name).await?;
        self.validate_table_constraints(&table_name).await?;
        self.validate_check_constraints(&table_name).await?;
        self.maybe_auto_analyze(&table_name, inserted.saturating_add(updated));
        self.refresh_table_file_stats_light(&table_name).await?;
        self.record_cdc_event(&table_name, "UPSERT", inserted.saturating_add(updated));

        Ok(QueryResult::Message(format!(
            "UPSERT complete: {inserted} inserted, {updated} updated, {skipped} skipped."
        )))
    }

    // ── Maintenance ────────────────────────────────────────────

    /// Handles `VACUUM table_name`.
    ///
    /// Reads all data, deletes fragment Parquet files, and writes a
    /// single optimized file. Respects index sort order if present.
    async fn handle_vacuum(&mut self, table_name: &str) -> Result<QueryResult, BoxError> {
        let _ = self.flush_all().await?;

        if !self.catalog.tables.contains_key(table_name) {
            return Err(format!("Table '{table_name}' does not exist").into());
        }

        self.backup_table_for_txn(table_name).await?;
        let pruned = self.apply_retention_policy(table_name).await?;

        let indexes = self.catalog.indexes_for_table(table_name);
        let order_clause = if let Some(idx) = indexes.first() {
            let parts: Vec<String> = idx
                .columns
                .iter()
                .map(|c| {
                    format!(
                        "\"{}\" {}",
                        c.name,
                        if c.ascending { "ASC" } else { "DESC" }
                    )
                })
                .collect();
            format!(" ORDER BY {}", parts.join(", "))
        } else {
            String::new()
        };

        let select_sql = format!("SELECT * FROM \"{table_name}\"{order_clause}");
        let df = self.ctx.sql(&select_sql).await?;
        let schema = Arc::new(df.schema().as_arrow().clone());
        let batches = df.collect().await?;
        let row_count: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();

        let old_files = self.list_parquet_files(table_name).await?;
        let file_count = old_files.len();

        self.rewrite_table(table_name, schema, batches).await?;
        let _ = self.handle_analyze(table_name).await?;

        Ok(QueryResult::Message(format!(
            "VACUUM: compacted {file_count} file(s) into 1 ({row_count} rows) for '{table_name}' (retention pruned {pruned} file(s))."
        )))
    }

    /// Handles `ANALYZE table_name`.
    ///
    /// Scans the table and collects per-column statistics (null count,
    /// distinct count, min, max) and stores them in the catalog.
    async fn handle_analyze(&mut self, table_name: &str) -> Result<QueryResult, BoxError> {
        if !self.catalog.tables.contains_key(table_name) {
            return Err(format!("Table '{table_name}' does not exist").into());
        }

        let table_meta = self.catalog.tables.get(table_name).unwrap().clone();

        let count_sql = format!("SELECT COUNT(*) AS c FROM \"{table_name}\"");
        let count_df = self.ctx.sql(&count_sql).await?;
        let count_batches = count_df.collect().await?;
        let row_count: u64 = count_batches
            .iter()
            .filter_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| (0..a.len()).map(move |i| a.value(i) as u64))
            })
            .flatten()
            .sum();

        let mut col_stats = HashMap::with_capacity(table_meta.columns.len());

        for col_def in &table_meta.columns {
            let col_name = &col_def.name;
            let stats_sql = format!(
                "SELECT \
                     COUNT(*) - COUNT(\"{col_name}\") AS null_count, \
                     APPROX_DISTINCT(\"{col_name}\") AS distinct_count, \
                     CAST(MIN(\"{col_name}\") AS VARCHAR) AS min_val, \
                     CAST(MAX(\"{col_name}\") AS VARCHAR) AS max_val \
                 FROM \"{table_name}\""
            );
            let stats_df = self.ctx.sql(&stats_sql).await?;
            let stats_batches = stats_df.collect().await?;

            if let Some(batch) = stats_batches.first() {
                let null_count = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map_or(0, |a| a.value(0) as u64);
                let distinct_count = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .map(|a| a.value(0));
                let min_value = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| {
                        if a.is_null(0) {
                            None
                        } else {
                            Some(a.value(0).to_string())
                        }
                    });
                let max_value = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .and_then(|a| {
                        if a.is_null(0) {
                            None
                        } else {
                            Some(a.value(0).to_string())
                        }
                    });

                col_stats.insert(
                    col_name.clone(),
                    ColumnStatistics {
                        null_count,
                        distinct_count,
                        min_value,
                        max_value,
                    },
                );
            }
        }

        let stats = TableStatistics {
            row_count,
            columns: col_stats,
        };
        self.catalog.set_statistics(table_name, stats).await?;
        self.refresh_table_file_stats(table_name).await?;

        Ok(QueryResult::Message(format!(
            "ANALYZE: collected statistics for '{table_name}' ({row_count} rows, {} columns).",
            table_meta.columns.len()
        )))
    }

    // ── Prepared statements ────────────────────────────────────

    /// Handles `PREPARE name AS statement`.
    async fn handle_prepare(
        &mut self,
        name: &str,
        sql_template: &str,
    ) -> Result<QueryResult, BoxError> {
        let logical_plan = match self.ctx.sql(sql_template).await {
            Ok(df) => Some(df.logical_plan().clone()),
            Err(_) => None,
        };
        if let (true, Some(ref plan)) = (is_read_only_sql(sql_template), &logical_plan) {
            self.plan_cache
                .insert(sql_template.to_string(), plan.clone());
        }
        self.prepared_statements.insert(
            name.to_string(),
            PreparedStatement {
                sql_template: sql_template.to_string(),
                logical_plan,
            },
        );
        Ok(QueryResult::Message(format!(
            "Statement '{name}' prepared."
        )))
    }

    // ── COPY FROM ───────────────────────────────────────────────

    /// Handles `COPY table FROM 'path'`.
    ///
    /// Detects format from file extension (`.csv`, `.parquet`, `.json`)
    /// and ingests data via `DataFusion`'s file readers.
    async fn handle_copy_from(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        let table_name = tokens
            .get(1)
            .ok_or("COPY FROM: missing table name")?
            .trim_matches('"');

        if !self.catalog.tables.contains_key(table_name) {
            return Err(format!("Table '{table_name}' does not exist").into());
        }

        self.wal_append_pending(sql)?;

        let from_idx = tokens
            .iter()
            .position(|t| t.eq_ignore_ascii_case("FROM"))
            .ok_or("COPY FROM: missing FROM keyword")?;

        let file_path = tokens
            .get(from_idx + 1)
            .ok_or("COPY FROM: missing file path")?
            .trim_end_matches(';')
            .trim_matches('\'')
            .trim_matches('"');

        let format = if std::path::Path::new(file_path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
        {
            "parquet"
        } else if std::path::Path::new(file_path)
            .extension()
            .is_some_and(|ext| {
                ext.eq_ignore_ascii_case("json") || ext.eq_ignore_ascii_case("ndjson")
            })
        {
            "json"
        } else {
            "csv"
        };

        let df = match format {
            "parquet" => {
                self.ctx
                    .read_parquet(file_path, ParquetReadOptions::default())
                    .await?
            }
            "json" => {
                self.ctx
                    .read_json(file_path, NdJsonReadOptions::default())
                    .await?
            }
            _ => {
                self.ctx
                    .read_csv(file_path, CsvReadOptions::default())
                    .await?
            }
        };

        let batches = df.collect().await?;
        let total_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        let mut missing_target_cols = 0usize;
        let mut source_only_cols = 0usize;

        if total_rows > 0 {
            let schema = batches[0].schema();
            let mem = MemTable::try_new(schema, vec![batches.clone()])?;
            self.ctx
                .register_table("__potato_copy_tmp", Arc::new(mem))?;
            let target_meta = self
                .catalog
                .tables
                .get(table_name)
                .ok_or_else(|| format!("Table '{table_name}' does not exist"))?;
            let source_fields: HashSet<String> = batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let target_cols = target_meta
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>();
            let projection = target_cols
                .iter()
                .map(|col_name| {
                    if source_fields.contains(col_name) {
                        format!("\"{col_name}\"")
                    } else {
                        missing_target_cols = missing_target_cols.saturating_add(1);
                        format!("NULL AS \"{col_name}\"")
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            source_only_cols = source_fields
                .iter()
                .filter(|name| !target_cols.iter().any(|c| c == *name))
                .count();
            let target_col_list = target_cols
                .iter()
                .map(|c| format!("\"{c}\""))
                .collect::<Vec<_>>()
                .join(", ");
            self.ctx
                .sql(&format!(
                    "INSERT INTO \"{table_name}\" ({target_col_list}) SELECT {projection} FROM __potato_copy_tmp"
                ))
                .await?
                .collect()
                .await?;
            self.ctx.deregister_table("__potato_copy_tmp")?;
        }

        self.validate_not_null_table(table_name).await?;
        self.validate_table_constraints(table_name).await?;
        self.validate_check_constraints(table_name).await?;
        self.maybe_auto_analyze(table_name, total_rows);
        self.refresh_table_file_stats(table_name).await?;
        self.record_cdc_event(table_name, "COPY_FROM", total_rows);

        Ok(QueryResult::Message(format!(
            "{total_rows} row(s) copied into '{table_name}' from '{file_path}' ({format}); missing target columns filled with NULL: {missing_target_cols}, source-only columns ignored: {source_only_cols}."
        )))
    }

    /// Handles `COPY table TO 'path'` for CSV/JSON/Parquet export.
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn handle_copy_to(&mut self, sql: &str) -> Result<QueryResult, BoxError> {
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        let table_name = tokens
            .get(1)
            .ok_or("COPY TO: missing table name")?
            .trim_matches('"');

        if !self.catalog.tables.contains_key(table_name) {
            return Err(format!("Table '{table_name}' does not exist").into());
        }

        let to_idx = tokens
            .iter()
            .position(|t| t.eq_ignore_ascii_case("TO"))
            .ok_or("COPY TO: missing TO keyword")?;
        let file_path = tokens
            .get(to_idx + 1)
            .ok_or("COPY TO: missing file path")?
            .trim_end_matches(';')
            .trim_matches('\'')
            .trim_matches('"');

        let df = self
            .ctx
            .sql(&format!("SELECT * FROM \"{table_name}\""))
            .await?;
        let batches = df.collect().await?;
        let total_rows: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();

        let output_path = PathBuf::from(file_path);
        if let Some(parent) = output_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = File::create(&output_path)?;

        let ext_matches = |ext_str: &str| -> bool {
            std::path::Path::new(file_path)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case(ext_str))
        };

        if ext_matches("parquet") {
            let schema = batches.first().map_or_else(
                || Arc::new(Schema::empty()),
                arrow::array::RecordBatch::schema,
            );
            let props = WriterProperties::builder()
                .set_compression(parse_parquet_compression(&parquet_compression_str()))
                .build();
            let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
            for batch in &batches {
                writer.write(batch)?;
            }
            writer.close()?;
        } else if ext_matches("json") || ext_matches("ndjson") {
            let mut writer = json::LineDelimitedWriter::new(file);
            for batch in &batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        } else {
            let mut writer = csv::Writer::new(file);
            for batch in &batches {
                writer.write(batch)?;
            }
        }

        Ok(QueryResult::Message(format!(
            "{total_rows} row(s) copied from '{table_name}' to '{file_path}'."
        )))
    }

    #[allow(dead_code)]
    fn validate_not_null_batches(
        &self,
        table_name: &str,
        batches: &[RecordBatch],
    ) -> Result<(), BoxError> {
        let Some(meta) = self.catalog.tables.get(table_name) else {
            return Ok(());
        };
        let non_nullable: Vec<&ColumnDef> = meta.columns.iter().filter(|c| !c.nullable).collect();
        if non_nullable.is_empty() {
            return Ok(());
        }

        for batch in batches {
            for col in &non_nullable {
                // Skip columns not present in this batch – they will be
                // validated at flush time via validate_not_null_table.
                let Ok(idx) = batch.schema().index_of(&col.name) else {
                    continue;
                };
                let arr = batch.column(idx);
                // Arrow's NullArray (DataType::Null) has null_count() == 0
                // but represents all-null values (SQL NULL literals).
                if arr.data_type() == &arrow::datatypes::DataType::Null || arr.null_count() > 0 {
                    return Err(
                        format!("NOT NULL constraint failed: '{table_name}.{}'", col.name).into(),
                    );
                }
            }
        }
        Ok(())
    }

    /// Positional NOT NULL validation for INSERT batches where column names
    /// may not match catalog names (e.g. VALUES-derived batches).
    fn validate_not_null_positional(
        &self,
        table_name: &str,
        target_columns: &[String],
        batches: &[RecordBatch],
    ) -> Result<(), BoxError> {
        let Some(meta) = self.catalog.tables.get(table_name) else {
            return Ok(());
        };
        let non_nullable_targets: Vec<(usize, &str)> = target_columns
            .iter()
            .enumerate()
            .filter(|(_, name)| meta.columns.iter().any(|c| c.name == **name && !c.nullable))
            .map(|(i, name)| (i, name.as_str()))
            .collect();
        if non_nullable_targets.is_empty() {
            return Ok(());
        }
        for batch in batches {
            for &(idx, col_name) in &non_nullable_targets {
                if idx < batch.num_columns() {
                    let col = batch.column(idx);
                    // Arrow's NullArray (DataType::Null) has null_count() == 0
                    // but represents all-null values (SQL NULL literals).
                    if col.data_type() == &arrow::datatypes::DataType::Null || col.null_count() > 0
                    {
                        return Err(format!(
                            "NOT NULL constraint failed: '{table_name}.{col_name}'"
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    /// Checks that none of the `values` already exist in the on-disk table.
    /// Uses an `IN` clause for single-column keys, and batched OR
    /// predicates (up to 500 per query) for composite keys to keep
    /// generated SQL small.
    async fn check_uniqueness_against_table(
        &self,
        table_name: &str,
        columns: &[String],
        values: &[Vec<String>],
        err_msg: &str,
    ) -> Result<(), BoxError> {
        const BATCH_SIZE: usize = 500;

        if columns.len() == 1 {
            let col = &columns[0];
            for chunk in values.chunks(BATCH_SIZE) {
                let in_list = chunk
                    .iter()
                    .map(|v| v[0].as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let sql = format!(
                    "SELECT COUNT(*) AS c FROM \"{table_name}\" WHERE \"{col}\" IN ({in_list})"
                );
                let df = self.ctx.sql(&sql).await?;
                let out = df.collect().await?;
                if scalar_count(&out) as usize > chunk.len() {
                    return Err(err_msg.into());
                }
            }
        } else {
            for chunk in values.chunks(BATCH_SIZE) {
                let where_clause = chunk
                    .iter()
                    .map(|vals| {
                        let pred = columns
                            .iter()
                            .zip(vals.iter())
                            .map(|(c, v)| format!("\"{c}\" = {v}"))
                            .collect::<Vec<_>>()
                            .join(" AND ");
                        format!("({pred})")
                    })
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let sql =
                    format!("SELECT COUNT(*) AS c FROM \"{table_name}\" WHERE {where_clause}");
                let df = self.ctx.sql(&sql).await?;
                let out = df.collect().await?;
                if scalar_count(&out) as usize > chunk.len() {
                    return Err(err_msg.into());
                }
            }
        }
        Ok(())
    }

    async fn validate_constraints_batch(
        &self,
        table_name: &str,
        target_columns: &[String],
        batches: &[RecordBatch],
    ) -> Result<(), BoxError> {
        if batches.is_empty() {
            return Ok(());
        }
        let Some(meta) = self.catalog.tables.get(table_name) else {
            return Ok(());
        };

        let column_positions: HashMap<&str, usize> = target_columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.as_str(), i))
            .collect();

        for constraint in &meta.constraints {
            match constraint {
                CatalogTableConstraint::PrimaryKey { columns } => {
                    if columns
                        .iter()
                        .any(|c| !column_positions.contains_key(c.as_str()))
                    {
                        return self.validate_table_constraints(table_name).await;
                    }

                    let mut seen = HashSet::new();
                    let mut values_for_lookup: Vec<Vec<String>> = Vec::new();
                    for batch in batches {
                        for row in 0..batch.num_rows() {
                            let tuple: Vec<String> = columns
                                .iter()
                                .map(|c| {
                                    let idx = column_positions[c.as_str()];
                                    array_value_to_sql_literal(batch.column(idx).as_ref(), row)
                                })
                                .collect();
                            if tuple.iter().any(|v| v == "NULL") {
                                return Err(format!(
                                    "PRIMARY KEY violation on '{table_name}' for ({})",
                                    columns.join(", ")
                                )
                                .into());
                            }
                            let key = tuple.join("\x1f");
                            if !seen.insert(key) {
                                return Err(format!(
                                    "PRIMARY KEY violation on '{table_name}' for ({})",
                                    columns.join(", ")
                                )
                                .into());
                            }
                            values_for_lookup.push(tuple);
                        }
                    }

                    if !values_for_lookup.is_empty() {
                        let err_msg = format!(
                            "PRIMARY KEY violation on '{table_name}' for ({})",
                            columns.join(", ")
                        );
                        self.check_uniqueness_against_table(
                            table_name,
                            columns,
                            &values_for_lookup,
                            &err_msg,
                        )
                        .await?;
                    }
                }
                CatalogTableConstraint::Unique { name, columns } => {
                    if columns
                        .iter()
                        .any(|c| !column_positions.contains_key(c.as_str()))
                    {
                        return self.validate_table_constraints(table_name).await;
                    }

                    let mut seen = HashSet::new();
                    let mut values_for_lookup: Vec<Vec<String>> = Vec::new();
                    for batch in batches {
                        for row in 0..batch.num_rows() {
                            let tuple: Vec<String> = columns
                                .iter()
                                .map(|c| {
                                    let idx = column_positions[c.as_str()];
                                    array_value_to_sql_literal(batch.column(idx).as_ref(), row)
                                })
                                .collect();
                            if tuple.iter().any(|v| v == "NULL") {
                                continue;
                            }
                            let key = tuple.join("\x1f");
                            if !seen.insert(key) {
                                return Err(format!(
                                    "UNIQUE constraint violation '{name}' on '{table_name}'"
                                )
                                .into());
                            }
                            values_for_lookup.push(tuple);
                        }
                    }

                    if !values_for_lookup.is_empty() {
                        let err_msg =
                            format!("UNIQUE constraint violation '{name}' on '{table_name}'");
                        self.check_uniqueness_against_table(
                            table_name,
                            columns,
                            &values_for_lookup,
                            &err_msg,
                        )
                        .await?;
                    }
                }
                CatalogTableConstraint::Check { name, expr } => {
                    let non_empty: Vec<&RecordBatch> =
                        batches.iter().filter(|b| b.num_rows() > 0).collect();
                    if non_empty.is_empty() {
                        continue;
                    }
                    if non_empty[0].num_columns() != target_columns.len() {
                        return self.validate_check_constraints(table_name).await;
                    }
                    let schema = non_empty[0].schema();
                    let all_batches: Vec<RecordBatch> = non_empty.into_iter().cloned().collect();
                    let tmp_name = format!(
                        "__potato_check_tmp_{}",
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|d| d.as_nanos())
                            .unwrap_or(0)
                    );
                    let mem = MemTable::try_new(schema.clone(), vec![all_batches])?;
                    self.ctx.register_table(&tmp_name, Arc::new(mem))?;
                    let select_cols = target_columns
                        .iter()
                        .enumerate()
                        .map(|(i, target)| {
                            let source = schema.field(i).name();
                            format!("\"{source}\" AS \"{target}\"")
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let check_sql = format!(
                        "SELECT COUNT(*) AS c FROM (SELECT {select_cols} FROM \"{tmp_name}\") AS __potato_chk WHERE NOT ({expr})"
                    );
                    let check_res = self.ctx.sql(&check_sql).await;
                    let violating = if let Ok(df) = check_res {
                        let out = df.collect().await?;
                        scalar_count(&out)
                    } else {
                        let _ = self.ctx.deregister_table(&tmp_name);
                        return self.validate_check_constraints(table_name).await;
                    };
                    let _ = self.ctx.deregister_table(&tmp_name);
                    if violating > 0 {
                        return Err(format!(
                            "CHECK constraint violation '{name}' on '{table_name}' ({violating} row(s))"
                        )
                        .into());
                    }
                }
                CatalogTableConstraint::ForeignKey {
                    name,
                    columns,
                    ref_table,
                    ref_columns,
                    ..
                } => {
                    if columns.len() != ref_columns.len() || columns.is_empty() {
                        return Err(format!(
                            "FOREIGN KEY '{name}' on '{table_name}' has invalid column mapping"
                        )
                        .into());
                    }
                    let predicates = columns
                        .iter()
                        .zip(ref_columns.iter())
                        .map(|(c, rc)| format!("child.\"{c}\" = parent.\"{rc}\""))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    let non_null = columns
                        .iter()
                        .map(|c| format!("child.\"{c}\" IS NOT NULL"))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    let null_ref = ref_columns
                        .iter()
                        .map(|rc| format!("parent.\"{rc}\" IS NULL"))
                        .collect::<Vec<_>>()
                        .join(" OR ");
                    let fk_sql = format!(
                        "SELECT COUNT(*) AS c FROM \"{table_name}\" child LEFT JOIN \"{ref_table}\" parent ON {predicates} WHERE ({non_null}) AND ({null_ref})"
                    );
                    let fk_df = self.ctx.sql(&fk_sql).await?;
                    let fk_batches = fk_df.collect().await?;
                    if scalar_count(&fk_batches) > 0 {
                        return Err(format!(
                            "FOREIGN KEY violation '{name}' on '{table_name}' referencing '{ref_table}'"
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    async fn validate_not_null_table(&self, table_name: &str) -> Result<(), BoxError> {
        let Some(meta) = self.catalog.tables.get(table_name) else {
            return Ok(());
        };
        let checks: Vec<String> = meta
            .columns
            .iter()
            .filter(|c| !c.nullable)
            .map(|c| format!("\"{}\" IS NULL", c.name))
            .collect();

        if checks.is_empty() {
            return Ok(());
        }

        let sql = format!(
            "SELECT COUNT(*) AS c FROM \"{table_name}\" WHERE {}",
            checks.join(" OR ")
        );
        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        let violating_rows: i64 = batches
            .iter()
            .filter_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|a| (0..a.len()).map(move |i| a.value(i)))
            })
            .flatten()
            .sum();

        if violating_rows > 0 {
            return Err(format!(
                "NOT NULL constraint failed for table '{table_name}' ({violating_rows} violating row(s))"
            )
            .into());
        }

        Ok(())
    }

    async fn validate_table_constraints(&self, table_name: &str) -> Result<(), BoxError> {
        let Some(meta) = self.catalog.tables.get(table_name) else {
            return Ok(());
        };

        for constraint in &meta.constraints {
            match constraint {
                CatalogTableConstraint::PrimaryKey { columns } => {
                    for col in columns {
                        let null_sql = format!(
                            "SELECT COUNT(*) AS c FROM \"{table_name}\" WHERE \"{col}\" IS NULL"
                        );
                        let null_df = self.ctx.sql(&null_sql).await?;
                        let null_batches = null_df.collect().await?;
                        let null_rows = scalar_count(&null_batches);
                        if null_rows > 0 {
                            return Err(format!(
                                "PRIMARY KEY violation on '{table_name}': column '{col}' contains NULL"
                            )
                            .into());
                        }
                    }

                    let group_cols = columns
                        .iter()
                        .map(|c| format!("\"{c}\""))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let dup_sql = format!(
                        "SELECT COUNT(*) AS c FROM (SELECT {group_cols} FROM \"{table_name}\" GROUP BY {group_cols} HAVING COUNT(*) > 1) AS dup"
                    );
                    let dup_df = self.ctx.sql(&dup_sql).await?;
                    let dup_batches = dup_df.collect().await?;
                    if scalar_count(&dup_batches) > 0 {
                        return Err(format!(
                            "PRIMARY KEY violation on '{table_name}' for ({})",
                            columns.join(", ")
                        )
                        .into());
                    }
                }
                CatalogTableConstraint::Unique { name, columns } => {
                    let group_cols = columns
                        .iter()
                        .map(|c| format!("\"{c}\""))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let dup_sql = format!(
                        "SELECT COUNT(*) AS c FROM (SELECT {group_cols} FROM \"{table_name}\" GROUP BY {group_cols} HAVING COUNT(*) > 1) AS dup"
                    );
                    let dup_df = self.ctx.sql(&dup_sql).await?;
                    let dup_batches = dup_df.collect().await?;
                    if scalar_count(&dup_batches) > 0 {
                        return Err(format!(
                            "UNIQUE constraint violation '{name}' on '{table_name}'"
                        )
                        .into());
                    }
                }
                CatalogTableConstraint::Check { .. } => {}
                CatalogTableConstraint::ForeignKey {
                    name,
                    columns,
                    ref_table,
                    ref_columns,
                    ..
                } => {
                    if columns.len() != ref_columns.len() || columns.is_empty() {
                        return Err(format!(
                            "FOREIGN KEY '{name}' on '{table_name}' has invalid column mapping"
                        )
                        .into());
                    }
                    let predicates = columns
                        .iter()
                        .zip(ref_columns.iter())
                        .map(|(c, rc)| format!("child.\"{c}\" = parent.\"{rc}\""))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    let non_null = columns
                        .iter()
                        .map(|c| format!("child.\"{c}\" IS NOT NULL"))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    let null_ref = ref_columns
                        .iter()
                        .map(|rc| format!("parent.\"{rc}\" IS NULL"))
                        .collect::<Vec<_>>()
                        .join(" OR ");
                    let fk_sql = format!(
                        "SELECT COUNT(*) AS c FROM \"{table_name}\" child LEFT JOIN \"{ref_table}\" parent ON {predicates} WHERE ({non_null}) AND ({null_ref})"
                    );
                    let fk_df = self.ctx.sql(&fk_sql).await?;
                    let fk_batches = fk_df.collect().await?;
                    if scalar_count(&fk_batches) > 0 {
                        return Err(format!(
                            "FOREIGN KEY violation '{name}' on '{table_name}' referencing '{ref_table}'"
                        )
                        .into());
                    }
                }
            }
        }

        Ok(())
    }

    async fn validate_check_constraints(&self, table_name: &str) -> Result<(), BoxError> {
        let Some(meta) = self.catalog.tables.get(table_name) else {
            return Ok(());
        };

        for constraint in &meta.constraints {
            if let CatalogTableConstraint::Check { name, expr } = constraint {
                let sql = format!("SELECT COUNT(*) AS c FROM \"{table_name}\" WHERE NOT ({expr})");
                let df = self.ctx.sql(&sql).await?;
                let batches = df.collect().await?;
                let violating = scalar_count(&batches);
                if violating > 0 {
                    return Err(format!(
                        "CHECK constraint violation '{name}' on '{table_name}' ({violating} row(s))"
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Handles `EXECUTE name(param1, param2, ...)`.
    ///
    /// Returns a boxed future to break the `execute -> handle_execute_prepared
    /// -> execute` recursion that Rust's async desugaring cannot size.
    fn handle_execute_prepared<'a>(
        &'a mut self,
        name: &'a str,
        parameters: &'a [sqlparser::ast::Expr],
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<QueryResult, BoxError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let stmt = self
                .prepared_statements
                .get(name)
                .ok_or_else(|| format!("Prepared statement '{name}' does not exist"))?
                .clone();

            if parameters.is_empty() && is_read_only_sql(&stmt.sql_template) {
                if let Some(plan) = stmt.logical_plan.clone() {
                    let _ = self.flush_all().await?;
                    if let Ok(df) = self.ctx.execute_logical_plan(plan).await {
                        let batches = df.collect().await?;
                        return Ok(QueryResult::Records(batches));
                    }
                }
            }

            let sql = substitute_parameters(&stmt.sql_template, parameters);
            self.execute(&sql).await
        })
    }
}

impl Drop for PotatoDB {
    fn drop(&mut self) {
        // Best-effort cleanup on graceful shutdown: truncate committed WAL
        // entries so normal restarts don't replay already-applied writes.
        // If there are buffered writes, keep WAL contents so recovery can
        // replay and materialize them on next startup.
        if self.replaying_wal {
            return;
        }
        if !self.write_buffer.is_empty() {
            return;
        }
        if let Some(wal) = self.wal.as_mut() {
            let _ = wal.checkpoint();
        }
    }
}

// ── Free functions ─────────────────────────────────────────────

/// Extracts the table name from a `DELETE` statement's FROM clause.
fn extract_delete_table_name(delete: &sqlparser::ast::Delete) -> Result<String, BoxError> {
    use sqlparser::ast::FromTable;

    let tables = match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };

    let table_with_joins = tables.first().ok_or("DELETE requires a FROM clause")?;

    Ok(table_with_joins.relation.to_string())
}

fn extract_mutated_table(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Insert(ins) => Some(ins.table.to_string()),
        Statement::Update { table, .. } => Some(table.relation.to_string()),
        Statement::Merge { table, .. } => match table {
            TableFactor::Table { name, .. } => Some(name.to_string().trim_matches('"').to_string()),
            _ => Some(table.to_string()),
        },
        Statement::Delete(del) => match &del.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables)
            | sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                tables.first().map(|t| t.relation.to_string())
            }
        },
        Statement::Drop { names, .. } => names.first().map(std::string::ToString::to_string),
        Statement::Truncate { table_names, .. } => {
            table_names.first().map(|tp| tp.name.to_string())
        }
        Statement::AlterTable { name, .. } => Some(name.to_string()),
        _ => None,
    }
}

/// Single-pass parameter substitution that processes `$N` placeholders
/// in reverse order so `$10` is not partially matched by `$1`.
fn substitute_parameters(template: &str, params: &[sqlparser::ast::Expr]) -> String {
    if params.is_empty() {
        return template.to_string();
    }
    let param_strs: Vec<String> = params
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    let mut result = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1].is_ascii_digit() {
            let start = i + 1;
            let mut end = start;
            while end < bytes.len() && bytes[end].is_ascii_digit() {
                end += 1;
            }
            if let Ok(idx) = template[start..end].parse::<usize>() {
                if idx >= 1 && idx <= param_strs.len() {
                    result.push_str(&param_strs[idx - 1]);
                    i = end;
                    continue;
                }
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

/// Extracts base table names from a read-only SQL string (SELECT, WITH, etc.).
/// Returns empty if parsing fails or the statement has no table references.
fn extract_table_names_from_readonly_sql(sql: &str) -> Vec<String> {
    let dialect = PostgreSqlDialect {};
    let Ok(stmts) = Parser::parse_sql(&dialect, sql) else {
        return Vec::new();
    };
    let Some(stmt) = stmts.first() else {
        return Vec::new();
    };
    let query = match stmt {
        Statement::Query(q) => q.as_ref(),
        Statement::Explain { statement, .. } => {
            if let Statement::Query(q) = statement.as_ref() {
                q.as_ref()
            } else {
                return Vec::new();
            }
        }
        _ => return Vec::new(),
    };
    let mut tables = Vec::new();
    extract_tables_from_set_expr(&query.body, &mut tables);
    tables
}

fn extract_tables_from_set_expr(expr: &SetExpr, out: &mut Vec<String>) {
    match expr {
        SetExpr::Select(sel) => {
            for twj in &sel.from {
                extract_tables_from_table_with_joins(twj, out);
            }
        }
        SetExpr::Query(q) => extract_tables_from_set_expr(&q.body, out),
        SetExpr::SetOperation { left, right, .. } => {
            extract_tables_from_set_expr(left, out);
            extract_tables_from_set_expr(right, out);
        }
        SetExpr::Values(_)
        | SetExpr::Insert(_)
        | SetExpr::Update(_)
        | SetExpr::Delete(_)
        | SetExpr::Merge(_)
        | SetExpr::Table(_) => {}
    }
}

fn extract_tables_from_table_with_joins(twj: &TableWithJoins, out: &mut Vec<String>) {
    extract_tables_from_table_factor(&twj.relation, out);
    for join in &twj.joins {
        extract_tables_from_table_factor(&join.relation, out);
    }
}

fn extract_tables_from_table_factor(factor: &TableFactor, out: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            out.push(name.to_string().trim_matches('"').to_string());
        }
        TableFactor::Derived { subquery, .. } => {
            extract_tables_from_set_expr(&subquery.body, out);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            extract_tables_from_table_with_joins(table_with_joins, out);
        }
        TableFactor::Pivot { table, .. }
        | TableFactor::Unpivot { table, .. }
        | TableFactor::MatchRecognize { table, .. } => {
            extract_tables_from_table_factor(table, out);
        }
        TableFactor::TableFunction { .. }
        | TableFactor::Function { .. }
        | TableFactor::UNNEST { .. }
        | TableFactor::JsonTable { .. }
        | TableFactor::OpenJsonTable { .. }
        | TableFactor::SemanticView { .. }
        | TableFactor::XmlTable { .. } => {}
    }
}

/// Returns the raw clause after `RETURNING`, if present.
fn extract_returning_clause(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let idx = upper.find(" RETURNING ")?;
    let clause = sql[idx + " RETURNING ".len()..]
        .trim()
        .trim_end_matches(';');
    if clause.is_empty() {
        None
    } else {
        Some(clause.to_string())
    }
}

/// Best-effort parser for `ALTER TABLE <old> RENAME TO <new>`.
fn parse_alter_table_rename(sql: &str) -> Option<(String, String)> {
    let tokens: Vec<&str> = sql.split_whitespace().collect();
    if tokens.len() < 6 {
        return None;
    }
    if !tokens[0].eq_ignore_ascii_case("ALTER")
        || !tokens[1].eq_ignore_ascii_case("TABLE")
        || !tokens[3].eq_ignore_ascii_case("RENAME")
        || !tokens[4].eq_ignore_ascii_case("TO")
    {
        return None;
    }
    let old_name = tokens[2].trim_matches('"').to_string();
    let new_name = tokens[5]
        .trim_matches('"')
        .trim_end_matches(';')
        .to_string();
    if old_name.is_empty() || new_name.is_empty() {
        None
    } else {
        Some((old_name, new_name))
    }
}

/// Parses `ALTER TABLE ... SET (retention = '30 days')` into seconds.
fn parse_retention_setting(sql: &str) -> Option<Option<u64>> {
    let lower = sql.to_lowercase();
    if !lower.starts_with("alter table") || !lower.contains("retention") {
        return None;
    }
    if lower.contains("retention = null") {
        return Some(None);
    }

    let first_quote = sql.find('\'')?;
    let rest = &sql[first_quote + 1..];
    let second_quote_rel = rest.find('\'')?;
    let value = &rest[..second_quote_rel];
    let parts = value.split_whitespace().collect::<Vec<_>>();
    if parts.is_empty() {
        return None;
    }
    let amount = parts[0].parse::<u64>().ok()?;
    let unit = parts.get(1).copied().unwrap_or("seconds").to_lowercase();
    let seconds = match unit.as_str() {
        "day" | "days" => amount.saturating_mul(24 * 60 * 60),
        "hour" | "hours" => amount.saturating_mul(60 * 60),
        "minute" | "minutes" => amount.saturating_mul(60),
        _ => amount,
    };
    Some(Some(seconds))
}

fn parse_create_function(sql: &str) -> Result<UdfDef, BoxError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let prefix = "CREATE FUNCTION";
    if !trimmed.to_uppercase().starts_with(prefix) {
        return Err("Expected CREATE FUNCTION".into());
    }
    let rest = trimmed[prefix.len()..].trim_start();
    let open_paren = rest
        .find('(')
        .ok_or("CREATE FUNCTION requires an argument list")?;
    let name = rest[..open_paren].trim().trim_matches('"').to_string();
    if name.is_empty() {
        return Err("CREATE FUNCTION requires a function name".into());
    }

    let close_paren =
        find_matching_paren(rest, open_paren).ok_or("Unclosed function argument list")?;
    let args_raw = &rest[open_paren + 1..close_paren];
    let args: Vec<String> = split_top_level_csv(args_raw)
        .into_iter()
        .filter_map(|arg| {
            let token = arg.trim();
            if token.is_empty() {
                return None;
            }
            let first = token.split_whitespace().next().unwrap_or("");
            Some(first.trim_matches('"').to_string())
        })
        .collect();

    let tail = rest[close_paren + 1..].trim();
    let returns_idx = find_ci(tail, "RETURNS").ok_or("CREATE FUNCTION requires RETURNS")?;
    let after_returns = tail[returns_idx + "RETURNS".len()..].trim_start();
    let as_idx = find_ci(after_returns, " AS ").ok_or("CREATE FUNCTION requires AS 'body'")?;
    let return_type = after_returns[..as_idx].trim().to_string();
    let body_raw = after_returns[as_idx + " AS ".len()..].trim();
    let body = body_raw
        .trim_matches('\'')
        .trim_matches('"')
        .trim()
        .to_string();
    if body.is_empty() {
        return Err("CREATE FUNCTION requires a non-empty body".into());
    }

    Ok(UdfDef {
        name,
        args,
        return_type,
        body,
    })
}

fn parse_drop_function(sql: &str) -> Result<(bool, String), BoxError> {
    let tokens: Vec<&str> = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect();
    if tokens.len() < 3 {
        return Err("DROP FUNCTION requires a name".into());
    }
    if !tokens[0].eq_ignore_ascii_case("DROP") || !tokens[1].eq_ignore_ascii_case("FUNCTION") {
        return Err("Expected DROP FUNCTION".into());
    }
    let (if_exists, name_idx) = if tokens.len() >= 5
        && tokens[2].eq_ignore_ascii_case("IF")
        && tokens[3].eq_ignore_ascii_case("EXISTS")
    {
        (true, 4)
    } else {
        (false, 2)
    };
    let name = tokens
        .get(name_idx)
        .ok_or("DROP FUNCTION requires a function name")?
        .trim_matches('"')
        .to_string();
    Ok((if_exists, name))
}

fn expand_user_defined_functions(
    sql: &str,
    udfs: &HashMap<String, UdfDef>,
) -> Result<String, BoxError> {
    let mut out = sql.to_string();
    let mut defs: Vec<&UdfDef> = udfs.values().collect();
    defs.sort_by(|a, b| b.name.len().cmp(&a.name.len()));
    for def in defs {
        out = expand_single_udf_call(&out, def)?;
    }
    Ok(out)
}

fn expand_single_udf_call(sql: &str, def: &UdfDef) -> Result<String, BoxError> {
    let mut out = String::new();
    let mut i = 0usize;
    let needle = format!("{}(", def.name.to_lowercase());
    let lower = sql.to_lowercase();

    while i < sql.len() {
        let Some(rel) = lower[i..].find(&needle) else {
            out.push_str(&sql[i..]);
            break;
        };
        let start = i + rel;
        if start > 0
            && sql[..start]
                .chars()
                .last()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            out.push_str(&sql[i..start + needle.len()]);
            i = start + needle.len();
            continue;
        }
        let open = start + def.name.len();
        let close = find_matching_paren(sql, open).ok_or("Unclosed function call")?;
        let args_src = &sql[open + 1..close];
        let args = split_top_level_csv(args_src);

        out.push_str(&sql[i..start]);
        if args.len() == def.args.len() {
            let mut body = def.body.clone();
            for (idx, arg) in args.iter().enumerate() {
                body = body.replace(&format!("${}", idx + 1), arg.trim());
            }
            for (arg_name, arg) in def.args.iter().zip(args.iter()) {
                body = body.replace(arg_name, arg.trim());
            }
            out.push('(');
            out.push_str(&body);
            out.push(')');
        } else {
            out.push_str(&sql[start..=close]);
        }
        i = close + 1;
    }
    Ok(out)
}

fn split_top_level_csv(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut depth = 0usize;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                cur.push(ch);
                loop {
                    match chars.next() {
                        Some('\'') => {
                            cur.push('\'');
                            if chars.peek() == Some(&'\'') {
                                cur.push(chars.next().unwrap_or('\''));
                            } else {
                                break;
                            }
                        }
                        Some(c) => cur.push(c),
                        None => break,
                    }
                }
            }
            '(' => {
                depth = depth.saturating_add(1);
                cur.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                cur.push(ch);
            }
            ',' if depth == 0 => {
                let item = cur.trim();
                if !item.is_empty() {
                    out.push(item.to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    let item = cur.trim();
    if !item.is_empty() {
        out.push(item.to_string());
    }
    out
}

fn find_matching_paren(s: &str, open_idx: usize) -> Option<usize> {
    if !s.is_char_boundary(open_idx) || s.get(open_idx..=open_idx)? != "(" {
        return None;
    }
    let mut depth = 0usize;
    let mut in_single_quote = false;
    for (idx, ch) in s.char_indices().skip_while(|(i, _)| *i < open_idx) {
        if ch == '\'' {
            in_single_quote = !in_single_quote;
            continue;
        }
        if in_single_quote {
            continue;
        }
        if ch == '(' {
            depth = depth.saturating_add(1);
        } else if ch == ')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(idx);
            }
        }
    }
    None
}

fn find_ci(haystack: &str, needle_upper: &str) -> Option<usize> {
    haystack.to_uppercase().find(&needle_upper.to_uppercase())
}

/// Replaces whole-word variable references in SQL for PL/pgSQL variable substitution.
/// Matches `var_name` only when it appears as a standalone identifier (not part of another).
fn substitute_plpgsql_var(sql: &str, var_name: &str, value: &str) -> String {
    if var_name.is_empty() {
        return sql.to_string();
    }
    let mut result = String::with_capacity(sql.len() + value.len());
    let mut i = 0;
    let sql_lower = sql.to_lowercase();
    let var_lower = var_name.to_lowercase();
    let len = var_lower.len();
    while i < sql.len() {
        if i + len <= sql.len() && sql_lower[i..i + len] == var_lower {
            let prev_ok = i == 0 || !is_plpgsql_identifier_char(sql.as_bytes()[i - 1]);
            let next_ok =
                i + len >= sql.len() || !is_plpgsql_identifier_char(sql.as_bytes()[i + len]);
            if prev_ok && next_ok {
                result.push_str(value);
                i += len;
                continue;
            }
        }
        result.push(sql.as_bytes()[i] as char);
        i += 1;
    }
    result
}

const fn is_plpgsql_identifier_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn extract_dollar_quoted_body(sql: &str) -> Option<String> {
    let start = sql.find("$$")?;
    let tail = &sql[start + 2..];
    let end = tail.find("$$")?;
    Some(tail[..end].to_string())
}

/// Rewrites subtraction expressions in SELECT items so that Date−Date
/// arithmetic produces an integer (days) instead of a Duration.
///
/// Called as a fallback when `DataFusion`'s type-coercion rejects
/// `Duration BETWEEN Int`.  Each `a - b` in a SELECT-item position is
/// replaced with `CAST(date_part('epoch', (a - b)) / 86400 AS BIGINT)`.
/// If the subtraction was numeric the retry will fail harmlessly and the
/// original error is returned to the caller.
fn rewrite_date_subtraction_sql(sql: &str) -> Option<String> {
    use sqlparser::ast::{
        BinaryOperator, CastKind, DataType as SqlDT, Expr as SqlE, FunctionArg, FunctionArgExpr,
        FunctionArguments, ObjectNamePart, Query, SelectItem, SetExpr,
    };

    fn rewrite_expr(e: &mut SqlE) {
        match e {
            SqlE::BinaryOp {
                op: BinaryOperator::Minus,
                ..
            } => {
                let original = e.clone();
                let epoch_lit =
                    SqlE::Value(sqlparser::ast::Value::SingleQuotedString("epoch".into()).into());
                let date_part_fn = SqlE::Function(sqlparser::ast::Function {
                    name: sqlparser::ast::ObjectName(vec![ObjectNamePart::Identifier(
                        sqlparser::ast::Ident::new("date_part"),
                    )]),
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(sqlparser::ast::FunctionArgumentList {
                        duplicate_treatment: None,
                        args: vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(epoch_lit)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlE::Nested(Box::new(
                                original,
                            )))),
                        ],
                        clauses: vec![],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                });
                let divided = SqlE::BinaryOp {
                    left: Box::new(date_part_fn),
                    op: BinaryOperator::Divide,
                    right: Box::new(SqlE::Value(
                        sqlparser::ast::Value::Number("86400".into(), false).into(),
                    )),
                };
                *e = SqlE::Cast {
                    expr: Box::new(SqlE::Nested(Box::new(divided))),
                    data_type: SqlDT::BigInt(None),
                    format: None,
                    kind: CastKind::Cast,
                };
            }
            _ => {
                visit_expr_children_mut(e, rewrite_expr);
            }
        }
    }

    fn visit_expr_children_mut(e: &mut SqlE, f: fn(&mut SqlE)) {
        match e {
            SqlE::Nested(inner) | SqlE::UnaryOp { expr: inner, .. } => f(inner),
            SqlE::BinaryOp { left, right, .. } => {
                f(left);
                f(right);
            }
            SqlE::Cast { expr, .. } => f(expr),
            SqlE::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(o) = operand {
                    f(o);
                }
                for cw in conditions {
                    f(&mut cw.condition);
                    f(&mut cw.result);
                }
                if let Some(el) = else_result {
                    f(el);
                }
            }
            SqlE::Between {
                expr, low, high, ..
            } => {
                f(expr);
                f(low);
                f(high);
            }
            SqlE::Function(func) => {
                if let FunctionArguments::List(ref mut list) = func.args {
                    for arg in &mut list.args {
                        match arg {
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(ex))
                            | FunctionArg::Named {
                                arg: FunctionArgExpr::Expr(ex),
                                ..
                            } => f(ex),
                            _ => {}
                        }
                    }
                }
            }
            SqlE::Subquery(q) => rewrite_query(q),
            _ => {}
        }
    }

    fn rewrite_select_items(items: &mut [SelectItem]) {
        for item in items {
            match item {
                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                    rewrite_expr(e);
                }
                _ => {}
            }
        }
    }

    fn rewrite_set_expr(se: &mut SetExpr) {
        match se {
            SetExpr::Select(sel) => rewrite_select_items(&mut sel.projection),
            SetExpr::Query(q) => rewrite_query(q),
            SetExpr::SetOperation { left, right, .. } => {
                rewrite_set_expr(left);
                rewrite_set_expr(right);
            }
            _ => {}
        }
    }

    fn rewrite_query(q: &mut Query) {
        for cte in q.with.iter_mut().flat_map(|w| w.cte_tables.iter_mut()) {
            rewrite_query(&mut cte.query);
        }
        rewrite_set_expr(&mut q.body);
    }

    let dialect = PostgreSqlDialect {};
    let mut stmts = Parser::parse_sql(&dialect, sql).ok()?;
    let stmt = stmts.first_mut()?;
    match stmt {
        Statement::Query(q) => rewrite_query(q),
        Statement::Explain { statement, .. } => {
            if let Statement::Query(q) = statement.as_mut() {
                rewrite_query(q);
            }
        }
        _ => return None,
    }
    Some(stmts[0].to_string())
}

fn rewrite_fulltext_match_sql(sql: &str, fulltext: &HashMap<String, FulltextIndexDef>) -> String {
    let upper = sql.to_uppercase();
    if !upper.contains("FTS_MATCH(") {
        return sql.to_string();
    }
    let Some(open) = upper.find("FTS_MATCH(") else {
        return sql.to_string();
    };
    let start_arg = open + "FTS_MATCH(".len();
    let rem = &sql[start_arg..];
    let Some(end) = rem.find(')') else {
        return sql.to_string();
    };
    let term = rem[..end]
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_string();
    if term.is_empty() {
        return sql.to_string();
    }

    for idx in fulltext.values() {
        let from_a = format!("FROM \"{}\"", idx.table_name);
        let from_b = format!("FROM {}", idx.table_name);
        if upper.contains(&from_a.to_uppercase()) || upper.contains(&from_b.to_uppercase()) {
            let escaped = term.replace('\'', "''");
            let predicate = idx
                .columns
                .iter()
                .map(|c| format!("CAST(\"{c}\" AS VARCHAR) ILIKE '%{escaped}%'"))
                .collect::<Vec<_>>()
                .join(" OR ");
            let replacement = format!("({predicate})");
            return sql.replacen(&sql[open..=(start_arg + end)], &replacement, 1);
        }
    }
    sql.to_string()
}

fn sql_constraints_to_catalog(constraints: &[SqlTableConstraint]) -> Vec<CatalogTableConstraint> {
    let mut out = Vec::new();
    for (idx, c) in constraints.iter().enumerate() {
        match c {
            SqlTableConstraint::PrimaryKey { columns, .. } => {
                out.push(CatalogTableConstraint::PrimaryKey {
                    columns: columns.iter().map(|c| c.column.expr.to_string()).collect(),
                });
            }
            SqlTableConstraint::Unique { name, columns, .. } => {
                out.push(CatalogTableConstraint::Unique {
                    name: name
                        .as_ref()
                        .map_or_else(|| format!("unique_{idx}"), |n| n.value.clone()),
                    columns: columns.iter().map(|c| c.column.expr.to_string()).collect(),
                });
            }
            SqlTableConstraint::Check { name, expr, .. } => {
                out.push(CatalogTableConstraint::Check {
                    name: name
                        .as_ref()
                        .map_or_else(|| format!("check_{idx}"), |n| n.value.clone()),
                    expr: expr.to_string(),
                });
            }
            SqlTableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => {
                out.push(CatalogTableConstraint::ForeignKey {
                    name: name
                        .as_ref()
                        .map_or_else(|| format!("fk_{idx}"), |n| n.value.clone()),
                    columns: columns.iter().map(|c| c.value.clone()).collect(),
                    ref_table: foreign_table.to_string(),
                    ref_columns: referred_columns.iter().map(|c| c.value.clone()).collect(),
                    on_delete: on_delete.as_ref().map(ReferentialAction::to_string),
                    on_update: on_update.as_ref().map(ReferentialAction::to_string),
                });
            }
            _ => {}
        }
    }
    out
}

fn scalar_count(batches: &[RecordBatch]) -> i64 {
    batches
        .iter()
        .filter_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| (0..a.len()).map(move |i| a.value(i)))
        })
        .flatten()
        .sum()
}

/// Rough in-memory size estimate for buffered batches.
fn estimate_batch_bytes(batches: &[RecordBatch]) -> usize {
    batches
        .iter()
        .map(|b| {
            b.columns()
                .iter()
                .map(arrow::array::Array::get_array_memory_size)
                .sum::<usize>()
        })
        .sum()
}

fn catalog_stats_to_df(stats: &TableStatistics, schema: &SchemaRef) -> DfStatistics {
    let column_statistics = schema
        .fields()
        .iter()
        .map(|field| {
            if let Some(col) = stats.columns.get(field.name()) {
                let min_value = col
                    .min_value
                    .as_deref()
                    .and_then(|v| parse_scalar_value(v, field.data_type()))
                    .map_or(Precision::Absent, Precision::Exact);
                let max_value = col
                    .max_value
                    .as_deref()
                    .and_then(|v| parse_scalar_value(v, field.data_type()))
                    .map_or(Precision::Absent, Precision::Exact);
                DfColumnStatistics {
                    null_count: Precision::Exact(col.null_count as usize),
                    max_value,
                    min_value,
                    sum_value: Precision::Absent,
                    distinct_count: col
                        .distinct_count
                        .map_or(Precision::Absent, |v| Precision::Exact(v as usize)),
                    byte_size: Precision::Absent,
                }
            } else {
                DfColumnStatistics::new_unknown()
            }
        })
        .collect();

    DfStatistics {
        num_rows: Precision::Exact(stats.row_count as usize),
        total_byte_size: Precision::Absent,
        column_statistics,
    }
}

fn file_stats_to_df(file_stats: &[FileStats], schema: &SchemaRef) -> Option<DfStatistics> {
    if file_stats.is_empty() {
        return None;
    }
    let mut num_rows = 0usize;
    let mut total_bytes = 0usize;
    let mut has_rows = false;
    for fs in file_stats {
        total_bytes = total_bytes.saturating_add(fs.size_bytes as usize);
        if let Some(rows) = fs.row_count {
            has_rows = true;
            num_rows = num_rows.saturating_add(rows as usize);
        }
    }
    Some(DfStatistics {
        num_rows: if has_rows {
            Precision::Exact(num_rows)
        } else {
            Precision::Absent
        },
        total_byte_size: Precision::Exact(total_bytes),
        column_statistics: vec![DfColumnStatistics::new_unknown(); schema.fields().len()],
    })
}

/// Best-effort parse of a string-encoded value into a `DataFusion`
/// `ScalarValue`, matching the Arrow data type.  Returns `None` for
/// types we cannot round-trip reliably.
fn parse_scalar_value(s: &str, dt: &DataType) -> Option<ScalarValue> {
    match dt {
        DataType::Int8 => s.parse::<i8>().ok().map(|v| ScalarValue::Int8(Some(v))),
        DataType::Int16 => s.parse::<i16>().ok().map(|v| ScalarValue::Int16(Some(v))),
        DataType::Int32 => s.parse::<i32>().ok().map(|v| ScalarValue::Int32(Some(v))),
        DataType::Int64 => s.parse::<i64>().ok().map(|v| ScalarValue::Int64(Some(v))),
        DataType::UInt8 => s.parse::<u8>().ok().map(|v| ScalarValue::UInt8(Some(v))),
        DataType::UInt16 => s.parse::<u16>().ok().map(|v| ScalarValue::UInt16(Some(v))),
        DataType::UInt32 => s.parse::<u32>().ok().map(|v| ScalarValue::UInt32(Some(v))),
        DataType::UInt64 => s.parse::<u64>().ok().map(|v| ScalarValue::UInt64(Some(v))),
        DataType::Float32 => s.parse::<f32>().ok().map(|v| ScalarValue::Float32(Some(v))),
        DataType::Float64 => s.parse::<f64>().ok().map(|v| ScalarValue::Float64(Some(v))),
        DataType::Utf8 | DataType::LargeUtf8 => Some(ScalarValue::Utf8(Some(s.to_string()))),
        DataType::Boolean => s
            .parse::<bool>()
            .ok()
            .map(|v| ScalarValue::Boolean(Some(v))),
        DataType::Date32 => s.parse::<chrono::NaiveDate>().ok().map(|d| {
            ScalarValue::Date32(Some(
                (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32,
            ))
        }),
        _ => None,
    }
}

/// Extracts per-column min/max values from Parquet row-group
/// statistics and merges them into the provided maps.
fn collect_minmax_from_parquet(
    pq_meta: &parquet::file::metadata::ParquetMetaData,
    min_values: &mut HashMap<String, String>,
    max_values: &mut HashMap<String, String>,
) {
    let schema_descr = pq_meta.file_metadata().schema_descr();
    for rg in pq_meta.row_groups() {
        for (i, col) in rg.columns().iter().enumerate() {
            let col_name = schema_descr.column(i).name().to_string();
            let Some(stats) = col.statistics() else {
                continue;
            };
            let Some(min_bytes) = stats.min_bytes_opt() else {
                continue;
            };
            let Some(max_bytes) = stats.max_bytes_opt() else {
                continue;
            };
            let min_s = String::from_utf8_lossy(min_bytes).to_string();
            let max_s = String::from_utf8_lossy(max_bytes).to_string();
            min_values
                .entry(col_name.clone())
                .and_modify(|v| {
                    if min_s < *v {
                        v.clone_from(&min_s);
                    }
                })
                .or_insert(min_s);
            max_values
                .entry(col_name)
                .and_modify(|v| {
                    if max_s > *v {
                        v.clone_from(&max_s);
                    }
                })
                .or_insert(max_s);
        }
    }
}

fn expr_to_i64(expr: &sqlparser::ast::Expr) -> Result<i64, BoxError> {
    expr.to_string()
        .trim_matches('\'')
        .parse::<i64>()
        .map_err(|_| format!("Expected integer expression, got '{expr}'").into())
}

fn resolve_conflict_columns(
    table_meta: &TableMeta,
    on_conflict: &OnConflict,
) -> Result<Vec<String>, BoxError> {
    if let Some(target) = &on_conflict.conflict_target {
        return Ok(match target {
            sqlparser::ast::ConflictTarget::Columns(cols) => {
                cols.iter().map(|c| c.value.clone()).collect()
            }
            sqlparser::ast::ConflictTarget::OnConstraint(name) => {
                let constraint_name = name.to_string().trim_matches('"').to_string();
                table_meta
                    .constraints
                    .iter()
                    .find_map(|c| match c {
                        CatalogTableConstraint::Unique { name, columns }
                            if *name == constraint_name =>
                        {
                            Some(columns.clone())
                        }
                        CatalogTableConstraint::PrimaryKey { columns }
                            if constraint_name.eq_ignore_ascii_case("primary")
                                || constraint_name.eq_ignore_ascii_case("primary_key") =>
                        {
                            Some(columns.clone())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        format!("Unknown ON CONFLICT constraint target: {constraint_name}")
                    })?
            }
        });
    }

    for c in &table_meta.constraints {
        match c {
            CatalogTableConstraint::PrimaryKey { columns }
            | CatalogTableConstraint::Unique { columns, .. } => return Ok(columns.clone()),
            CatalogTableConstraint::Check { .. } | CatalogTableConstraint::ForeignKey { .. } => {}
        }
    }
    Ok(Vec::new())
}

/// Extracts a string representation from an array cell for FTS tokenization.
fn array_value_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return String::new();
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int8Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int16Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return a.value(row).to_string();
    }
    String::new()
}

fn array_value_to_sql_literal(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int8Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int16Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<UInt64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Float32Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return if a.value(row) { "TRUE" } else { "FALSE" }.to_string();
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return format!("'{}'", a.value(row).replace('\'', "''"));
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return format!("'{}'", a.value(row).replace('\'', "''"));
    }
    if matches!(array.data_type(), DataType::FixedSizeBinary(16)) {
        if let Some(a) = array
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        {
            let bytes = a.value(row);
            if bytes.len() == 16 {
                let uuid = format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    bytes[0],
                    bytes[1],
                    bytes[2],
                    bytes[3],
                    bytes[4],
                    bytes[5],
                    bytes[6],
                    bytes[7],
                    bytes[8],
                    bytes[9],
                    bytes[10],
                    bytes[11],
                    bytes[12],
                    bytes[13],
                    bytes[14],
                    bytes[15]
                );
                return format!("'{uuid}'");
            }
        }
    }
    format!(
        "'{}'",
        format!("{:?}", array.slice(row, 1)).replace('\'', "''")
    )
}

// SQL helper routines are maintained in `sql_helpers` to keep this file smaller.

/// Converts a sqlparser column definition into a catalog [`ColumnDef`].
fn sql_column_to_catalog(col: &SqlColumnDef) -> ColumnDef {
    let nullable = !col
        .options
        .iter()
        .any(|opt| matches!(opt.option, ColumnOption::NotNull));
    ColumnDef {
        name: col.name.value.clone(),
        data_type: col.data_type.to_string(),
        nullable,
    }
}

/// Builds an Arrow [`Schema`] from a slice of catalog [`ColumnDef`]s.
fn columns_to_schema(columns: &[ColumnDef]) -> Result<SchemaRef, BoxError> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = sql_string_to_arrow(&col.data_type)?;
            let mut metadata = HashMap::new();
            metadata.insert("potatodb.sql_type".to_string(), col.data_type.clone());
            Ok(Field::new(&col.name, dt, col.nullable).with_metadata(metadata))
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Parses a SQL type string into an Arrow [`DataType`] by round-tripping
/// through sqlparser.
fn sql_string_to_arrow(sql_type: &str) -> Result<DataType, BoxError> {
    let dialect = PostgreSqlDialect {};
    let dummy = format!("CREATE TABLE _t (_c {sql_type})");
    let stmts = Parser::parse_sql(&dialect, &dummy)?;
    if let Statement::CreateTable(create) = &stmts[0] {
        if let Some(col) = create.columns.first() {
            return sqlparser_type_to_arrow(&col.data_type);
        }
    }
    Err(format!("Cannot parse SQL type: {sql_type}").into())
}

/// Maps a sqlparser [`SqlDataType`] to an Arrow [`DataType`].
fn sqlparser_type_to_arrow(sql_type: &SqlDataType) -> Result<DataType, BoxError> {
    let sql_upper = sql_type.to_string().to_uppercase();
    if sql_upper == "UUID" {
        return Ok(DataType::FixedSizeBinary(16));
    }
    if sql_upper.starts_with("INTERVAL") {
        return Ok(DataType::Duration(TimeUnit::Microsecond));
    }
    if sql_upper == "JSON" || sql_upper == "JSONB" {
        return Ok(DataType::Utf8);
    }
    if let Some(inner) = sql_upper.strip_suffix("[]") {
        let inner = inner.trim();
        if !inner.is_empty() {
            let inner_dt = sql_string_to_arrow(inner)?;
            return Ok(DataType::List(Arc::new(Field::new("item", inner_dt, true))));
        }
    }
    if let Some(inner) = sql_upper
        .strip_prefix("ARRAY<")
        .and_then(|s| s.strip_suffix('>'))
    {
        let inner = inner.trim();
        let inner_dt = sql_string_to_arrow(inner)?;
        return Ok(DataType::List(Arc::new(Field::new("item", inner_dt, true))));
    }
    match sql_type {
        SqlDataType::Boolean => Ok(DataType::Boolean),
        SqlDataType::TinyInt(_) => Ok(DataType::Int8),
        SqlDataType::SmallInt(_) => Ok(DataType::Int16),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(DataType::Int32),
        SqlDataType::BigInt(_) => Ok(DataType::Int64),
        SqlDataType::Real => Ok(DataType::Float32),
        SqlDataType::Float(_) | SqlDataType::Double(_) | SqlDataType::DoublePrecision => {
            Ok(DataType::Float64)
        }
        SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::Char(_)
        | SqlDataType::CharVarying(_)
        | SqlDataType::String(_) => Ok(DataType::Utf8),
        SqlDataType::Date => Ok(DataType::Date32),
        SqlDataType::Timestamp(_, tz_info) => {
            let tz = match tz_info {
                TimezoneInfo::WithTimeZone => Some(Arc::from("UTC")),
                _ => None,
            };
            Ok(DataType::Timestamp(TimeUnit::Microsecond, tz))
        }
        SqlDataType::Numeric(info) | SqlDataType::Decimal(info) | SqlDataType::Dec(info) => {
            let (precision, scale) = match info {
                ExactNumberInfo::PrecisionAndScale(p, s) => (*p as u8, *s as i8),
                ExactNumberInfo::Precision(p) => (*p as u8, 0),
                ExactNumberInfo::None => (38, 10),
            };
            Ok(DataType::Decimal128(precision, scale))
        }
        SqlDataType::Bytea | SqlDataType::Blob(_) => Ok(DataType::Binary),
        other => Err(format!("Unsupported SQL type: {other}").into()),
    }
}

/// Maps a SQL type string to a `PostgreSQL` `pg_type` OID for `pg_catalog` compatibility.
fn sql_type_to_pg_oid(sql_type: &str) -> i32 {
    let upper = sql_type.to_uppercase();
    if upper.contains("BIGINT") {
        20 // int8
    } else if upper.contains("SMALLINT") {
        21 // int2
    } else if upper.contains("INT") {
        23 // int4
    } else if upper.contains("VARCHAR") || upper.contains("CHAR") {
        1043 // varchar
    } else if upper.contains("BOOL") {
        16 // bool
    } else if upper.contains("FLOAT") || upper.contains("DOUBLE") {
        701 // float8
    } else if upper.contains("REAL") {
        700 // float4
    } else if upper == "TEXT" || upper.contains("TEXT") {
        25 // text
    } else if upper.contains("TIMESTAMP") {
        1114 // timestamp
    } else if upper.contains("DATE") {
        1082 // date
    } else if upper.contains("UUID") {
        2950 // uuid
    } else if upper.contains("DECIMAL") || upper.contains("NUMERIC") {
        1700 // numeric
    } else {
        25 // default to text
    }
}

/// Best-effort conversion from Arrow [`DataType`] back to a SQL type string.
/// Used by `CREATE TABLE ... AS SELECT` to infer column types.
fn arrow_type_to_sql_string(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 | DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 | DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 | DataType::UInt32 => "INT".to_string(),
        DataType::Int64 | DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMP WITH TIME ZONE".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Duration(_) => "INTERVAL".to_string(),
        DataType::FixedSizeBinary(16) => "UUID".to_string(),
        DataType::List(field) | DataType::LargeList(field) => {
            format!("{}[]", arrow_type_to_sql_string(field.data_type()))
        }
        DataType::Decimal128(p, s) => format!("DECIMAL({p},{s})"),
        DataType::Binary | DataType::LargeBinary => "BYTEA".to_string(),
        _ => "VARCHAR".to_string(),
    }
}

/// Splits a SQL script into individual statements on `;` delimiters.
///
/// Correctly handles:
/// - Single-quoted string literals (including `''` escapes)
/// - `--` line comments
/// - `/* ... */` block comments (including nested)
/// - Strips leading/trailing whitespace from each statement
/// - Skips empty statements
fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            // Single-quoted string literal — consume until matching quote.
            '\'' => {
                current.push(chars.next().unwrap());
                loop {
                    match chars.next() {
                        Some('\'') => {
                            current.push('\'');
                            // Escaped quote ('')
                            if chars.peek() == Some(&'\'') {
                                current.push(chars.next().unwrap());
                            } else {
                                break;
                            }
                        }
                        Some(c) => current.push(c),
                        None => break,
                    }
                }
            }
            // Possible comment start
            '-' => {
                chars.next();
                if chars.peek() == Some(&'-') {
                    // Line comment — skip to end of line.
                    chars.next();
                    while let Some(&c) = chars.peek() {
                        chars.next();
                        if c == '\n' {
                            current.push(' ');
                            break;
                        }
                    }
                } else {
                    current.push('-');
                }
            }
            '/' => {
                chars.next();
                if chars.peek() == Some(&'*') {
                    // Block comment — skip until */.
                    chars.next();
                    let mut depth = 1u32;
                    while depth > 0 {
                        match chars.next() {
                            Some('*') if chars.peek() == Some(&'/') => {
                                chars.next();
                                depth -= 1;
                            }
                            Some('/') if chars.peek() == Some(&'*') => {
                                chars.next();
                                depth += 1;
                            }
                            Some(_) => {}
                            None => break,
                        }
                    }
                    current.push(' ');
                } else {
                    current.push('/');
                }
            }
            // Statement terminator
            ';' => {
                chars.next();
                let stmt = current.trim().to_string();
                if !stmt.is_empty() {
                    statements.push(format!("{stmt};"));
                }
                current.clear();
            }
            // Normal character
            _ => {
                current.push(chars.next().unwrap());
            }
        }
    }

    // Trailing statement without semicolon
    let stmt = current.trim().to_string();
    if !stmt.is_empty() {
        statements.push(stmt);
    }

    statements
}
