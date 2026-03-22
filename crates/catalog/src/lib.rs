//! Persistent catalog that stores table, index, and view metadata.
//!
//! The catalog is serialized as JSON and persisted through an [`ObjectStore`],
//! making it work identically for local filesystems and S3.
//!
//! When an explicit transaction is active (`in_transaction == true`),
//! mutations accumulate in memory and [`save`](Catalog::save) becomes a
//! no-op.  Call [`force_save`](Catalog::force_save) (on `COMMIT`) to
//! flush, or [`restore`](Catalog::restore) (on `ROLLBACK`) to discard.

use std::collections::HashMap;
use std::sync::Arc;

use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A single column definition within a table schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// SQL type stored as a string (e.g. `"INT"`, `"VARCHAR"`).
    pub data_type: String,
    /// Whether the column accepts NULL values.
    pub nullable: bool,
}

/// Per-column statistics collected by `ANALYZE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    pub null_count: u64,
    pub distinct_count: Option<u64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

/// Aggregate statistics for a table, collected by `ANALYZE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    pub row_count: u64,
    pub columns: HashMap<String, ColumnStatistics>,
}

/// Lightweight per-file statistics captured from Parquet footers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStats {
    pub path: String,
    pub row_count: Option<u64>,
    #[serde(default)]
    pub min_values: HashMap<String, String>,
    #[serde(default)]
    pub max_values: HashMap<String, String>,
    pub size_bytes: u64,
    pub created_at: Option<i64>,
}

/// Persisted table-level constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<String>,
    },
    Unique {
        name: String,
        columns: Vec<String>,
    },
    Check {
        name: String,
        expr: String,
    },
    ForeignKey {
        name: String,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: Option<String>,
        on_update: Option<String>,
    },
}

/// A single deletion-vector entry tracking deleted row indices in a file.
/// Used for future incremental DML optimizations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionEntry {
    pub file_path: String,
    pub deleted_row_indices: Vec<u64>,
    pub timestamp_ms: i64,
}

/// Metadata for a single table, persisted in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    /// Table name.
    pub name: String,
    /// Ordered list of columns that make up the table schema.
    pub columns: Vec<ColumnDef>,
    /// Storage location: local directory, `s3://...`, or `memory://...` table listing URL.
    pub path: String,
    /// Hive-style partition columns (empty when unpartitioned).
    #[serde(default)]
    pub partition_columns: Vec<String>,
    /// Optional statistics from the last `ANALYZE` run.
    #[serde(default)]
    pub statistics: Option<TableStatistics>,
    /// Optional retention policy in seconds.
    #[serde(default)]
    pub retention_seconds: Option<u64>,
    /// Table-level constraints (`PRIMARY KEY`, `UNIQUE`, `CHECK`).
    #[serde(default)]
    pub constraints: Vec<TableConstraint>,
    /// Optional per-file stats for predicate-aware pruning and diagnostics.
    #[serde(default)]
    pub file_stats: Vec<FileStats>,
    /// Deletion vectors for incremental UPDATE/DELETE (foundational metadata).
    #[serde(default)]
    pub deletion_vectors: Vec<DeletionEntry>,
}

/// A single column reference within an index, with its sort direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexColumn {
    /// Column name.
    pub name: String,
    /// `true` for ASC, `false` for DESC.
    pub ascending: bool,
}

/// Metadata for a sort index, persisted in the catalog.
///
/// An index defines the physical sort order of a table's Parquet files.
/// `DataFusion` uses this to skip row groups, avoid re-sorting for
/// `ORDER BY`, and terminate `LIMIT` queries early.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    /// Index name (unique across the catalog).
    pub name: String,
    /// Name of the table this index belongs to.
    pub table_name: String,
    /// Columns that define the sort order.
    pub columns: Vec<IndexColumn>,
    /// Whether this index is advisory-only and does not reflect current
    /// physical parquet order.
    #[serde(default)]
    pub logical_only: bool,
    /// Whether this index is the table's primary physical ordering.
    #[serde(default)]
    pub primary: bool,
}

/// Metadata for a view, persisted in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewDef {
    /// View name (unique across the catalog).
    pub name: String,
    /// The SQL query that defines this view.
    pub sql: String,
    /// Whether this is a materialized view.
    #[serde(default)]
    pub materialized: bool,
    /// Backing table name for materialized views.
    #[serde(default)]
    pub backing_table: Option<String>,
}

/// Metadata for a sequence object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceDef {
    pub name: String,
    pub current_value: i64,
    pub increment: i64,
    pub min_value: Option<i64>,
    pub max_value: Option<i64>,
}

/// Metadata for a lightweight SQL function macro.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UdfDef {
    pub name: String,
    pub args: Vec<String>,
    pub return_type: String,
    pub body: String,
}

/// A structured privilege entry (e.g. `SELECT ON orders`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Privilege {
    /// Privilege kind: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALL`, or a custom string.
    pub kind: String,
    /// Optional table scope. `None` means the privilege applies to all tables.
    pub table: Option<String>,
}

/// Persisted user account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDef {
    pub name: String,
    #[serde(default)]
    pub password: String,
}

/// Persisted role definition with its privileges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleDef {
    pub name: String,
    #[serde(default)]
    pub privileges: Vec<Privilege>,
}

/// Persisted trigger definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerDef {
    pub name: String,
    pub table: String,
    pub event: String,
    pub timing: String,
    pub body: String,
}

/// A single migration record for schema versioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub version: u64,
    pub description: String,
    pub sql: String,
    pub applied_at_ms: i64,
}

/// On-disk representation of the catalog JSON.
#[derive(Serialize, Deserialize)]
struct CatalogData {
    tables: HashMap<String, TableMeta>,
    #[serde(default)]
    indexes: HashMap<String, IndexDef>,
    #[serde(default)]
    views: HashMap<String, ViewDef>,
    #[serde(default)]
    sequences: HashMap<String, SequenceDef>,
    #[serde(default)]
    udfs: HashMap<String, UdfDef>,
    #[serde(default)]
    users: HashMap<String, UserDef>,
    #[serde(default)]
    roles: HashMap<String, RoleDef>,
    #[serde(default)]
    user_roles: HashMap<String, Vec<String>>,
    #[serde(default)]
    triggers: HashMap<String, TriggerDef>,
    #[serde(default)]
    schema_version: u64,
    #[serde(default)]
    migrations: Vec<MigrationRecord>,
}

/// A point-in-time snapshot of the full catalog state, captured at
/// `BEGIN` and used by `ROLLBACK` to restore state.
#[derive(Clone)]
pub struct CatalogSnapshot {
    pub tables: HashMap<String, TableMeta>,
    pub indexes: HashMap<String, IndexDef>,
    pub views: HashMap<String, ViewDef>,
    pub sequences: HashMap<String, SequenceDef>,
    pub udfs: HashMap<String, UdfDef>,
    pub users: HashMap<String, UserDef>,
    pub roles: HashMap<String, RoleDef>,
    pub user_roles: HashMap<String, Vec<String>>,
    pub triggers: HashMap<String, TriggerDef>,
    pub schema_version: u64,
    pub migrations: Vec<MigrationRecord>,
}

/// In-memory catalog backed by an [`ObjectStore`] for persistence.
///
/// Every mutation (add/remove table, index, or view) is followed by a full
/// save to the backing store -- unless an explicit transaction is
/// active, in which case saves are deferred until `COMMIT`.
pub struct Catalog {
    /// All registered tables, keyed by table name.
    pub tables: HashMap<String, TableMeta>,
    /// All registered indexes, keyed by index name.
    pub indexes: HashMap<String, IndexDef>,
    /// All registered views, keyed by view name.
    pub views: HashMap<String, ViewDef>,
    /// All registered sequences, keyed by sequence name.
    pub sequences: HashMap<String, SequenceDef>,
    /// All registered SQL functions, keyed by function name.
    pub udfs: HashMap<String, UdfDef>,
    /// Persisted user accounts, keyed by username.
    pub users: HashMap<String, UserDef>,
    /// Persisted role definitions, keyed by role name.
    pub roles: HashMap<String, RoleDef>,
    /// Maps username -> list of role names.
    pub user_roles: HashMap<String, Vec<String>>,
    /// Persisted trigger definitions, keyed by trigger name.
    pub triggers: HashMap<String, TriggerDef>,
    /// Schema version number for migrations.
    pub schema_version: u64,
    /// Migration records (version, description, SQL, `applied_at_ms`).
    pub migrations: Vec<MigrationRecord>,
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    /// When `true`, [`save`](Self::save) is a no-op; mutations
    /// accumulate in memory until [`force_save`](Self::force_save).
    in_transaction: bool,
    /// Set by mutations; cleared by [`flush_if_dirty`](Self::flush_if_dirty).
    dirty: bool,
}

impl Catalog {
    /// Loads the catalog from the backing store.
    ///
    /// If the catalog file does not exist yet (first run), an empty
    /// catalog is returned. Legacy catalog files that contain only a
    /// tables map are upgraded transparently.
    ///
    /// # Errors
    ///
    /// Returns an error if the backing store cannot be read or the
    /// catalog data cannot be deserialized.
    pub async fn load(
        store: Arc<dyn ObjectStore>,
        catalog_path: ObjPath,
    ) -> Result<Self, BoxError> {
        match store.get(&catalog_path).await {
            Ok(result) => {
                let bytes = result.bytes().await?;
                if let Ok(data) = serde_json::from_slice::<CatalogData>(&bytes) {
                    Ok(Self {
                        tables: data.tables,
                        indexes: data.indexes,
                        views: data.views,
                        sequences: data.sequences,
                        udfs: data.udfs,
                        users: data.users,
                        roles: data.roles,
                        user_roles: data.user_roles,
                        triggers: data.triggers,
                        schema_version: data.schema_version,
                        migrations: data.migrations,
                        store,
                        path: catalog_path,
                        in_transaction: false,
                        dirty: false,
                    })
                } else {
                    let tables: HashMap<String, TableMeta> = serde_json::from_slice(&bytes)?;
                    Ok(Self {
                        tables,
                        indexes: HashMap::new(),
                        views: HashMap::new(),
                        sequences: HashMap::new(),
                        udfs: HashMap::new(),
                        users: HashMap::new(),
                        roles: HashMap::new(),
                        user_roles: HashMap::new(),
                        triggers: HashMap::new(),
                        schema_version: 0,
                        migrations: Vec::new(),
                        store,
                        path: catalog_path,
                        in_transaction: false,
                        dirty: false,
                    })
                }
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Self {
                tables: HashMap::new(),
                indexes: HashMap::new(),
                views: HashMap::new(),
                sequences: HashMap::new(),
                udfs: HashMap::new(),
                users: HashMap::new(),
                roles: HashMap::new(),
                user_roles: HashMap::new(),
                triggers: HashMap::new(),
                schema_version: 0,
                migrations: Vec::new(),
                store,
                path: catalog_path,
                in_transaction: false,
                dirty: false,
            }),
            Err(e) => Err(e.into()),
        }
    }

    /// Serializes the full catalog to the backing store.
    ///
    /// This is a **no-op** while an explicit transaction is active.
    /// Use [`force_save`](Self::force_save) from `COMMIT` to persist.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or the backing store write fails.
    #[allow(clippy::unused_async)]
    pub async fn save(&mut self) -> Result<(), BoxError> {
        if self.in_transaction {
            return Ok(());
        }
        self.dirty = true;
        Ok(())
    }

    /// Writes the catalog to the backing store if any mutations have
    /// occurred since the last flush.  Called once at the end of each
    /// statement to batch multiple in-memory mutations into a single
    /// I/O operation.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or the backing store write fails.
    pub async fn flush_if_dirty(&mut self) -> Result<(), BoxError> {
        if !self.dirty {
            return Ok(());
        }
        self.dirty = false;
        self.write_to_store().await
    }

    /// Persists the catalog regardless of transaction state.
    ///
    /// Called by `COMMIT` to flush deferred mutations.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or the backing store write fails.
    pub async fn force_save(&mut self) -> Result<(), BoxError> {
        self.dirty = false;
        self.write_to_store().await
    }

    async fn write_to_store(&self) -> Result<(), BoxError> {
        let data = CatalogData {
            tables: self.tables.clone(),
            indexes: self.indexes.clone(),
            views: self.views.clone(),
            sequences: self.sequences.clone(),
            udfs: self.udfs.clone(),
            users: self.users.clone(),
            roles: self.roles.clone(),
            user_roles: self.user_roles.clone(),
            triggers: self.triggers.clone(),
            schema_version: self.schema_version,
            migrations: self.migrations.clone(),
        };
        let json = serde_json::to_string_pretty(&data)?;
        let payload = PutPayload::from_bytes(json.into_bytes().into());
        self.store.put(&self.path, payload).await?;
        Ok(())
    }

    /// Captures a point-in-time snapshot of all tables, indexes, and views.
    #[must_use]
    pub fn snapshot(&self) -> CatalogSnapshot {
        CatalogSnapshot {
            tables: self.tables.clone(),
            indexes: self.indexes.clone(),
            views: self.views.clone(),
            sequences: self.sequences.clone(),
            udfs: self.udfs.clone(),
            users: self.users.clone(),
            roles: self.roles.clone(),
            user_roles: self.user_roles.clone(),
            triggers: self.triggers.clone(),
            schema_version: self.schema_version,
            migrations: self.migrations.clone(),
        }
    }

    /// Restores all catalog state from a previously captured snapshot.
    pub fn restore(&mut self, snap: CatalogSnapshot) {
        self.tables = snap.tables;
        self.indexes = snap.indexes;
        self.views = snap.views;
        self.sequences = snap.sequences;
        self.udfs = snap.udfs;
        self.users = snap.users;
        self.roles = snap.roles;
        self.user_roles = snap.user_roles;
        self.triggers = snap.triggers;
        self.schema_version = snap.schema_version;
        self.migrations = snap.migrations;
    }

    /// Enables or disables transaction mode.
    ///
    /// While `true`, [`save`](Self::save) becomes a no-op.
    pub const fn set_in_transaction(&mut self, active: bool) {
        self.in_transaction = active;
    }

    /// Returns whether the catalog is inside an explicit transaction.
    #[must_use]
    pub const fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Registers a new table and persists the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn add_table(&mut self, meta: TableMeta) -> Result<(), BoxError> {
        self.tables.insert(meta.name.clone(), meta);
        self.save().await
    }

    /// Removes a table and all of its associated indexes, then persists.
    ///
    /// Returns the removed [`TableMeta`] if the table existed.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn remove_table(&mut self, name: &str) -> Result<Option<TableMeta>, BoxError> {
        let meta = self.tables.remove(name);
        self.indexes.retain(|_, idx| idx.table_name != name);
        self.save().await?;
        Ok(meta)
    }

    /// Registers a new index and persists the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn add_index(&mut self, def: IndexDef) -> Result<(), BoxError> {
        self.indexes.insert(def.name.clone(), def);
        self.save().await
    }

    /// Removes an index by name and persists the catalog.
    ///
    /// Returns the removed [`IndexDef`] if it existed.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn remove_index(&mut self, name: &str) -> Result<Option<IndexDef>, BoxError> {
        let def = self.indexes.remove(name);
        self.save().await?;
        Ok(def)
    }

    /// Returns all indexes that belong to the given table.
    #[must_use]
    pub fn indexes_for_table(&self, table_name: &str) -> Vec<&IndexDef> {
        self.indexes
            .values()
            .filter(|idx| idx.table_name == table_name)
            .collect()
    }

    /// Registers a new view and persists the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn add_view(&mut self, def: ViewDef) -> Result<(), BoxError> {
        self.views.insert(def.name.clone(), def);
        self.save().await
    }

    /// Removes a view by name and persists the catalog.
    ///
    /// Returns the removed [`ViewDef`] if it existed.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn remove_view(&mut self, name: &str) -> Result<Option<ViewDef>, BoxError> {
        let def = self.views.remove(name);
        self.save().await?;
        Ok(def)
    }

    /// Stores table statistics collected by ANALYZE.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn set_statistics(
        &mut self,
        table_name: &str,
        stats: TableStatistics,
    ) -> Result<(), BoxError> {
        if let Some(meta) = self.tables.get_mut(table_name) {
            meta.statistics = Some(stats);
            self.save().await?;
        }
        Ok(())
    }

    /// Stores per-file statistics for a table.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn set_file_stats(
        &mut self,
        table_name: &str,
        file_stats: Vec<FileStats>,
    ) -> Result<(), BoxError> {
        if let Some(meta) = self.tables.get_mut(table_name) {
            meta.file_stats = file_stats;
            self.save().await?;
        }
        Ok(())
    }

    /// Registers a new sequence and persists the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn add_sequence(&mut self, def: SequenceDef) -> Result<(), BoxError> {
        self.sequences.insert(def.name.clone(), def);
        self.save().await
    }

    /// Registers a SQL function and persists the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn add_udf(&mut self, def: UdfDef) -> Result<(), BoxError> {
        self.udfs.insert(def.name.clone(), def);
        self.save().await
    }

    /// Removes a SQL function by name and persists the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn remove_udf(&mut self, name: &str) -> Result<Option<UdfDef>, BoxError> {
        let def = self.udfs.remove(name);
        self.save().await?;
        Ok(def)
    }

    /// Removes a sequence by name and persists the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if persisting the catalog fails.
    pub async fn remove_sequence(&mut self, name: &str) -> Result<Option<SequenceDef>, BoxError> {
        let def = self.sequences.remove(name);
        self.save().await?;
        Ok(def)
    }

    /// Returns the next value for a sequence and persists the updated state.
    ///
    /// # Errors
    ///
    /// Returns an error if the sequence does not exist, has reached its
    /// min/max bounds, or if persisting the catalog fails.
    pub async fn next_sequence_value(&mut self, name: &str) -> Result<i64, BoxError> {
        let seq = self
            .sequences
            .get_mut(name)
            .ok_or_else(|| format!("Sequence '{name}' does not exist"))?;

        let next = seq.current_value;
        let advanced = seq.current_value.saturating_add(seq.increment);
        if let Some(min) = seq.min_value
            && advanced < min
        {
            return Err(format!("Sequence '{name}' reached minimum value").into());
        }
        if let Some(max) = seq.max_value
            && advanced > max
        {
            return Err(format!("Sequence '{name}' reached maximum value").into());
        }
        seq.current_value = advanced;
        self.save().await?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjPath;

    async fn test_catalog(tmp: &std::path::Path) -> Catalog {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(tmp).unwrap());
        Catalog::load(store, ObjPath::from("catalog.json"))
            .await
            .unwrap()
    }

    fn sample_table(name: &str) -> TableMeta {
        TableMeta {
            name: name.to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "INT".to_string(),
                    nullable: false,
                },
                ColumnDef {
                    name: "val".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                },
            ],
            path: format!("/tmp/{name}"),
            partition_columns: vec![],
            statistics: None,
            retention_seconds: None,
            constraints: vec![],
            file_stats: vec![],
            deletion_vectors: vec![],
        }
    }

    #[tokio::test]
    async fn test_empty_catalog_load() {
        let tmp = tempfile::tempdir().unwrap();
        let catalog = test_catalog(tmp.path()).await;
        assert!(catalog.tables.is_empty());
        assert!(catalog.indexes.is_empty());
        assert!(catalog.views.is_empty());
        assert!(catalog.sequences.is_empty());
        assert!(catalog.udfs.is_empty());
        assert!(!catalog.in_transaction());
    }

    #[tokio::test]
    async fn test_add_and_remove_table() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog.add_table(sample_table("users")).await.unwrap();
        assert_eq!(catalog.tables.len(), 1);
        assert!(catalog.tables.contains_key("users"));

        let removed = catalog.remove_table("users").await.unwrap();
        assert!(removed.is_some());
        assert!(catalog.tables.is_empty());

        let removed_again = catalog.remove_table("users").await.unwrap();
        assert!(removed_again.is_none());
    }

    #[tokio::test]
    async fn test_add_and_remove_index() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog.add_table(sample_table("events")).await.unwrap();
        catalog
            .add_index(IndexDef {
                name: "idx_events_id".to_string(),
                table_name: "events".to_string(),
                columns: vec![IndexColumn {
                    name: "id".to_string(),
                    ascending: true,
                }],
                logical_only: false,
                primary: true,
            })
            .await
            .unwrap();

        assert_eq!(catalog.indexes.len(), 1);
        assert_eq!(catalog.indexes_for_table("events").len(), 1);
        assert!(catalog.indexes_for_table("other").is_empty());

        let removed = catalog.remove_index("idx_events_id").await.unwrap();
        assert!(removed.is_some());
        assert!(catalog.indexes.is_empty());
    }

    #[tokio::test]
    async fn test_remove_table_cascades_indexes() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog.add_table(sample_table("t")).await.unwrap();
        catalog
            .add_index(IndexDef {
                name: "idx1".to_string(),
                table_name: "t".to_string(),
                columns: vec![IndexColumn {
                    name: "id".to_string(),
                    ascending: true,
                }],
                logical_only: false,
                primary: true,
            })
            .await
            .unwrap();
        catalog
            .add_index(IndexDef {
                name: "idx2".to_string(),
                table_name: "t".to_string(),
                columns: vec![IndexColumn {
                    name: "val".to_string(),
                    ascending: false,
                }],
                logical_only: true,
                primary: false,
            })
            .await
            .unwrap();
        assert_eq!(catalog.indexes.len(), 2);

        catalog.remove_table("t").await.unwrap();
        assert!(
            catalog.indexes.is_empty(),
            "indexes should be removed with table"
        );
    }

    #[tokio::test]
    async fn test_add_and_remove_view() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog
            .add_view(ViewDef {
                name: "v1".to_string(),
                sql: "SELECT 1".to_string(),
                materialized: false,
                backing_table: None,
            })
            .await
            .unwrap();
        assert_eq!(catalog.views.len(), 1);

        let removed = catalog.remove_view("v1").await.unwrap();
        assert!(removed.is_some());
        assert!(catalog.views.is_empty());
    }

    #[tokio::test]
    async fn test_add_and_remove_sequence() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog
            .add_sequence(SequenceDef {
                name: "seq1".to_string(),
                current_value: 1,
                increment: 1,
                min_value: None,
                max_value: None,
            })
            .await
            .unwrap();
        assert_eq!(catalog.sequences.len(), 1);

        let removed = catalog.remove_sequence("seq1").await.unwrap();
        assert!(removed.is_some());
        assert!(catalog.sequences.is_empty());
    }

    #[tokio::test]
    async fn test_next_sequence_value() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog
            .add_sequence(SequenceDef {
                name: "counter".to_string(),
                current_value: 1,
                increment: 1,
                min_value: None,
                max_value: None,
            })
            .await
            .unwrap();

        assert_eq!(catalog.next_sequence_value("counter").await.unwrap(), 1);
        assert_eq!(catalog.next_sequence_value("counter").await.unwrap(), 2);
        assert_eq!(catalog.next_sequence_value("counter").await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_sequence_max_bound() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog
            .add_sequence(SequenceDef {
                name: "bounded".to_string(),
                current_value: 1,
                increment: 1,
                min_value: None,
                max_value: Some(2),
            })
            .await
            .unwrap();

        assert_eq!(catalog.next_sequence_value("bounded").await.unwrap(), 1);
        assert!(
            catalog.next_sequence_value("bounded").await.is_err(),
            "should fail at max boundary"
        );
    }

    #[tokio::test]
    async fn test_sequence_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        assert!(catalog.next_sequence_value("nope").await.is_err());
    }

    #[tokio::test]
    async fn test_snapshot_and_restore() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog.add_table(sample_table("t")).await.unwrap();
        let snap = catalog.snapshot();

        catalog.add_table(sample_table("t2")).await.unwrap();
        assert_eq!(catalog.tables.len(), 2);

        catalog.restore(snap);
        assert_eq!(catalog.tables.len(), 1);
        assert!(catalog.tables.contains_key("t"));
        assert!(!catalog.tables.contains_key("t2"));
    }

    #[tokio::test]
    async fn test_transaction_mode_defers_save() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog.set_in_transaction(true);
        assert!(catalog.in_transaction());

        catalog.add_table(sample_table("t")).await.unwrap();

        let catalog2 = test_catalog(tmp.path()).await;
        assert!(
            catalog2.tables.is_empty(),
            "save should be deferred in transaction mode"
        );

        catalog.force_save().await.unwrap();

        let catalog3 = test_catalog(tmp.path()).await;
        assert_eq!(catalog3.tables.len(), 1, "force_save should persist");
    }

    #[tokio::test]
    async fn test_persistence_round_trip() {
        let tmp = tempfile::tempdir().unwrap();

        {
            let mut catalog = test_catalog(tmp.path()).await;
            catalog.add_table(sample_table("users")).await.unwrap();
            catalog
                .add_index(IndexDef {
                    name: "idx".to_string(),
                    table_name: "users".to_string(),
                    columns: vec![IndexColumn {
                        name: "id".to_string(),
                        ascending: true,
                    }],
                    logical_only: false,
                    primary: true,
                })
                .await
                .unwrap();
            catalog
                .add_view(ViewDef {
                    name: "v".to_string(),
                    sql: "SELECT * FROM users".to_string(),
                    materialized: false,
                    backing_table: None,
                })
                .await
                .unwrap();
            catalog
                .add_sequence(SequenceDef {
                    name: "s".to_string(),
                    current_value: 5,
                    increment: 1,
                    min_value: None,
                    max_value: None,
                })
                .await
                .unwrap();
            catalog.flush_if_dirty().await.unwrap();
        }

        {
            let catalog = test_catalog(tmp.path()).await;
            assert_eq!(catalog.tables.len(), 1);
            assert_eq!(catalog.indexes.len(), 1);
            assert_eq!(catalog.views.len(), 1);
            assert_eq!(catalog.sequences.len(), 1);
            assert_eq!(catalog.sequences["s"].current_value, 5);
        }
    }

    #[tokio::test]
    async fn test_set_statistics() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog.add_table(sample_table("t")).await.unwrap();
        catalog
            .set_statistics(
                "t",
                TableStatistics {
                    row_count: 100,
                    columns: HashMap::new(),
                },
            )
            .await
            .unwrap();

        let stats = catalog.tables["t"].statistics.as_ref().unwrap();
        assert_eq!(stats.row_count, 100);
    }

    #[tokio::test]
    async fn test_set_file_stats() {
        let tmp = tempfile::tempdir().unwrap();
        let mut catalog = test_catalog(tmp.path()).await;

        catalog.add_table(sample_table("t")).await.unwrap();
        catalog
            .set_file_stats(
                "t",
                vec![FileStats {
                    path: "data.parquet".to_string(),
                    row_count: Some(50),
                    min_values: HashMap::new(),
                    max_values: HashMap::new(),
                    size_bytes: 1024,
                    created_at: None,
                }],
            )
            .await
            .unwrap();

        assert_eq!(catalog.tables["t"].file_stats.len(), 1);
        assert_eq!(catalog.tables["t"].file_stats[0].size_bytes, 1024);
    }
}
