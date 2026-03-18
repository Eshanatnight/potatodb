//! Arrow IPC-based write-ahead log for INSERT data.
//!
//! Persists `RecordBatch`es to disk in Arrow IPC file format so that
//! buffered rows survive crashes.  On recovery the batches are read
//! back and flushed to Parquet without re-executing SQL.
//!
//! ## On-disk layout
//!
//! ```text
//! {dir}/
//!   {table_name}/
//!     000001.arrow   ← one Arrow IPC file per append call
//!     000002.arrow
//!   another_table/
//!     000001.arrow
//! ```

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufWriter, Cursor, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// Controls when the Arrow WAL forces data to stable storage.
#[derive(Debug, Clone, Copy)]
pub enum ArrowWalSyncPolicy {
    /// `sync_data()` after every `append`.
    Always,
    /// `sync_data()` every N `append` calls (best-effort).
    EveryNAppends(u64),
    /// `sync_data()` at most once per interval (best-effort).
    EveryInterval(Duration),
    /// Never call `sync_data()` (fastest, least durable).
    Never,
}

impl Default for ArrowWalSyncPolicy {
    fn default() -> Self {
        Self::Always
    }
}

/// Configuration for `ArrowWal`.
#[derive(Debug, Clone, Copy)]
pub struct ArrowWalConfig {
    pub sync_policy: ArrowWalSyncPolicy,
    /// Initial capacity for the reusable IPC scratch buffer.
    pub scratch_capacity_bytes: usize,
}

impl Default for ArrowWalConfig {
    fn default() -> Self {
        Self {
            sync_policy: ArrowWalSyncPolicy::Always,
            scratch_capacity_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Arrow IPC write-ahead log.
///
/// Each `append` creates a new numbered `.arrow` file under a
/// per-table subdirectory.  This keeps the WAL append-only and avoids
/// expensive read-modify-write cycles.
pub struct ArrowWal {
    dir: PathBuf,
    seq: u64,
    /// Table directories already created so we can skip `create_dir_all`.
    known_dirs: HashSet<String>,
    cfg: ArrowWalConfig,
    scratch: Vec<u8>,
    appends_since_sync: u64,
    last_sync_at: Instant,
}

impl ArrowWal {
    /// Opens (or creates) the Arrow WAL directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or read.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_config(dir, ArrowWalConfig::default())
    }

    /// Opens (or creates) the Arrow WAL directory with a custom configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or read.
    pub fn open_with_config(dir: impl AsRef<Path>, cfg: ArrowWalConfig) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let seq = Self::max_existing_seq(&dir).unwrap_or(0);

        Ok(Self {
            dir,
            seq,
            known_dirs: HashSet::new(),
            cfg,
            scratch: Vec::with_capacity(cfg.scratch_capacity_bytes),
            appends_since_sync: 0,
            last_sync_at: Instant::now(),
        })
    }

    /// Appends `batches` for `table` as a new Arrow IPC file.
    ///
    /// # Errors
    ///
    /// Returns an error if the table directory cannot be created, the IPC file
    /// cannot be written, or Arrow serialization fails.
    pub fn append(&mut self, table: &str, batches: &[RecordBatch]) -> io::Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();

        let table_dir = self.dir.join(table);
        if self.known_dirs.insert(table.to_string()) {
            fs::create_dir_all(&table_dir)?;
        }

        self.seq += 1;
        let file_path = table_dir.join(format!("{:06}.arrow", self.seq));

        // Encode into a reusable in-memory buffer first to avoid repeated
        // small allocations inside the IPC writer path.
        self.scratch.clear();
        let approx = batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>();
        // IPC overhead varies; reserve a bit more than the raw array memory.
        let target_cap = approx
            .saturating_mul(2)
            .max(self.cfg.scratch_capacity_bytes);
        if self.scratch.capacity() < target_cap {
            self.scratch.reserve(target_cap - self.scratch.capacity());
        }

        let cursor = Cursor::new(&mut self.scratch);
        let mut writer = FileWriter::try_new(cursor, &schema)
            .map_err(|e| io::Error::other(format!("Arrow IPC writer init: {e}")))?;

        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| io::Error::other(format!("Arrow IPC write: {e}")))?;
        }

        writer
            .finish()
            .map_err(|e| io::Error::other(format!("Arrow IPC finish: {e}")))?;

        // Persist the encoded bytes to disk.
        let file = fs::File::create(&file_path)?;
        let mut buf_writer = BufWriter::with_capacity(256 * 1024, file);
        buf_writer.write_all(&self.scratch)?;
        buf_writer.flush()?;

        self.appends_since_sync = self.appends_since_sync.saturating_add(1);
        let should_sync = match self.cfg.sync_policy {
            ArrowWalSyncPolicy::Always => true,
            ArrowWalSyncPolicy::EveryNAppends(n) => n > 0 && (self.appends_since_sync % n == 0),
            ArrowWalSyncPolicy::EveryInterval(d) => d.is_zero() || self.last_sync_at.elapsed() >= d,
            ArrowWalSyncPolicy::Never => false,
        };

        if should_sync {
            buf_writer.get_ref().sync_data()?;
            self.last_sync_at = Instant::now();
            self.appends_since_sync = 0;
        }

        Ok(())
    }

    /// Reads all Arrow IPC files and returns `RecordBatch`es grouped
    /// by table name.  Files are read in sorted order so that the
    /// original insertion order is preserved.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be read or any Arrow IPC
    /// file is invalid.
    pub fn recover(dir: impl AsRef<Path>) -> io::Result<HashMap<String, Vec<RecordBatch>>> {
        let dir = dir.as_ref();
        if !dir.exists() {
            return Ok(HashMap::new());
        }

        let mut result: HashMap<String, Vec<RecordBatch>> = HashMap::new();

        let mut table_dirs: Vec<_> = fs::read_dir(dir)?
            .filter_map(Result::ok)
            .filter(|e| e.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
            .collect();
        table_dirs.sort_by_key(std::fs::DirEntry::file_name);

        for table_entry in table_dirs {
            let table_name = table_entry.file_name().to_string_lossy().to_string();

            let mut arrow_files: Vec<_> = fs::read_dir(table_entry.path())?
                .filter_map(Result::ok)
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "arrow"))
                .collect();
            arrow_files.sort_by_key(std::fs::DirEntry::file_name);

            let batches = result.entry(table_name).or_default();

            for file_entry in arrow_files {
                let file = fs::File::open(file_entry.path())?;
                let reader = FileReader::try_new(file, None).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Arrow IPC read {}: {e}", file_entry.path().display()),
                    )
                })?;
                for batch_result in reader {
                    let batch = batch_result.map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("Arrow IPC batch read {}: {e}", file_entry.path().display()),
                        )
                    })?;
                    batches.push(batch);
                }
            }
        }

        Ok(result)
    }

    /// Removes all Arrow IPC files for `table`.
    ///
    /// # Errors
    ///
    /// Returns an error if the table directory cannot be removed.
    pub fn checkpoint_table(&mut self, table: &str) -> io::Result<()> {
        let table_dir = self.dir.join(table);
        if table_dir.exists() {
            fs::remove_dir_all(&table_dir)?;
        }
        self.known_dirs.remove(table);
        Ok(())
    }

    /// Removes all Arrow IPC files for every table.
    ///
    /// # Errors
    ///
    /// Returns an error if any table directory cannot be read or removed.
    pub fn checkpoint_all(&mut self) -> io::Result<()> {
        if self.dir.exists() {
            for entry in fs::read_dir(&self.dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    fs::remove_dir_all(entry.path())?;
                }
            }
        }
        self.known_dirs.clear();
        Ok(())
    }

    /// Scans all subdirectories to find the highest existing sequence
    /// number so that new appends continue from there.
    fn max_existing_seq(dir: &Path) -> Option<u64> {
        let mut max_seq: Option<u64> = None;
        let entries = fs::read_dir(dir).ok()?;
        for entry in entries.filter_map(Result::ok) {
            if !entry.file_type().ok()?.is_dir() {
                continue;
            }
            let sub_entries = fs::read_dir(entry.path()).ok()?;
            for sub in sub_entries.filter_map(Result::ok) {
                if let Some(stem) = sub.path().file_stem() {
                    if let Ok(n) = stem.to_string_lossy().parse::<u64>() {
                        max_seq = Some(max_seq.map_or(n, |m: u64| m.max(n)));
                    }
                }
            }
        }
        max_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::Duration;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    names.iter().map(|s| Some(*s)).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_append_and_recover() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("_arrow_wal");

        {
            let mut wal = ArrowWal::open(&dir).unwrap();
            let batch1 = test_batch(&[1, 2], &["a", "b"]);
            let batch2 = test_batch(&[3], &["c"]);
            wal.append("users", &[batch1]).unwrap();
            wal.append("users", &[batch2]).unwrap();
            wal.append("orders", &[test_batch(&[10], &["x"])]).unwrap();
        }

        let recovered = ArrowWal::recover(&dir).unwrap();
        assert_eq!(recovered.len(), 2);
        let user_rows: usize = recovered["users"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(user_rows, 3);
        assert_eq!(recovered["orders"][0].num_rows(), 1);
    }

    #[test]
    fn test_checkpoint_table() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("_arrow_wal");

        let mut wal = ArrowWal::open(&dir).unwrap();
        wal.append("t1", &[test_batch(&[1], &["a"])]).unwrap();
        wal.append("t2", &[test_batch(&[2], &["b"])]).unwrap();

        wal.checkpoint_table("t1").unwrap();

        let recovered = ArrowWal::recover(&dir).unwrap();
        assert!(!recovered.contains_key("t1"));
        assert!(recovered.contains_key("t2"));
    }

    #[test]
    fn test_checkpoint_all() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("_arrow_wal");

        let mut wal = ArrowWal::open(&dir).unwrap();
        wal.append("t1", &[test_batch(&[1], &["a"])]).unwrap();
        wal.append("t2", &[test_batch(&[2], &["b"])]).unwrap();

        wal.checkpoint_all().unwrap();

        let recovered = ArrowWal::recover(&dir).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_recover_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("nonexistent");
        let recovered = ArrowWal::recover(&dir).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_append_empty_batches_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("_arrow_wal");

        let mut wal = ArrowWal::open(&dir).unwrap();
        wal.append("t1", &[]).unwrap();

        let recovered = ArrowWal::recover(&dir).unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_seq_continues_after_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("_arrow_wal");

        {
            let mut wal = ArrowWal::open(&dir).unwrap();
            wal.append("t1", &[test_batch(&[1], &["a"])]).unwrap();
            assert_eq!(wal.seq, 1);
        }

        {
            let mut wal = ArrowWal::open(&dir).unwrap();
            assert_eq!(wal.seq, 1);
            wal.append("t1", &[test_batch(&[2], &["b"])]).unwrap();
            assert_eq!(wal.seq, 2);
        }

        let recovered = ArrowWal::recover(&dir).unwrap();
        let rows: usize = recovered["t1"].iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
    }

    #[test]
    fn test_open_with_config_and_append() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("_arrow_wal");

        let cfg = ArrowWalConfig {
            sync_policy: ArrowWalSyncPolicy::EveryNAppends(10),
            scratch_capacity_bytes: 128 * 1024,
        };
        let mut wal = ArrowWal::open_with_config(&dir, cfg).unwrap();
        wal.append("t1", &[test_batch(&[1], &["a"])]).unwrap();

        let recovered = ArrowWal::recover(&dir).unwrap();
        assert_eq!(recovered["t1"][0].num_rows(), 1);

        let cfg2 = ArrowWalConfig {
            sync_policy: ArrowWalSyncPolicy::EveryInterval(Duration::from_millis(50)),
            scratch_capacity_bytes: 128 * 1024,
        };
        let mut wal2 = ArrowWal::open_with_config(&dir, cfg2).unwrap();
        wal2.append("t2", &[test_batch(&[2], &["b"])]).unwrap();

        let recovered2 = ArrowWal::recover(&dir).unwrap();
        assert!(recovered2.contains_key("t2"));
    }
}
