//! Append-only write-ahead log for crash recovery.
//!
//! Every DML operation is journaled before execution. On clean
//! shutdown the WAL is truncated (checkpoint). On crash recovery the
//! engine replays committed entries and discards incomplete ones.
//!
//! ## On-disk format
//!
//! The WAL file is a sequence of variable-length entries:
//!
//! ```text
//! ┌──────────┬──────────┬──────────┬────────┬─────────────┐
//! │ len: u32 │ crc: u32 │ txn: u64 │ st: u8 │ sql: [u8]   │
//! └──────────┴──────────┴──────────┴────────┴─────────────┘
//! ```
//!
//! - `len` -- byte length of everything after `len` (crc + txn + status + sql).
//! - `crc` -- CRC-32C over (txn ++ status ++ sql).
//! - `txn` -- transaction id (`0` for auto-commit statements).
//! - `st`  -- entry status: `0 = Pending`, `1 = Committed`, `2 = Aborted`.
//! - `sql` -- the UTF-8 SQL statement.

pub mod arrow_wal;
pub use arrow_wal::{ArrowWal, ArrowWalConfig, ArrowWalSyncPolicy};

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// Entry status tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EntryStatus {
    Pending = 0,
    Committed = 1,
    Aborted = 2,
}

impl TryFrom<u8> for EntryStatus {
    type Error = io::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Committed),
            2 => Ok(Self::Aborted),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid WAL entry status: {value}"),
            )),
        }
    }
}

/// A single WAL entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub txn_id: u64,
    pub status: EntryStatus,
    pub sql: String,
}

/// Append-only write-ahead log.
pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Wal {
    /// Opens (or creates) the WAL file at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or opened.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    /// Writes one entry to the buffer and flushes, but does **not**
    /// `sync_data`.  Use this for `Pending` entries where durability
    /// is deferred until the corresponding commit/checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if writing to the WAL file fails.
    pub fn append_no_sync(&mut self, entry: &WalEntry) -> io::Result<()> {
        self.write_entry(entry)?;
        self.writer.flush()?;
        Ok(())
    }

    /// Appends one entry, flushes, and syncs to disk for full
    /// durability.  Used for commit markers and abort markers.
    ///
    /// # Errors
    ///
    /// Returns an error if writing to or syncing the WAL file fails.
    pub fn append(&mut self, entry: &WalEntry) -> io::Result<()> {
        self.write_entry(entry)?;
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    fn write_entry(&mut self, entry: &WalEntry) -> io::Result<()> {
        let sql_bytes = entry.sql.as_bytes();
        let payload_len = 8 + 1 + sql_bytes.len(); // txn + status + sql
        let crc = {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&entry.txn_id.to_le_bytes());
            hasher.update(&[entry.status as u8]);
            hasher.update(sql_bytes);
            hasher.finalize()
        };

        #[allow(clippy::cast_possible_truncation)]
        let total_len = (4 + payload_len) as u32; // crc + payload
        self.writer.write_all(&total_len.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&entry.txn_id.to_le_bytes())?;
        self.writer.write_all(&[entry.status as u8])?;
        self.writer.write_all(sql_bytes)?;
        Ok(())
    }

    /// Appends a commit marker without truncating the WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if appending to the WAL fails.
    pub fn commit_no_checkpoint(&mut self, txn_id: u64) -> io::Result<()> {
        self.append(&WalEntry {
            txn_id,
            status: EntryStatus::Committed,
            sql: String::new(),
        })
    }

    /// Marks all entries with `txn_id` as committed by appending a
    /// commit-marker entry (empty SQL, Committed status).
    ///
    /// # Errors
    ///
    /// Returns an error if appending to the WAL fails.
    pub fn commit(&mut self, txn_id: u64) -> io::Result<()> {
        self.commit_no_checkpoint(txn_id)
    }

    /// Marks all entries with `txn_id` as aborted by appending an
    /// abort-marker entry.
    ///
    /// # Errors
    ///
    /// Returns an error if appending to the WAL fails.
    pub fn abort(&mut self, txn_id: u64) -> io::Result<()> {
        self.append(&WalEntry {
            txn_id,
            status: EntryStatus::Aborted,
            sql: String::new(),
        })
    }

    /// Truncates the WAL file (checkpoint). Called after successful
    /// recovery or on clean shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be truncated.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        drop(std::mem::replace(
            &mut self.writer,
            BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&self.path)?,
            ),
        ));
        Ok(())
    }

    /// Truncates the WAL only when its on-disk size exceeds `threshold_bytes`.
    ///
    /// Returns `true` when a checkpoint was performed.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing or checkpointing the WAL fails.
    pub fn maybe_checkpoint(&mut self, threshold_bytes: u64) -> io::Result<bool> {
        self.writer.flush()?;
        let len = std::fs::metadata(&self.path)?.len();
        if threshold_bytes == 0 || len >= threshold_bytes {
            self.checkpoint()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Reads all entries from the WAL and returns those belonging to
    /// committed transactions (i.e. transactions that have a
    /// `Committed` marker entry). Pending and aborted entries are
    /// discarded.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL file cannot be read or contains
    /// invalid entry data.
    ///
    /// # Panics
    ///
    /// Panics if a payload slice cannot be converted to a fixed-size
    /// byte array (should not happen with valid WAL data).
    pub fn recover(path: impl AsRef<Path>) -> io::Result<Vec<WalEntry>> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let total_len = u32::from_le_bytes(len_buf) as usize;
            if total_len < 4 + 8 + 1 {
                break; // corrupted entry
            }

            let mut buf = vec![0u8; total_len];
            match reader.read_exact(&mut buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let stored_crc = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
            let payload = &buf[4..];

            let computed_crc = {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(payload);
                hasher.finalize()
            };

            if stored_crc != computed_crc {
                break; // corrupted -- stop reading
            }

            let txn_id = u64::from_le_bytes(payload[0..8].try_into().unwrap());
            let status = EntryStatus::try_from(payload[8])?;
            let sql = String::from_utf8_lossy(&payload[9..]).to_string();

            entries.push(WalEntry {
                txn_id,
                status,
                sql,
            });
        }

        // Determine which transaction IDs were committed
        let mut committed_txns = std::collections::HashSet::new();
        let mut aborted_txns = std::collections::HashSet::new();
        for entry in &entries {
            match entry.status {
                EntryStatus::Committed => {
                    committed_txns.insert(entry.txn_id);
                }
                EntryStatus::Aborted => {
                    aborted_txns.insert(entry.txn_id);
                }
                EntryStatus::Pending => {}
            }
        }

        // Return pending entries whose txn_id was later committed,
        // plus auto-commit entries (txn_id == 0 are always committed).
        let replay: Vec<WalEntry> = entries
            .into_iter()
            .filter(|e| {
                e.status == EntryStatus::Pending
                    && !e.sql.is_empty()
                    && (e.txn_id == 0 || committed_txns.contains(&e.txn_id))
                    && !aborted_txns.contains(&e.txn_id)
            })
            .collect();

        Ok(replay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_and_recover() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(&WalEntry {
                txn_id: 1,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (1);".to_string(),
            })
            .unwrap();
            wal.append(&WalEntry {
                txn_id: 1,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (2);".to_string(),
            })
            .unwrap();
            wal.commit(1).unwrap();

            // Uncommitted entry
            wal.append(&WalEntry {
                txn_id: 2,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (99);".to_string(),
            })
            .unwrap();
        }

        let replay = Wal::recover(&wal_path).unwrap();
        assert_eq!(replay.len(), 2, "should recover 2 committed entries");
        assert_eq!(replay[0].sql, "INSERT INTO t VALUES (1);");
        assert_eq!(replay[1].sql, "INSERT INTO t VALUES (2);");
    }

    #[test]
    fn test_checkpoint_clears_wal() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        let mut wal = Wal::open(&wal_path).unwrap();
        wal.append(&WalEntry {
            txn_id: 0,
            status: EntryStatus::Pending,
            sql: "INSERT INTO t VALUES (1);".to_string(),
        })
        .unwrap();
        wal.checkpoint().unwrap();

        let replay = Wal::recover(&wal_path).unwrap();
        assert!(replay.is_empty(), "WAL should be empty after checkpoint");
    }

    #[test]
    fn test_recover_empty_file() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");
        let replay = Wal::recover(&wal_path).unwrap();
        assert!(replay.is_empty());
    }

    #[test]
    fn test_aborted_txn_not_replayed() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(&WalEntry {
                txn_id: 1,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (1);".to_string(),
            })
            .unwrap();
            wal.abort(1).unwrap();
        }

        let replay = Wal::recover(&wal_path).unwrap();
        assert!(replay.is_empty(), "aborted txn should not be replayed");
    }

    #[test]
    fn test_maybe_checkpoint_threshold() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        let mut wal = Wal::open(&wal_path).unwrap();
        wal.append(&WalEntry {
            txn_id: 0,
            status: EntryStatus::Pending,
            sql: "INSERT INTO t VALUES (1);".to_string(),
        })
        .unwrap();
        wal.commit_no_checkpoint(0).unwrap();

        let skipped = wal.maybe_checkpoint(u64::MAX).unwrap();
        assert!(!skipped, "threshold too high should skip checkpoint");

        let did_checkpoint = wal.maybe_checkpoint(1).unwrap();
        assert!(did_checkpoint, "low threshold should checkpoint");
        let replay = Wal::recover(&wal_path).unwrap();
        assert!(replay.is_empty(), "WAL should be empty after checkpoint");
    }

    #[test]
    fn test_auto_commit_entries_replayed() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(&WalEntry {
                txn_id: 0,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (1);".to_string(),
            })
            .unwrap();
            wal.commit(0).unwrap();

            wal.append(&WalEntry {
                txn_id: 0,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (2);".to_string(),
            })
            .unwrap();
            wal.commit(0).unwrap();
        }

        let replay = Wal::recover(&wal_path).unwrap();
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].sql, "INSERT INTO t VALUES (1);");
        assert_eq!(replay[1].sql, "INSERT INTO t VALUES (2);");
    }

    #[test]
    fn test_multi_transaction_interleaving() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(&WalEntry {
                txn_id: 1,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (1);".to_string(),
            })
            .unwrap();
            wal.append(&WalEntry {
                txn_id: 2,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (2);".to_string(),
            })
            .unwrap();
            wal.append(&WalEntry {
                txn_id: 1,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (3);".to_string(),
            })
            .unwrap();

            wal.commit(1).unwrap();
            wal.abort(2).unwrap();
        }

        let replay = Wal::recover(&wal_path).unwrap();
        assert_eq!(replay.len(), 2, "only txn 1 entries should be replayed");
        assert_eq!(replay[0].sql, "INSERT INTO t VALUES (1);");
        assert_eq!(replay[1].sql, "INSERT INTO t VALUES (3);");
    }

    #[test]
    fn test_corrupted_entry_stops_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(&WalEntry {
                txn_id: 0,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (1);".to_string(),
            })
            .unwrap();
            wal.commit(0).unwrap();
        }

        {
            use std::fs::OpenOptions;
            use std::io::Write;
            let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            file.write_all(b"garbage data that breaks CRC").unwrap();
        }

        let replay = Wal::recover(&wal_path).unwrap();
        assert_eq!(
            replay.len(),
            1,
            "should recover valid entries before corruption"
        );
        assert_eq!(replay[0].sql, "INSERT INTO t VALUES (1);");
    }

    #[test]
    fn test_recover_nonexistent_file() {
        let replay = Wal::recover("/tmp/nonexistent_wal_file_12345.log").unwrap();
        assert!(replay.is_empty());
    }

    #[test]
    fn test_entry_status_try_from() {
        assert_eq!(EntryStatus::try_from(0u8).unwrap(), EntryStatus::Pending);
        assert_eq!(EntryStatus::try_from(1u8).unwrap(), EntryStatus::Committed);
        assert_eq!(EntryStatus::try_from(2u8).unwrap(), EntryStatus::Aborted);
        assert!(EntryStatus::try_from(3u8).is_err());
        assert!(EntryStatus::try_from(255u8).is_err());
    }

    #[test]
    fn test_multiple_appends_single_wal() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        let mut wal = Wal::open(&wal_path).unwrap();
        for i in 0..100 {
            wal.append(&WalEntry {
                txn_id: 0,
                status: EntryStatus::Pending,
                sql: format!("INSERT INTO t VALUES ({i});"),
            })
            .unwrap();
        }
        wal.commit(0).unwrap();

        let replay = Wal::recover(&wal_path).unwrap();
        assert_eq!(replay.len(), 100);
    }

    #[test]
    fn test_abort_then_commit_same_txn_id() {
        let tmp = tempfile::tempdir().unwrap();
        let wal_path = tmp.path().join("wal.log");

        {
            let mut wal = Wal::open(&wal_path).unwrap();
            wal.append(&WalEntry {
                txn_id: 1,
                status: EntryStatus::Pending,
                sql: "INSERT INTO t VALUES (1);".to_string(),
            })
            .unwrap();
            wal.abort(1).unwrap();
            wal.commit(1).unwrap();
        }

        let replay = Wal::recover(&wal_path).unwrap();
        assert!(
            replay.is_empty(),
            "aborted txn should not be replayed even if a commit marker also exists"
        );
    }
}
