use arrow::array::{Array, Int32Array, StringArray};
use chrono::Utc;
use potatodb_engine::{PotatoDB, QueryResult};
use potatodb_wal::{EntryStatus, Wal, WalEntry};

#[tokio::test]
async fn test_create_insert_select_drop() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
        .await
        .unwrap();

    // CREATE TABLE
    match db
        .execute("CREATE TABLE users (id INT, name VARCHAR, email VARCHAR);")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("created"), "got: {msg}"),
        QueryResult::Records(_) => panic!("expected message"),
    }

    // INSERT rows
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com');")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com');")
        .await
        .unwrap();

    // SELECT *
    match db
        .execute("SELECT * FROM users ORDER BY id;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 3, "expected 3 rows, got {total}");
        }
        QueryResult::Message(msg) => panic!("expected records, got: {msg}"),
    }

    // SELECT with WHERE
    match db
        .execute("SELECT name FROM users WHERE id = 2;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "expected 1 row, got {total}");
        }
        QueryResult::Message(msg) => panic!("expected records, got: {msg}"),
    }

    // SELECT with aggregation
    match db
        .execute("SELECT COUNT(*) AS cnt FROM users;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        QueryResult::Message(msg) => panic!("expected records, got: {msg}"),
    }

    // DROP TABLE
    match db.execute("DROP TABLE users;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("dropped"), "got: {msg}"),
        QueryResult::Records(_) => panic!("expected message"),
    }

    // Table should be gone
    assert!(db.execute("SELECT * FROM users;").await.is_err());
}

#[tokio::test]
async fn test_persistence_across_restarts() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    // Session 1: create table and insert
    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("CREATE TABLE items (id INT, label VARCHAR);")
            .await
            .unwrap();
        db.execute("INSERT INTO items VALUES (10, 'widget');")
            .await
            .unwrap();
    }

    // Session 2: data should persist
    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        match db
            .execute("SELECT * FROM items ORDER BY id;")
            .await
            .unwrap()
        {
            QueryResult::Records(batches) => {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total, 1, "expected 1 row after restart, got {total}");
            }
            QueryResult::Message(msg) => panic!("expected records, got: {msg}"),
        }
    }
}

#[tokio::test]
async fn test_create_index_sorts_data() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE events (id INT, ts INT, label VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO events VALUES (3, 300, 'c');")
        .await
        .unwrap();
    db.execute("INSERT INTO events VALUES (1, 100, 'a');")
        .await
        .unwrap();
    db.execute("INSERT INTO events VALUES (2, 200, 'b');")
        .await
        .unwrap();

    match db
        .execute("CREATE INDEX idx_events_ts ON events (ts);")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("created"), "got: {msg}"),
        QueryResult::Records(_) => panic!("expected message"),
    }

    match db.execute("SELECT ts FROM events;").await.unwrap() {
        QueryResult::Records(batches) => {
            let mut ts_vals: Vec<i32> = Vec::new();
            for batch in &batches {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..col.len() {
                    ts_vals.push(col.value(i));
                }
            }
            assert_eq!(ts_vals, vec![100, 200, 300]);
        }
        QueryResult::Message(msg) => panic!("expected records, got: {msg}"),
    }

    match db
        .execute("SELECT COUNT(*) AS cnt FROM events;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        QueryResult::Message(msg) => panic!("expected records, got: {msg}"),
    }

    match db.execute("DROP INDEX idx_events_ts;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("dropped"), "got: {msg}"),
        QueryResult::Records(_) => panic!("expected message"),
    }

    match db.execute("SELECT * FROM events;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 3);
        }
        QueryResult::Message(msg) => panic!("expected records, got: {msg}"),
    }

    db.execute("CREATE INDEX idx2 ON events (id DESC);")
        .await
        .unwrap();
    db.execute("DROP TABLE events;").await.unwrap();
    assert!(db.execute("SELECT * FROM events;").await.is_err());
}

#[tokio::test]
async fn test_index_persists_across_restarts() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("CREATE TABLE scores (player VARCHAR, score INT);")
            .await
            .unwrap();
        db.execute("INSERT INTO scores VALUES ('zara', 50);")
            .await
            .unwrap();
        db.execute("INSERT INTO scores VALUES ('alice', 90);")
            .await
            .unwrap();
        db.execute("CREATE INDEX idx_score ON scores (score DESC);")
            .await
            .unwrap();
    }

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        match db.execute("SELECT score FROM scores;").await.unwrap() {
            QueryResult::Records(batches) => {
                let mut vals: Vec<i32> = Vec::new();
                for batch in &batches {
                    let col = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap();
                    for i in 0..col.len() {
                        vals.push(col.value(i));
                    }
                }
                assert_eq!(vals, vec![90, 50], "expected DESC sort order after restart");
            }
            QueryResult::Message(msg) => panic!("expected records, got: {msg}"),
        }
    }
}

// ── Transaction tests ──────────────────────────────────────────

#[tokio::test]
async fn test_begin_commit_persists() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("BEGIN;").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("INSERT INTO t VALUES (2);").await.unwrap();
    db.execute("COMMIT;").await.unwrap();

    match db.execute("SELECT COUNT(*) AS n FROM t;").await.unwrap() {
        QueryResult::Records(b) => {
            let n: usize = b.iter().map(|b| b.num_rows()).sum();
            assert_eq!(n, 1);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_rollback_reverts_inserts() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();

    db.execute("BEGIN;").await.unwrap();
    db.execute("INSERT INTO t VALUES (2);").await.unwrap();
    db.execute("INSERT INTO t VALUES (3);").await.unwrap();
    db.execute("ROLLBACK;").await.unwrap();

    match db.execute("SELECT * FROM t;").await.unwrap() {
        QueryResult::Records(b) => {
            let total: usize = b.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "rollback should leave only the pre-txn row");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_rollback_reverts_create_table() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("BEGIN;").await.unwrap();
    db.execute("CREATE TABLE ephemeral (x INT);").await.unwrap();
    db.execute("INSERT INTO ephemeral VALUES (42);")
        .await
        .unwrap();
    db.execute("ROLLBACK;").await.unwrap();

    assert!(
        db.execute("SELECT * FROM ephemeral;").await.is_err(),
        "table should not exist after rollback"
    );
}

#[tokio::test]
async fn test_rollback_reverts_drop_table() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE keepers (id INT);").await.unwrap();
    db.execute("INSERT INTO keepers VALUES (1);").await.unwrap();

    db.execute("BEGIN;").await.unwrap();
    db.execute("DROP TABLE keepers;").await.unwrap();
    assert!(
        db.execute("SELECT * FROM keepers;").await.is_err(),
        "table should be invisible during the txn"
    );
    db.execute("ROLLBACK;").await.unwrap();

    match db.execute("SELECT * FROM keepers;").await.unwrap() {
        QueryResult::Records(b) => {
            let total: usize = b.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "table should reappear after rollback");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_nested_begin_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("BEGIN;").await.unwrap();
    let err = db.execute("BEGIN;").await;
    assert!(err.is_err(), "nested BEGIN should fail");
    db.execute("ROLLBACK;").await.unwrap();
}

#[tokio::test]
async fn test_create_index_allowed_in_transaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("BEGIN;").await.unwrap();
    db.execute("CREATE INDEX idx ON t (id);").await.unwrap();
    db.execute("COMMIT;").await.unwrap();
}

// ── EXPLAIN tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_explain() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();

    match db.execute("EXPLAIN SELECT * FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            assert!(!batches.is_empty(), "EXPLAIN should return plan rows");
        }
        QueryResult::Message(msg) => panic!("expected records from EXPLAIN, got: {msg}"),
    }
}

// ── CTAS tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_create_table_as_select() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE src (id INT, val INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO src VALUES (1, 10);").await.unwrap();
    db.execute("INSERT INTO src VALUES (2, 20);").await.unwrap();
    db.execute("INSERT INTO src VALUES (3, 30);").await.unwrap();

    match db
        .execute("CREATE TABLE dst AS SELECT id, val * 2 AS doubled FROM src WHERE id > 1;")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("created"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db.execute("SELECT * FROM dst ORDER BY id;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2, "CTAS should have 2 rows");
        }
        _ => panic!("expected records"),
    }
}

// ── DELETE tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_delete_with_where() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a');").await.unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b');").await.unwrap();
    db.execute("INSERT INTO t VALUES (3, 'c');").await.unwrap();

    match db.execute("DELETE FROM t WHERE id = 2;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("1 row(s) deleted"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db.execute("SELECT * FROM t ORDER BY id;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2, "should have 2 rows after delete");
            let mut ids: Vec<i32> = Vec::new();
            for b in &batches {
                let col = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                for i in 0..col.len() {
                    ids.push(col.value(i));
                }
            }
            assert_eq!(ids, vec![1, 3]);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_delete_all() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("INSERT INTO t VALUES (2);").await.unwrap();

    match db.execute("DELETE FROM t;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("2 row(s) deleted"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db.execute("SELECT COUNT(*) AS n FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_delete_allowed_in_transaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("BEGIN;").await.unwrap();
    db.execute("DELETE FROM t WHERE id = 1;").await.unwrap();
    db.execute("COMMIT;").await.unwrap();
}

// ── UPDATE tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_update_with_where() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice');")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob');")
        .await
        .unwrap();

    match db
        .execute("UPDATE t SET name = 'ALICE' WHERE id = 1;")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("1 row(s) updated"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db
        .execute("SELECT name FROM t WHERE id = 1;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(col.value(0), "ALICE");
        }
        _ => panic!("expected records"),
    }

    match db
        .execute("SELECT name FROM t WHERE id = 2;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(col.value(0), "bob");
        }
        _ => panic!("expected records"),
    }
}

// ── ALTER TABLE tests ─────────────────────────────────────────

#[tokio::test]
async fn test_alter_table_add_column() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();

    match db
        .execute("ALTER TABLE t ADD COLUMN name VARCHAR;")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("added"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    db.execute("INSERT INTO t VALUES (2, 'bob');")
        .await
        .unwrap();

    match db.execute("SELECT * FROM t ORDER BY id;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_alter_table_drop_column() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR, extra INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 99);")
        .await
        .unwrap();

    match db
        .execute("ALTER TABLE t DROP COLUMN extra;")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("dropped"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db.execute("SELECT * FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            assert_eq!(
                batches[0].num_columns(),
                2,
                "should have 2 columns after drop"
            );
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_alter_table_rename_column() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, old_name VARCHAR);")
        .await
        .unwrap();

    match db
        .execute("ALTER TABLE t RENAME COLUMN old_name TO new_name;")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("renamed"), "got: {msg}"),
        _ => panic!("expected message"),
    }
}

// ── View tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_create_and_query_view() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, val INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10);").await.unwrap();
    db.execute("INSERT INTO t VALUES (2, 20);").await.unwrap();

    match db
        .execute("CREATE VIEW big_vals AS SELECT * FROM t WHERE val > 15;")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("created"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db.execute("SELECT * FROM big_vals;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "view should return 1 row");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_view_persists_across_restarts() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("CREATE TABLE t (id INT, val INT);")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10);").await.unwrap();
        db.execute("CREATE VIEW v AS SELECT val FROM t;")
            .await
            .unwrap();
    }

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        match db.execute("SELECT * FROM v;").await.unwrap() {
            QueryResult::Records(batches) => {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total, 1, "view should work after restart");
            }
            _ => panic!("expected records"),
        }
    }
}

#[tokio::test]
async fn test_drop_view() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t;")
        .await
        .unwrap();

    match db.execute("DROP VIEW v;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("dropped"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    assert!(db.execute("SELECT * FROM v;").await.is_err());
}

// ── VACUUM tests ──────────────────────────────────────────────

#[tokio::test]
async fn test_vacuum_compacts_files() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({i});"))
            .await
            .unwrap();
    }

    match db.execute("VACUUM t;").await.unwrap() {
        QueryResult::Message(msg) => {
            assert!(msg.contains("compacted"), "got: {msg}");
            assert!(msg.contains("5 rows"), "got: {msg}");
        }
        _ => panic!("expected message"),
    }

    match db.execute("SELECT COUNT(*) AS n FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "COUNT should still return one row");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_vacuum_analyze() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');")
        .await
        .unwrap();

    match db.execute("VACUUM ANALYZE t;").await.unwrap() {
        QueryResult::Message(msg) => {
            assert!(msg.contains("compacted"), "got: {msg}");
            assert!(msg.contains("statistics"), "got: {msg}");
        }
        _ => panic!("expected message"),
    }

    match db.execute("SELECT COUNT(*) AS n FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "COUNT should still return one row");
        }
        _ => panic!("expected records"),
    }
}

// ── ANALYZE tests ─────────────────────────────────────────────

#[tokio::test]
async fn test_analyze_collects_stats() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice');")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob');")
        .await
        .unwrap();

    match db.execute("ANALYZE t;").await.unwrap() {
        QueryResult::Message(msg) => {
            assert!(msg.contains("statistics"), "got: {msg}");
            assert!(msg.contains("2 rows"), "got: {msg}");
        }
        _ => panic!("expected message"),
    }
}

// ── Prepared statement tests ──────────────────────────────────

#[tokio::test]
async fn test_prepare_and_execute() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice');")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob');")
        .await
        .unwrap();

    db.execute("PREPARE find_by_id AS SELECT * FROM t WHERE id = $1;")
        .await
        .unwrap();

    match db.execute("EXECUTE find_by_id(1);").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "should find 1 row");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_not_null_insert_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT NOT NULL, name VARCHAR);")
        .await
        .unwrap();

    let result = db.execute("INSERT INTO t VALUES (NULL, 'bad');").await;
    assert!(
        result.is_err(),
        "INSERT with NULL into NOT NULL column should fail"
    );
}

#[tokio::test]
async fn test_wal_recovery_replays_committed_entry() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("CREATE TABLE t (id INT);").await.unwrap();
        db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    }

    let wal_path = data_dir.join("wal.log");
    {
        let mut wal = Wal::open(&wal_path).unwrap();
        wal.append(&WalEntry {
            txn_id: 0,
            status: EntryStatus::Pending,
            sql: "INSERT INTO t VALUES (2);".to_string(),
        })
        .unwrap();
        wal.commit(0).unwrap();
    }

    let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
        .await
        .unwrap();
    match db.execute("SELECT COUNT(*) AS n FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "COUNT should return one row");
        }
        _ => panic!("expected records"),
    }

    match db.execute("SELECT * FROM t ORDER BY id;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2, "WAL replay should re-apply committed INSERT");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_truncate_table() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("INSERT INTO t VALUES (2);").await.unwrap();

    match db.execute("TRUNCATE TABLE t;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("truncated"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db.execute("SELECT * FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 0);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_alter_table_rename_to() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE old_name (id INT);").await.unwrap();
    db.execute("INSERT INTO old_name VALUES (1);")
        .await
        .unwrap();

    match db
        .execute("ALTER TABLE old_name RENAME TO new_name;")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("renamed"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    assert!(db.execute("SELECT * FROM old_name;").await.is_err());
    match db.execute("SELECT * FROM new_name;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_explain_format_json() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    match db
        .execute("EXPLAIN (FORMAT JSON) SELECT * FROM t;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => assert!(!batches.is_empty()),
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_delete_returning() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a');").await.unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b');").await.unwrap();

    match db
        .execute("DELETE FROM t WHERE id = 2 RETURNING id, name;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_update_returning() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a');").await.unwrap();

    match db
        .execute("UPDATE t SET name = 'A' WHERE id = 1 RETURNING id, name;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_copy_to_csv() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = tmp.path().join("out.csv");
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice');")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'bob');")
        .await
        .unwrap();

    let sql = format!("COPY t TO '{}';", csv_path.to_string_lossy());
    match db.execute(&sql).await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("copied"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    assert!(csv_path.exists(), "COPY TO should create output file");
}

#[tokio::test]
async fn test_primary_key_constraint() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR, PRIMARY KEY (id));")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a');").await.unwrap();
    let dup = db.execute("INSERT INTO t VALUES (1, 'b');").await;
    assert!(dup.is_err(), "duplicate PK insert should fail");
}

#[tokio::test]
async fn test_unique_constraint() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, email VARCHAR, UNIQUE (email));")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a@example.com');")
        .await
        .unwrap();
    let dup = db
        .execute("INSERT INTO t VALUES (2, 'a@example.com');")
        .await;
    assert!(dup.is_err(), "duplicate UNIQUE insert should fail");
}

#[tokio::test]
async fn test_check_constraint() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, age INT, CHECK (age >= 0));")
        .await
        .unwrap();
    let bad = db.execute("INSERT INTO t VALUES (1, -1);").await;
    assert!(bad.is_err(), "CHECK constraint violation should fail");
}

#[tokio::test]
async fn test_insert_on_conflict_do_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a');").await.unwrap();
    db.execute("INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO NOTHING;")
        .await
        .unwrap();

    match db.execute("SELECT * FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_insert_on_conflict_do_update() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a');").await.unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 'b') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;",
    )
    .await
    .unwrap();

    match db
        .execute("SELECT name FROM t WHERE id = 1;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(col.value(0), "b");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_sequence_nextval() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE SEQUENCE seq_ids;").await.unwrap();
    db.execute("CREATE TABLE t (id BIGINT, name VARCHAR);")
        .await
        .unwrap();

    db.execute("INSERT INTO t VALUES (nextval('seq_ids'), 'a');")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (nextval('seq_ids'), 'b');")
        .await
        .unwrap();

    match db.execute("SELECT id FROM t ORDER BY id;").await.unwrap() {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(col.value(0), 1);
            assert_eq!(col.value(1), 2);
        }
        _ => panic!("expected records"),
    }

    db.execute("DROP SEQUENCE seq_ids;").await.unwrap();
}

#[tokio::test]
async fn test_materialized_view_refresh() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE src (id INT, val INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO src VALUES (1, 10);").await.unwrap();
    db.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM src;")
        .await
        .unwrap();

    db.execute("INSERT INTO src VALUES (2, 20);").await.unwrap();
    db.execute("REFRESH MATERIALIZED VIEW mv;").await.unwrap();

    match db.execute("SELECT COUNT(*) AS n FROM mv;").await.unwrap() {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(col.value(0), 2);
        }
        _ => panic!("expected records"),
    }
}

// ── Data type tests ───────────────────────────────────────────

#[tokio::test]
async fn test_data_types_boolean_bigint_float() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE typed (flag BOOLEAN, big BIGINT, ratio DOUBLE, label VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO typed VALUES (true, 9223372036854775807, 3.14, 'pi');")
        .await
        .unwrap();
    db.execute("INSERT INTO typed VALUES (false, -1, 0.0, 'zero');")
        .await
        .unwrap();

    match db
        .execute("SELECT flag, big, ratio FROM typed ORDER BY label;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2);

            let bools = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .unwrap();
            assert!(bools.value(0));

            let bigs = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(bigs.value(0), 9223372036854775807i64);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_data_type_timestamp_and_date() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE events (id INT, ts TIMESTAMP, d DATE);")
        .await
        .unwrap();
    db.execute("INSERT INTO events VALUES (1, '2025-06-15 10:30:00', '2025-06-15');")
        .await
        .unwrap();

    match db.execute("SELECT * FROM events;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        _ => panic!("expected records"),
    }
}

// ── JOIN tests ────────────────────────────────────────────────

#[tokio::test]
async fn test_inner_join() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE customers (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("CREATE TABLE orders (id INT, customer_id INT, amount INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'Alice');")
        .await
        .unwrap();
    db.execute("INSERT INTO customers VALUES (2, 'Bob');")
        .await
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1, 100);")
        .await
        .unwrap();
    db.execute("INSERT INTO orders VALUES (11, 1, 200);")
        .await
        .unwrap();
    db.execute("INSERT INTO orders VALUES (12, 2, 50);")
        .await
        .unwrap();

    match db
        .execute(
            "SELECT c.name, o.amount \
             FROM customers c JOIN orders o ON c.id = o.customer_id \
             ORDER BY o.amount;",
        )
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 3);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_left_join() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE a (id INT, val VARCHAR);")
        .await
        .unwrap();
    db.execute("CREATE TABLE b (a_id INT, extra VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 'x');").await.unwrap();
    db.execute("INSERT INTO a VALUES (2, 'y');").await.unwrap();
    db.execute("INSERT INTO b VALUES (1, 'matched');")
        .await
        .unwrap();

    match db
        .execute(
            "SELECT a.id, b.extra \
             FROM a LEFT JOIN b ON a.id = b.a_id \
             ORDER BY a.id;",
        )
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2, "LEFT JOIN should include unmatched rows");
        }
        _ => panic!("expected records"),
    }
}

// ── Subquery tests ────────────────────────────────────────────

#[tokio::test]
async fn test_subquery_in_where() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE products (id INT, price INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 10);")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (2, 50);")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (3, 100);")
        .await
        .unwrap();

    match db
        .execute(
            "SELECT id FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY id;",
        )
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "only product with price 100 is above avg ~53");
        }
        _ => panic!("expected records"),
    }
}

// ── CTE tests ─────────────────────────────────────────────────

#[tokio::test]
async fn test_cte_with_clause() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE sales (region VARCHAR, amount INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO sales VALUES ('east', 100);")
        .await
        .unwrap();
    db.execute("INSERT INTO sales VALUES ('east', 200);")
        .await
        .unwrap();
    db.execute("INSERT INTO sales VALUES ('west', 50);")
        .await
        .unwrap();

    match db
        .execute(
            "WITH totals AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) \
             SELECT region FROM totals WHERE total > 100 ORDER BY region;",
        )
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(col.value(0), "east");
        }
        _ => panic!("expected records"),
    }
}

// ── Window function tests ─────────────────────────────────────

#[tokio::test]
async fn test_window_function_row_number() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE ranked (id INT, score INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO ranked VALUES (1, 90);")
        .await
        .unwrap();
    db.execute("INSERT INTO ranked VALUES (2, 80);")
        .await
        .unwrap();
    db.execute("INSERT INTO ranked VALUES (3, 95);")
        .await
        .unwrap();

    match db
        .execute(
            "SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM ranked ORDER BY rn;",
        )
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 3);
        }
        _ => panic!("expected records"),
    }
}

// ── GROUP BY / HAVING tests ───────────────────────────────────

#[tokio::test]
async fn test_group_by_having() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE logs (level VARCHAR, msg VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO logs VALUES ('INFO', 'a');")
        .await
        .unwrap();
    db.execute("INSERT INTO logs VALUES ('ERROR', 'b');")
        .await
        .unwrap();
    db.execute("INSERT INTO logs VALUES ('ERROR', 'c');")
        .await
        .unwrap();
    db.execute("INSERT INTO logs VALUES ('INFO', 'd');")
        .await
        .unwrap();
    db.execute("INSERT INTO logs VALUES ('ERROR', 'e');")
        .await
        .unwrap();

    match db
        .execute(
            "SELECT level, COUNT(*) AS cnt FROM logs \
             GROUP BY level HAVING COUNT(*) > 2 ORDER BY level;",
        )
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "only ERROR has count > 2");
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(col.value(0), "ERROR");
        }
        _ => panic!("expected records"),
    }
}

// ── Multi-value INSERT tests ──────────────────────────────────

#[tokio::test]
async fn test_multi_value_insert() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE mv (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO mv VALUES (1, 'a'), (2, 'b'), (3, 'c');")
        .await
        .unwrap();

    match db.execute("SELECT * FROM mv ORDER BY id;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 3);
        }
        _ => panic!("expected records"),
    }
}

// ── NULL handling tests ───────────────────────────────────────

#[tokio::test]
async fn test_null_is_null_is_not_null() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE nullable (id INT, val VARCHAR);")
        .await
        .unwrap();
    db.execute(
        "INSERT INTO nullable VALUES (1, 'present'), (2, CAST(NULL AS VARCHAR)), (3, 'also_present');",
    )
    .await
    .unwrap();

    match db
        .execute("SELECT id FROM nullable WHERE val IS NULL;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), 2);
        }
        _ => panic!("expected records"),
    }

    match db
        .execute("SELECT COUNT(*) AS cnt FROM nullable WHERE val IS NOT NULL;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(col.value(0), 2);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_coalesce() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL);").await.unwrap();

    match db
        .execute("SELECT COALESCE(name, 'default') AS n FROM t;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(col.value(0), "default");
        }
        _ => panic!("expected records"),
    }
}

// ── UPDATE all rows tests ─────────────────────────────────────

#[tokio::test]
async fn test_update_all_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, status VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'pending');")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'pending');")
        .await
        .unwrap();

    match db.execute("UPDATE t SET status = 'done';").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("2 row(s) updated"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db
        .execute("SELECT COUNT(*) AS n FROM t WHERE status = 'done';")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(col.value(0), 2);
        }
        _ => panic!("expected records"),
    }
}

// ── Transaction rejection tests ───────────────────────────────

#[tokio::test]
async fn test_update_allowed_in_transaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("BEGIN;").await.unwrap();
    db.execute("UPDATE t SET id = 2 WHERE id = 1;")
        .await
        .unwrap();
    db.execute("COMMIT;").await.unwrap();
}

#[tokio::test]
async fn test_vacuum_allowed_in_transaction() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("BEGIN;").await.unwrap();
    db.execute("VACUUM t;").await.unwrap();
    db.execute("COMMIT;").await.unwrap();
}

// ── Backup / restore tests ────────────────────────────────────

#[tokio::test]
async fn test_backup_and_restore() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let archive_path = tmp.path().join("backup.tar.gz");

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("CREATE TABLE items (id INT, label VARCHAR);")
            .await
            .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'alpha');")
            .await
            .unwrap();
        db.execute("INSERT INTO items VALUES (2, 'beta');")
            .await
            .unwrap();
        db.execute("FLUSH;").await.unwrap();
        db.backup(archive_path.to_string_lossy().as_ref())
            .await
            .unwrap();
    }

    assert!(archive_path.exists(), "backup archive should exist");

    // Restore into the same data directory (table paths in catalog are absolute).
    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("DROP TABLE items;").await.unwrap();
        assert!(db.execute("SELECT * FROM items;").await.is_err());

        db.restore(archive_path.to_string_lossy().as_ref())
            .await
            .unwrap();

        match db
            .execute("SELECT * FROM items ORDER BY id;")
            .await
            .unwrap()
        {
            QueryResult::Records(batches) => {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total, 2, "restored db should have 2 rows");
            }
            _ => panic!("expected records"),
        }
    }
}

// ── execute_readonly tests ────────────────────────────────────

#[tokio::test]
async fn test_execute_readonly_returns_results() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();

    match db.execute_readonly("SELECT * FROM t;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1);
        }
        _ => panic!("expected records"),
    }
}

// ── Query log tests ───────────────────────────────────────────

#[tokio::test]
async fn test_recent_queries_logged() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("SELECT * FROM t;").await.unwrap();

    let log = db.recent_queries();
    assert!(
        log.len() >= 3,
        "should have at least 3 entries, got {}",
        log.len()
    );
}

// ── IF EXISTS / IF NOT EXISTS tests ───────────────────────────

#[tokio::test]
async fn test_create_table_if_not_exists() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    match db
        .execute("CREATE TABLE IF NOT EXISTS t (id INT);")
        .await
        .unwrap()
    {
        QueryResult::Message(msg) => assert!(msg.contains("skipping"), "got: {msg}"),
        _ => panic!("expected message"),
    }
}

#[tokio::test]
async fn test_drop_table_if_exists_no_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    let result = db.execute("DROP TABLE IF EXISTS nonexistent;").await;
    assert!(
        result.is_ok(),
        "DROP IF EXISTS on missing table should not error"
    );
}

#[tokio::test]
async fn test_create_table_duplicate_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    assert!(
        db.execute("CREATE TABLE t (id INT);").await.is_err(),
        "duplicate CREATE TABLE without IF NOT EXISTS should fail"
    );
}

// ── Sequence persistence tests ────────────────────────────────

#[tokio::test]
async fn test_sequence_persists_across_restarts() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("CREATE SEQUENCE myseq;").await.unwrap();
        db.execute("CREATE TABLE t (id BIGINT, name VARCHAR);")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (nextval('myseq'), 'first');")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (nextval('myseq'), 'second');")
            .await
            .unwrap();
    }

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (nextval('myseq'), 'third');")
            .await
            .unwrap();

        match db.execute("SELECT id FROM t ORDER BY id;").await.unwrap() {
            QueryResult::Records(batches) => {
                let col = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                assert_eq!(col.value(0), 1);
                assert_eq!(col.value(1), 2);
                assert_eq!(col.value(2), 3, "sequence should continue after restart");
            }
            _ => panic!("expected records"),
        }
    }
}

// ── FLUSH tests ───────────────────────────────────────────────

#[tokio::test]
async fn test_flush_statement() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();

    match db.execute("FLUSH;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("flushed"), "got: {msg}"),
        _ => panic!("expected message"),
    }
}

#[tokio::test]
async fn test_flush_specific_table() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();

    match db.execute("FLUSH TABLE t;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("flushed"), "got: {msg}"),
        _ => panic!("expected message"),
    }
}

// ── Multiple constraints combined ─────────────────────────────

#[tokio::test]
async fn test_combined_constraints_reject_violations() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute(
        "CREATE TABLE strict_t (id INT NOT NULL, email VARCHAR, age INT, \
         PRIMARY KEY (id), UNIQUE (email), CHECK (age >= 0));",
    )
    .await
    .unwrap();

    db.execute("INSERT INTO strict_t VALUES (1, 'a@b.com', 25);")
        .await
        .unwrap();

    assert!(
        db.execute("INSERT INTO strict_t VALUES (NULL, 'b@c.com', 30);")
            .await
            .is_err(),
        "NOT NULL violation should return error"
    );
    assert!(
        db.execute("INSERT INTO strict_t VALUES (2, 'a@b.com', 30);")
            .await
            .is_err(),
        "UNIQUE violation should return error"
    );
    assert!(
        db.execute("INSERT INTO strict_t VALUES (3, 'c@d.com', -1);")
            .await
            .is_err(),
        "CHECK violation should return error"
    );
}

// ── Error handling tests ──────────────────────────────────────

#[tokio::test]
async fn test_insert_into_nonexistent_table() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    assert!(
        db.execute("INSERT INTO ghost VALUES (1);").await.is_err(),
        "insert into non-existent table should fail"
    );
}

#[tokio::test]
async fn test_select_from_nonexistent_table() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    assert!(
        db.execute("SELECT * FROM ghost;").await.is_err(),
        "select from non-existent table should fail"
    );
}

// ── COPY FROM CSV test ────────────────────────────────────────

#[tokio::test]
async fn test_copy_from_csv() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = tmp.path().join("input.csv");
    std::fs::write(&csv_path, "id,name\n1,alice\n2,bob\n").unwrap();

    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();
    db.execute("CREATE TABLE imported (id INT, name VARCHAR);")
        .await
        .unwrap();

    let sql = format!("COPY imported FROM '{}';", csv_path.to_string_lossy());
    match db.execute(&sql).await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("copied"), "got: {msg}"),
        _ => panic!("expected message"),
    }

    match db
        .execute("SELECT * FROM imported ORDER BY id;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2);
        }
        _ => panic!("expected records"),
    }
}

// ── COPY TO JSON test ─────────────────────────────────────────

#[tokio::test]
async fn test_copy_to_json() {
    let tmp = tempfile::tempdir().unwrap();
    let json_path = tmp.path().join("out.json");
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, name VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice');")
        .await
        .unwrap();

    let sql = format!("COPY t TO '{}';", json_path.to_string_lossy());
    match db.execute(&sql).await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("copied"), "got: {msg}"),
        _ => panic!("expected message"),
    }
    assert!(json_path.exists());
}

// ── Materialized view persistence test ────────────────────────

#[tokio::test]
async fn test_materialized_view_persists_across_restarts() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("CREATE TABLE src (id INT, val INT);")
            .await
            .unwrap();
        db.execute("INSERT INTO src VALUES (1, 10);").await.unwrap();
        db.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM src;")
            .await
            .unwrap();
    }

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        match db.execute("SELECT * FROM mv;").await.unwrap() {
            QueryResult::Records(batches) => {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total, 1, "matview should persist after restart");
            }
            _ => panic!("expected records"),
        }
    }
}

// ── DISTINCT / ORDER BY / LIMIT / OFFSET tests ───────────────

#[tokio::test]
async fn test_distinct() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (val VARCHAR);").await.unwrap();
    db.execute("INSERT INTO t VALUES ('a');").await.unwrap();
    db.execute("INSERT INTO t VALUES ('b');").await.unwrap();
    db.execute("INSERT INTO t VALUES ('a');").await.unwrap();

    match db
        .execute("SELECT DISTINCT val FROM t ORDER BY val;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_limit_and_offset() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({i});"))
            .await
            .unwrap();
    }

    match db
        .execute("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 3);
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(col.value(0), 3);
        }
        _ => panic!("expected records"),
    }
}

// ── Introspection method tests ────────────────────────────────

#[tokio::test]
async fn test_table_names_and_view_names() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE alpha (id INT);").await.unwrap();
    db.execute("CREATE TABLE beta (id INT);").await.unwrap();
    db.execute("CREATE VIEW vw AS SELECT * FROM alpha;")
        .await
        .unwrap();

    let tables = db.table_names();
    assert!(tables.contains(&"alpha".to_string()));
    assert!(tables.contains(&"beta".to_string()));

    let views = db.view_names();
    assert!(views.contains(&"vw".to_string()));
}

#[tokio::test]
async fn test_indexes_introspection() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, val INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10);").await.unwrap();
    db.execute("CREATE INDEX idx_t_val ON t (val);")
        .await
        .unwrap();

    let indexes = db.indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].0, "idx_t_val");
    assert_eq!(indexes[0].1, "t");
}

// ── Large dataset test ────────────────────────────────────────

#[tokio::test]
async fn test_large_insert_and_aggregate() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE big (id INT, val INT);")
        .await
        .unwrap();

    let mut values = Vec::new();
    for i in 1..=500 {
        values.push(format!("({i}, {})", i * 2));
    }
    let insert_sql = format!("INSERT INTO big VALUES {};", values.join(", "));
    db.execute(&insert_sql).await.unwrap();

    match db
        .execute("SELECT COUNT(*) AS cnt, SUM(val) AS total FROM big;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let cnt = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(cnt.value(0), 500);

            let total = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(total.value(0), 250500);
        }
        _ => panic!("expected records"),
    }
}

// ── Transaction: commit persists across restarts ──────────────

#[tokio::test]
async fn test_committed_transaction_persists_across_restarts() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        db.execute("CREATE TABLE t (id INT);").await.unwrap();
        db.execute("BEGIN;").await.unwrap();
        db.execute("INSERT INTO t VALUES (1);").await.unwrap();
        db.execute("INSERT INTO t VALUES (2);").await.unwrap();
        db.execute("COMMIT;").await.unwrap();
    }

    {
        let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None)
            .await
            .unwrap();
        match db.execute("SELECT * FROM t ORDER BY id;").await.unwrap() {
            QueryResult::Records(batches) => {
                let total: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(total, 2, "committed txn data should persist");
            }
            _ => panic!("expected records"),
        }
    }
}

// ── Rollback: DML + DDL interleave ───────────────────────────

#[tokio::test]
async fn test_rollback_reverts_mixed_ddl_dml() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE existing (id INT);").await.unwrap();
    db.execute("INSERT INTO existing VALUES (1);")
        .await
        .unwrap();

    db.execute("BEGIN;").await.unwrap();
    db.execute("CREATE TABLE new_table (x INT);").await.unwrap();
    db.execute("INSERT INTO existing VALUES (2);")
        .await
        .unwrap();
    db.execute("ROLLBACK;").await.unwrap();

    assert!(
        db.execute("SELECT * FROM new_table;").await.is_err(),
        "new table should not exist after rollback"
    );

    match db.execute("SELECT * FROM existing;").await.unwrap() {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 1, "only pre-txn row should remain");
        }
        _ => panic!("expected records"),
    }
}

// ── DROP INDEX IF EXISTS ──────────────────────────────────────

#[tokio::test]
async fn test_drop_index_if_exists_no_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    let result = db.execute("DROP INDEX IF EXISTS nonexistent;").await;
    assert!(result.is_ok(), "DROP INDEX IF EXISTS should not error");
}

// ── DROP VIEW IF EXISTS ───────────────────────────────────────

#[tokio::test]
async fn test_drop_view_if_exists_no_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    let result = db.execute("DROP VIEW IF EXISTS nonexistent;").await;
    assert!(result.is_ok(), "DROP VIEW IF EXISTS should not error");
}

// ── Aggregate functions ───────────────────────────────────────

#[tokio::test]
async fn test_aggregate_min_max_avg() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE nums (val INT);").await.unwrap();
    db.execute("INSERT INTO nums VALUES (10), (20), (30);")
        .await
        .unwrap();

    match db
        .execute("SELECT MIN(val) AS mn, MAX(val) AS mx, AVG(val) AS av FROM nums;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let mn = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(mn.value(0), 10);

            let mx = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(mx.value(0), 30);
        }
        _ => panic!("expected records"),
    }
}

// ── CASE expression test ──────────────────────────────────────

#[tokio::test]
async fn test_case_expression() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE t (id INT, score INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 90), (2, 50), (3, 75);")
        .await
        .unwrap();

    match db
        .execute(
            "SELECT id, CASE WHEN score >= 80 THEN 'A' \
             WHEN score >= 70 THEN 'B' ELSE 'C' END AS grade \
             FROM t ORDER BY id;",
        )
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let grades = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(grades.value(0), "A");
            assert_eq!(grades.value(1), "C");
            assert_eq!(grades.value(2), "B");
        }
        _ => panic!("expected records"),
    }
}

// ── New feature coverage ───────────────────────────────────────

#[tokio::test]
async fn test_new_types_and_jsonb_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE typed (id UUID, dur INTERVAL, tags INT[], payload JSONB);")
        .await
        .unwrap();
    db.execute("INSERT INTO typed VALUES (NULL, NULL, NULL, '{\"k\":1}');")
        .await
        .unwrap();

    match db.execute("SELECT payload FROM typed;").await.unwrap() {
        QueryResult::Records(batches) => {
            let col = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(col.value(0), "{\"k\":1}");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_multiple_indexes_and_introspection() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE midx (a INT, b INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO midx VALUES (1, 10), (2, 20), (3, 15);")
        .await
        .unwrap();
    db.execute("CREATE INDEX idx_midx_a ON midx (a);")
        .await
        .unwrap();
    db.execute("CREATE INDEX idx_midx_b ON midx (b DESC);")
        .await
        .unwrap();

    let indexes = db.indexes();
    assert!(indexes
        .iter()
        .any(|(n, t)| n == "idx_midx_a" && t == "midx"));
    assert!(indexes
        .iter()
        .any(|(n, t)| n == "idx_midx_b" && t == "midx"));
}

#[tokio::test]
async fn test_create_drop_function_and_function_names() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE FUNCTION add1(x INT) RETURNS INT AS '$1 + 1';")
        .await
        .unwrap();
    assert!(db.function_names().contains(&"add1".to_string()));

    match db.execute("SELECT add1(41);").await.unwrap() {
        QueryResult::Records(batches) => {
            if let Some(vals) = batches[0].column(0).as_any().downcast_ref::<Int32Array>() {
                assert_eq!(vals.value(0), 42);
            } else {
                let vals = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                assert_eq!(vals.value(0), 42);
            }
        }
        _ => panic!("expected records"),
    }

    db.execute("DROP FUNCTION add1;").await.unwrap();
    assert!(!db.function_names().contains(&"add1".to_string()));
}

#[tokio::test]
async fn test_partition_by_range_create_insert_select() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE p (ts INT, v INT) PARTITION BY RANGE (ts);")
        .await
        .unwrap();
    db.execute("INSERT INTO p VALUES (1, 10), (2, 20);")
        .await
        .unwrap();
    assert!(db.table_names().contains(&"p".to_string()));
}

#[tokio::test]
async fn test_copy_from_parquet_schema_evolution() {
    let tmp = tempfile::tempdir().unwrap();
    let parquet_path = tmp.path().join("src.parquet");
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE srcp (id INT, name VARCHAR, extra INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO srcp VALUES (1, 'a', 10), (2, 'b', 20);")
        .await
        .unwrap();
    db.execute(&format!(
        "COPY srcp TO '{}';",
        parquet_path.to_string_lossy()
    ))
    .await
    .unwrap();

    db.execute("CREATE TABLE dstp (id INT, name VARCHAR, missing_col INT);")
        .await
        .unwrap();
    db.execute(&format!(
        "COPY dstp FROM '{}';",
        parquet_path.to_string_lossy()
    ))
    .await
    .unwrap();

    match db
        .execute("SELECT id, name, missing_col FROM dstp ORDER BY id;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2);
            let miss = batches[0]
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert!(miss.is_null(0));
            assert!(miss.is_null(1));
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_destructive_ops_allowed_in_transaction_and_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE txw (id INT, v INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO txw VALUES (1, 10), (2, 20), (3, 30);")
        .await
        .unwrap();

    db.execute("BEGIN;").await.unwrap();
    db.execute("UPDATE txw SET v = v + 1 WHERE id <= 2;")
        .await
        .unwrap();
    db.execute("DELETE FROM txw WHERE id = 3;").await.unwrap();
    db.execute("CREATE INDEX idx_txw_v ON txw (v DESC);")
        .await
        .unwrap();
    db.execute("VACUUM txw;").await.unwrap();
    db.execute("COMMIT;").await.unwrap();

    match db.execute("SELECT COUNT(*) FROM txw;").await.unwrap() {
        QueryResult::Records(batches) => {
            let cnt = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(cnt.value(0), 2);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_rollback_restores_rewritten_table_data() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE txr (id INT, v INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO txr VALUES (1, 10), (2, 20), (3, 30);")
        .await
        .unwrap();

    db.execute("BEGIN;").await.unwrap();
    db.execute("UPDATE txr SET v = 999 WHERE id = 1;")
        .await
        .unwrap();
    db.execute("DELETE FROM txr WHERE id = 2;").await.unwrap();
    db.execute("CREATE INDEX idx_txr_v ON txr (v);")
        .await
        .unwrap();
    db.execute("ROLLBACK;").await.unwrap();

    match db
        .execute("SELECT id, v FROM txr ORDER BY id;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 3);
            let v = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(v.value(0), 10);
            assert_eq!(v.value(1), 20);
            assert_eq!(v.value(2), 30);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_time_travel_as_of_timestamp() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    let as_of = Utc::now().timestamp_millis();
    db.execute("CREATE TABLE tt (id INT PRIMARY KEY);")
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    db.execute("INSERT INTO tt VALUES (1);").await.unwrap();

    match db
        .execute(&format!("SELECT COUNT(*) FROM tt AS OF TIMESTAMP {as_of};"))
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let cnt = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(cnt.value(0), 0);
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_foreign_key_restrict_and_insert_validation() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE parent (id INT PRIMARY KEY);")
        .await
        .unwrap();
    db.execute(
        "CREATE TABLE child_r (pid INT, FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE RESTRICT);",
    )
    .await
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1);").await.unwrap();
    db.execute("FLUSH TABLE parent;").await.unwrap();
    db.execute("INSERT INTO child_r VALUES (1);").await.unwrap();
    assert!(
        db.execute("INSERT INTO child_r VALUES (999);")
            .await
            .is_err(),
        "FK should reject missing parent references"
    );

    assert!(db
        .execute("DELETE FROM parent WHERE id = 1;")
        .await
        .is_err());
}

#[tokio::test]
async fn test_plan_cache_invalidation_and_rbac_commands() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE pc (id INT);").await.unwrap();
    db.execute("INSERT INTO pc VALUES (1);").await.unwrap();
    db.execute("SELECT COUNT(*) FROM pc;").await.unwrap();
    db.execute("SELECT COUNT(*) FROM pc;").await.unwrap();
    db.execute("INSERT INTO pc VALUES (2);").await.unwrap();
    match db.execute("SELECT COUNT(*) FROM pc;").await.unwrap() {
        QueryResult::Records(batches) => {
            let cnt = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(cnt.value(0), 2);
        }
        _ => panic!("expected records"),
    }

    db.execute("CREATE USER bob WITH PASSWORD 'secret';")
        .await
        .unwrap();
    db.execute("CREATE ROLE analyst;").await.unwrap();
    db.execute("GRANT SELECT ON pc TO analyst;").await.unwrap();
    db.execute("REVOKE SELECT ON pc FROM analyst;")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_table_retention_policy_statement() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE rt (id INT);").await.unwrap();
    db.execute("INSERT INTO rt VALUES (1), (2);").await.unwrap();
    db.execute("ALTER TABLE rt SET (retention = '0 seconds');")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_fulltext_procedure_do_cdc_and_notifications() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE docs (id INT, body VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'rust database'), (2, 'other text');")
        .await
        .unwrap();
    db.execute("CREATE FULLTEXT INDEX idx_docs_body ON docs(body);")
        .await
        .unwrap();
    match db
        .execute("SELECT id FROM docs WHERE fts_match('rust') ORDER BY id;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(ids.value(0), 1);
        }
        _ => panic!("expected records"),
    }

    db.execute("CREATE TABLE proc_t (id INT);").await.unwrap();
    db.execute(
        "CREATE PROCEDURE add_rows() AS $$ INSERT INTO proc_t VALUES (1); INSERT INTO proc_t VALUES (2); $$;",
    )
    .await
    .unwrap();
    db.execute("CALL add_rows();").await.unwrap();
    db.execute("DO $$ INSERT INTO proc_t VALUES (3); $$;")
        .await
        .unwrap();
    match db.execute("SELECT COUNT(*) FROM proc_t;").await.unwrap() {
        QueryResult::Records(batches) => {
            let cnt = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            assert_eq!(cnt.value(0), 3);
        }
        _ => panic!("expected records"),
    }

    db.execute("LISTEN jobs;").await.unwrap();
    db.execute("NOTIFY jobs, 'hello';").await.unwrap();

    db.execute("UPDATE docs SET body = 'rust db' WHERE id = 2;")
        .await
        .unwrap();
    db.execute("DELETE FROM docs WHERE id = 2;").await.unwrap();
    match db
        .execute("SELECT table, op FROM potatodb_cdc WHERE table = 'docs';")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(
                total >= 3,
                "expected insert/update/delete CDC rows, got {total}"
            );
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_generate_series() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    match db
        .execute("SELECT * FROM generate_series(1, 5);")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 5, "expected 5 rows from generate_series(1,5)");
            let col = batches[0].column(0);
            let vals = col
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            for (i, v) in (1..=5).enumerate() {
                assert_eq!(vals.value(i), v as i64);
            }
        }
        _ => panic!("expected records from generate_series"),
    }

    // With step
    match db
        .execute("SELECT * FROM generate_series(0, 10, 2);")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 6, "expected 6 rows from generate_series(0,10,2)");
        }
        _ => panic!("expected records"),
    }
}

#[tokio::test]
async fn test_do_block_declare_and_variables() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();

    db.execute("CREATE TABLE do_test (id INT, msg VARCHAR);")
        .await
        .unwrap();

    // DO block with DECLARE and variable substitution
    db.execute(
        r#"DO $$
        DECLARE
            n INT := 42;
            s VARCHAR := 'hello';
        BEGIN
            INSERT INTO do_test VALUES (n, s);
            INSERT INTO do_test VALUES (1, 'world');
        END;
        $$;"#,
    )
    .await
    .unwrap();

    match db
        .execute("SELECT id, msg FROM do_test ORDER BY id;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            assert_eq!(batches[0].num_rows(), 2);
            let ids = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let msgs = batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(ids.value(0), 1);
            assert_eq!(msgs.value(0), "world");
            assert_eq!(ids.value(1), 42);
            assert_eq!(msgs.value(1), "hello");
        }
        _ => panic!("expected records"),
    }

    // RAISE NOTICE is a no-op
    db.execute(
        r#"DO $$ BEGIN RAISE NOTICE 'test message'; END; $$;"#,
    )
    .await
    .unwrap();
}
