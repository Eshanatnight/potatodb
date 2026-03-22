//! Integration tests for ephemeral in-memory storage (`:memory:` / `memory://...`).

use potatodb_engine::{PotatoDB, QueryResult};

fn row_count(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn expect_records(result: QueryResult) -> Vec<arrow::record_batch::RecordBatch> {
    match result {
        QueryResult::Records(b) => b,
        QueryResult::Message(m) => panic!("expected records, got message: {m}"),
    }
}

#[tokio::test]
async fn in_memory_colon_memory_normalizes_data_url() {
    let db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    assert!(db.is_in_memory());
    assert_eq!(db.data_url(), "memory://potatodb");
}

#[tokio::test]
async fn in_memory_bare_memory_alias() {
    let db = PotatoDB::new("memory".to_string(), None).await.unwrap();
    assert!(db.is_in_memory());
    assert_eq!(db.data_url(), "memory://potatodb");
}

#[tokio::test]
async fn in_memory_host_and_key_prefix() {
    let db = PotatoDB::new("memory://myhost/ns/prefix".to_string(), None)
        .await
        .unwrap();
    assert!(db.is_in_memory());
    assert_eq!(db.data_url(), "memory://myhost/ns/prefix");
}

#[tokio::test]
async fn in_memory_create_insert_select() {
    let mut db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    assert!(db.is_in_memory());

    db.execute("CREATE TABLE mem_t (id INT, v VARCHAR);")
        .await
        .unwrap();
    db.execute("INSERT INTO mem_t VALUES (1, 'x');")
        .await
        .unwrap();

    let batches = expect_records(
        db.execute("SELECT * FROM mem_t ORDER BY id;")
            .await
            .unwrap(),
    );
    assert_eq!(row_count(&batches), 1);
}

#[tokio::test]
async fn in_memory_join_two_tables() {
    let mut db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    db.execute("CREATE TABLE a (id INT, x INT);").await.unwrap();
    db.execute("CREATE TABLE b (id INT, a_id INT);")
        .await
        .unwrap();
    db.execute("INSERT INTO a VALUES (1, 10);").await.unwrap();
    db.execute("INSERT INTO b VALUES (1, 1);").await.unwrap();

    let batches = expect_records(
        db.execute("SELECT a.x FROM a JOIN b ON a.id = b.a_id;")
            .await
            .unwrap(),
    );
    assert_eq!(row_count(&batches), 1);
}

#[tokio::test]
async fn in_memory_transaction_commit() {
    let mut db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("BEGIN;").await.unwrap();
    db.execute("INSERT INTO t VALUES (42);").await.unwrap();
    db.execute("COMMIT;").await.unwrap();

    let batches = expect_records(db.execute("SELECT * FROM t;").await.unwrap());
    assert_eq!(row_count(&batches), 1);
}

#[tokio::test]
async fn in_memory_transaction_rollback_discards_writes() {
    let mut db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("BEGIN;").await.unwrap();
    db.execute("INSERT INTO t VALUES (2);").await.unwrap();
    db.execute("ROLLBACK;").await.unwrap();

    let batches = expect_records(db.execute("SELECT * FROM t ORDER BY id;").await.unwrap());
    assert_eq!(row_count(&batches), 1);
}

#[tokio::test]
async fn in_memory_backup_is_rejected() {
    let db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    let err = db
        .backup("/tmp/potatodb_in_mem_backup.tar.gz")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("local data directories") && msg.contains("in-memory"),
        "unexpected backup error: {msg}"
    );
}

#[tokio::test]
async fn in_memory_restore_is_rejected() {
    let mut db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    let err = db
        .restore("/tmp/potatodb_in_mem_restore.tar.gz")
        .await
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("local data directories"),
        "unexpected restore error: {msg}"
    );
}

#[tokio::test]
async fn in_memory_flush_and_vacuum() {
    let mut db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1);").await.unwrap();
    db.execute("FLUSH;").await.unwrap();
    db.execute("VACUUM t;").await.unwrap();

    let batches = expect_records(db.execute("SELECT * FROM t;").await.unwrap());
    assert_eq!(row_count(&batches), 1);
}

#[tokio::test]
async fn in_memory_execute_readonly() {
    let mut db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (7);").await.unwrap();

    match db.execute_readonly("SELECT * FROM t;").await.unwrap() {
        QueryResult::Records(batches) => assert_eq!(row_count(&batches), 1),
        QueryResult::Message(m) => panic!("expected records, got: {m}"),
    }
}

#[tokio::test]
async fn in_memory_drop_table() {
    let mut db = PotatoDB::new(":memory:".to_string(), None).await.unwrap();
    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("DROP TABLE t;").await.unwrap();
    assert!(db.execute("SELECT * FROM t;").await.is_err());
}
