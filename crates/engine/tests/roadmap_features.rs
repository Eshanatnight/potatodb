use potatodb_engine::{PotatoDB, QueryResult};

#[tokio::test]
async fn test_explain_option_list_normalizes() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();
    db.execute("CREATE TABLE t (id INT);").await.unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3);")
        .await
        .unwrap();

    match db
        .execute("EXPLAIN (FORMAT JSON, ANALYZE) SELECT * FROM t WHERE id > 1;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => assert!(!batches.is_empty()),
        QueryResult::Message(msg) => panic!("expected records, got message: {msg}"),
    }
}

#[tokio::test]
async fn test_checkpoint_command_executes() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();
    db.execute("CREATE TABLE ck (id INT);").await.unwrap();
    db.execute("INSERT INTO ck VALUES (1);").await.unwrap();
    match db.execute("CHECKPOINT;").await.unwrap() {
        QueryResult::Message(msg) => assert!(msg.contains("Checkpoint completed")),
        QueryResult::Records(_) => panic!("expected checkpoint message"),
    }
}

#[tokio::test]
async fn test_copy_from_reports_diagnostics() {
    let tmp = tempfile::tempdir().unwrap();
    let csv_path = tmp.path().join("input.csv");
    std::fs::write(&csv_path, "id,name\n1,alice\n2,bob\n").unwrap();

    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();
    db.execute("CREATE TABLE users (id INT, name VARCHAR, age INT);")
        .await
        .unwrap();
    let copy_sql = format!("COPY users FROM '{}';", csv_path.to_string_lossy());
    match db.execute(&copy_sql).await.unwrap() {
        QueryResult::Message(msg) => {
            assert!(msg.contains("copied into 'users'"));
            assert!(msg.contains("missing target columns filled with NULL"));
            assert!(msg.contains("(csv)"));
        }
        QueryResult::Records(_) => panic!("expected copy message"),
    }
}

#[tokio::test]
async fn test_system_status_virtual_table() {
    let tmp = tempfile::tempdir().unwrap();
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
        .await
        .unwrap();
    db.execute("CREATE TABLE s (id INT);").await.unwrap();
    db.execute("INSERT INTO s VALUES (1);").await.unwrap();
    match db
        .execute("SELECT * FROM potatodb_system_status;")
        .await
        .unwrap()
    {
        QueryResult::Records(batches) => {
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);
            assert!(batches[0].num_columns() >= 6);
        }
        QueryResult::Message(msg) => panic!("expected records, got message: {msg}"),
    }
}
