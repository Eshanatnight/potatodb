/// Database maintenance: indexes, views, sequences, backup/restore, and more.
///
/// Demonstrates: CREATE INDEX, VACUUM, ANALYZE, FLUSH, Views, Materialized
/// Views, Sequences, Backup/Restore, COPY, CREATE TABLE AS SELECT.
///
/// Run with:
///   cargo run --example maintenance
use potatodb_engine::PotatoDB;
use potatodb_examples::{print_result, section, BoxError};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().join("db");
    std::fs::create_dir_all(&data_dir)?;
    let mut db = PotatoDB::new(data_dir.to_string_lossy().to_string(), None).await?;

    // ── Seed some data ────────────────────────────────────────
    db.execute("CREATE TABLE events (id INT, ts INT, severity VARCHAR, message VARCHAR);")
        .await?;
    db.execute(
        "INSERT INTO events VALUES \
         (1, 300, 'ERROR',   'disk full'), \
         (2, 100, 'INFO',    'startup'), \
         (3, 500, 'WARNING', 'high memory'), \
         (4, 200, 'ERROR',   'timeout'), \
         (5, 400, 'INFO',    'shutdown');",
    )
    .await?;

    // ── CREATE INDEX (physical sort order) ────────────────────
    section("CREATE INDEX");
    let res = db
        .execute("CREATE INDEX idx_events_ts ON events (ts);")
        .await?;
    print_result("Create index on timestamp", &res);

    let res = db.execute("SELECT id, ts, severity FROM events;").await?;
    print_result("Events are now physically sorted by ts", &res);

    println!(
        "\nWith the index, ORDER BY ts queries skip the sort step \
         and LIMIT queries terminate early."
    );

    // ── VACUUM (compaction) ───────────────────────────────────
    section("VACUUM");
    for i in 6..=10 {
        db.execute(&format!(
            "INSERT INTO events VALUES ({i}, {}, 'INFO', 'event {i}');",
            i * 100
        ))
        .await?;
    }
    println!("Inserted 5 more rows (each creates a separate Parquet file).");

    let res = db.execute("VACUUM events;").await?;
    print_result("Compact fragmented files", &res);

    // ── ANALYZE (statistics) ──────────────────────────────────
    section("ANALYZE");
    let res = db.execute("ANALYZE events;").await?;
    print_result("Collect optimizer statistics", &res);

    // ── FLUSH (write buffer) ──────────────────────────────────
    section("FLUSH");
    db.execute("INSERT INTO events VALUES (11, 1100, 'DEBUG', 'buffered row');")
        .await?;
    let res = db.execute("FLUSH TABLE events;").await?;
    print_result("Flush buffered inserts to Parquet", &res);

    // ── Views ─────────────────────────────────────────────────
    section("VIEWS");
    let res = db
        .execute("CREATE VIEW errors AS SELECT * FROM events WHERE severity = 'ERROR';")
        .await?;
    print_result("Create view", &res);

    let res = db.execute("SELECT * FROM errors ORDER BY ts;").await?;
    print_result("Query view: only ERROR events", &res);

    // ── Materialized views ────────────────────────────────────
    section("MATERIALIZED VIEWS");
    let res = db
        .execute(
            "CREATE MATERIALIZED VIEW severity_counts AS \
             SELECT severity, COUNT(*) AS cnt FROM events GROUP BY severity;",
        )
        .await?;
    print_result("Create materialized view", &res);

    let res = db
        .execute("SELECT * FROM severity_counts ORDER BY cnt DESC;")
        .await?;
    print_result("Materialized view results", &res);

    db.execute("INSERT INTO events VALUES (12, 1200, 'ERROR', 'new error');")
        .await?;
    let res = db
        .execute("REFRESH MATERIALIZED VIEW severity_counts;")
        .await?;
    print_result("Refresh after new data", &res);

    let res = db
        .execute("SELECT * FROM severity_counts ORDER BY cnt DESC;")
        .await?;
    print_result("Updated counts", &res);

    // ── Sequences ─────────────────────────────────────────────
    section("SEQUENCES");
    db.execute("CREATE SEQUENCE ticket_seq;").await?;
    db.execute("CREATE TABLE tickets (id BIGINT, title VARCHAR);")
        .await?;

    db.execute("INSERT INTO tickets VALUES (nextval('ticket_seq'), 'Fix login bug');")
        .await?;
    db.execute("INSERT INTO tickets VALUES (nextval('ticket_seq'), 'Add dark mode');")
        .await?;
    db.execute("INSERT INTO tickets VALUES (nextval('ticket_seq'), 'Update docs');")
        .await?;

    let res = db.execute("SELECT * FROM tickets ORDER BY id;").await?;
    print_result("Auto-incrementing IDs via sequence", &res);

    // ── CREATE TABLE AS SELECT ────────────────────────────────
    section("CREATE TABLE AS SELECT");
    let res = db
        .execute(
            "CREATE TABLE recent_errors AS \
             SELECT id, ts, message FROM events \
             WHERE severity = 'ERROR' AND ts > 150;",
        )
        .await?;
    print_result("Create table from query", &res);

    let res = db
        .execute("SELECT * FROM recent_errors ORDER BY ts;")
        .await?;
    print_result("New table contents", &res);

    // ── COPY TO (export) ──────────────────────────────────────
    section("COPY TO (export)");
    let csv_path = tmp.path().join("events_export.csv");
    let res = db
        .execute(&format!("COPY events TO '{}';", csv_path.to_string_lossy()))
        .await?;
    print_result("Export events to CSV", &res);
    println!("Exported to: {}", csv_path.display());

    // ── Backup and restore ────────────────────────────────────
    section("BACKUP / RESTORE");
    let archive = tmp.path().join("backup.tar.gz");
    db.backup(archive.to_string_lossy().as_ref()).await?;
    println!("Backup created: {}", archive.display());

    let restore_dir = tmp.path().join("restored");
    std::fs::create_dir_all(&restore_dir)?;
    let mut db2 = PotatoDB::new(restore_dir.to_string_lossy().to_string(), None).await?;
    db2.restore(archive.to_string_lossy().as_ref()).await?;

    let res = db2
        .execute("SELECT COUNT(*) AS event_count FROM events;")
        .await?;
    print_result("Row count in restored database", &res);

    // ── DROP INDEX ────────────────────────────────────────────
    section("DROP INDEX");
    let res = db.execute("DROP INDEX idx_events_ts;").await?;
    print_result("Drop index", &res);

    // ── Cleanup ───────────────────────────────────────────────
    db.execute("DROP SEQUENCE ticket_seq;").await?;
    db.execute("DROP VIEW errors;").await?;
    db.execute("DROP TABLE tickets;").await?;
    db.execute("DROP TABLE recent_errors;").await?;
    db.execute("DROP TABLE events;").await?;

    println!("\nDone!");
    Ok(())
}
