/// Transaction control: atomic commits and rollbacks.
///
/// Demonstrates: BEGIN, COMMIT, ROLLBACK, and persistence guarantees.
///
/// Run with:
///   cargo run --example transactions
use potatodb_engine::PotatoDB;
use potatodb_examples::{print_result, section, BoxError};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().to_string_lossy().to_string();

    // ── Auto-commit (default behavior) ────────────────────────
    section("Auto-commit mode");
    {
        let mut db = PotatoDB::new(data_dir.clone(), None).await?;

        db.execute("CREATE TABLE ledger (id INT, description VARCHAR, amount DOUBLE);")
            .await?;
        db.execute("INSERT INTO ledger VALUES (1, 'Initial deposit', 1000.00);")
            .await?;
        db.execute("INSERT INTO ledger VALUES (2, 'Coffee', -4.50);")
            .await?;
        println!("Each statement auto-commits individually.");

        let res = db.execute("SELECT * FROM ledger ORDER BY id;").await?;
        print_result("Ledger after auto-commit inserts", &res);
    }

    // ── BEGIN / COMMIT (atomic batch) ─────────────────────────
    section("BEGIN / COMMIT");
    {
        let mut db = PotatoDB::new(data_dir.clone(), None).await?;

        let res = db.execute("BEGIN;").await?;
        print_result("Begin transaction", &res);

        db.execute("INSERT INTO ledger VALUES (3, 'Salary', 5000.00);")
            .await?;
        db.execute("INSERT INTO ledger VALUES (4, 'Rent', -1500.00);")
            .await?;
        db.execute("INSERT INTO ledger VALUES (5, 'Groceries', -120.00);")
            .await?;
        println!("Inserted 3 rows inside transaction (not yet committed).");

        let res = db.execute("COMMIT;").await?;
        print_result("Commit transaction", &res);

        let res = db
            .execute("SELECT COUNT(*) AS entries, SUM(amount) AS balance FROM ledger;")
            .await?;
        print_result("Ledger summary after commit", &res);
    }

    // ── BEGIN / ROLLBACK (discard changes) ────────────────────
    section("BEGIN / ROLLBACK");
    {
        let mut db = PotatoDB::new(data_dir.clone(), None).await?;

        let res = db
            .execute("SELECT COUNT(*) AS before_count FROM ledger;")
            .await?;
        print_result("Row count before transaction", &res);

        db.execute("BEGIN;").await?;
        db.execute("INSERT INTO ledger VALUES (6, 'Bad purchase', -9999.99);")
            .await?;
        db.execute("INSERT INTO ledger VALUES (7, 'Another mistake', -5000.00);")
            .await?;
        println!("Inserted 2 rows inside transaction...");

        let res = db.execute("ROLLBACK;").await?;
        print_result("Rollback transaction", &res);

        let res = db
            .execute("SELECT COUNT(*) AS after_count FROM ledger;")
            .await?;
        print_result("Row count after rollback (unchanged)", &res);
    }

    // ── Rollback reverting a CREATE TABLE ─────────────────────
    section("ROLLBACK reverts DDL");
    {
        let mut db = PotatoDB::new(data_dir.clone(), None).await?;

        db.execute("BEGIN;").await?;
        db.execute("CREATE TABLE temp_data (x INT);").await?;
        db.execute("INSERT INTO temp_data VALUES (42);").await?;
        println!("Created and populated 'temp_data' inside transaction...");

        db.execute("ROLLBACK;").await?;
        println!("Rolled back.");

        match db.execute("SELECT * FROM temp_data;").await {
            Err(_) => println!("Confirmed: 'temp_data' does not exist after rollback."),
            Ok(_) => println!("Unexpected: table still exists!"),
        }
    }

    // ── Rollback reverting a DROP TABLE ───────────────────────
    section("ROLLBACK restores dropped table");
    {
        let mut db = PotatoDB::new(data_dir.clone(), None).await?;

        db.execute("BEGIN;").await?;
        db.execute("DROP TABLE ledger;").await?;
        println!("Dropped 'ledger' inside transaction...");

        match db.execute("SELECT * FROM ledger;").await {
            Err(_) => println!("Table is invisible during transaction (expected)."),
            Ok(_) => println!("Unexpected: table still visible!"),
        }

        db.execute("ROLLBACK;").await?;
        println!("Rolled back.");

        let res = db
            .execute("SELECT COUNT(*) AS restored_rows FROM ledger;")
            .await?;
        print_result("Ledger restored after rollback", &res);
    }

    // ── Committed data persists across restarts ───────────────
    section("Persistence across restarts");
    {
        let mut db = PotatoDB::new(data_dir.clone(), None).await?;
        let res = db.execute("SELECT * FROM ledger ORDER BY id;").await?;
        print_result("Ledger survives engine restart", &res);
    }

    // ── Cleanup ───────────────────────────────────────────────
    {
        let mut db = PotatoDB::new(data_dir.clone(), None).await?;
        db.execute("DROP TABLE ledger;").await?;
    }

    println!("\nDone!");
    Ok(())
}
