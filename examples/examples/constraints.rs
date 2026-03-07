/// Data integrity: constraints, upserts, and error handling.
///
/// Demonstrates: PRIMARY KEY, UNIQUE, NOT NULL, CHECK, ON CONFLICT.
///
/// Run with:
///   cargo run --example constraints
use potatodb_engine::PotatoDB;
use potatodb_examples::{print_result, section, BoxError};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let tmp = tempfile::tempdir()?;
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None).await?;

    // ── PRIMARY KEY ───────────────────────────────────────────
    section("PRIMARY KEY constraint");
    db.execute("CREATE TABLE pk_demo (id INT, name VARCHAR, PRIMARY KEY (id));")
        .await?;
    db.execute("INSERT INTO pk_demo VALUES (1, 'Alice');")
        .await?;
    db.execute("INSERT INTO pk_demo VALUES (2, 'Bob');").await?;

    let res = db.execute("SELECT * FROM pk_demo ORDER BY id;").await?;
    print_result("Rows with unique PKs", &res);

    match db
        .execute("INSERT INTO pk_demo VALUES (1, 'Duplicate');")
        .await
    {
        Err(e) => println!("Duplicate PK rejected: {e}"),
        Ok(_) => println!("Insert succeeded (no conflict detected)."),
    }

    // ── UNIQUE ────────────────────────────────────────────────
    section("UNIQUE constraint");
    db.execute("CREATE TABLE unique_demo (id INT, email VARCHAR, UNIQUE (email));")
        .await?;
    db.execute("INSERT INTO unique_demo VALUES (1, 'alice@example.com');")
        .await?;

    match db
        .execute("INSERT INTO unique_demo VALUES (2, 'alice@example.com');")
        .await
    {
        Err(e) => println!("Duplicate email rejected: {e}"),
        Ok(_) => println!("Insert succeeded (no conflict detected)."),
    }

    // ── NOT NULL ──────────────────────────────────────────────
    section("NOT NULL constraint");
    db.execute("CREATE TABLE notnull_demo (id INT NOT NULL, name VARCHAR);")
        .await?;
    db.execute("INSERT INTO notnull_demo VALUES (1, 'Valid');")
        .await?;

    match db
        .execute("INSERT INTO notnull_demo VALUES (NULL, 'Missing ID');")
        .await
    {
        Err(e) => println!("NULL in NOT NULL column rejected: {e}"),
        Ok(_) => println!("Insert succeeded (no constraint detected)."),
    }

    // ── CHECK ─────────────────────────────────────────────────
    section("CHECK constraint");
    db.execute("CREATE TABLE check_demo (id INT, age INT, CHECK (age >= 0));")
        .await?;
    db.execute("INSERT INTO check_demo VALUES (1, 25);").await?;

    match db.execute("INSERT INTO check_demo VALUES (2, -5);").await {
        Err(e) => println!("Negative age rejected: {e}"),
        Ok(_) => println!("Insert succeeded (no constraint detected)."),
    }

    // ── Combined constraints ──────────────────────────────────
    section("Combined constraints");
    db.execute(
        "CREATE TABLE accounts ( \
             id    INT NOT NULL, \
             email VARCHAR, \
             name  VARCHAR NOT NULL, \
             age   INT, \
             PRIMARY KEY (id), \
             UNIQUE (email), \
             CHECK (age >= 0) \
         );",
    )
    .await?;
    db.execute("INSERT INTO accounts VALUES (1, 'alice@co.com', 'Alice', 30);")
        .await?;
    db.execute("INSERT INTO accounts VALUES (2, 'bob@co.com',   'Bob',   25);")
        .await?;
    db.execute("INSERT INTO accounts VALUES (3, 'carol@co.com', 'Carol', 40);")
        .await?;

    let res = db.execute("SELECT * FROM accounts ORDER BY id;").await?;
    print_result("Accounts with PK + UNIQUE + NOT NULL + CHECK", &res);

    // ── ON CONFLICT DO NOTHING (skip duplicates) ──────────────
    section("UPSERT: ON CONFLICT DO NOTHING");
    db.execute("CREATE TABLE inventory (sku INT, name VARCHAR, qty INT, PRIMARY KEY (sku));")
        .await?;
    db.execute(
        "INSERT INTO inventory VALUES \
         (100, 'Widget', 50), \
         (200, 'Gadget', 30);",
    )
    .await?;

    let res = db
        .execute(
            "INSERT INTO inventory VALUES (100, 'Duplicate Widget', 999) \
             ON CONFLICT (sku) DO NOTHING;",
        )
        .await?;
    print_result("Attempt to insert duplicate SKU", &res);

    let res = db.execute("SELECT * FROM inventory ORDER BY sku;").await?;
    print_result("Original row unchanged", &res);

    // ── ON CONFLICT DO UPDATE (merge) ─────────────────────────
    section("UPSERT: ON CONFLICT DO UPDATE");
    let res = db
        .execute(
            "INSERT INTO inventory VALUES (200, 'Gadget Pro', 45) \
             ON CONFLICT (sku) DO UPDATE SET \
                 name = EXCLUDED.name, \
                 qty = EXCLUDED.qty;",
        )
        .await?;
    print_result("Upsert: update Gadget to Gadget Pro", &res);

    let res = db.execute("SELECT * FROM inventory ORDER BY sku;").await?;
    print_result("After upsert (Gadget updated)", &res);

    // Insert a brand-new row via upsert (no conflict)
    let res = db
        .execute(
            "INSERT INTO inventory VALUES (300, 'Gizmo', 10) \
             ON CONFLICT (sku) DO UPDATE SET qty = EXCLUDED.qty;",
        )
        .await?;
    print_result("Upsert: insert new Gizmo (no conflict)", &res);

    let res = db.execute("SELECT * FROM inventory ORDER BY sku;").await?;
    print_result("Final inventory", &res);

    // ── Cleanup ───────────────────────────────────────────────
    db.execute("DROP TABLE pk_demo;").await?;
    db.execute("DROP TABLE unique_demo;").await?;
    db.execute("DROP TABLE notnull_demo;").await?;
    db.execute("DROP TABLE check_demo;").await?;
    db.execute("DROP TABLE accounts;").await?;
    db.execute("DROP TABLE inventory;").await?;

    println!("\nDone!");
    Ok(())
}
