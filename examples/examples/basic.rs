/// Basic CRUD operations with PotatoDB.
///
/// Demonstrates: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE.
///
/// Run with:
///   cargo run --example basic
use potatodb_engine::PotatoDB;
use potatodb_examples::{print_result, section, BoxError};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let tmp = tempfile::tempdir()?;
    let mut db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None).await?;

    // ── Create a table ────────────────────────────────────────
    section("CREATE TABLE");
    let res = db
        .execute("CREATE TABLE users (id INT, name VARCHAR, email VARCHAR, age INT);")
        .await?;
    print_result("Create users table", &res);

    // ── Insert rows (single and multi-row) ────────────────────
    section("INSERT");
    db.execute("INSERT INTO users VALUES (1, 'Alice',   'alice@example.com',   32);")
        .await?;
    db.execute("INSERT INTO users VALUES (2, 'Bob',     'bob@example.com',     28);")
        .await?;
    db.execute(
        "INSERT INTO users VALUES \
         (3, 'Charlie', 'charlie@example.com', 45), \
         (4, 'Diana',   'diana@example.com',   36), \
         (5, 'Eve',     'eve@example.com',     24);",
    )
    .await?;
    println!("Inserted 5 rows (mix of single and multi-row INSERT).");

    // ── Select all rows ───────────────────────────────────────
    section("SELECT *");
    let res = db.execute("SELECT * FROM users ORDER BY id;").await?;
    print_result("All users", &res);

    // ── Filtering with WHERE ──────────────────────────────────
    section("WHERE / ORDER BY / LIMIT");
    let res = db
        .execute("SELECT name, age FROM users WHERE age > 30 ORDER BY age DESC;")
        .await?;
    print_result("Users older than 30", &res);

    let res = db
        .execute("SELECT name FROM users ORDER BY name LIMIT 3;")
        .await?;
    print_result("First 3 users alphabetically", &res);

    let res = db
        .execute("SELECT name FROM users ORDER BY id LIMIT 2 OFFSET 2;")
        .await?;
    print_result("2 users starting from offset 2", &res);

    // ── Aggregations ──────────────────────────────────────────
    section("AGGREGATIONS");
    let res = db
        .execute(
            "SELECT COUNT(*) AS total, \
                    MIN(age) AS youngest, \
                    MAX(age) AS oldest, \
                    AVG(age) AS avg_age \
             FROM users;",
        )
        .await?;
    print_result("User statistics", &res);

    // ── Update rows ───────────────────────────────────────────
    section("UPDATE");
    let res = db
        .execute("UPDATE users SET email = 'alice@newdomain.com' WHERE name = 'Alice';")
        .await?;
    print_result("Update Alice's email", &res);

    let res = db
        .execute("SELECT name, email FROM users WHERE name = 'Alice';")
        .await?;
    print_result("Verify Alice's new email", &res);

    // ── Delete rows ───────────────────────────────────────────
    section("DELETE");
    let res = db.execute("DELETE FROM users WHERE age < 25;").await?;
    print_result("Delete users younger than 25", &res);

    let res = db
        .execute("SELECT name, age FROM users ORDER BY id;")
        .await?;
    print_result("Remaining users", &res);

    // ── DISTINCT ──────────────────────────────────────────────
    section("DISTINCT");
    db.execute("INSERT INTO users VALUES (6, 'Alice', 'alice2@example.com', 29);")
        .await?;
    let res = db
        .execute("SELECT DISTINCT name FROM users ORDER BY name;")
        .await?;
    print_result("Distinct names", &res);

    // ── Drop the table ────────────────────────────────────────
    section("DROP TABLE");
    let res = db.execute("DROP TABLE users;").await?;
    print_result("Drop users table", &res);

    println!("\nDone!");
    Ok(())
}
