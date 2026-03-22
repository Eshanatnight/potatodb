/// End-to-end performance test suite for PotatoDB.
///
/// Runs a representative workload, captures wall-clock timings, and
/// outputs a JSON report.  Optionally compares against a baseline
/// report from a previous run so you can spot regressions between
/// versions.
///
/// Usage:
///   cargo run --release --example perftest
///   cargo run --release --example perftest --features use-jemalloc --no-default-features
///   cargo run --release --example perftest -- --save results.json
///   cargo run --release --example perftest -- --baseline results.json
///   cargo run --release --example perftest -- --baseline old.json --save new.json
///   cargo run --release --example perftest -- --scale 2     # 2x data
///   cargo run --release --example perftest -- --iterations 7
#[cfg(all(feature = "use-mimalloc", feature = "use-jemalloc"))]
compile_error!(
    "Enable only one allocator: use default `use-mimalloc`, or `jemalloc` with \
     `--no-default-features --features use-jemalloc` on the `potatodb-examples` crate."
);

#[cfg(all(feature = "use-mimalloc", not(feature = "use-jemalloc")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(not(feature = "use-mimalloc"), feature = "use-jemalloc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "use-mimalloc", not(feature = "use-jemalloc")))]
const ALLOCATOR_NAME: &str = "mimalloc";
#[cfg(all(not(feature = "use-mimalloc"), feature = "use-jemalloc"))]
const ALLOCATOR_NAME: &str = "jemalloc";
#[cfg(not(any(feature = "use-mimalloc", feature = "use-jemalloc")))]
const ALLOCATOR_NAME: &str = "system";

use std::collections::BTreeMap;
use std::time::Instant;

use potatodb_engine::{PotatoDB, QueryResult};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ── CLI args (hand-rolled to avoid extra dep) ────────────────────

struct Args {
    baseline: Option<String>,
    save: Option<String>,
    scale: usize,
    iterations: usize,
}

fn parse_args() -> Args {
    let args: Vec<String> = std::env::args().collect();
    let mut baseline = None;
    let mut save = None;
    let mut scale = 1;
    let mut iterations = 5;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--baseline" | "-b" => {
                i += 1;
                baseline = args.get(i).cloned();
            }
            "--save" | "-s" => {
                i += 1;
                save = args.get(i).cloned();
            }
            "--scale" => {
                i += 1;
                scale = args.get(i).and_then(|v| v.parse().ok()).unwrap_or(1);
            }
            "--iterations" | "-n" => {
                i += 1;
                iterations = args.get(i).and_then(|v| v.parse().ok()).unwrap_or(5);
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: perftest [--baseline FILE] [--save FILE] [--scale N] [--iterations N]"
                );
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    Args {
        baseline,
        save,
        scale,
        iterations,
    }
}

// ── Benchmark result types ───────────────────────────────────────

#[derive(Clone)]
struct BenchResult {
    median_ms: f64,
    mean_ms: f64,
    min_ms: f64,
    max_ms: f64,
    p95_ms: f64,
    iterations: usize,
}

fn to_json_value(r: &BenchResult) -> serde_json::Value {
    serde_json::json!({
        "median_ms": (r.median_ms * 1000.0).round() / 1000.0,
        "mean_ms":   (r.mean_ms   * 1000.0).round() / 1000.0,
        "min_ms":    (r.min_ms    * 1000.0).round() / 1000.0,
        "max_ms":    (r.max_ms    * 1000.0).round() / 1000.0,
        "p95_ms":    (r.p95_ms    * 1000.0).round() / 1000.0,
        "iterations": r.iterations,
    })
}

fn stats(timings: &mut [f64]) -> BenchResult {
    timings.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = timings.len();
    let median_ms = if n % 2 == 1 {
        timings[n / 2]
    } else {
        (timings[n / 2 - 1] + timings[n / 2]) / 2.0
    };
    let mean_ms = timings.iter().sum::<f64>() / n as f64;
    let p95_idx = ((n as f64) * 0.95).ceil() as usize;
    let p95_ms = timings[p95_idx.min(n - 1)];
    BenchResult {
        median_ms,
        mean_ms,
        min_ms: timings[0],
        max_ms: timings[n - 1],
        p95_ms,
        iterations: n,
    }
}

// ── Helpers ──────────────────────────────────────────────────────

async fn exec(db: &mut PotatoDB, sql: &str) {
    db.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn exec_timed(db: &mut PotatoDB, sql: &str) -> f64 {
    let start = Instant::now();
    db.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    start.elapsed().as_secs_f64() * 1000.0
}

async fn row_count(db: &mut PotatoDB, sql: &str) -> usize {
    match db.execute(sql).await.unwrap() {
        QueryResult::Records(batches) => {
            use arrow::array::Int64Array;
            batches
                .first()
                .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
                .map_or(0, |a| a.value(0) as usize)
        }
        _ => 0,
    }
}

async fn run_bench(
    db: &mut PotatoDB,
    name: &str,
    sql: &str,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    // Warm-up run
    let _ = db.execute(sql).await;

    let mut timings = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        timings.push(exec_timed(db, sql).await);
    }
    let r = stats(&mut timings);
    eprint_result(name, &r);
    results.insert(name.to_string(), r);
}

fn eprint_result(name: &str, r: &BenchResult) {
    eprintln!(
        "  {:<40} median {:>10.3} ms   p95 {:>10.3} ms   (n={})",
        name, r.median_ms, r.p95_ms, r.iterations,
    );
}

// ── Schema setup ─────────────────────────────────────────────────

async fn seed_database(db: &mut PotatoDB, scale: usize) {
    let customers = 5_000 * scale;
    let products = 2_000 * scale;
    let orders = 10_000 * scale;

    exec(
        db,
        "CREATE TABLE IF NOT EXISTS customers (
        id INT PRIMARY KEY, name VARCHAR NOT NULL,
        email VARCHAR NOT NULL, city VARCHAR
    )",
    )
    .await;

    exec(
        db,
        "CREATE TABLE IF NOT EXISTS products (
        id INT PRIMARY KEY, name VARCHAR NOT NULL,
        price INT NOT NULL, category VARCHAR
    )",
    )
    .await;

    exec(
        db,
        "CREATE TABLE IF NOT EXISTS orders (
        id INT PRIMARY KEY, customer_id INT NOT NULL,
        product_id INT NOT NULL, quantity INT NOT NULL,
        order_date DATE NOT NULL
    )",
    )
    .await;

    exec(
        db,
        &format!(
            "INSERT INTO customers SELECT
            gs.value AS id,
            'Customer ' || gs.value AS name,
            'cust_' || gs.value || '@test.com' AS email,
            CASE gs.value % 8
                WHEN 0 THEN 'Seattle'   WHEN 1 THEN 'Portland'
                WHEN 2 THEN 'Denver'    WHEN 3 THEN 'Austin'
                WHEN 4 THEN 'Chicago'   WHEN 5 THEN 'New York'
                WHEN 6 THEN 'Boston'    ELSE 'Miami'
            END AS city
        FROM generate_series(1, {customers}) AS gs"
        ),
    )
    .await;

    exec(
        db,
        &format!(
            "INSERT INTO products SELECT
            gs.value AS id,
            'Product ' || gs.value AS name,
            1 + ((gs.value * 7919 + 104729) % 999) AS price,
            CASE gs.value % 5
                WHEN 0 THEN 'Electronics'  WHEN 1 THEN 'Office'
                WHEN 2 THEN 'Home'         WHEN 3 THEN 'Accessories'
                ELSE 'Other'
            END AS category
        FROM generate_series(1, {products}) AS gs"
        ),
    )
    .await;

    exec(
        db,
        &format!(
            "INSERT INTO orders SELECT
            gs.value AS id,
            ((gs.value * 104729 + 1) % {customers}) + 1 AS customer_id,
            ((gs.value * 7919 + 17) % {products}) + 1   AS product_id,
            (gs.value % 10) + 1 AS quantity,
            CURRENT_DATE - (gs.value % 365) AS order_date
        FROM generate_series(1, {orders}) AS gs"
        ),
    )
    .await;

    exec(db, "FLUSH").await;
}

// ── Benchmark categories ─────────────────────────────────────────

async fn bench_bulk_insert(
    db: &mut PotatoDB,
    iterations: usize,
    scale: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    let rows = 1_000 * scale;

    let mut timings = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let offset = 1_000_000 + i * rows;
        let sql = format!(
            "INSERT INTO customers SELECT
                gs.value + {offset} AS id,
                'BulkCust ' || gs.value AS name,
                'bulk' || gs.value || '@test.com' AS email,
                'BulkCity' AS city
            FROM generate_series(1, {rows}) AS gs"
        );
        timings.push(exec_timed(db, &sql).await);
    }
    let r = stats(&mut timings);
    let name = format!("bulk_insert_{rows}_rows");
    eprint_result(&name, &r);
    results.insert(name, r);

    exec(db, "DELETE FROM customers WHERE city = 'BulkCity'").await;
}

async fn bench_point_lookups(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    exec(
        db,
        "CREATE INDEX IF NOT EXISTS idx_cust_id ON customers (id)",
    )
    .await;

    run_bench(
        db,
        "point_lookup_indexed",
        "SELECT * FROM customers WHERE id = 2500",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "point_lookup_by_name",
        "SELECT * FROM customers WHERE name = 'Customer 1234'",
        iterations,
        results,
    )
    .await;
}

async fn bench_scans(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    run_bench(
        db,
        "full_scan_count",
        "SELECT COUNT(*) FROM orders",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "range_scan",
        "SELECT * FROM products WHERE price BETWEEN 100 AND 200",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "filter_like",
        "SELECT * FROM customers WHERE name LIKE 'Customer 1%'",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "filter_in_list",
        "SELECT * FROM customers WHERE city IN ('Seattle', 'Austin', 'Miami')",
        iterations,
        results,
    )
    .await;
}

async fn bench_aggregations(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    run_bench(
        db,
        "agg_sum_avg",
        "SELECT SUM(price), AVG(price) FROM products",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "agg_group_by",
        "SELECT category, COUNT(*), AVG(price), MIN(price), MAX(price)
         FROM products GROUP BY category",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "agg_group_by_having",
        "SELECT city, COUNT(*) AS cnt FROM customers
         GROUP BY city HAVING COUNT(*) > 100",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "agg_distinct_city",
        "SELECT COUNT(DISTINCT city) FROM customers",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "agg_approx_distinct",
        "SELECT APPROX_DISTINCT(city) FROM customers",
        iterations,
        results,
    )
    .await;
}

async fn bench_joins(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    run_bench(
        db,
        "join_inner_3way",
        "SELECT COUNT(*) FROM orders o
         JOIN customers c ON o.customer_id = c.id
         JOIN products p ON o.product_id = p.id",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "join_inner_agg",
        "SELECT c.city, SUM(o.quantity * p.price) AS revenue
         FROM orders o
         JOIN customers c ON o.customer_id = c.id
         JOIN products p ON o.product_id = p.id
         GROUP BY c.city ORDER BY revenue DESC LIMIT 10",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "join_left_count",
        "SELECT c.id, COUNT(o.id) AS order_count
         FROM customers c
         LEFT JOIN orders o ON c.id = o.customer_id
         GROUP BY c.id
         HAVING COUNT(o.id) > 0
         LIMIT 20",
        iterations,
        results,
    )
    .await;
}

async fn bench_subqueries_ctes(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    run_bench(
        db,
        "subquery_scalar",
        "SELECT id, name, price,
            (SELECT AVG(price) FROM products) AS global_avg
         FROM products WHERE id <= 100",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "subquery_in",
        "SELECT * FROM customers
         WHERE id IN (SELECT customer_id FROM orders WHERE quantity >= 8)
         LIMIT 50",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "subquery_exists",
        "SELECT * FROM customers c
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.quantity > 8)
         LIMIT 50",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "cte_chained",
        "WITH order_totals AS (
            SELECT o.customer_id, SUM(o.quantity * p.price) AS total
            FROM orders o JOIN products p ON o.product_id = p.id
            GROUP BY o.customer_id
         ),
         ranked AS (
            SELECT customer_id, total,
                   RANK() OVER (ORDER BY total DESC) AS rnk
            FROM order_totals
         )
         SELECT * FROM ranked WHERE rnk <= 10",
        iterations,
        results,
    )
    .await;
}

async fn bench_window_functions(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    run_bench(
        db,
        "window_row_number",
        "SELECT id, name, price, category,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) AS rn
         FROM products WHERE id <= 500",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "window_running_total",
        "SELECT id, price,
            SUM(price) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS running
         FROM products WHERE id <= 500",
        iterations,
        results,
    )
    .await;
}

async fn bench_sorting(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    run_bench(
        db,
        "sort_order_limit_10",
        "SELECT * FROM orders ORDER BY quantity DESC LIMIT 10",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "sort_order_limit_1000",
        "SELECT * FROM orders ORDER BY quantity DESC LIMIT 1000",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "sort_multi_column",
        "SELECT * FROM products ORDER BY category, price DESC LIMIT 100",
        iterations,
        results,
    )
    .await;
}

async fn bench_dml(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    exec(
        db,
        "CREATE TABLE IF NOT EXISTS dml_bench (id INT PRIMARY KEY, val VARCHAR)",
    )
    .await;
    exec(
        db,
        "INSERT INTO dml_bench SELECT gs.value, 'v' || gs.value
              FROM generate_series(1, 5000) AS gs",
    )
    .await;
    exec(db, "FLUSH").await;

    run_bench(
        db,
        "update_500_rows",
        "UPDATE dml_bench SET val = 'updated' WHERE id <= 500",
        iterations,
        results,
    )
    .await;

    run_bench(
        db,
        "delete_100_rows",
        "DELETE FROM dml_bench WHERE id BETWEEN 4900 AND 5000",
        iterations,
        results,
    )
    .await;

    exec(db, "DROP TABLE dml_bench").await;
}

async fn bench_ddl(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    {
        let mut timings = Vec::with_capacity(iterations);
        for i in 0..iterations {
            let name = format!("_perf_idx_{i}");
            let sql = format!("CREATE INDEX {name} ON orders (customer_id)");
            timings.push(exec_timed(db, &sql).await);
            exec(db, &format!("DROP INDEX {name}")).await;
        }
        let r = stats(&mut timings);
        eprint_result("create_index_on_orders", &r);
        results.insert("create_index_on_orders".into(), r);
    }

    run_bench(
        db,
        "analyze_customers",
        "ANALYZE customers",
        iterations,
        results,
    )
    .await;
}

async fn bench_transactions(
    db: &mut PotatoDB,
    iterations: usize,
    results: &mut BTreeMap<String, BenchResult>,
) {
    exec(
        db,
        "CREATE TABLE IF NOT EXISTS txn_bench (id INT PRIMARY KEY, val INT)",
    )
    .await;

    let mut timings = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let offset = 500_000 + i * 100;
        let start = Instant::now();
        exec(db, "BEGIN").await;
        exec(
            db,
            &format!(
                "INSERT INTO txn_bench SELECT gs.value + {offset}, gs.value
             FROM generate_series(1, 100) AS gs"
            ),
        )
        .await;
        exec(db, "COMMIT").await;
        timings.push(start.elapsed().as_secs_f64() * 1000.0);
    }
    let r = stats(&mut timings);
    eprint_result("txn_insert_100_commit", &r);
    results.insert("txn_insert_100_commit".into(), r);

    exec(db, "DROP TABLE txn_bench").await;
}

// ── Comparison / report ──────────────────────────────────────────

fn print_comparison(current: &BTreeMap<String, BenchResult>, baseline: &serde_json::Value) {
    let base_results = baseline.get("benchmarks").and_then(|v| v.as_object());
    let Some(base) = base_results else {
        eprintln!("Baseline file has no 'benchmarks' object, skipping comparison.");
        return;
    };

    eprintln!();
    eprintln!(
        "  {:<40} {:>12} {:>12} {:>10}",
        "Benchmark", "Baseline", "Current", "Change"
    );
    eprintln!("  {}", "-".repeat(78));

    for (name, cur) in current {
        let Some(base_entry) = base.get(name) else {
            eprintln!(
                "  {:<40} {:>12} {:>10.3} ms {:>10}",
                name, "N/A", cur.median_ms, "new"
            );
            continue;
        };
        let base_median = base_entry
            .get("median_ms")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        if base_median == 0.0 {
            continue;
        }

        let pct = ((cur.median_ms - base_median) / base_median) * 100.0;
        let arrow = if pct < -5.0 {
            " faster"
        } else if pct > 5.0 {
            " SLOWER"
        } else {
            ""
        };
        eprintln!(
            "  {:<40} {:>9.3} ms {:>9.3} ms {:>+8.1}%{}",
            name, base_median, cur.median_ms, pct, arrow
        );
    }

    // Show benchmarks removed in current run
    for key in base.keys() {
        if !current.contains_key(key) {
            let base_median = base[key]
                .get("median_ms")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.0);
            eprintln!(
                "  {:<40} {:>9.3} ms {:>12} {:>10}",
                key, base_median, "N/A", "removed"
            );
        }
    }
    eprintln!();
}

// ── Main ─────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    eprintln!("PotatoDB Performance Test");
    let args = parse_args();
    let tmp = tempfile::tempdir()?;
    let path = tmp.path().to_string_lossy().to_string();
    eprintln!("  path: {}", &path);
    let mut db = PotatoDB::new(path, None).await?;

    eprintln!("  allocator:  {}", ALLOCATOR_NAME);
    eprintln!("  scale:      {}x", args.scale);
    eprintln!("  iterations: {}", args.iterations);
    eprintln!();

    // ── Seed ─────────────────────────────────────────────────────
    eprintln!("Seeding database...");
    let seed_start = Instant::now();
    seed_database(&mut db, args.scale).await;
    let seed_ms = seed_start.elapsed().as_secs_f64() * 1000.0;

    let n_customers = row_count(&mut db, "SELECT COUNT(*) FROM customers").await;
    let n_products = row_count(&mut db, "SELECT COUNT(*) FROM products").await;
    let n_orders = row_count(&mut db, "SELECT COUNT(*) FROM orders").await;
    eprintln!(
        "  Seeded in {seed_ms:.0} ms  (customers: {n_customers}, products: {n_products}, orders: {n_orders})"
    );
    eprintln!();

    let iters = args.iterations;
    let mut results = BTreeMap::new();

    results.insert(
        "seed_database".into(),
        BenchResult {
            median_ms: seed_ms,
            mean_ms: seed_ms,
            min_ms: seed_ms,
            max_ms: seed_ms,
            p95_ms: seed_ms,
            iterations: 1,
        },
    );

    // ── Run benchmarks ───────────────────────────────────────────
    eprintln!("Running benchmarks...");
    eprintln!();

    eprintln!("[Bulk Insert]");
    bench_bulk_insert(&mut db, iters, args.scale, &mut results).await;

    eprintln!("[Point Lookups]");
    bench_point_lookups(&mut db, iters, &mut results).await;

    eprintln!("[Scans & Filters]");
    bench_scans(&mut db, iters, &mut results).await;

    eprintln!("[Aggregations]");
    bench_aggregations(&mut db, iters, &mut results).await;

    eprintln!("[Joins]");
    bench_joins(&mut db, iters, &mut results).await;

    eprintln!("[Subqueries & CTEs]");
    bench_subqueries_ctes(&mut db, iters, &mut results).await;

    eprintln!("[Window Functions]");
    bench_window_functions(&mut db, iters, &mut results).await;

    eprintln!("[Sorting & Pagination]");
    bench_sorting(&mut db, iters, &mut results).await;

    eprintln!("[DML]");
    bench_dml(&mut db, iters, &mut results).await;

    eprintln!("[DDL & Maintenance]");
    bench_ddl(&mut db, iters, &mut results).await;

    eprintln!("[Transactions]");
    bench_transactions(&mut db, iters, &mut results).await;

    // ── Build JSON report ────────────────────────────────────────
    let benchmarks: serde_json::Map<String, serde_json::Value> = results
        .iter()
        .map(|(k, v)| (k.clone(), to_json_value(v)))
        .collect();

    let report = serde_json::json!({
        "tool": "potatodb-perftest",
        "allocator": ALLOCATOR_NAME,
        "timestamp_unix": unix_timestamp_secs(),
        "scale": args.scale,
        "iterations": args.iterations,
        "row_counts": {
            "customers": n_customers,
            "products": n_products,
            "orders": n_orders,
        },
        "benchmarks": benchmarks,
    });

    // Print JSON report to stdout
    println!("{}", serde_json::to_string_pretty(&report)?);

    // ── Save if requested ────────────────────────────────────────
    if let Some(ref path) = args.save {
        std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
        eprintln!("Results saved to {path}");
    }

    // ── Compare with baseline ────────────────────────────────────
    if let Some(ref path) = args.baseline {
        let data = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("Cannot read baseline {path}: {e}"));
        let baseline: serde_json::Value = serde_json::from_str(&data)
            .unwrap_or_else(|e| panic!("Cannot parse baseline {path}: {e}"));
        print_comparison(&results, &baseline);
    }

    Ok(())
}

fn unix_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
