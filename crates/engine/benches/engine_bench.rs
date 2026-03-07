use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use potatodb_engine::{PotatoDB, QueryResult};
use tokio::runtime::Runtime;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn new_db(rt: &Runtime) -> (tempfile::TempDir, PotatoDB) {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let db = rt
        .block_on(PotatoDB::new(
            tmp.path().to_string_lossy().into_owned(),
            None,
        ))
        .expect("open db");
    (tmp, db)
}

fn seed_events(rt: &Runtime, db: &mut PotatoDB, rows: usize) {
    rt.block_on(async {
        db.execute("CREATE TABLE IF NOT EXISTS events (id INT, grp INT, ts INT, payload VARCHAR);")
            .await
            .unwrap();
        let mut vals = Vec::with_capacity(rows);
        for i in 0..rows {
            vals.push(format!("({i}, {}, {}, 'p{i}')", i % 16, i % 1000));
        }
        db.execute(&format!("INSERT INTO events VALUES {};", vals.join(",")))
            .await
            .unwrap();
    });
}

fn exec(rt: &Runtime, db: &mut PotatoDB, sql: &str) {
    rt.block_on(db.execute(sql)).expect(sql);
}

fn exec_rw(rt: &Runtime, db: &mut PotatoDB, sql: &str) -> QueryResult {
    rt.block_on(db.execute(black_box(sql))).expect(sql)
}

fn exec_ro(rt: &Runtime, db: &mut PotatoDB, sql: &str) -> QueryResult {
    rt.block_on(db.execute_readonly(black_box(sql))).expect(sql)
}

fn assert_records(r: QueryResult) {
    match r {
        QueryResult::Records(b) => {
            black_box(b.len());
        }
        QueryResult::Message(m) => panic!("expected records, got: {m}"),
    }
}

// ---------------------------------------------------------------------------
// 1. Point lookups & scans
// ---------------------------------------------------------------------------

fn bench_point_lookups(c: &mut Criterion) {
    let mut g = c.benchmark_group("point_lookup");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 10_000);
    exec(&rt, &mut db, "CREATE INDEX idx_events_id ON events (id);");

    g.bench_function("indexed_eq", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events WHERE id = 4242;",
            ))
        });
    });

    g.bench_function("non_indexed_eq", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events WHERE payload = 'p999';",
            ))
        });
    });

    g.bench_function("range_scan", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events WHERE id BETWEEN 1000 AND 1100;",
            ))
        });
    });

    g.bench_function("multi_predicate", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events WHERE grp = 3 AND ts > 500 AND ts < 600;",
            ))
        });
    });

    g.bench_function("is_null_check", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT COUNT(*) FROM events WHERE payload IS NOT NULL;",
            ))
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 2. Aggregations
// ---------------------------------------------------------------------------

fn bench_aggregations(c: &mut Criterion) {
    let mut g = c.benchmark_group("aggregation");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 10_000);

    g.bench_function("count_star", |b| {
        b.iter(|| assert_records(exec_ro(&rt, &mut db, "SELECT COUNT(*) FROM events;")));
    });

    g.bench_function("count_where", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT COUNT(*) FROM events WHERE grp = 7;",
            ))
        });
    });

    g.bench_function("sum_min_max_avg", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT SUM(ts), MIN(ts), MAX(ts), AVG(ts) FROM events;",
            ))
        });
    });

    g.bench_function("group_by_16", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT grp, COUNT(*), SUM(ts) FROM events GROUP BY grp ORDER BY grp;",
            ))
        });
    });

    g.bench_function("group_by_having", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT grp, COUNT(*) AS c FROM events GROUP BY grp HAVING COUNT(*) > 600;",
            ))
        });
    });

    g.bench_function("distinct", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT DISTINCT grp FROM events ORDER BY grp;",
            ))
        });
    });

    g.bench_function("count_distinct", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT COUNT(DISTINCT ts) FROM events;",
            ))
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 3. Joins
// ---------------------------------------------------------------------------

fn bench_joins(c: &mut Criterion) {
    let mut g = c.benchmark_group("join");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);

    exec(
        &rt,
        &mut db,
        "CREATE TABLE orders (oid INT, uid INT, total DOUBLE);",
    );
    exec(
        &rt,
        &mut db,
        "CREATE TABLE users (uid INT PRIMARY KEY, name VARCHAR);",
    );
    let mut u = Vec::new();
    for i in 0..200 {
        u.push(format!("({i}, 'user_{i}')"));
    }
    exec(
        &rt,
        &mut db,
        &format!("INSERT INTO users VALUES {};", u.join(",")),
    );
    let mut o = Vec::new();
    for i in 0..5_000 {
        o.push(format!("({i}, {}, {:.2})", i % 200, (i as f64) * 1.5));
    }
    exec(
        &rt,
        &mut db,
        &format!("INSERT INTO orders VALUES {};", o.join(",")),
    );

    g.bench_function("inner_join_agg", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT u.name, SUM(o.total) FROM orders o INNER JOIN users u ON o.uid = u.uid GROUP BY u.name ORDER BY u.name LIMIT 10;",
            ))
        });
    });

    g.bench_function("left_join_count", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT u.name, COUNT(o.oid) FROM users u LEFT JOIN orders o ON u.uid = o.uid GROUP BY u.name ORDER BY u.name LIMIT 10;",
            ))
        });
    });

    g.bench_function("self_join", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT a.oid, b.oid FROM orders a JOIN orders b ON a.uid = b.uid WHERE a.oid < 50 AND b.oid < 50 AND a.oid < b.oid;",
            ))
        });
    });

    g.bench_function("cross_join_small", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT a.uid, b.uid FROM users a CROSS JOIN users b WHERE a.uid < 10 AND b.uid < 10;",
            ))
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 4. Subqueries & CTEs
// ---------------------------------------------------------------------------

fn bench_subqueries(c: &mut Criterion) {
    let mut g = c.benchmark_group("subquery_cte");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 10_000);

    g.bench_function("scalar_subquery", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id, ts - (SELECT AVG(ts) FROM events) AS diff FROM events WHERE id < 100;",
            ))
        });
    });

    g.bench_function("in_subquery", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events WHERE grp IN (SELECT DISTINCT grp FROM events WHERE ts < 200) ORDER BY id LIMIT 50;",
            ))
        });
    });

    g.bench_function("exists_subquery", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id FROM events e WHERE EXISTS (SELECT 1 FROM events e2 WHERE e2.grp = e.grp AND e2.ts > 900) AND e.id < 200;",
            ))
        });
    });

    g.bench_function("cte_simple", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "WITH top AS (SELECT grp, COUNT(*) AS c FROM events GROUP BY grp ORDER BY c DESC LIMIT 3) SELECT e.id FROM events e JOIN top t ON e.grp = t.grp ORDER BY e.id LIMIT 50;",
            ))
        });
    });

    g.bench_function("cte_chained", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "WITH a AS (SELECT grp, COUNT(*) AS c FROM events GROUP BY grp), b AS (SELECT grp FROM a WHERE c > 500) SELECT COUNT(*) FROM events WHERE grp IN (SELECT grp FROM b);",
            ))
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 5. Window functions
// ---------------------------------------------------------------------------

fn bench_window(c: &mut Criterion) {
    let mut g = c.benchmark_group("window");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 5_000);

    g.bench_function("row_number", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ts) AS rn FROM events ORDER BY id LIMIT 100;",
            ))
        });
    });

    g.bench_function("rank_and_lag", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id, RANK() OVER (ORDER BY ts) AS r, LAG(ts) OVER (ORDER BY ts) AS prev_ts FROM events ORDER BY id LIMIT 100;",
            ))
        });
    });

    g.bench_function("ntile", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id, NTILE(4) OVER (ORDER BY id) AS bucket FROM events ORDER BY id LIMIT 100;",
            ))
        });
    });

    g.bench_function("sum_running_total", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id, SUM(ts) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM events ORDER BY id LIMIT 100;",
            ))
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 6. Sorting & pagination
// ---------------------------------------------------------------------------

fn bench_sort_limit(c: &mut Criterion) {
    let mut g = c.benchmark_group("sort_limit");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 10_000);

    g.bench_function("order_by_limit_10", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events ORDER BY ts DESC LIMIT 10;",
            ))
        });
    });

    g.bench_function("order_by_limit_1000", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events ORDER BY ts DESC LIMIT 1000;",
            ))
        });
    });

    g.bench_function("offset_limit", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events ORDER BY id LIMIT 50 OFFSET 5000;",
            ))
        });
    });

    g.bench_function("multi_column_sort", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM events ORDER BY grp ASC, ts DESC LIMIT 100;",
            ))
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 7. DML: insert, update, delete
// ---------------------------------------------------------------------------

fn bench_dml(c: &mut Criterion) {
    let mut g = c.benchmark_group("dml");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("insert_100_rows", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE ins100 (id INT, v INT);");
                (tmp, db)
            },
            |(_tmp, mut db)| {
                let mut vals = Vec::with_capacity(100);
                for i in 0..100 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO ins100 VALUES {};", vals.join(",")),
                );
            },
        );
    });

    g.bench_function("insert_1000_rows", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE ins (id INT, v INT);");
                (tmp, db)
            },
            |(_tmp, mut db)| {
                let mut vals = Vec::with_capacity(1000);
                for i in 0..1000 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO ins VALUES {};", vals.join(",")),
                );
            },
        );
    });

    g.bench_function("insert_5000_rows", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE ins5k (id INT, v INT);");
                (tmp, db)
            },
            |(_tmp, mut db)| {
                let mut vals = Vec::with_capacity(5000);
                for i in 0..5000 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO ins5k VALUES {};", vals.join(",")),
                );
            },
        );
    });

    g.bench_function("update_500_rows", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE upd (id INT, v INT);");
                let mut vals = Vec::with_capacity(1000);
                for i in 0..1000 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO upd VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "UPDATE upd SET v = v + 1 WHERE id < 500;");
            },
        );
    });

    g.bench_function("delete_500_rows", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE del (id INT, v INT);");
                let mut vals = Vec::with_capacity(1000);
                for i in 0..1000 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO del VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "DELETE FROM del WHERE id >= 500;");
            },
        );
    });

    g.bench_function("delete_returning", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE delr (id INT, v INT);");
                let mut vals = Vec::with_capacity(200);
                for i in 0..200 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO delr VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                assert_records(exec_rw(
                    &rt,
                    &mut db,
                    "DELETE FROM delr WHERE id < 100 RETURNING *;",
                ));
            },
        );
    });

    g.bench_function("update_returning", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE updr (id INT, v INT);");
                let mut vals = Vec::with_capacity(200);
                for i in 0..200 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO updr VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                assert_records(exec_rw(
                    &rt,
                    &mut db,
                    "UPDATE updr SET v = v + 1 WHERE id < 100 RETURNING *;",
                ));
            },
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 8. DDL: create table, create index, vacuum, alter, truncate
// ---------------------------------------------------------------------------

fn bench_ddl(c: &mut Criterion) {
    let mut g = c.benchmark_group("ddl");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("create_table", |b| {
        b.iter_with_setup(
            || new_db(&rt),
            |(_tmp, mut db)| {
                exec(
                    &rt,
                    &mut db,
                    "CREATE TABLE t (a INT, b VARCHAR, c DOUBLE, d BOOLEAN, e TIMESTAMP);",
                );
            },
        );
    });

    g.bench_function("create_table_with_constraints", |b| {
        b.iter_with_setup(
            || new_db(&rt),
            |(_tmp, mut db)| {
                exec(
                    &rt,
                    &mut db,
                    "CREATE TABLE tc (id INT PRIMARY KEY, email VARCHAR UNIQUE, name VARCHAR NOT NULL, age INT, CHECK (age >= 0));",
                );
            },
        );
    });

    g.bench_function("create_index_5k_rows", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE cidx (id INT, v INT);");
                let mut vals = Vec::with_capacity(5000);
                for i in 0..5000 {
                    vals.push(format!("({i}, {})", i % 100));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO cidx VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "CREATE INDEX idx_cidx_v ON cidx (v);");
            },
        );
    });

    g.bench_function("vacuum_5k_rows", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE vac (id INT, v INT);");
                for batch in 0..5 {
                    let mut vals = Vec::with_capacity(1000);
                    for i in 0..1000 {
                        let id = batch * 1000 + i;
                        vals.push(format!("({id}, {i})"));
                    }
                    exec(
                        &rt,
                        &mut db,
                        &format!("INSERT INTO vac VALUES {};", vals.join(",")),
                    );
                }
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "VACUUM vac;");
            },
        );
    });

    g.bench_function("truncate_table", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE trunc (id INT, v INT);");
                let mut vals = Vec::with_capacity(2000);
                for i in 0..2000 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO trunc VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "TRUNCATE TABLE trunc;");
            },
        );
    });

    g.bench_function("alter_table_add_column", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE alt (id INT);");
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "ALTER TABLE alt ADD COLUMN v INT;");
            },
        );
    });

    g.bench_function("ctas_2k_rows", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE src (id INT, v INT);");
                let mut vals = Vec::with_capacity(2000);
                for i in 0..2000 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO src VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(
                    &rt,
                    &mut db,
                    "CREATE TABLE dst AS SELECT id, v * 2 AS doubled FROM src WHERE id < 1000;",
                );
            },
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 9. Transactions
// ---------------------------------------------------------------------------

fn bench_transactions(c: &mut Criterion) {
    let mut g = c.benchmark_group("transaction");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("begin_insert_commit", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE txn (id INT, v INT);");
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "BEGIN;");
                exec(&rt, &mut db, "INSERT INTO txn VALUES (1, 10);");
                exec(&rt, &mut db, "INSERT INTO txn VALUES (2, 20);");
                exec(&rt, &mut db, "COMMIT;");
            },
        );
    });

    g.bench_function("begin_insert_rollback", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE txnr (id INT, v INT);");
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "BEGIN;");
                exec(&rt, &mut db, "INSERT INTO txnr VALUES (1, 10);");
                exec(&rt, &mut db, "INSERT INTO txnr VALUES (2, 20);");
                exec(&rt, &mut db, "ROLLBACK;");
            },
        );
    });

    g.bench_function("txn_update_delete_commit", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE txnud (id INT, v INT);");
                let mut vals = Vec::with_capacity(500);
                for i in 0..500 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO txnud VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "BEGIN;");
                exec(&rt, &mut db, "UPDATE txnud SET v = v + 1 WHERE id < 100;");
                exec(&rt, &mut db, "DELETE FROM txnud WHERE id >= 400;");
                exec(&rt, &mut db, "COMMIT;");
            },
        );
    });

    g.bench_function("txn_create_index_commit", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE txni (id INT, v INT);");
                let mut vals = Vec::with_capacity(500);
                for i in 0..500 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO txni VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "BEGIN;");
                exec(&rt, &mut db, "CREATE INDEX idx_txni ON txni (v);");
                exec(&rt, &mut db, "COMMIT;");
            },
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 10. Scalability: vary row count
// ---------------------------------------------------------------------------

fn bench_scalability(c: &mut Criterion) {
    let mut g = c.benchmark_group("scalability");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    for &rows in &[1_000usize, 5_000, 20_000, 50_000] {
        g.bench_with_input(BenchmarkId::new("count_star", rows), &rows, |b, &rows| {
            let (_tmp, mut db) = new_db(&rt);
            seed_events(&rt, &mut db, rows);
            b.iter(|| {
                assert_records(exec_ro(&rt, &mut db, "SELECT COUNT(*) FROM events;"));
            });
        });

        g.bench_with_input(BenchmarkId::new("group_by_16", rows), &rows, |b, &rows| {
            let (_tmp, mut db) = new_db(&rt);
            seed_events(&rt, &mut db, rows);
            b.iter(|| {
                assert_records(exec_ro(
                    &rt,
                    &mut db,
                    "SELECT grp, COUNT(*) FROM events GROUP BY grp;",
                ));
            });
        });

        g.bench_with_input(BenchmarkId::new("full_scan", rows), &rows, |b, &rows| {
            let (_tmp, mut db) = new_db(&rt);
            seed_events(&rt, &mut db, rows);
            b.iter(|| {
                assert_records(exec_ro(&rt, &mut db, "SELECT * FROM events;"));
            });
        });
    }

    g.finish();
}

// ---------------------------------------------------------------------------
// 11. UDFs & procedures (use mutable execute for UDF expansion)
// ---------------------------------------------------------------------------

fn bench_udf_procedure(c: &mut Criterion) {
    let mut g = c.benchmark_group("udf_procedure");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 5_000);
    exec(
        &rt,
        &mut db,
        "CREATE FUNCTION double_id(x INT) RETURNS INT AS '$1 * 2';",
    );
    exec(&rt, &mut db, "CREATE TABLE proc_sink (id INT);");
    exec(
        &rt,
        &mut db,
        "CREATE PROCEDURE seed_proc() AS $$ INSERT INTO proc_sink VALUES (1); INSERT INTO proc_sink VALUES (2); $$;",
    );

    g.bench_function("udf_select", |b| {
        b.iter(|| {
            assert_records(exec_rw(
                &rt,
                &mut db,
                "SELECT double_id(id) FROM events WHERE id < 100;",
            ));
        });
    });

    g.bench_function("call_procedure", |b| {
        b.iter(|| {
            exec(&rt, &mut db, "CALL seed_proc();");
        });
    });

    g.bench_function("do_block", |b| {
        b.iter(|| {
            exec(&rt, &mut db, "DO $$ INSERT INTO proc_sink VALUES (99); $$;");
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 12. Constraints
// ---------------------------------------------------------------------------

fn bench_constraints(c: &mut Criterion) {
    let mut g = c.benchmark_group("constraints");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("insert_with_pk_unique_check", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(
                    &rt,
                    &mut db,
                    "CREATE TABLE cst (id INT PRIMARY KEY, email VARCHAR UNIQUE, name VARCHAR NOT NULL, age INT, CHECK (age >= 0));",
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                let mut vals = Vec::with_capacity(500);
                for i in 0..500 {
                    vals.push(format!("({i}, 'e{i}@x.com', 'n{i}', {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO cst VALUES {};", vals.join(",")),
                );
            },
        );
    });

    g.bench_function("upsert_on_conflict_do_update", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(
                    &rt,
                    &mut db,
                    "CREATE TABLE ups (id INT PRIMARY KEY, v INT);",
                );
                let mut vals = Vec::with_capacity(200);
                for i in 0..200 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO ups VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                let mut vals = Vec::with_capacity(200);
                for i in 0..200 {
                    vals.push(format!("({i}, {})", i + 1));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!(
                        "INSERT INTO ups VALUES {} ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v;",
                        vals.join(",")
                    ),
                );
            },
        );
    });

    g.bench_function("upsert_on_conflict_do_nothing", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(
                    &rt,
                    &mut db,
                    "CREATE TABLE upsn (id INT PRIMARY KEY, v INT);",
                );
                let mut vals = Vec::with_capacity(200);
                for i in 0..200 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO upsn VALUES {};", vals.join(",")),
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                let mut vals = Vec::with_capacity(200);
                for i in 0..200 {
                    vals.push(format!("({i}, {})", i + 1));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!(
                        "INSERT INTO upsn VALUES {} ON CONFLICT (id) DO NOTHING;",
                        vals.join(",")
                    ),
                );
            },
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 13. COPY import/export
// ---------------------------------------------------------------------------

fn bench_copy(c: &mut Criterion) {
    let mut g = c.benchmark_group("copy");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("copy_to_csv_5k", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE exp (id INT, v INT);");
                let mut vals = Vec::with_capacity(5000);
                for i in 0..5000 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO exp VALUES {};", vals.join(",")),
                );
                let csv_path = tmp.path().join("out.csv");
                (tmp, db, csv_path)
            },
            |(tmp, mut db, csv_path)| {
                exec(
                    &rt,
                    &mut db,
                    &format!("COPY exp TO '{}';", csv_path.to_string_lossy()),
                );
                drop(tmp);
            },
        );
    });

    g.bench_function("copy_from_csv_5k", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE imp (id INT, v INT);");
                let csv_path = tmp.path().join("in.csv");
                let mut csv = String::from("id,v\n");
                for i in 0..5000 {
                    csv.push_str(&format!("{i},{i}\n"));
                }
                std::fs::write(&csv_path, csv).unwrap();
                (tmp, db, csv_path)
            },
            |(tmp, mut db, csv_path)| {
                exec(
                    &rt,
                    &mut db,
                    &format!("COPY imp FROM '{}';", csv_path.to_string_lossy()),
                );
                drop(tmp);
            },
        );
    });

    g.bench_function("copy_to_parquet_5k", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE TABLE expp (id INT, v INT);");
                let mut vals = Vec::with_capacity(5000);
                for i in 0..5000 {
                    vals.push(format!("({i}, {i})"));
                }
                exec(
                    &rt,
                    &mut db,
                    &format!("INSERT INTO expp VALUES {};", vals.join(",")),
                );
                let pq_path = tmp.path().join("out.parquet");
                (tmp, db, pq_path)
            },
            |(tmp, mut db, pq_path)| {
                exec(
                    &rt,
                    &mut db,
                    &format!("COPY expp TO '{}';", pq_path.to_string_lossy()),
                );
                drop(tmp);
            },
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 14. Materialized views
// ---------------------------------------------------------------------------

fn bench_matview(c: &mut Criterion) {
    let mut g = c.benchmark_group("matview");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("create_matview_5k", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                seed_events(&rt, &mut db, 5_000);
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(
                    &rt,
                    &mut db,
                    "CREATE MATERIALIZED VIEW mv AS SELECT grp, COUNT(*) AS c FROM events GROUP BY grp;",
                );
            },
        );
    });

    g.bench_function("refresh_matview_5k", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                seed_events(&rt, &mut db, 5_000);
                exec(
                    &rt,
                    &mut db,
                    "CREATE MATERIALIZED VIEW mv_r AS SELECT grp, COUNT(*) AS c FROM events GROUP BY grp;",
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                exec(&rt, &mut db, "REFRESH MATERIALIZED VIEW mv_r;");
            },
        );
    });

    g.bench_function("read_matview", |b| {
        let (_tmp, mut db) = new_db(&rt);
        seed_events(&rt, &mut db, 5_000);
        exec(
            &rt,
            &mut db,
            "CREATE MATERIALIZED VIEW mv_rd AS SELECT grp, COUNT(*) AS c FROM events GROUP BY grp;",
        );
        b.iter(|| {
            assert_records(exec_ro(&rt, &mut db, "SELECT * FROM mv_rd ORDER BY grp;"));
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 15. Explain
// ---------------------------------------------------------------------------

fn bench_explain(c: &mut Criterion) {
    let mut g = c.benchmark_group("explain");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 5_000);

    g.bench_function("explain_select", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "EXPLAIN SELECT grp, COUNT(*) FROM events GROUP BY grp;",
            ));
        });
    });

    g.bench_function("explain_format_json", |b| {
        b.iter(|| {
            assert_records(exec_rw(
                &rt,
                &mut db,
                "EXPLAIN (FORMAT JSON) SELECT * FROM events WHERE id < 100;",
            ));
        });
    });

    exec(
        &rt,
        &mut db,
        "CREATE TABLE expl_u (uid INT PRIMARY KEY, name VARCHAR);",
    );
    exec(&rt, &mut db, "INSERT INTO expl_u VALUES (1, 'a');");

    g.bench_function("explain_join", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "EXPLAIN SELECT * FROM events e JOIN expl_u u ON e.grp = u.uid;",
            ));
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 16. Sequences
// ---------------------------------------------------------------------------

fn bench_sequences(c: &mut Criterion) {
    let mut g = c.benchmark_group("sequence");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("nextval_100", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                exec(&rt, &mut db, "CREATE SEQUENCE bench_seq;");
                exec(&rt, &mut db, "CREATE TABLE seq_t (id INT, v INT);");
                (tmp, db)
            },
            |(_tmp, mut db)| {
                for _ in 0..100 {
                    exec(
                        &rt,
                        &mut db,
                        "INSERT INTO seq_t VALUES (nextval('bench_seq'), 1);",
                    );
                }
            },
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 17. Views
// ---------------------------------------------------------------------------

fn bench_views(c: &mut Criterion) {
    let mut g = c.benchmark_group("view");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 5_000);
    exec(
        &rt,
        &mut db,
        "CREATE VIEW grp_summary AS SELECT grp, COUNT(*) AS c, SUM(ts) AS s FROM events GROUP BY grp;",
    );

    g.bench_function("select_from_view", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT * FROM grp_summary ORDER BY grp;",
            ));
        });
    });

    g.bench_function("join_through_view", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT e.id, g.c FROM events e JOIN grp_summary g ON e.grp = g.grp WHERE e.id < 100 ORDER BY e.id;",
            ));
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 18. Expressions & functions
// ---------------------------------------------------------------------------

fn bench_expressions(c: &mut Criterion) {
    let mut g = c.benchmark_group("expression");
    let rt = Runtime::new().unwrap();
    let (_tmp, mut db) = new_db(&rt);
    seed_events(&rt, &mut db, 5_000);

    g.bench_function("case_when", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id, CASE WHEN grp < 4 THEN 'low' WHEN grp < 12 THEN 'mid' ELSE 'high' END AS band FROM events ORDER BY id LIMIT 100;",
            ));
        });
    });

    g.bench_function("coalesce_concat", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id, COALESCE(payload, 'none') || '_suffix' AS p FROM events ORDER BY id LIMIT 100;",
            ));
        });
    });

    g.bench_function("arithmetic", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT id, (ts * 2 + grp - 1) / 3 AS calc FROM events ORDER BY id LIMIT 100;",
            ));
        });
    });

    g.bench_function("like_filter", |b| {
        b.iter(|| {
            assert_records(exec_ro(
                &rt,
                &mut db,
                "SELECT COUNT(*) FROM events WHERE payload LIKE 'p1%';",
            ));
        });
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 19. Prepared statements
// ---------------------------------------------------------------------------

fn bench_prepared(c: &mut Criterion) {
    let mut g = c.benchmark_group("prepared");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("prepare_and_execute", |b| {
        b.iter_with_setup(
            || {
                let (tmp, mut db) = new_db(&rt);
                seed_events(&rt, &mut db, 5_000);
                exec(
                    &rt,
                    &mut db,
                    "PREPARE find_grp AS SELECT * FROM events WHERE grp = $1;",
                );
                (tmp, db)
            },
            |(_tmp, mut db)| {
                assert_records(exec_rw(&rt, &mut db, "EXECUTE find_grp(7);"));
            },
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// 20. DB open/close lifecycle
// ---------------------------------------------------------------------------

fn bench_lifecycle(c: &mut Criterion) {
    let mut g = c.benchmark_group("lifecycle");
    g.sample_size(10);
    let rt = Runtime::new().unwrap();

    g.bench_function("open_empty_db", |b| {
        b.iter_with_setup(
            || tempfile::tempdir().expect("create tempdir"),
            |tmp| {
                let _db = rt
                    .block_on(PotatoDB::new(
                        tmp.path().to_string_lossy().into_owned(),
                        None,
                    ))
                    .expect("open db");
            },
        );
    });

    g.bench_function("open_db_with_tables", |b| {
        b.iter_with_setup(
            || {
                let tmp = tempfile::tempdir().expect("create tempdir");
                let mut db = rt
                    .block_on(PotatoDB::new(
                        tmp.path().to_string_lossy().into_owned(),
                        None,
                    ))
                    .unwrap();
                for i in 0..5 {
                    exec(&rt, &mut db, &format!("CREATE TABLE t{i} (id INT, v INT);"));
                    let mut vals = Vec::with_capacity(500);
                    for j in 0..500 {
                        vals.push(format!("({j}, {j})"));
                    }
                    exec(
                        &rt,
                        &mut db,
                        &format!("INSERT INTO t{i} VALUES {};", vals.join(",")),
                    );
                }
                drop(db);
                tmp
            },
            |tmp| {
                let _db = rt
                    .block_on(PotatoDB::new(
                        tmp.path().to_string_lossy().into_owned(),
                        None,
                    ))
                    .expect("reopen db");
            },
        );
    });

    g.finish();
}

// ---------------------------------------------------------------------------
// Register all groups
// ---------------------------------------------------------------------------

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(20);
    targets =
        bench_point_lookups,
        bench_aggregations,
        bench_joins,
        bench_subqueries,
        bench_window,
        bench_sort_limit,
        bench_dml,
        bench_ddl,
        bench_transactions,
        bench_scalability,
        bench_udf_procedure,
        bench_constraints,
        bench_copy,
        bench_matview,
        bench_explain,
        bench_sequences,
        bench_views,
        bench_expressions,
        bench_prepared,
        bench_lifecycle
);
criterion_main!(benches);
