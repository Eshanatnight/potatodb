use std::sync::Arc;
use std::time::Instant;

use clap::Parser;
use mimalloc::MiMalloc;
use potatodb_engine::{QueryResult, S3Config};
use tokio::sync::RwLock;

use potatodb_tui::ThemeChoice;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser)]
#[command(name = "potatodb", about = "A Parquet-backed SQL database")]
struct Cli {
    /// Data location: local path, S3 URL (<s3://bucket/prefix>), or in-memory (`:memory:` / `memory://...`)
    #[arg(long, default_value = "./potatodb_data")]
    data_dir: String,

    /// S3-compatible endpoint URL (e.g. <http://localhost:9000> for `MinIO`)
    #[arg(long)]
    s3_endpoint: Option<String>,

    /// AWS / S3 region
    #[arg(long, default_value = "us-east-1")]
    s3_region: String,

    /// Allow plain HTTP (non-TLS) connections to S3
    #[arg(long)]
    s3_allow_http: bool,

    /// Local directory for write-ahead logs (optional)
    #[arg(long)]
    wal_dir: Option<String>,

    /// Use the line-mode REPL instead of the TUI
    #[arg(long)]
    repl: bool,

    /// TUI colour theme: 'catppuccin-mocha' or 'potato (default)'
    #[arg(long, default_value = "potato", env = "POTATODB_THEME")]
    theme: ThemeChoice,

    /// Execute SQL file(s) and exit. Can be specified multiple times.
    #[arg(short = 'f', long = "file")]
    files: Vec<String>,

    /// Print per-statement execution time when running SQL files.
    #[arg(long)]
    timing: bool,

    /// Start HTTP API server on this address (e.g. 127.0.0.1:8080).
    #[arg(long)]
    http_addr: Option<String>,
}

#[tokio::main(worker_threads = 8)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // --- Transparent Huge Pages ---
    // Switch THP from "madvise" to "always" so all heap allocations (Arrow
    // buffers, Parquet I/O buffers, hash tables) automatically use 2 MiB
    // pages, reducing L2 DTLB misses measured by uProf across ZSTD, memcpy,
    // and memset hot paths.
    let _ = std::fs::write("/sys/kernel/mm/transparent_hugepage/enabled", "always");
    let _ = std::fs::write("/sys/kernel/mm/transparent_hugepage/defrag", "defer+madvise");

    // --- mimalloc tuning ---
    // Amortize cross-thread free-list collection. The default (10 ms) causes
    // _mi_page_free_collect to pointer-chase remote-thread blocks very
    // frequently, each traversal hitting DRAM (Backend_Bound.Memory = 90 %).
    // A longer delay batches frees so the linked-list walk encounters more
    // cache-warm blocks, cutting CPI from ~14 toward ~2–3.
    if std::env::var_os("MIMALLOC_PURGE_DELAY").is_none() {
        unsafe { std::env::set_var("MIMALLOC_PURGE_DELAY", "100") };
    }

    // Use OS-level large/huge pages when available to reduce DTLB misses on
    // large hash tables and Arrow buffers.
    if std::env::var_os("MIMALLOC_LARGE_OS_PAGES").is_none() {
        unsafe { std::env::set_var("MIMALLOC_LARGE_OS_PAGES", "1") };
    }
    // Allow mimalloc to use 2 MiB huge pages via transparent huge pages.
    if std::env::var_os("MIMALLOC_ALLOW_LARGE_OS_PAGES").is_none() {
        unsafe { std::env::set_var("MIMALLOC_ALLOW_LARGE_OS_PAGES", "1") };
    }
    // Disable eager commit to reduce the number of arenas that accumulate
    // cross-thread free blocks, lowering pressure on _mi_page_free_collect.
    if std::env::var_os("MIMALLOC_ARENA_EAGER_COMMIT").is_none() {
        unsafe { std::env::set_var("MIMALLOC_ARENA_EAGER_COMMIT", "0") };
    }
    // Defer returning memory to the OS so mi_heap_collect_ex (CPI 18.9)
    // runs less frequently during query execution.
    if std::env::var_os("MIMALLOC_DECOMMIT_DELAY").is_none() {
        unsafe { std::env::set_var("MIMALLOC_DECOMMIT_DELAY", "1000") };
    }

    let cli = Cli::parse();

    let data_dir = cli.data_dir.clone();

    let s3_config = if data_dir.starts_with("s3://") {
        Some(S3Config {
            endpoint: cli.s3_endpoint,
            region: Some(cli.s3_region),
            allow_http: cli.s3_allow_http,
            wal_dir: cli.wal_dir,
        })
    } else {
        None
    };

    let mut db = potatodb_engine::PotatoDB::new(data_dir, s3_config).await?;

    if !cli.files.is_empty() {
        let mut had_error = false;
        let show_timing = cli.timing;
        for path in &cli.files {
            println!("Executing: {path}");
            let file_start = Instant::now();
            match db
                .execute_file_with_callback(path, false, |_stmt, result| match result {
                    Ok(QueryResult::Records(batches)) => {
                        println!("{}", potatodb_display::format_batches_truncated(batches));
                        println!("({} row(s))", potatodb_display::row_count(batches));
                        println!();
                    }
                    Ok(QueryResult::Message(msg)) => {
                        println!("{msg}");
                        println!();
                    }
                    Err(e) => {
                        eprintln!("ERROR: {e}");
                        eprintln!();
                        had_error = true;
                    }
                })
                .await
            {
                Ok(()) => {
                    if show_timing {
                        let total = file_start.elapsed();
                        println!(
                            "Total file execution: {:.3} ms",
                            total.as_secs_f64() * 1000.0
                        );
                    }
                }
                Err(e) => {
                    eprintln!("ERROR: {e}");
                    had_error = true;
                }
            }
        }
        std::process::exit(i32::from(had_error));
    }

    if let Some(http_addr) = cli.http_addr {
        let shared = Arc::new(RwLock::new(db));
        println!("Starting HTTP API on {http_addr}");
        potatodb_http::start_http(shared, &http_addr).await?;
        return Ok(());
    }

    if cli.repl {
        potatodb_repl::run(&mut db).await?;
    } else {
        potatodb_tui::run(&mut db, cli.theme).await?;
    }

    Ok(())
}
