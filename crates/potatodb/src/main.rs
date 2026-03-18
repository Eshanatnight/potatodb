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
    /// Data location: local path (./data) or S3 URL (<s3://bucket/prefix>)
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if std::env::var_os("MIMALLOC_PURGE_DELAY").is_none() {
        std::env::set_var("MIMALLOC_PURGE_DELAY", "10");
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
            match db.execute_file(path, false).await {
                Ok(results) => {
                    for (_stmt, result) in results {
                        match result {
                            Ok(QueryResult::Records(batches)) => {
                                println!(
                                    "{}",
                                    potatodb_display::format_batches_truncated(&batches)
                                );
                                println!("({} row(s))", potatodb_display::row_count(&batches));
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
                        }
                    }
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
