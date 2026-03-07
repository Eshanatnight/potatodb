use clap::Parser;
use potatodb_engine::{QueryResult, S3Config};
use potatodb_tui::ThemeChoice;
use std::sync::Arc;
use tokio::sync::RwLock;

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

    /// Use the line-mode REPL instead of the TUI
    #[arg(long)]
    repl: bool,

    /// TUI colour theme: 'catppuccin-mocha' or 'potato (default)'
    #[arg(long, default_value = "potato", env = "POTATODB_THEME")]
    theme: ThemeChoice,

    /// Execute SQL file(s) and exit. Can be specified multiple times.
    #[arg(short = 'f', long = "file")]
    files: Vec<String>,

    /// Start HTTP API server on this address (e.g. 127.0.0.1:8080).
    #[arg(long)]
    http_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();

    let data_dir = cli.data_dir.clone();

    let s3_config = if data_dir.starts_with("s3://") {
        Some(S3Config {
            endpoint: cli.s3_endpoint,
            region: Some(cli.s3_region),
            allow_http: cli.s3_allow_http,
        })
    } else {
        None
    };

    let mut db = potatodb_engine::PotatoDB::new(data_dir, s3_config).await?;

    if !cli.files.is_empty() {
        let mut had_error = false;
        for path in &cli.files {
            println!("── Executing: {path}");
            match db.execute_file(path, false).await {
                Ok(results) => {
                    for (stmt, result) in results {
                        match result {
                            Ok(QueryResult::Records(batches)) => {
                                let rows = potatodb_display::row_count(&batches);
                                println!("{}", potatodb_display::format_batches(&batches));
                                println!("({rows} row(s))\n");
                            }
                            Ok(QueryResult::Message(msg)) => {
                                println!("{msg}");
                            }
                            Err(e) => {
                                eprintln!("ERROR in statement:\n  {stmt}\n  {e}\n");
                                had_error = true;
                            }
                        }
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
