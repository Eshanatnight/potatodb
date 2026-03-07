/// Starts a PotatoDB server with the PostgreSQL wire protocol.
///
/// Once running, connect with any PostgreSQL client:
///
///   psql -h 127.0.0.1 -p 5433 -U potatodb
///   Password: potatodb
///
/// Run with:
///   cargo run --example pgwire_server
///
/// Environment variables:
///   POTATODB_USER      – login user  (default: "potatodb")
///   POTATODB_PASSWORD   – login pass  (default: "potatodb")
///
/// Auto-vacuum (optional):
///   POTATODB_AUTO_VACUUM_INTERVAL_SECS   – check interval (0 = off, default)
///   POTATODB_AUTO_VACUUM_FILE_THRESHOLD   – file-count trigger  (default: 25)
///   POTATODB_AUTO_VACUUM_BYTES_THRESHOLD  – byte-size trigger   (default: 256 MiB)
///   POTATODB_AUTO_VACUUM_AGE_SECS         – oldest-file trigger (default: 3600)
use potatodb_server::start_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let data_dir =
        std::env::var("POTATODB_DATA_DIR").unwrap_or_else(|_| "./pgwire_example_data".to_string());
    let bind = std::env::var("POTATODB_BIND").unwrap_or_else(|_| "127.0.0.1:5433".to_string());

    eprintln!("Starting PotatoDB pgwire server…");
    eprintln!("  data dir : {data_dir}");
    eprintln!("  bind addr: {bind}");
    eprintln!();
    eprintln!("Connect with:");
    eprintln!("  psql -h 127.0.0.1 -p 5433 -U potatodb");
    eprintln!("  Password: potatodb");

    start_server(&data_dir, &bind).await
}
