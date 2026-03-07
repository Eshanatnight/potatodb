use potatodb_display::{format_batches, row_count};
use potatodb_engine::QueryResult;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub fn print_result(label: &str, result: &QueryResult) {
    println!("\n--- {label} ---");
    match result {
        QueryResult::Records(batches) => {
            println!("{}", format_batches(batches));
            println!("({} rows)", row_count(batches));
        }
        QueryResult::Message(msg) => println!("{msg}"),
    }
}

pub fn section(title: &str) {
    let bar = "=".repeat(60);
    println!("\n{bar}");
    println!("  {title}");
    println!("{bar}");
}
