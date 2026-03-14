#![allow(clippy::too_many_lines, clippy::option_if_let_else)]
//! Line-mode interactive SQL REPL.
//!
//! Provides readline editing, persistent history, multi-line input
//! (accumulates until a `;` terminator), and special backslash commands.

use std::borrow::Cow;

use chrono::Utc;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::Validator;
use rustyline::Editor;
use rustyline::{Context, Helper};

use potatodb_display as display;
use potatodb_engine::{PotatoDB, QueryResult};

const ANSI_RESET: &str = "\x1b[0m";
const ANSI_BOLD_BLUE: &str = "\x1b[1;34m";
const ANSI_GREEN: &str = "\x1b[32m";
const ANSI_CYAN: &str = "\x1b[36m";
const ANSI_DIM: &str = "\x1b[2m";
const ANSI_BOLD_GREEN: &str = "\x1b[1;32m";

/// ASCII art banner printed once at REPL startup.
const BANNER: &str = r"
 ____        _        _        ____  ____
|  _ \ ___  | |_ __ _| |_ ___ |  _ \| __ )
| |_) / _ \ | __/ _` | __/ _ \| | | |  _ \
|  __/ (_) || || (_| | || (_) | |_| | |_) |
|_|   \___/  \__\__,_|\__\___/|____/|____/

Type SQL statements terminated with ';'
Special commands: \q, \dt, \d <table>, \di, \dv, \i <file>, .source <file>,
                  \backup, \restore, .import, .export
";

#[derive(Clone, Default)]
struct ReplHelper {
    words: Vec<String>,
}

impl Helper for ReplHelper {}
impl Validator for ReplHelper {}
impl Hinter for ReplHelper {
    type Hint = String;
}

impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if line.is_empty() || line.starts_with('\\') || line.starts_with('.') {
            return Cow::Borrowed(line);
        }
        Cow::Owned(highlight_sql(line))
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Owned(format!("{ANSI_BOLD_GREEN}{prompt}{ANSI_RESET}"))
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Owned(format!("{ANSI_DIM}{hint}{ANSI_RESET}"))
    }

    fn highlight_char(
        &self,
        _line: &str,
        _pos: usize,
        _kind: rustyline::highlight::CmdKind,
    ) -> bool {
        true
    }
}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let safe_pos = pos.min(line.len());
        let start = line[..safe_pos]
            .rfind(|c: char| c.is_whitespace())
            .map_or(0, |i| i + 1);
        let prefix = &line[start..safe_pos];
        let prefix_lower = prefix.to_lowercase();

        let matches = self
            .words
            .iter()
            .filter(|w| w.to_lowercase().starts_with(&prefix_lower))
            .map(|w| Pair {
                display: w.clone(),
                replacement: w.clone(),
            })
            .collect::<Vec<_>>();

        Ok((start, matches))
    }
}

/// Runs the interactive REPL loop until the user quits.
///
/// Input lines are accumulated into a buffer until a line ending with
/// `;` is encountered, at which point the full statement is executed.
/// History is saved to `~/.potatodb_history`.
///
/// # Errors
///
/// Returns an error if readline initialization or history I/O fails.
pub async fn run(db: &mut PotatoDB) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut rl = Editor::<ReplHelper, DefaultHistory>::new()?;
    rl.set_helper(Some(ReplHelper {
        words: completion_words(db),
    }));
    let history_file = dirs_home().join(".potatodb_history");
    let _ = rl.load_history(&history_file);

    println!("{BANNER}");

    let mut buffer = String::new();

    loop {
        let prompt = if buffer.is_empty() {
            "potatodb> "
        } else {
            "       -> "
        };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                if buffer.is_empty() {
                    match trimmed {
                        "\\q" | "quit" | "exit" => {
                            break;
                        }
                        "\\dt" => {
                            execute_and_print(db, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name").await;
                            continue;
                        }
                        "\\di" => {
                            print_indexes(db);
                            continue;
                        }
                        "\\dv" => {
                            print_views(db);
                            continue;
                        }
                        s if s.starts_with("\\backup ") => {
                            let path = s[8..].trim();
                            match db.backup(path).await {
                                Ok(()) => println!("Backup created at {path}\n"),
                                Err(e) => eprintln!("ERROR: {e}"),
                            }
                            continue;
                        }
                        s if s.starts_with("\\restore ") => {
                            let path = s[9..].trim();
                            match db.restore(path).await {
                                Ok(()) => println!("Restore completed from {path}\n"),
                                Err(e) => eprintln!("ERROR: {e}"),
                            }
                            if let Some(helper) = rl.helper_mut() {
                                helper.words = completion_words(db);
                            }
                            continue;
                        }
                        s if s.starts_with("\\d ") => {
                            let table = s[3..].trim();
                            execute_and_print(db, &format!("DESCRIBE {table}")).await;
                            continue;
                        }
                        s if s.starts_with("\\i ") || s.starts_with(".source ") => {
                            let path = if let Some(stripped) = s.strip_prefix("\\i ") {
                                stripped.trim()
                            } else if let Some(stripped) = s.strip_prefix(".source ") {
                                stripped.trim()
                            } else {
                                unreachable!()
                            };
                            execute_file_and_print(db, path).await;
                            if let Some(helper) = rl.helper_mut() {
                                helper.words = completion_words(db);
                            }
                            continue;
                        }
                        s if s.starts_with(".import ") => {
                            if let Some(sql) = parse_io_command(s, true) {
                                execute_and_print(db, &sql).await;
                            } else {
                                eprintln!("Usage: .import <csv|json|parquet> <table> <path>");
                            }
                            continue;
                        }
                        s if s.starts_with(".export ") => {
                            if let Some(sql) = parse_io_command(s, false) {
                                execute_and_print(db, &sql).await;
                            } else {
                                eprintln!("Usage: .export <csv|json|parquet> <table> <path>");
                            }
                            continue;
                        }
                        "" => continue,
                        _ => {}
                    }
                }

                // Strip line comments and skip pure-comment / empty lines.
                let effective = strip_line_comment(trimmed);
                if effective.is_empty() {
                    continue;
                }

                buffer.push_str(&effective);
                buffer.push(' ');

                if effective.ends_with(';') {
                    let sql = buffer.trim().to_string();
                    let _ = rl.add_history_entry(&sql);
                    execute_and_print(db, &sql).await;
                    buffer.clear();
                    if let Some(helper) = rl.helper_mut() {
                        helper.words = completion_words(db);
                    }
                }
            }
            Err(ReadlineError::Interrupted | ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error: {err}");
                break;
            }
        }
    }

    let _ = rl.save_history(&history_file);
    Ok(())
}

/// Executes a SQL statement and prints the results or error to stdout.
async fn execute_and_print(db: &mut PotatoDB, sql: &str) {
    let start = Utc::now();
    match db.execute(sql).await {
        Ok(QueryResult::Records(batches)) => {
            let elapsed = Utc::now() - start;
            println!("{}", display::format_batches_truncated(&batches));
            println!(
                "({} row(s), {}ms)",
                display::row_count(&batches),
                elapsed.num_milliseconds()
            );
        }
        Ok(QueryResult::Message(msg)) => {
            let elapsed = Utc::now() - start;
            println!("{msg}");
            println!("({elapsed})");
        }
        Err(e) => {
            eprintln!("ERROR: {e}");
        }
    }
    println!();
}

/// Executes all statements from a SQL file and prints each result.
async fn execute_file_and_print(db: &mut PotatoDB, path: &str) {
    println!("Executing: {path}");
    match db.execute_file(path, true).await {
        Ok(results) => {
            for (stmt, result) in results {
                match result {
                    Ok(QueryResult::Records(batches)) => {
                        println!("{}", display::format_batches_truncated(&batches));
                        println!("({} row(s))", display::row_count(&batches));
                    }
                    Ok(QueryResult::Message(msg)) => {
                        println!("{msg}");
                    }
                    Err(e) => {
                        eprintln!("ERROR in statement:\n  {stmt}\n  {e}");
                    }
                }
            }
            println!();
        }
        Err(e) => {
            eprintln!("ERROR: {e}\n");
        }
    }
}

/// Returns the user's home directory, falling back to `.` if `$HOME` is unset.
fn dirs_home() -> std::path::PathBuf {
    std::env::var("HOME").map_or_else(|_| std::path::PathBuf::from("."), std::path::PathBuf::from)
}

fn completion_words(db: &PotatoDB) -> Vec<String> {
    let mut words = vec![
        "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "TABLE", "VIEW",
        "INDEX", "WHERE", "FROM", "INTO", "VALUES", "JOIN", "ORDER", "BY", "GROUP", "LIMIT",
        "COPY", "TRUNCATE", "ANALYZE", "VACUUM", "BEGIN", "COMMIT", "ROLLBACK", "EXPLAIN",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<Vec<_>>();

    for table in db.table_names() {
        words.push(table.clone());
        for col in db.table_columns(&table) {
            words.push(col);
        }
    }
    words.sort();
    words.dedup();
    words
}

fn print_indexes(db: &PotatoDB) {
    let mut idx = db.indexes();
    idx.sort();
    if idx.is_empty() {
        println!("(no indexes)\n");
        return;
    }
    for (name, table) in idx {
        println!("{name}\t{table}");
    }
    println!();
}

fn print_views(db: &PotatoDB) {
    let mut views = db.view_names();
    views.sort();
    if views.is_empty() {
        println!("(no views)\n");
        return;
    }
    for view in views {
        println!("{view}");
    }
    println!();
}

fn parse_io_command(command: &str, is_import: bool) -> Option<String> {
    let parts = command.split_whitespace().collect::<Vec<_>>();
    if parts.len() < 4 {
        return None;
    }
    let format = parts[1].to_lowercase();
    if !matches!(format.as_str(), "csv" | "json" | "parquet") {
        return None;
    }
    let table = parts[2];
    let path = parts[3];
    Some(if is_import {
        format!("COPY {table} FROM '{path}'")
    } else {
        format!("COPY {table} TO '{path}'")
    })
}

/// Strips a trailing `-- …` line comment from a single input line,
/// respecting single-quoted string literals. Returns the trimmed
/// remainder (may be empty for pure-comment lines).
fn strip_line_comment(line: &str) -> String {
    let mut result = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            '\'' => {
                result.push(chars.next().unwrap());
                loop {
                    match chars.next() {
                        Some('\'') => {
                            result.push('\'');
                            if chars.peek() == Some(&'\'') {
                                result.push(chars.next().unwrap());
                            } else {
                                break;
                            }
                        }
                        Some(c) => result.push(c),
                        None => break,
                    }
                }
            }
            '-' => {
                chars.next();
                if chars.peek() == Some(&'-') {
                    // Rest of the line is a comment — discard it.
                    break;
                }
                result.push('-');
            }
            _ => {
                result.push(chars.next().unwrap());
            }
        }
    }

    result.trim().to_string()
}

// ---------------------------------------------------------------------------
// SQL syntax highlighting
// ---------------------------------------------------------------------------

fn highlight_sql(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    let mut chars = input.char_indices().peekable();

    while let Some(&(i, ch)) = chars.peek() {
        if ch == '\'' {
            out.push_str(ANSI_GREEN);
            out.push('\'');
            chars.next();
            loop {
                match chars.next() {
                    Some((_, '\'')) => {
                        out.push('\'');
                        if chars.peek().map(|&(_, c)| c) == Some('\'') {
                            out.push(chars.next().unwrap().1);
                        } else {
                            break;
                        }
                    }
                    Some((_, c)) => out.push(c),
                    None => break,
                }
            }
            out.push_str(ANSI_RESET);
            continue;
        }

        if ch == '-' && input[i..].starts_with("--") {
            out.push_str(ANSI_DIM);
            for (_, c) in chars.by_ref() {
                out.push(c);
            }
            out.push_str(ANSI_RESET);
            continue;
        }

        if ch.is_ascii_digit() {
            out.push_str(ANSI_CYAN);
            while let Some(&(_, c)) = chars.peek() {
                if c.is_ascii_digit() || c == '.' {
                    out.push(c);
                    chars.next();
                } else {
                    break;
                }
            }
            out.push_str(ANSI_RESET);
            continue;
        }

        if ch.is_ascii_alphabetic() || ch == '_' {
            let start = i;
            chars.next();
            while let Some(&(_, c)) = chars.peek() {
                if c.is_ascii_alphanumeric() || c == '_' {
                    chars.next();
                } else {
                    break;
                }
            }
            let end = chars.peek().map_or(input.len(), |&(e, _)| e);
            let word = &input[start..end];
            if is_sql_keyword(word) {
                out.push_str(ANSI_BOLD_BLUE);
                out.push_str(word);
                out.push_str(ANSI_RESET);
            } else {
                out.push_str(word);
            }
            continue;
        }

        if ch == ';' {
            out.push_str(ANSI_DIM);
            out.push(';');
            out.push_str(ANSI_RESET);
            chars.next();
            continue;
        }

        out.push(ch);
        chars.next();
    }

    out
}

#[rustfmt::skip]
fn is_sql_keyword(word: &str) -> bool {
    let upper = word.to_ascii_uppercase();
    matches!(
        upper.as_str(),
        "ADD" | "ALL" | "ALTER" | "ANALYZE" | "AND" | "ANY" | "AS" | "ASC" | "AVG"
        | "BEGIN" | "BETWEEN" | "BIGINT" | "BINARY" | "BLOB" | "BOOL" | "BOOLEAN" | "BY"
        | "CASCADE" | "CASE" | "CAST" | "CHAR" | "CHECK" | "COALESCE" | "COLUMN"
        | "COMMIT" | "CONFLICT" | "CONSTRAINT" | "COPY" | "COUNT" | "CREATE" | "CROSS"
        | "CURRENT"
        | "DATABASE" | "DATE" | "DECIMAL" | "DEFAULT" | "DELETE" | "DESC" | "DESCRIBE"
        | "DISTINCT" | "DO" | "DOUBLE" | "DROP"
        | "ELSE" | "END" | "EXCEPT" | "EXISTS" | "EXPLAIN"
        | "FALSE" | "FETCH" | "FIRST" | "FLOAT" | "FOLLOWING" | "FOR" | "FOREIGN" | "FROM"
        | "FULL"
        | "GROUP"
        | "HAVING"
        | "IF" | "IGNORE" | "ILIKE" | "IN" | "INDEX" | "INNER" | "INSERT" | "INT"
        | "INTEGER" | "INTERSECT" | "INTO" | "IS"
        | "JOIN"
        | "KEY"
        | "LEFT" | "LIKE" | "LIMIT"
        | "MATERIALIZED" | "MAX" | "MERGE" | "MIN"
        | "NATURAL" | "NEXT" | "NOT" | "NOTHING" | "NULL" | "NULLIF" | "NULLS" | "NUMERIC"
        | "OFFSET" | "ON" | "ONLY" | "OR" | "ORDER" | "OUTER" | "OVER"
        | "PARTITION" | "PRECEDING" | "PRIMARY"
        | "RANGE" | "REAL" | "RECURSIVE" | "REFERENCES" | "RELEASE" | "RENAME" | "REPLACE"
        | "RESTRICT" | "RETURNING" | "RIGHT" | "ROLLBACK" | "ROW" | "ROWS"
        | "SAVEPOINT" | "SCHEMA" | "SELECT" | "SERIAL" | "SET" | "SMALLINT" | "SOME" | "SUM"
        | "TABLE" | "TEMP" | "TEMPORARY" | "TEXT" | "THEN" | "TIMESTAMP" | "TINYINT" | "TO"
        | "TRUE" | "TRUNCATE" | "TYPE"
        | "UNBOUNDED" | "UNION" | "UNIQUE" | "UPDATE" | "USING"
        | "VACUUM" | "VALUES" | "VARCHAR" | "VIEW"
        | "WHEN" | "WHERE" | "WINDOW" | "WITH"
    )
}
