#![allow(
    clippy::cast_possible_truncation,
    clippy::option_if_let_else,
    clippy::branches_sharing_code,
    clippy::too_many_lines
)]
//! Full-screen terminal UI built with ratatui.
//!
//! Launched via `cargo run`. Provides a three-panel layout with a table
//! sidebar (with schema preview), scrollable results pane using the
//! ratatui `Table` widget, multi-line SQL input with syntax
//! highlighting, persistent history, mouse support, help overlay,
//! result export, and responsive layout.

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use ratatui::{
    crossterm::event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, KeyModifiers,
        MouseButton, MouseEventKind,
    },
    layout::{Constraint, Layout, Position, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, BorderType, Cell, Clear, HighlightSpacing, List, ListItem, Paragraph, Row,
        Scrollbar, ScrollbarOrientation, ScrollbarState, Table, TableState,
    },
    DefaultTerminal, Frame,
};

use potatodb_engine::{PotatoDB, QueryResult};

// ── SQL keywords for syntax highlighting ──────────────────────

static SQL_KEYWORDS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    [
        "SELECT",
        "FROM",
        "WHERE",
        "INSERT",
        "INTO",
        "VALUES",
        "UPDATE",
        "SET",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "TABLE",
        "INDEX",
        "VIEW",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "OUTER",
        "CROSS",
        "ON",
        "AND",
        "OR",
        "NOT",
        "IN",
        "IS",
        "NULL",
        "AS",
        "ORDER",
        "BY",
        "GROUP",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "ASC",
        "DESC",
        "DISTINCT",
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "PRIMARY",
        "KEY",
        "UNIQUE",
        "CHECK",
        "FOREIGN",
        "REFERENCES",
        "CONSTRAINT",
        "DEFAULT",
        "IF",
        "EXISTS",
        "LIKE",
        "ILIKE",
        "BETWEEN",
        "UNION",
        "ALL",
        "ANY",
        "SOME",
        "WITH",
        "RECURSIVE",
        "RETURNING",
        "EXPLAIN",
        "ANALYZE",
        "VACUUM",
        "TRUNCATE",
        "COPY",
        "GRANT",
        "REVOKE",
        "CASCADE",
        "RESTRICT",
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "VARCHAR",
        "TEXT",
        "BOOL",
        "BOOLEAN",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
        "DATE",
        "TIMESTAMP",
        "UUID",
        "SERIAL",
        "TRUE",
        "FALSE",
        "COALESCE",
        "CAST",
        "EXTRACT",
        "INTERVAL",
        "SEQUENCE",
        "FUNCTION",
        "PROCEDURE",
        "CALL",
        "DO",
        "PREPARE",
        "EXECUTE",
        "MATERIALIZED",
        "REFRESH",
        "FLUSH",
        "SHOW",
        "USE",
        "DESCRIBE",
        "NOTIFY",
        "LISTEN",
        "OVER",
        "PARTITION",
        "ROW_NUMBER",
        "RANK",
        "LAG",
        "LEAD",
        "NTILE",
        "WINDOW",
        "ROWS",
        "RANGE",
        "PRECEDING",
        "FOLLOWING",
        "UNBOUNDED",
        "CURRENT",
        "CONFLICT",
        "NOTHING",
        "UPSERT",
    ]
    .into_iter()
    .collect()
});

// ── Color themes ───────────────────────────────────────────────

/// Which colour palette the TUI should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ThemeChoice {
    #[default]
    CatppuccinMocha,
    Potato,
}

impl std::fmt::Display for ThemeChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CatppuccinMocha => write!(f, "Catppuccin Mocha"),
            Self::Potato => write!(f, "Potato"),
        }
    }
}

impl std::str::FromStr for ThemeChoice {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "catppuccin" | "catppuccin-mocha" | "mocha" => Ok(Self::CatppuccinMocha),
            "potato" | "spud" => Ok(Self::Potato),
            other => Err(format!(
                "unknown theme '{other}'; expected 'catppuccin-mocha' or 'potato'"
            )),
        }
    }
}

struct Theme {
    base: Color,
    mantle: Color,
    crust: Color,
    surface0: Color,
    surface1: Color,
    overlay0: Color,
    subtext0: Color,
    subtext1: Color,
    text: Color,
    accent1: Color,
    accent2: Color,
    error: Color,
    number: Color,
    success: Color,
    keyword: Color,
    string_lit: Color,
    name: &'static str,
}

impl Theme {
    const fn catppuccin_mocha() -> Self {
        Self {
            base: Color::Rgb(30, 30, 46),
            mantle: Color::Rgb(24, 24, 37),
            crust: Color::Rgb(17, 17, 27),
            surface0: Color::Rgb(49, 50, 68),
            surface1: Color::Rgb(69, 71, 90),
            overlay0: Color::Rgb(108, 112, 134),
            subtext0: Color::Rgb(166, 173, 200),
            subtext1: Color::Rgb(186, 194, 222),
            text: Color::Rgb(205, 214, 244),
            accent1: Color::Rgb(180, 190, 254),
            accent2: Color::Rgb(203, 166, 247),
            error: Color::Rgb(243, 139, 168),
            number: Color::Rgb(250, 179, 135),
            success: Color::Rgb(166, 227, 161),
            keyword: Color::Rgb(137, 180, 250),
            string_lit: Color::Rgb(166, 227, 161),
            name: "Catppuccin Mocha",
        }
    }

    const fn potato() -> Self {
        Self {
            base: Color::Rgb(43, 33, 24),
            mantle: Color::Rgb(35, 26, 18),
            crust: Color::Rgb(28, 20, 13),
            surface0: Color::Rgb(62, 48, 35),
            surface1: Color::Rgb(87, 68, 50),
            overlay0: Color::Rgb(139, 119, 101),
            subtext0: Color::Rgb(186, 168, 140),
            subtext1: Color::Rgb(210, 195, 170),
            text: Color::Rgb(240, 230, 210),
            accent1: Color::Rgb(218, 185, 107),
            accent2: Color::Rgb(196, 148, 72),
            error: Color::Rgb(204, 85, 61),
            number: Color::Rgb(230, 172, 80),
            success: Color::Rgb(124, 165, 75),
            keyword: Color::Rgb(218, 185, 107),
            string_lit: Color::Rgb(124, 165, 75),
            name: "Potato",
        }
    }

    const fn from_choice(choice: ThemeChoice) -> Self {
        match choice {
            ThemeChoice::CatppuccinMocha => Self::catppuccin_mocha(),
            ThemeChoice::Potato => Self::potato(),
        }
    }
}

// ── Focus / Actions ───────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Focus {
    Sidebar,
    Results,
    Query,
}

impl Focus {
    const fn next(self) -> Self {
        match self {
            Self::Sidebar => Self::Results,
            Self::Results => Self::Query,
            Self::Query => Self::Sidebar,
        }
    }
    const fn prev(self) -> Self {
        match self {
            Self::Sidebar => Self::Query,
            Self::Results => Self::Sidebar,
            Self::Query => Self::Results,
        }
    }
}

enum Action {
    None,
    Execute(String),
    PreviewTable(String),
    Quit,
}

// ── Popup state ───────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum Popup {
    None,
    Help,
    Export,
}

// ── Toast notification ────────────────────────────────────────

struct Toast {
    message: String,
    expires: Instant,
}

// ── Structured query results ──────────────────────────────────

struct QueryResults {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    column_widths: Vec<u16>,
    message: Option<String>,
}

impl QueryResults {
    fn from_batches(batches: &[RecordBatch]) -> Self {
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            let headers: Vec<String> = batches
                .first()
                .map(|b| {
                    b.schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect()
                })
                .unwrap_or_default();
            let widths: Vec<u16> = headers.iter().map(|h| h.len() as u16).collect();
            return Self {
                headers,
                rows: Vec::new(),
                column_widths: widths,
                message: None,
            };
        }

        let schema = batches[0].schema();
        let headers: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let num_cols = headers.len();
        let mut col_widths: Vec<u16> = headers.iter().map(|h| h.len() as u16).collect();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let fmt_opts = arrow::util::display::FormatOptions::default();

        for batch in batches {
            let formatters: Vec<_> = (0..num_cols)
                .filter_map(|c| {
                    arrow::util::display::ArrayFormatter::try_new(
                        batch.column(c).as_ref(),
                        &fmt_opts,
                    )
                    .ok()
                })
                .collect();
            if formatters.len() != num_cols {
                continue;
            }
            for row_idx in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(num_cols);
                for (col_idx, fmt) in formatters.iter().enumerate() {
                    let val = fmt.value(row_idx).to_string();
                    col_widths[col_idx] = col_widths[col_idx].max(val.len() as u16);
                    row.push(val);
                }
                rows.push(row);
            }
        }

        Self {
            headers,
            rows,
            column_widths: col_widths,
            message: None,
        }
    }

    const fn from_message(msg: String) -> Self {
        Self {
            headers: Vec::new(),
            rows: Vec::new(),
            column_widths: Vec::new(),
            message: Some(msg),
        }
    }

    fn from_error(err: &str) -> Self {
        Self {
            headers: Vec::new(),
            rows: Vec::new(),
            column_widths: Vec::new(),
            message: Some(format!("ERROR: {err}")),
        }
    }

    const fn is_empty(&self) -> bool {
        self.headers.is_empty() && self.message.is_none()
    }

    const fn row_count(&self) -> usize {
        self.rows.len()
    }

    fn export_csv(&self) -> String {
        let mut out = self.headers.join(",");
        out.push('\n');
        for row in &self.rows {
            let line: Vec<String> = row
                .iter()
                .map(|c| {
                    if c.contains(',') || c.contains('"') || c.contains('\n') {
                        format!("\"{}\"", c.replace('"', "\"\""))
                    } else {
                        c.clone()
                    }
                })
                .collect();
            out.push_str(&line.join(","));
            out.push('\n');
        }
        out
    }

    fn export_json(&self) -> String {
        let mut objects = Vec::new();
        for row in &self.rows {
            let pairs: Vec<String> = self
                .headers
                .iter()
                .zip(row.iter())
                .map(|(h, v)| format!("    \"{h}\": \"{}\"", v.replace('"', "\\\"")))
                .collect();
            objects.push(format!("  {{\n{}\n  }}", pairs.join(",\n")));
        }
        format!("[\n{}\n]", objects.join(",\n"))
    }
}

// ── App state ──────────────────────────────────────────────────

/// Active tab-completion session state.
struct Completion {
    /// Byte offset in the current line where the prefix starts.
    start_col: usize,
    /// All matching candidates.
    candidates: Vec<String>,
    /// Index into `candidates` of the currently shown completion.
    index: usize,
}

struct App {
    focus: Focus,
    input_lines: Vec<String>,
    cursor_row: usize,
    cursor_col: usize,
    results: QueryResults,
    table_state: TableState,
    h_scroll: usize,
    visible_height: u16,
    history: Vec<String>,
    history_pos: Option<usize>,
    status: String,
    elapsed: String,
    tables: Vec<String>,
    selected_table: usize,
    schema_cache: HashMap<String, Vec<(String, String)>>,
    data_url: String,
    theme: Theme,
    popup: Popup,
    toast: Option<Toast>,
    export_path: String,
    export_format: usize,
    spinner_state: usize,
    sidebar_area: Rect,
    results_area: Rect,
    query_area: Rect,
    history_file: Option<PathBuf>,
    completion: Option<Completion>,
}

const HISTORY_MAX: usize = 1000;
const EXPORT_FORMATS: [&str; 3] = ["CSV", "JSON", "SQL"];

impl App {
    fn new(data_url: String, theme_choice: ThemeChoice) -> Self {
        let history_file = dirs_history_path();
        let history = history_file
            .as_ref()
            .and_then(|p| std::fs::read_to_string(p).ok())
            .map(|contents| {
                contents
                    .lines()
                    .map(|l| l.replace("\\n", "\n"))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        Self {
            focus: Focus::Query,
            input_lines: vec![String::new()],
            cursor_row: 0,
            cursor_col: 0,
            results: QueryResults {
                headers: Vec::new(),
                rows: Vec::new(),
                column_widths: Vec::new(),
                message: None,
            },
            table_state: TableState::default(),
            h_scroll: 0,
            visible_height: 20,
            history,
            history_pos: None,
            status: String::new(),
            elapsed: String::new(),
            tables: Vec::new(),
            selected_table: 0,
            schema_cache: HashMap::new(),
            data_url,
            theme: Theme::from_choice(theme_choice),
            popup: Popup::None,
            toast: None,
            export_path: String::from("results.csv"),
            export_format: 0,
            spinner_state: 0,
            sidebar_area: Rect::default(),
            results_area: Rect::default(),
            query_area: Rect::default(),
            history_file,
            completion: None,
        }
    }

    fn input_text(&self) -> String {
        self.input_lines.join("\n")
    }

    fn save_history_entry(&mut self, sql: &str) {
        let entry = sql.trim().to_string();
        if entry.is_empty() {
            return;
        }
        if self.history.last().map(String::as_str) != Some(&entry) {
            self.history.push(entry);
        }
        if self.history.len() > HISTORY_MAX {
            self.history.drain(0..self.history.len() - HISTORY_MAX);
        }
        self.history_pos = None;
        self.persist_history();
    }

    fn persist_history(&self) {
        let Some(path) = &self.history_file else {
            return;
        };
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let Ok(mut f) = std::fs::File::create(path) else {
            return;
        };
        for entry in &self.history {
            let _ = writeln!(f, "{}", entry.replace('\n', "\\n"));
        }
    }

    fn clear_input(&mut self) {
        self.input_lines = vec![String::new()];
        self.cursor_row = 0;
        self.cursor_col = 0;
        self.completion = None;
    }

    fn dismiss_completion(&mut self) {
        self.completion = None;
    }

    /// Extracts the word fragment immediately before the cursor.
    fn word_before_cursor(&self) -> (usize, String) {
        let line = &self.input_lines[self.cursor_row];
        let before = &line[..self.cursor_col];
        let start = before
            .rfind(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .map_or(0, |i| i + 1);
        (start, before[start..].to_string())
    }

    /// Builds a sorted, deduplicated list of completion candidates
    /// matching `prefix` (case-insensitive).
    fn build_candidates(&self, prefix: &str) -> Vec<String> {
        let upper = prefix.to_uppercase();
        let mut seen = HashSet::new();
        let mut out = Vec::new();

        for table in &self.tables {
            if table.to_uppercase().starts_with(&upper) && seen.insert(table.clone()) {
                out.push(table.clone());
            }
        }
        for cols in self.schema_cache.values() {
            for (col_name, _) in cols {
                if col_name.to_uppercase().starts_with(&upper) && seen.insert(col_name.clone()) {
                    out.push(col_name.clone());
                }
            }
        }
        for &kw in SQL_KEYWORDS.iter() {
            if kw.starts_with(&upper) {
                let display = if prefix.chars().next().is_some_and(char::is_lowercase) {
                    kw.to_lowercase()
                } else {
                    kw.to_string()
                };
                if seen.insert(display.clone()) {
                    out.push(display);
                }
            }
        }
        out.sort_by(|a, b| {
            let a_is_table = self.tables.contains(a);
            let b_is_table = self.tables.contains(b);
            b_is_table.cmp(&a_is_table).then_with(|| a.cmp(b))
        });
        out
    }

    /// Triggers or cycles tab completion.
    fn tab_complete(&mut self) {
        if let Some(ref mut comp) = self.completion {
            if comp.candidates.is_empty() {
                self.completion = None;
                return;
            }
            let line = &mut self.input_lines[self.cursor_row];
            line.replace_range(comp.start_col..self.cursor_col, "");
            self.cursor_col = comp.start_col;

            comp.index = (comp.index + 1) % comp.candidates.len();
            let next = comp.candidates[comp.index].clone();
            let line = &mut self.input_lines[self.cursor_row];
            line.insert_str(self.cursor_col, &next);
            self.cursor_col += next.len();
        } else {
            let (start_col, prefix) = self.word_before_cursor();
            if prefix.is_empty() {
                return;
            }
            let candidates = self.build_candidates(&prefix);
            if candidates.is_empty() {
                return;
            }
            let first = candidates[0].clone();
            let line = &mut self.input_lines[self.cursor_row];
            line.replace_range(start_col..self.cursor_col, &first);
            self.cursor_col = start_col + first.len();
            self.completion = Some(Completion {
                start_col,
                candidates,
                index: 0,
            });
        }
    }

    fn set_toast(&mut self, msg: String) {
        self.toast = Some(Toast {
            message: msg,
            expires: Instant::now() + Duration::from_secs(3),
        });
    }

    fn tick_toast(&mut self) {
        if let Some(t) = &self.toast {
            if Instant::now() >= t.expires {
                self.toast = None;
            }
        }
    }

    // ── Key handling ──────────────────────────────────────────

    fn handle_key(&mut self, key: event::KeyEvent) -> Action {
        if self.popup == Popup::Help {
            if matches!(key.code, KeyCode::Esc | KeyCode::F(1) | KeyCode::Char('q')) {
                self.popup = Popup::None;
            }
            return Action::None;
        }
        if self.popup == Popup::Export {
            return self.handle_export_key(key);
        }

        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL, KeyCode::Char('c' | 'q')) => return Action::Quit,
            (_, KeyCode::Char('q')) if self.focus != Focus::Query => return Action::Quit,
            (_, KeyCode::F(1)) => {
                self.popup = Popup::Help;
                return Action::None;
            }
            (_, KeyCode::Char('?')) if self.focus != Focus::Query => {
                self.popup = Popup::Help;
                return Action::None;
            }
            (KeyModifiers::CONTROL, KeyCode::Char('s')) => {
                if !self.results.is_empty() && !self.results.headers.is_empty() {
                    self.popup = Popup::Export;
                } else {
                    self.set_toast("No results to export".into());
                }
                return Action::None;
            }
            (KeyModifiers::CONTROL, KeyCode::Char('l')) => {
                self.results = QueryResults::from_message(String::new());
                self.results.message = None;
                self.table_state = TableState::default();
                return Action::None;
            }
            (_, KeyCode::Tab) if self.focus == Focus::Query => {
                self.tab_complete();
                return Action::None;
            }
            (_, KeyCode::Tab) => {
                self.focus = self.focus.next();
                return Action::None;
            }
            (KeyModifiers::SHIFT, KeyCode::BackTab) => {
                self.focus = self.focus.prev();
                return Action::None;
            }
            (_, KeyCode::Esc) => {
                self.popup = Popup::None;
                return Action::None;
            }
            _ => {}
        }

        match self.focus {
            Focus::Query => self.handle_query_key(key),
            Focus::Sidebar => self.handle_sidebar_key(key),
            Focus::Results => self.handle_results_key(key),
        }
    }

    fn try_execute(&mut self) -> Action {
        let raw = self.input_text();
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Action::None;
        }
        self.save_history_entry(&raw);
        self.clear_input();
        if trimmed.starts_with('\\') {
            return Action::Execute(trimmed.to_string());
        }
        let sql = strip_sql_comments(&raw);
        if sql.is_empty() {
            return Action::None;
        }
        Action::Execute(sql)
    }

    fn handle_query_key(&mut self, key: event::KeyEvent) -> Action {
        self.dismiss_completion();
        match (key.modifiers, key.code) {
            (KeyModifiers::CONTROL | KeyModifiers::ALT, KeyCode::Enter)
            | (KeyModifiers::CONTROL, KeyCode::Char('j'))
            | (_, KeyCode::F(5)) => self.try_execute(),
            (_, KeyCode::Enter) => {
                let text = self.input_text();
                let trimmed = text.trim();
                if trimmed.starts_with('\\') && !trimmed.is_empty() {
                    return self.try_execute();
                }
                if trimmed.ends_with(';') && !trimmed.is_empty() {
                    return self.try_execute();
                }
                let line = self.input_lines[self.cursor_row].clone();
                let rest = line[self.cursor_col..].to_string();
                self.input_lines[self.cursor_row].truncate(self.cursor_col);
                self.cursor_row += 1;
                self.input_lines.insert(self.cursor_row, rest);
                self.cursor_col = 0;
                Action::None
            }
            (KeyModifiers::CONTROL, KeyCode::Char('u')) => {
                self.input_lines[self.cursor_row].drain(..self.cursor_col);
                self.cursor_col = 0;
                Action::None
            }
            (KeyModifiers::CONTROL, KeyCode::Char('k')) => {
                self.input_lines[self.cursor_row].truncate(self.cursor_col);
                Action::None
            }
            (KeyModifiers::CONTROL, KeyCode::Char('w')) => {
                self.clear_input();
                Action::None
            }
            (KeyModifiers::CONTROL, KeyCode::Char('d')) => {
                if let Some(name) = self.tables.get(self.selected_table).cloned() {
                    Action::Execute(format!(
                        "SELECT column_name, data_type, is_nullable \
                         FROM information_schema.columns \
                         WHERE table_name = '{name}'"
                    ))
                } else {
                    Action::None
                }
            }
            (_, KeyCode::Up) => {
                if self.cursor_row > 0 {
                    self.cursor_row -= 1;
                    self.cursor_col = self.cursor_col.min(self.input_lines[self.cursor_row].len());
                } else if !self.history.is_empty() {
                    let idx = match self.history_pos {
                        Some(i) if i > 0 => i - 1,
                        Some(i) => i,
                        None => self.history.len() - 1,
                    };
                    self.history_pos = Some(idx);
                    let entry = self.history[idx].clone();
                    self.input_lines = entry.split('\n').map(String::from).collect();
                    self.cursor_row = self.input_lines.len().saturating_sub(1);
                    self.cursor_col = self.input_lines[self.cursor_row].len();
                }
                Action::None
            }
            (_, KeyCode::Down) => {
                if self.cursor_row + 1 < self.input_lines.len() {
                    self.cursor_row += 1;
                    self.cursor_col = self.cursor_col.min(self.input_lines[self.cursor_row].len());
                } else if let Some(idx) = self.history_pos {
                    if idx + 1 < self.history.len() {
                        self.history_pos = Some(idx + 1);
                        let entry = self.history[idx + 1].clone();
                        self.input_lines = entry.split('\n').map(String::from).collect();
                        self.cursor_row = 0;
                        self.cursor_col = 0;
                    } else {
                        self.history_pos = None;
                        self.clear_input();
                    }
                }
                Action::None
            }
            (_, KeyCode::Left) => {
                if self.cursor_col > 0 {
                    self.cursor_col -= 1;
                } else if self.cursor_row > 0 {
                    self.cursor_row -= 1;
                    self.cursor_col = self.input_lines[self.cursor_row].len();
                }
                Action::None
            }
            (_, KeyCode::Right) => {
                if self.cursor_col < self.input_lines[self.cursor_row].len() {
                    self.cursor_col += 1;
                } else if self.cursor_row + 1 < self.input_lines.len() {
                    self.cursor_row += 1;
                    self.cursor_col = 0;
                }
                Action::None
            }
            (_, KeyCode::Home) => {
                self.cursor_col = 0;
                Action::None
            }
            (_, KeyCode::End) => {
                self.cursor_col = self.input_lines[self.cursor_row].len();
                Action::None
            }
            (_, KeyCode::Backspace) => {
                if self.cursor_col > 0 {
                    self.cursor_col -= 1;
                    self.input_lines[self.cursor_row].remove(self.cursor_col);
                } else if self.cursor_row > 0 {
                    let removed = self.input_lines.remove(self.cursor_row);
                    self.cursor_row -= 1;
                    self.cursor_col = self.input_lines[self.cursor_row].len();
                    self.input_lines[self.cursor_row].push_str(&removed);
                }
                Action::None
            }
            (_, KeyCode::Delete) => {
                if self.cursor_col < self.input_lines[self.cursor_row].len() {
                    self.input_lines[self.cursor_row].remove(self.cursor_col);
                } else if self.cursor_row + 1 < self.input_lines.len() {
                    let next = self.input_lines.remove(self.cursor_row + 1);
                    self.input_lines[self.cursor_row].push_str(&next);
                }
                Action::None
            }
            (KeyModifiers::NONE | KeyModifiers::SHIFT, KeyCode::Char(c)) => {
                self.input_lines[self.cursor_row].insert(self.cursor_col, c);
                self.cursor_col += 1;
                Action::None
            }
            _ => Action::None,
        }
    }

    fn handle_sidebar_key(&mut self, key: event::KeyEvent) -> Action {
        match key.code {
            KeyCode::Up => {
                if !self.tables.is_empty() {
                    self.selected_table = self.selected_table.saturating_sub(1);
                }
                Action::None
            }
            KeyCode::Down => {
                if !self.tables.is_empty() {
                    self.selected_table =
                        (self.selected_table + 1).min(self.tables.len().saturating_sub(1));
                }
                Action::None
            }
            KeyCode::Enter => {
                if let Some(name) = self.tables.get(self.selected_table) {
                    Action::PreviewTable(name.clone())
                } else {
                    Action::None
                }
            }
            _ => Action::None,
        }
    }

    fn handle_results_key(&mut self, key: event::KeyEvent) -> Action {
        let max_scroll = self
            .results
            .rows
            .len()
            .saturating_sub(self.visible_height as usize);
        match key.code {
            KeyCode::Up => {
                let sel = self.table_state.selected().unwrap_or(0).saturating_sub(1);
                self.table_state.select(Some(sel));
                Action::None
            }
            KeyCode::Down => {
                let sel = self.table_state.selected().map_or(0, |s| {
                    (s + 1).min(self.results.rows.len().saturating_sub(1))
                });
                self.table_state.select(Some(sel));
                Action::None
            }
            KeyCode::Left => {
                self.h_scroll = self.h_scroll.saturating_sub(1);
                Action::None
            }
            KeyCode::Right => {
                self.h_scroll =
                    (self.h_scroll + 1).min(self.results.headers.len().saturating_sub(1));
                Action::None
            }
            KeyCode::PageUp => {
                let sel = self
                    .table_state
                    .selected()
                    .unwrap_or(0)
                    .saturating_sub(self.visible_height as usize / 2);
                self.table_state.select(Some(sel));
                Action::None
            }
            KeyCode::PageDown => {
                let sel = self.table_state.selected().map_or(0, |s| {
                    (s + self.visible_height as usize / 2)
                        .min(self.results.rows.len().saturating_sub(1))
                });
                self.table_state.select(Some(sel));
                Action::None
            }
            KeyCode::Home => {
                self.table_state.select(Some(0));
                Action::None
            }
            KeyCode::End => {
                if !self.results.rows.is_empty() {
                    self.table_state.select(Some(self.results.rows.len() - 1));
                }
                let _ = max_scroll;
                Action::None
            }
            _ => Action::None,
        }
    }

    fn handle_export_key(&mut self, key: event::KeyEvent) -> Action {
        match key.code {
            KeyCode::Esc => {
                self.popup = Popup::None;
            }
            KeyCode::Tab => {
                self.export_format = (self.export_format + 1) % EXPORT_FORMATS.len();
                let ext = EXPORT_FORMATS[self.export_format].to_lowercase();
                if let Some(dot) = self.export_path.rfind('.') {
                    self.export_path.truncate(dot + 1);
                    self.export_path.push_str(&ext);
                }
            }
            KeyCode::Enter => {
                let content = match self.export_format {
                    0 => self.results.export_csv(),
                    1 => self.results.export_json(),
                    _ => {
                        let table = self
                            .tables
                            .get(self.selected_table)
                            .cloned()
                            .unwrap_or_else(|| "table".into());
                        export_sql(&self.results, &table)
                    }
                };
                match std::fs::write(&self.export_path, &content) {
                    Ok(()) => self.set_toast(format!(
                        "Exported {} rows to {}",
                        self.results.rows.len(),
                        self.export_path
                    )),
                    Err(e) => self.set_toast(format!("Export failed: {e}")),
                }
                self.popup = Popup::None;
            }
            KeyCode::Backspace => {
                self.export_path.pop();
            }
            KeyCode::Char(c) => {
                self.export_path.push(c);
            }
            _ => {}
        }
        Action::None
    }

    // ── Mouse handling ────────────────────────────────────────

    fn handle_mouse(&mut self, mouse: event::MouseEvent) -> Action {
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                let x = mouse.column;
                let y = mouse.row;
                if self.sidebar_area.contains(Position::new(x, y)) {
                    self.focus = Focus::Sidebar;
                    let inner_y = y.saturating_sub(self.sidebar_area.y + 1) as usize;
                    if inner_y < self.tables.len() {
                        self.selected_table = inner_y;
                    }
                } else if self.results_area.contains(Position::new(x, y)) {
                    self.focus = Focus::Results;
                    let inner_y = y.saturating_sub(self.results_area.y + 2) as usize;
                    if inner_y < self.results.rows.len() {
                        self.table_state.select(Some(inner_y));
                    }
                } else if self.query_area.contains(Position::new(x, y)) {
                    self.focus = Focus::Query;
                }
            }
            MouseEventKind::ScrollUp => {
                if self.focus == Focus::Results {
                    let sel = self.table_state.selected().unwrap_or(0).saturating_sub(3);
                    self.table_state.select(Some(sel));
                }
            }
            MouseEventKind::ScrollDown => {
                if self.focus == Focus::Results {
                    let sel = self.table_state.selected().map_or(0, |s| {
                        (s + 3).min(self.results.rows.len().saturating_sub(1))
                    });
                    self.table_state.select(Some(sel));
                }
            }
            _ => {}
        }
        Action::None
    }

    // ── Data loading ──────────────────────────────────────────

    async fn run_meta_command(&mut self, db: &mut PotatoDB, cmd: &str) {
        let start = Instant::now();
        let trimmed = cmd.trim();

        let sql_translation = match trimmed {
            "\\dt" => Some(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = 'public' ORDER BY table_name"
                    .to_string(),
            ),
            "\\dv" => {
                let mut views = db.view_names();
                views.sort();
                self.set_meta_results(
                    vec!["view_name".into()],
                    views.into_iter().map(|v| vec![v]).collect(),
                    start,
                );
                return;
            }
            "\\di" => {
                let mut idx = db.indexes();
                idx.sort();
                self.set_meta_results(
                    vec!["index_name".into(), "table_name".into()],
                    idx.into_iter().map(|(n, t)| vec![n, t]).collect(),
                    start,
                );
                return;
            }
            "\\ds" => {
                let mut seqs = db.sequence_names();
                seqs.sort();
                self.set_meta_results(
                    vec!["sequence_name".into()],
                    seqs.into_iter().map(|s| vec![s]).collect(),
                    start,
                );
                return;
            }
            "\\df" => {
                let mut fns = db.function_names();
                fns.sort();
                self.set_meta_results(
                    vec!["function_name".into()],
                    fns.into_iter().map(|f| vec![f]).collect(),
                    start,
                );
                return;
            }
            "\\du" => {
                let mut users = db.user_info();
                users.sort_by(|a, b| a.0.cmp(&b.0));
                self.set_meta_results(
                    vec!["user_name".into(), "roles".into()],
                    users
                        .into_iter()
                        .map(|(u, r)| vec![u, r.join(", ")])
                        .collect(),
                    start,
                );
                return;
            }
            s if s.starts_with("\\d ") => {
                let table = s[3..].trim().trim_matches('"');
                Some(format!("DESCRIBE {table}"))
            }
            _ => {
                self.results = QueryResults::from_error(&format!("Unknown command: {cmd}"));
                self.status = "Error".into();
                self.elapsed = format_elapsed(start.elapsed());
                return;
            }
        };

        if let Some(translated) = sql_translation {
            self.execute_sql(db, &translated).await;
        }
    }

    fn set_meta_results(&mut self, headers: Vec<String>, rows: Vec<Vec<String>>, start: Instant) {
        let count = rows.len();
        let widths: Vec<u16> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| {
                let max_data = rows
                    .iter()
                    .map(|r| r.get(i).map_or(0, String::len))
                    .max()
                    .unwrap_or(0);
                h.len().max(max_data) as u16
            })
            .collect();
        self.results = QueryResults {
            headers,
            rows,
            column_widths: widths,
            message: None,
        };
        self.status = format!("{count} row(s)");
        self.elapsed = format_elapsed(start.elapsed());
        self.table_state = TableState::default();
        if count > 0 {
            self.table_state.select(Some(0));
        }
    }

    async fn run_query(&mut self, db: &mut PotatoDB, sql: &str) {
        if sql.trim().starts_with('\\') {
            self.run_meta_command(db, sql).await;
            return;
        }
        self.execute_sql(db, sql).await;
    }

    async fn execute_sql(&mut self, db: &mut PotatoDB, sql: &str) {
        self.spinner_state = self.spinner_state.wrapping_add(1);
        let start = Instant::now();
        match db.execute(sql).await {
            Ok(QueryResult::Records(batches)) => {
                let count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
                self.results = QueryResults::from_batches(&batches);
                self.status = format!("{count} rows");
                self.elapsed = format_elapsed(start.elapsed());
                self.table_state = TableState::default();
                if count > 0 {
                    self.table_state.select(Some(0));
                }
            }
            Ok(QueryResult::Message(msg)) => {
                self.status.clone_from(&msg);
                self.elapsed = format_elapsed(start.elapsed());
                self.set_toast(msg.clone());
                self.results = QueryResults::from_message(msg);
            }
            Err(e) => {
                self.results = QueryResults::from_error(&e.to_string());
                self.status = "Query failed".into();
                self.elapsed = format_elapsed(start.elapsed());
            }
        }
        self.h_scroll = 0;
        self.load_tables(db).await;
    }

    async fn load_tables(&mut self, db: &mut PotatoDB) {
        let sql = "SELECT table_name FROM information_schema.tables \
                   WHERE table_schema = 'public' \
                   ORDER BY table_name";
        if let Ok(QueryResult::Records(batches)) = db.execute(sql).await {
            self.tables.clear();
            for batch in &batches {
                let col_idx = batch
                    .schema()
                    .fields()
                    .iter()
                    .position(|f| f.name() == "table_name")
                    .unwrap_or(0);
                let col = batch.column(col_idx);
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            self.tables.push(arr.value(i).to_string());
                        }
                    }
                }
            }
            self.tables.sort();
            if self.selected_table >= self.tables.len() {
                self.selected_table = self.tables.len().saturating_sub(1);
            }
        }
    }

    async fn load_schema(&mut self, db: &mut PotatoDB, table_name: &str) {
        if self.schema_cache.contains_key(table_name) {
            return;
        }
        let sql = format!(
            "SELECT column_name, data_type \
             FROM information_schema.columns \
             WHERE table_name = '{table_name}' \
             ORDER BY ordinal_position"
        );
        if let Ok(QueryResult::Records(batches)) = db.execute(&sql).await {
            let mut cols = Vec::new();
            for batch in &batches {
                let name_col = batch.column(0);
                let type_col = batch.column(1);
                if let (Some(names), Some(types)) = (
                    name_col.as_any().downcast_ref::<StringArray>(),
                    type_col.as_any().downcast_ref::<StringArray>(),
                ) {
                    for i in 0..names.len() {
                        if !names.is_null(i) && !types.is_null(i) {
                            cols.push((names.value(i).to_string(), types.value(i).to_string()));
                        }
                    }
                }
            }
            self.schema_cache.insert(table_name.to_string(), cols);
        }
    }
}

fn export_sql(results: &QueryResults, table: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let cols = results.headers.join(", ");
    for row in &results.rows {
        let vals: Vec<String> = row
            .iter()
            .map(|v| {
                if v.eq_ignore_ascii_case("null") {
                    "NULL".to_string()
                } else {
                    format!("'{}'", v.replace('\'', "''"))
                }
            })
            .collect();
        let _ = writeln!(
            out,
            "INSERT INTO \"{table}\" ({cols}) VALUES ({});",
            vals.join(", ")
        );
    }
    out
}

fn dirs_history_path() -> Option<PathBuf> {
    std::env::var("HOME")
        .ok()
        .map(|h| PathBuf::from(h).join(".potatodb_history"))
}

fn format_elapsed(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms < 1000.0 {
        format!("{ms:.1}ms")
    } else {
        format!("{:.2}s", d.as_secs_f64())
    }
}

// ── SQL syntax highlighting ────────────────────────────────────

fn highlight_sql_line<'a>(line: &'a str, t: &Theme) -> Line<'a> {
    if line.is_empty() {
        return Line::from("");
    }
    let mut spans: Vec<Span<'a>> = Vec::new();
    let bytes = line.as_bytes();
    let len = bytes.len();
    let mut pos = 0;

    while pos < len {
        let ch = bytes[pos];
        if ch == b'\'' {
            let start = pos;
            pos += 1;
            while pos < len {
                if bytes[pos] == b'\'' {
                    pos += 1;
                    if pos < len && bytes[pos] == b'\'' {
                        pos += 1;
                    } else {
                        break;
                    }
                } else {
                    pos += 1;
                }
            }
            spans.push(Span::styled(
                &line[start..pos],
                Style::default().fg(t.string_lit),
            ));
        } else if ch == b'-' && pos + 1 < len && bytes[pos + 1] == b'-' {
            spans.push(Span::styled(
                &line[pos..],
                Style::default()
                    .fg(t.overlay0)
                    .add_modifier(Modifier::ITALIC),
            ));
            pos = len;
        } else if ch.is_ascii_whitespace() {
            let start = pos;
            while pos < len && bytes[pos].is_ascii_whitespace() {
                pos += 1;
            }
            spans.push(Span::raw(&line[start..pos]));
        } else if ch.is_ascii_digit()
            || (ch == b'-' && pos + 1 < len && bytes[pos + 1].is_ascii_digit())
        {
            let start = pos;
            if ch == b'-' {
                pos += 1;
            }
            while pos < len && (bytes[pos].is_ascii_digit() || bytes[pos] == b'.') {
                pos += 1;
            }
            spans.push(Span::styled(
                &line[start..pos],
                Style::default().fg(t.number),
            ));
        } else if ch.is_ascii_alphanumeric() || ch == b'_' {
            let start = pos;
            while pos < len && (bytes[pos].is_ascii_alphanumeric() || bytes[pos] == b'_') {
                pos += 1;
            }
            let word = &line[start..pos];
            if SQL_KEYWORDS.contains(word.to_uppercase().as_str()) {
                spans.push(Span::styled(
                    word,
                    Style::default().fg(t.keyword).add_modifier(Modifier::BOLD),
                ));
            } else {
                spans.push(Span::styled(word, Style::default().fg(t.text)));
            }
        } else {
            let start = pos;
            pos += 1;
            spans.push(Span::styled(
                &line[start..pos],
                Style::default().fg(t.overlay0),
            ));
        }
    }
    Line::from(spans)
}

// ── Cell styling ──────────────────────────────────────────────

fn style_cell(value: &str, t: &Theme) -> Style {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("null") {
        return Style::default()
            .fg(t.overlay0)
            .add_modifier(Modifier::ITALIC);
    }
    if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
        return Style::default().fg(t.success).add_modifier(Modifier::BOLD);
    }
    if !trimmed.is_empty()
        && trimmed
            .bytes()
            .all(|b| b.is_ascii_digit() || b == b'.' || b == b'-')
        && trimmed.bytes().any(|b| b.is_ascii_digit())
    {
        return Style::default().fg(t.number);
    }
    Style::default().fg(t.text)
}

// ── Rendering ──────────────────────────────────────────────────

fn draw(frame: &mut Frame, app: &mut App) {
    let t = &app.theme;
    frame.render_widget(
        Block::default().style(Style::default().bg(t.base)),
        frame.area(),
    );

    let term_width = frame.area().width;
    let show_sidebar = term_width >= 60;

    let input_height = (app.input_lines.len() as u16 + 2).clamp(3, 8);

    let [title_area, body_area, query_area, status_area] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Min(4),
        Constraint::Length(input_height),
        Constraint::Length(1),
    ])
    .areas(frame.area());

    draw_title_bar(frame, app, title_area);

    if show_sidebar {
        let [sidebar_area, main_area] =
            Layout::horizontal([Constraint::Length(24), Constraint::Min(20)]).areas(body_area);
        app.sidebar_area = sidebar_area;
        app.results_area = main_area;
        draw_sidebar(frame, app, sidebar_area);
        draw_results(frame, app, main_area);
    } else {
        app.sidebar_area = Rect::default();
        app.results_area = body_area;
        draw_results(frame, app, body_area);
    }

    app.query_area = query_area;
    draw_query_input(frame, app, query_area);
    draw_status_bar(frame, app, status_area);

    match app.popup {
        Popup::Help => draw_help_popup(frame, app),
        Popup::Export => draw_export_popup(frame, app),
        Popup::None => {}
    }

    if app.focus == Focus::Query {
        draw_completion_popup(frame, app);
    }
}

fn draw_title_bar(frame: &mut Frame, app: &App, area: Rect) {
    let t = &app.theme;
    let label = " pdb ";
    let path_text = format!("PotatoDB: {}", app.data_url);
    let theme_label = format!("[{}]", t.name);
    let left_len = label.len() + 2 + path_text.len();
    let pad = (area.width as usize).saturating_sub(left_len + theme_label.len() + 1);

    let line = Line::from(vec![
        Span::styled(
            label,
            Style::default()
                .fg(t.crust)
                .bg(t.accent2)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled("  ", Style::default()),
        Span::styled(&path_text, Style::default().fg(t.subtext0)),
        Span::styled(format!("{:>width$}", "", width = pad), Style::default()),
        Span::styled(theme_label, Style::default().fg(t.overlay0)),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(t.mantle)),
        area,
    );
}

fn draw_sidebar(frame: &mut Frame, app: &App, area: Rect) {
    let t = &app.theme;
    let focused = app.focus == Focus::Sidebar;
    let border_color = if focused { t.accent2 } else { t.surface1 };
    let count = app.tables.len();

    let schema = app
        .tables
        .get(app.selected_table)
        .and_then(|name| app.schema_cache.get(name));
    let has_schema = schema.is_some_and(|s| !s.is_empty());

    let [table_area, schema_area] = if has_schema {
        Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(area)
    } else {
        Layout::vertical([Constraint::Percentage(100), Constraint::Length(0)]).areas(area)
    };

    let table_block = Block::bordered()
        .border_type(BorderType::Plain)
        .title(Line::from(vec![Span::styled(
            format!(" Tables ({count}) "),
            Style::default()
                .fg(if focused { t.accent2 } else { t.subtext0 })
                .add_modifier(Modifier::BOLD),
        )]))
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(t.base));

    let items: Vec<ListItem> = if app.tables.is_empty() {
        vec![ListItem::new(Line::styled(
            "  (no tables)",
            Style::default().fg(t.overlay0),
        ))]
    } else {
        app.tables
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let selected = i == app.selected_table;
                let marker = if selected { "► " } else { "  " };
                let style = if selected {
                    Style::default().fg(t.accent2).add_modifier(Modifier::BOLD)
                } else {
                    Style::default().fg(t.subtext1)
                };
                ListItem::new(Line::from(vec![
                    Span::styled(
                        marker,
                        if selected {
                            Style::default().fg(t.accent2)
                        } else {
                            Style::default().fg(t.surface1)
                        },
                    ),
                    Span::styled(name.as_str(), style),
                ]))
            })
            .collect()
    };
    frame.render_widget(List::new(items).block(table_block), table_area);

    if let Some(cols) = schema {
        let schema_block = Block::bordered()
            .border_type(BorderType::Plain)
            .title(Line::from(vec![Span::styled(
                " Schema ",
                Style::default()
                    .fg(if focused { t.accent2 } else { t.subtext0 })
                    .add_modifier(Modifier::BOLD),
            )]))
            .border_style(Style::default().fg(border_color))
            .style(Style::default().bg(t.base));

        let schema_items: Vec<ListItem> = cols
            .iter()
            .map(|(name, dtype)| {
                ListItem::new(Line::from(vec![
                    Span::styled(format!(" {name}"), Style::default().fg(t.accent1)),
                    Span::styled(" : ", Style::default().fg(t.overlay0)),
                    Span::styled(dtype.as_str(), Style::default().fg(t.subtext0)),
                ]))
            })
            .collect();
        frame.render_widget(List::new(schema_items).block(schema_block), schema_area);
    }
}

fn draw_results(frame: &mut Frame, app: &mut App, area: Rect) {
    let t = &app.theme;
    let focused = app.focus == Focus::Results;
    let border_color = if focused { t.accent2 } else { t.surface1 };
    let row_count = app.results.row_count();

    let title_text = if let Some(name) = app.tables.get(app.selected_table) {
        if row_count > 0 {
            format!(" {name} ({row_count} rows) ")
        } else {
            format!(" {name} ")
        }
    } else if row_count > 0 {
        format!(" Results ({row_count} rows) ")
    } else {
        " Results ".to_string()
    };

    let block = Block::bordered()
        .border_type(BorderType::Plain)
        .title(Line::from(vec![Span::styled(
            title_text,
            Style::default()
                .fg(if focused { t.accent2 } else { t.subtext0 })
                .add_modifier(Modifier::BOLD),
        )]))
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(t.base));

    if let Some(msg) = &app.results.message {
        let msg_style = if msg.starts_with("ERROR:") {
            Style::default().fg(t.error).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(t.success)
        };
        frame.render_widget(
            Paragraph::new(Line::styled(format!("  {msg}"), msg_style)).block(block),
            area,
        );
        return;
    }

    if app.results.is_empty() {
        frame.render_widget(
            Paragraph::new(Line::styled(
                "  Select a table or run a query (end with ; or press F5)",
                Style::default()
                    .fg(t.overlay0)
                    .add_modifier(Modifier::ITALIC),
            ))
            .block(block),
            area,
        );
        return;
    }

    let visible_headers: Vec<&String> = app.results.headers.iter().skip(app.h_scroll).collect();
    let visible_widths: Vec<Constraint> = app
        .results
        .column_widths
        .iter()
        .skip(app.h_scroll)
        .map(|w| Constraint::Min((*w).max(4) + 2))
        .collect();

    let header_cells: Vec<Cell> = visible_headers
        .iter()
        .map(|h| {
            Cell::from(h.as_str())
                .style(Style::default().fg(t.accent1).add_modifier(Modifier::BOLD))
        })
        .collect();
    let header = Row::new(header_cells)
        .style(Style::default().bg(t.mantle))
        .height(1);

    let data_rows: Vec<Row> = app
        .results
        .rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let bg = if i % 2 == 0 { t.base } else { t.surface0 };
            let cells: Vec<Cell> = row
                .iter()
                .skip(app.h_scroll)
                .map(|val| Cell::from(val.as_str()).style(style_cell(val, t)))
                .collect();
            Row::new(cells).style(Style::default().bg(bg))
        })
        .collect();

    let table = Table::new(data_rows, &visible_widths)
        .header(header)
        .block(block)
        .row_highlight_style(Style::default().bg(t.surface1).add_modifier(Modifier::BOLD))
        .highlight_spacing(HighlightSpacing::Always);

    frame.render_stateful_widget(table, area, &mut app.table_state);

    let inner_height = area.height.saturating_sub(3) as usize;
    if app.results.rows.len() > inner_height {
        let mut sb = ScrollbarState::new(app.results.rows.len())
            .position(app.table_state.selected().unwrap_or(0))
            .viewport_content_length(inner_height);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .thumb_style(Style::default().fg(t.accent2))
                .track_style(Style::default().fg(t.surface0)),
            area,
            &mut sb,
        );
    }
}

fn draw_query_input(frame: &mut Frame, app: &App, area: Rect) {
    let t = &app.theme;
    let focused = app.focus == Focus::Query;
    let border_color = if focused { t.accent2 } else { t.surface1 };

    let block = Block::bordered()
        .border_type(BorderType::Plain)
        .title(Line::from(vec![Span::styled(
            format!(" SQL (history: {}) [; or F5 to run] ", app.history.len()),
            Style::default()
                .fg(if focused { t.accent2 } else { t.subtext0 })
                .add_modifier(Modifier::BOLD),
        )]))
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(t.base));

    let is_empty = app.input_lines.len() == 1 && app.input_lines[0].is_empty();
    if is_empty && focused {
        frame.render_widget(
            Paragraph::new(Line::styled(
                "Type a SQL query here...",
                Style::default()
                    .fg(t.overlay0)
                    .add_modifier(Modifier::ITALIC),
            ))
            .block(block),
            area,
        );
    } else {
        let lines: Vec<Line> = app
            .input_lines
            .iter()
            .map(|l| highlight_sql_line(l, t))
            .collect();
        frame.render_widget(Paragraph::new(lines).block(block), area);
    }

    if focused {
        frame.set_cursor_position(Position::new(
            area.x + 1 + app.cursor_col as u16,
            area.y + 1 + app.cursor_row as u16,
        ));
    }
}

fn draw_status_bar(frame: &mut Frame, app: &App, area: Rect) {
    let t = &app.theme;
    let left = if let Some(toast) = &app.toast {
        toast.message.clone()
    } else if app.status.is_empty() {
        String::new()
    } else {
        format!("{} ({})", app.status, app.elapsed)
    };

    let right = "F1:help Tab:focus Ctrl+S:export q:quit";
    let pad = (area.width as usize).saturating_sub(left.len() + right.len() + 2);

    let left_style = if app.toast.is_some() {
        Style::default().fg(t.success).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(t.subtext0).add_modifier(Modifier::BOLD)
    };

    let line = Line::from(vec![
        Span::styled(format!(" {left}"), left_style),
        Span::styled(format!("{:>width$}", "", width = pad), Style::default()),
        Span::styled(format!("{right} "), Style::default().fg(t.overlay0)),
    ]);
    frame.render_widget(
        Paragraph::new(line).style(Style::default().bg(t.mantle)),
        area,
    );
}

// ── Popups ────────────────────────────────────────────────────

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect::new(x, y, width.min(area.width), height.min(area.height))
}

fn draw_help_popup(frame: &mut Frame, app: &App) {
    let t = &app.theme;
    let area = centered_rect(58, 24, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::bordered()
        .border_type(BorderType::Double)
        .title(Line::from(vec![Span::styled(
            " Keybindings ",
            Style::default().fg(t.accent2).add_modifier(Modifier::BOLD),
        )]))
        .border_style(Style::default().fg(t.accent2))
        .style(Style::default().bg(t.mantle));

    let help_lines = vec![
        Line::styled(
            "  GLOBAL",
            Style::default().fg(t.accent1).add_modifier(Modifier::BOLD),
        ),
        Line::styled(
            "  Tab / Shift+Tab    Switch focus",
            Style::default().fg(t.text),
        ),
        Line::styled("  Ctrl+C / Ctrl+Q    Quit", Style::default().fg(t.text)),
        Line::styled(
            "  F1 / ?             Toggle this help",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Ctrl+S             Export results",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Ctrl+L             Clear results",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Esc                Dismiss popup",
            Style::default().fg(t.text),
        ),
        Line::styled("", Style::default()),
        Line::styled(
            "  QUERY INPUT",
            Style::default().fg(t.accent1).add_modifier(Modifier::BOLD),
        ),
        Line::styled(
            "  ; + Enter / F5     Execute query",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Ctrl+J / Alt+Enter Execute query (alt)",
            Style::default().fg(t.text),
        ),
        Line::styled("  Enter              New line", Style::default().fg(t.text)),
        Line::styled(
            "  Up/Down            History / move cursor",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Ctrl+U             Clear to start of line",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Ctrl+K             Clear to end of line",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Ctrl+W             Clear all input",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Tab                Autocomplete (cycle)",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Ctrl+D             Describe selected table",
            Style::default().fg(t.text),
        ),
        Line::styled("", Style::default()),
        Line::styled(
            "  RESULTS",
            Style::default().fg(t.accent1).add_modifier(Modifier::BOLD),
        ),
        Line::styled(
            "  Up/Down            Select row",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  Left/Right         Horizontal scroll",
            Style::default().fg(t.text),
        ),
        Line::styled(
            "  PgUp/PgDn          Page scroll",
            Style::default().fg(t.text),
        ),
    ];

    frame.render_widget(Paragraph::new(help_lines).block(block), area);
}

fn draw_export_popup(frame: &mut Frame, app: &App) {
    let t = &app.theme;
    let area = centered_rect(50, 9, frame.area());
    frame.render_widget(Clear, area);

    let block = Block::bordered()
        .border_type(BorderType::Double)
        .title(Line::from(vec![Span::styled(
            " Export Results ",
            Style::default().fg(t.accent2).add_modifier(Modifier::BOLD),
        )]))
        .border_style(Style::default().fg(t.accent2))
        .style(Style::default().bg(t.mantle));

    let format_spans: Vec<Span> = EXPORT_FORMATS
        .iter()
        .enumerate()
        .flat_map(|(i, f)| {
            let style = if i == app.export_format {
                Style::default()
                    .fg(t.accent2)
                    .add_modifier(Modifier::BOLD)
                    .add_modifier(Modifier::UNDERLINED)
            } else {
                Style::default().fg(t.subtext1)
            };
            vec![Span::styled(format!(" {f} "), style), Span::raw(" ")]
        })
        .collect();

    let lines = vec![
        Line::from(""),
        Line::from(vec![Span::styled(
            "  Format: ",
            Style::default().fg(t.text),
        )]),
        Line::from(format_spans),
        Line::from(vec![
            Span::styled("  Path:   ", Style::default().fg(t.text)),
            Span::styled(app.export_path.as_str(), Style::default().fg(t.accent1)),
            Span::styled("_", Style::default().fg(t.accent2)),
        ]),
        Line::from(""),
        Line::styled(
            "  Tab: format  Enter: save  Esc: cancel",
            Style::default().fg(t.overlay0),
        ),
    ];

    frame.render_widget(Paragraph::new(lines).block(block), area);
}

fn draw_completion_popup(frame: &mut Frame, app: &App) {
    let Some(comp) = &app.completion else {
        return;
    };
    if comp.candidates.is_empty() {
        return;
    }
    let t = &app.theme;
    let max_show = 8.min(comp.candidates.len());
    let start = if comp.index >= max_show {
        comp.index - max_show + 1
    } else {
        0
    };
    let visible: Vec<&String> = comp.candidates.iter().skip(start).take(max_show).collect();

    let width = visible.iter().map(|s| s.len()).max().unwrap_or(10) as u16 + 4;
    let height = visible.len() as u16 + 2;

    let x = app.query_area.x + 1 + comp.start_col as u16;
    let y = app.query_area.y.saturating_sub(height);
    let popup_area = Rect::new(
        x.min(frame.area().width.saturating_sub(width)),
        y,
        width.min(frame.area().width),
        height.min(frame.area().height),
    );

    frame.render_widget(Clear, popup_area);

    let block = Block::bordered()
        .border_type(BorderType::Plain)
        .border_style(Style::default().fg(t.surface1))
        .style(Style::default().bg(t.mantle));

    let items: Vec<ListItem> = visible
        .iter()
        .enumerate()
        .map(|(vi, candidate)| {
            let global_idx = start + vi;
            let style = if global_idx == comp.index {
                Style::default()
                    .fg(t.crust)
                    .bg(t.accent2)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(t.text)
            };
            ListItem::new(Line::styled(format!(" {candidate}"), style))
        })
        .collect();

    frame.render_widget(List::new(items).block(block), popup_area);
}

// ── Main loop ──────────────────────────────────────────────────

/// Starts the full-screen TUI.
///
/// # Errors
///
/// Returns an error if terminal initialization or I/O fails.
pub async fn run(
    db: &mut PotatoDB,
    theme: ThemeChoice,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    ratatui::crossterm::execute!(std::io::stdout(), EnableMouseCapture)?;
    let mut terminal = ratatui::init();
    let result = run_app(&mut terminal, db, theme).await;
    ratatui::restore();
    let _ = ratatui::crossterm::execute!(std::io::stdout(), DisableMouseCapture);
    result
}

async fn run_app(
    terminal: &mut DefaultTerminal,
    db: &mut PotatoDB,
    theme_choice: ThemeChoice,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut app = App::new(db.data_url().to_string(), theme_choice);
    app.load_tables(db).await;

    if let Some(first) = app.tables.first().cloned() {
        app.load_schema(db, &first).await;
        let sql = format!("SELECT * FROM \"{first}\" LIMIT 100");
        app.run_query(db, &sql).await;
    }

    loop {
        let size = terminal.size()?;
        app.visible_height = size.height.saturating_sub(7);
        app.tick_toast();

        terminal.draw(|f| draw(f, &mut app))?;

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press => match app.handle_key(key) {
                    Action::Execute(sql) => {
                        app.status = "Running...".into();
                        terminal.draw(|f| draw(f, &mut app))?;
                        app.run_query(db, &sql).await;
                    }
                    Action::PreviewTable(name) => {
                        app.load_schema(db, &name).await;
                        let sql = format!("SELECT * FROM \"{name}\" LIMIT 100");
                        app.status = "Loading...".into();
                        terminal.draw(|f| draw(f, &mut app))?;
                        app.run_query(db, &sql).await;
                    }
                    Action::Quit => return Ok(()),
                    Action::None => {}
                },
                Event::Mouse(mouse) => {
                    if let Action::PreviewTable(name) = app.handle_mouse(mouse) {
                        app.load_schema(db, &name).await;
                        let sql = format!("SELECT * FROM \"{name}\" LIMIT 100");
                        app.run_query(db, &sql).await;
                    }
                }
                _ => {}
            }
        }
    }
}

// ── Comment stripping ──────────────────────────────────────────

fn strip_sql_comments(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();

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
                    chars.next();
                    while let Some(&c) = chars.peek() {
                        if c == '\n' {
                            result.push(' ');
                            chars.next();
                            break;
                        }
                        chars.next();
                    }
                } else {
                    result.push('-');
                }
            }
            '/' => {
                chars.next();
                if chars.peek() == Some(&'*') {
                    chars.next();
                    let mut depth = 1u32;
                    while depth > 0 {
                        match chars.next() {
                            Some('*') if chars.peek() == Some(&'/') => {
                                chars.next();
                                depth -= 1;
                            }
                            Some('/') if chars.peek() == Some(&'*') => {
                                chars.next();
                                depth += 1;
                            }
                            Some(_) => {}
                            None => break,
                        }
                    }
                    result.push(' ');
                } else {
                    result.push('/');
                }
            }
            _ => {
                result.push(chars.next().unwrap());
            }
        }
    }

    result.trim().to_string()
}
