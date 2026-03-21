#![allow(
    non_camel_case_types,
    unsafe_op_in_unsafe_fn,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::option_if_let_else,
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation
)]
//! C / C++ FFI bindings for the `PotatoDB` engine.
//!
//! Exposes an opaque `potato_db` handle and `potato_result` handle that
//! can be used from any language with C calling convention support.
//! A header-only C++ wrapper (`potatodb.hpp`) provides RAII semantics on
//! top of this C API.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use tokio::runtime::Runtime;

use potatodb_engine::{PotatoDB, QueryResult, QueryResultStream, S3Config};

/// Opaque database handle visible to C.
pub struct potato_db {
    db: PotatoDB,
    rt: Runtime,
    last_error: Option<CString>,
}

/// Tag indicating what kind of result a query produced.
#[repr(C)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum potato_result_kind {
    POTATO_RESULT_RECORDS = 0,
    POTATO_RESULT_MESSAGE = 1,
}

/// Opaque query-result handle visible to C.
pub struct potato_result {
    kind: potato_result_kind,
    batches: Vec<RecordBatch>,
    message: Option<CString>,
    row_count: usize,
    column_names: Vec<CString>,
    display_cache: Option<CString>,
}

/// Column type tag exposed to C callers.
#[repr(C)]
pub enum potato_column_type {
    POTATO_TYPE_NULL = 0,
    POTATO_TYPE_BOOL = 1,
    POTATO_TYPE_INT32 = 2,
    POTATO_TYPE_INT64 = 3,
    POTATO_TYPE_FLOAT = 4,
    POTATO_TYPE_DOUBLE = 5,
    POTATO_TYPE_STRING = 6,
    POTATO_TYPE_DATE = 7,
    POTATO_TYPE_TIMESTAMP = 8,
    POTATO_TYPE_DECIMAL = 9,
    POTATO_TYPE_BINARY = 10,
    POTATO_TYPE_UUID = 11,
    POTATO_TYPE_INTERVAL = 12,
    POTATO_TYPE_ARRAY = 13,
    POTATO_TYPE_JSON = 14,
    POTATO_TYPE_OTHER = 99,
}

/// Opaque string-list handle for C.
pub struct potato_string_list {
    items: Vec<CString>,
}

/// Opaque index-list handle for C.
pub struct potato_index_list {
    names: Vec<CString>,
    tables: Vec<CString>,
}

/// Opaque result-list handle for C (returned by `execute_file`).
pub struct potato_result_list {
    entries: Vec<ResultListEntry>,
}

struct ResultListEntry {
    sql: CString,
    result: Option<potato_result>,
    error: Option<CString>,
}

/// Opaque query-log handle for C.
pub struct potato_query_log {
    entries: Vec<QueryLogCEntry>,
}

struct QueryLogCEntry {
    sql: CString,
    duration_ms: u64,
    rows: usize,
}

/// Opaque streaming result handle for C.
///
/// # Safety
///
/// The `potato_db` that created this stream must remain alive (not closed)
/// for the entire lifetime of the stream handle.
pub struct potato_stream {
    rt: *const Runtime,
    inner: StreamInner,
}

enum StreamInner {
    Stream(futures::stream::BoxStream<'static, Result<RecordBatch, arrow::error::ArrowError>>),
    Message(Option<CString>),
    Exhausted,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn set_error(db: &mut potato_db, msg: String) {
    db.last_error = CString::new(msg).ok();
}

unsafe fn cstr_to_str<'a>(p: *const c_char) -> Option<&'a str> {
    if p.is_null() {
        return None;
    }
    CStr::from_ptr(p).to_str().ok()
}

fn collect_column_names(batches: &[RecordBatch]) -> Vec<CString> {
    let Some(first) = batches.first() else {
        return Vec::new();
    };
    first
        .schema()
        .fields()
        .iter()
        .filter_map(|f| CString::new(f.name().as_str()).ok())
        .collect()
}

fn query_result_to_handle(result: QueryResult) -> *mut potato_result {
    match result {
        QueryResult::Records(batches) => {
            let row_count = batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            let column_names = collect_column_names(&batches);
            Box::into_raw(Box::new(potato_result {
                kind: potato_result_kind::POTATO_RESULT_RECORDS,
                batches,
                message: None,
                row_count,
                column_names,
                display_cache: None,
            }))
        }
        QueryResult::Message(msg) => {
            let c_msg = CString::new(msg).ok();
            Box::into_raw(Box::new(potato_result {
                kind: potato_result_kind::POTATO_RESULT_MESSAGE,
                batches: Vec::new(),
                message: c_msg,
                row_count: 0,
                column_names: Vec::new(),
                display_cache: None,
            }))
        }
    }
}

/// Maps a global row index across multiple batches to a (batch, `local_row`) pair.
fn resolve_row(batches: &[RecordBatch], row: usize) -> Option<(&RecordBatch, usize)> {
    let mut offset = 0usize;
    for batch in batches {
        let n = batch.num_rows();
        if row < offset + n {
            return Some((batch, row - offset));
        }
        offset += n;
    }
    None
}

/// Converts any Arrow array value at the given index to its string
/// representation using Arrow's display formatting.
fn arrow_value_to_string(arr: &dyn Array, index: usize) -> Option<String> {
    use arrow::util::display::ArrayFormatter;
    use arrow::util::display::FormatOptions;

    if arr.is_null(index) {
        return None;
    }
    let options = FormatOptions::default();
    let formatter = ArrayFormatter::try_new(arr, &options).ok()?;
    Some(formatter.value(index).to_string())
}

// ---------------------------------------------------------------------------
// Database lifecycle
// ---------------------------------------------------------------------------

/// Opens a database at `data_dir` (local path, `s3://` URL, or in-memory
/// `:memory:` / `memory://...`).
///
/// Returns a handle on success, or `NULL` on failure. When S3 is used,
/// pass endpoint / region / `allow_http`; set pointers to `NULL` / `false`
/// for defaults. In-memory URLs ignore S3 parameters.
///
/// # Safety
/// Any non-NULL pointer argument must be valid for reads for the duration of
/// this call. Handle pointers returned by this API must later be released with
/// their corresponding free/close function and not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_open(
    data_dir: *const c_char,
    s3_endpoint: *const c_char,
    s3_region: *const c_char,
    s3_allow_http: bool,
    wal_dir: *const c_char,
) -> *mut potato_db {
    let data_dir = match cstr_to_str(data_dir) {
        Some(s) => s.to_string(),
        None => return ptr::null_mut(),
    };

    let s3_config = if data_dir.starts_with("s3://") {
        Some(S3Config {
            endpoint: cstr_to_str(s3_endpoint).map(String::from),
            region: cstr_to_str(s3_region).map(String::from),
            allow_http: s3_allow_http,
            wal_dir: cstr_to_str(wal_dir).map(String::from),
        })
    } else {
        None
    };

    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return ptr::null_mut(),
    };

    let db = match rt.block_on(PotatoDB::new(data_dir, s3_config)) {
        Ok(db) => db,
        Err(_) => return ptr::null_mut(),
    };

    Box::into_raw(Box::new(potato_db {
        db,
        rt,
        last_error: None,
    }))
}

/// Opens a database (convenience wrapper — no S3 parameters).
///
/// `data_dir` may be a filesystem path, `:memory:` / `memory://...`, etc.
///
/// # Safety
/// Any non-NULL pointer argument must be valid for reads for the duration of
/// this call. Handle pointers returned by this API must later be released with
/// their corresponding free/close function and not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_open_local(data_dir: *const c_char) -> *mut potato_db {
    potato_open(data_dir, ptr::null(), ptr::null(), false, ptr::null())
}

/// Closes the database and frees all associated memory.
///
/// # Safety
/// `db` must either be NULL or a pointer previously returned by
/// `potato_open`/`potato_open_local` that has not already been closed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_close(db: *mut potato_db) {
    if !db.is_null() {
        drop(Box::from_raw(db));
    }
}

/// Returns the last error message, or `NULL` if the last operation succeeded.
///
/// The returned pointer is valid until the next call on this handle.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library that
/// remains alive for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_last_error(db: *const potato_db) -> *const c_char {
    if db.is_null() {
        return ptr::null();
    }
    match (*db).last_error.as_ref() {
        Some(s) => s.as_ptr(),
        None => ptr::null(),
    }
}

// ---------------------------------------------------------------------------
// Metadata introspection
// ---------------------------------------------------------------------------

/// Returns whether the database is inside an explicit `BEGIN` transaction.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn potato_in_transaction(db: *const potato_db) -> bool {
    if db.is_null() {
        return false;
    }
    (*db).db.in_transaction()
}

/// Returns the data URL / directory path for the database.
///
/// The caller must free the returned string with `potato_string_free`.
/// Returns NULL if `db` is NULL.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_data_url(db: *const potato_db) -> *mut c_char {
    if db.is_null() {
        return ptr::null_mut();
    }
    let url = (*db).db.data_url();
    CString::new(url)
        .map(CString::into_raw)
        .unwrap_or(ptr::null_mut())
}

/// Returns a list of all table names in the database.
///
/// The caller must free the result with `potato_string_list_free`.
/// Returns NULL if `db` is NULL.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_table_names(db: *const potato_db) -> *mut potato_string_list {
    if db.is_null() {
        return ptr::null_mut();
    }
    let names = (*db).db.table_names();
    let items = names
        .into_iter()
        .filter_map(|n| CString::new(n).ok())
        .collect();
    Box::into_raw(Box::new(potato_string_list { items }))
}

/// Returns a list of column names for the given table.
///
/// The caller must free the result with `potato_string_list_free`.
/// Returns NULL if `db` or `table_name` is NULL.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library, and
/// `table_name` must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_table_columns(
    db: *const potato_db,
    table_name: *const c_char,
) -> *mut potato_string_list {
    if db.is_null() {
        return ptr::null_mut();
    }
    let table_name = match cstr_to_str(table_name) {
        Some(s) => s,
        None => return ptr::null_mut(),
    };
    let cols = (*db).db.table_columns(table_name);
    let items = cols
        .into_iter()
        .filter_map(|c| CString::new(c).ok())
        .collect();
    Box::into_raw(Box::new(potato_string_list { items }))
}

/// Returns a list of all view names in the database.
///
/// The caller must free the result with `potato_string_list_free`.
/// Returns NULL if `db` is NULL.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_view_names(db: *const potato_db) -> *mut potato_string_list {
    if db.is_null() {
        return ptr::null_mut();
    }
    let names = (*db).db.view_names();
    let items = names
        .into_iter()
        .filter_map(|n| CString::new(n).ok())
        .collect();
    Box::into_raw(Box::new(potato_string_list { items }))
}

/// Returns a list of all user-defined SQL function names.
///
/// The caller must free the result with `potato_string_list_free`.
/// Returns NULL if `db` is NULL.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_function_names(db: *const potato_db) -> *mut potato_string_list {
    if db.is_null() {
        return ptr::null_mut();
    }
    let names = (*db).db.function_names();
    let items = names
        .into_iter()
        .filter_map(|n| CString::new(n).ok())
        .collect();
    Box::into_raw(Box::new(potato_string_list { items }))
}

/// Returns a list of all indexes as `(index_name, table_name)` pairs.
///
/// The caller must free the result with `potato_index_list_free`.
/// Returns NULL if `db` is NULL.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_indexes(db: *const potato_db) -> *mut potato_index_list {
    if db.is_null() {
        return ptr::null_mut();
    }
    let pairs = (*db).db.indexes();
    let mut names = Vec::with_capacity(pairs.len());
    let mut tables = Vec::with_capacity(pairs.len());
    for (n, t) in pairs {
        if let (Ok(cn), Ok(ct)) = (CString::new(n), CString::new(t)) {
            names.push(cn);
            tables.push(ct);
        }
    }
    Box::into_raw(Box::new(potato_index_list { names, tables }))
}

// ---------------------------------------------------------------------------
// String-list accessors
// ---------------------------------------------------------------------------

/// Returns the number of strings in the list.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn potato_string_list_count(list: *const potato_string_list) -> usize {
    if list.is_null() {
        return 0;
    }
    (*list).items.len()
}

/// Returns the string at `index`, or NULL if out of range.
///
/// The returned pointer is valid for the lifetime of the list handle.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_string_list_get(
    list: *const potato_string_list,
    index: usize,
) -> *const c_char {
    if list.is_null() {
        return ptr::null();
    }
    let list = &*list;
    if index >= list.items.len() {
        return ptr::null();
    }
    list.items[index].as_ptr()
}

/// Frees a string list.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library
/// that has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_string_list_free(list: *mut potato_string_list) {
    if !list.is_null() {
        drop(Box::from_raw(list));
    }
}

// ---------------------------------------------------------------------------
// Index-list accessors
// ---------------------------------------------------------------------------

/// Returns the number of indexes in the list.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn potato_index_list_count(list: *const potato_index_list) -> usize {
    if list.is_null() {
        return 0;
    }
    (*list).names.len()
}

/// Returns the index name at position `index`, or NULL if out of range.
///
/// The returned pointer is valid for the lifetime of the list handle.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_index_list_name(
    list: *const potato_index_list,
    index: usize,
) -> *const c_char {
    if list.is_null() {
        return ptr::null();
    }
    let list = &*list;
    if index >= list.names.len() {
        return ptr::null();
    }
    list.names[index].as_ptr()
}

/// Returns the table name for the index at position `index`, or NULL
/// if out of range.
///
/// The returned pointer is valid for the lifetime of the list handle.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_index_list_table(
    list: *const potato_index_list,
    index: usize,
) -> *const c_char {
    if list.is_null() {
        return ptr::null();
    }
    let list = &*list;
    if index >= list.tables.len() {
        return ptr::null();
    }
    list.tables[index].as_ptr()
}

/// Frees an index list.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library
/// that has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_index_list_free(list: *mut potato_index_list) {
    if !list.is_null() {
        drop(Box::from_raw(list));
    }
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

/// Executes a SQL statement and returns a result handle.
///
/// Returns `NULL` on error -- call `potato_last_error` for details.
/// The caller must free the result with `potato_result_free`.
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and `sql`
/// must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_execute(
    db: *mut potato_db,
    sql: *const c_char,
) -> *mut potato_result {
    if db.is_null() {
        return ptr::null_mut();
    }
    let db = &mut *db;
    db.last_error = None;

    let sql = if let Some(s) = cstr_to_str(sql) {
        s
    } else {
        set_error(db, "NULL sql pointer".into());
        return ptr::null_mut();
    };

    match db.rt.block_on(db.db.execute(sql)) {
        Ok(result) => query_result_to_handle(result),
        Err(e) => {
            set_error(db, e.to_string());
            ptr::null_mut()
        }
    }
}

/// Executes a read-only SQL statement and returns a result handle.
///
/// Returns `NULL` on error -- call `potato_last_error` for details.
/// The caller must free the result with `potato_result_free`.
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and `sql`
/// must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_execute_readonly(
    db: *mut potato_db,
    sql: *const c_char,
) -> *mut potato_result {
    if db.is_null() {
        return ptr::null_mut();
    }
    let db = &mut *db;
    db.last_error = None;

    let sql = if let Some(s) = cstr_to_str(sql) {
        s
    } else {
        set_error(db, "NULL sql pointer".into());
        return ptr::null_mut();
    };

    match db.rt.block_on(db.db.execute_readonly(sql)) {
        Ok(result) => query_result_to_handle(result),
        Err(e) => {
            set_error(db, e.to_string());
            ptr::null_mut()
        }
    }
}

// ---------------------------------------------------------------------------
// Execute file
// ---------------------------------------------------------------------------

/// Executes all SQL statements from a `.sql` file.
///
/// Returns a result-list handle on success, NULL on error (e.g. file
/// not found). Individual statement errors are accessible through the
/// `potato_result_list_error` accessor.
///
/// The caller must free the result with `potato_result_list_free`.
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and
/// `path` must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_execute_file(
    db: *mut potato_db,
    path: *const c_char,
    continue_on_error: bool,
) -> *mut potato_result_list {
    if db.is_null() {
        return ptr::null_mut();
    }
    let db = &mut *db;
    db.last_error = None;

    let path = if let Some(s) = cstr_to_str(path) {
        s
    } else {
        set_error(db, "NULL path pointer".into());
        return ptr::null_mut();
    };

    match db.rt.block_on(db.db.execute_file(path, continue_on_error)) {
        Ok(results) => {
            let entries = results
                .into_iter()
                .map(|(sql, res)| {
                    let c_sql = CString::new(sql).unwrap_or_default();
                    match res {
                        Ok(qr) => {
                            let handle = query_result_to_handle(qr);
                            ResultListEntry {
                                sql: c_sql,
                                result: Some(*Box::from_raw(handle)),
                                error: None,
                            }
                        }
                        Err(e) => ResultListEntry {
                            sql: c_sql,
                            result: None,
                            error: CString::new(e.to_string()).ok(),
                        },
                    }
                })
                .collect();
            Box::into_raw(Box::new(potato_result_list { entries }))
        }
        Err(e) => {
            set_error(db, e.to_string());
            ptr::null_mut()
        }
    }
}

// ---------------------------------------------------------------------------
// Result-list accessors
// ---------------------------------------------------------------------------

/// Returns the number of statement results in the list.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn potato_result_list_count(list: *const potato_result_list) -> usize {
    if list.is_null() {
        return 0;
    }
    (*list).entries.len()
}

/// Returns the SQL text of the statement at `index`, or NULL if out of range.
///
/// The returned pointer is valid for the lifetime of the list handle.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_list_sql(
    list: *const potato_result_list,
    index: usize,
) -> *const c_char {
    if list.is_null() {
        return ptr::null();
    }
    let list = &*list;
    if index >= list.entries.len() {
        return ptr::null();
    }
    list.entries[index].sql.as_ptr()
}

/// Returns a borrowed pointer to the result at `index`, or NULL if
/// the statement failed.
///
/// The returned pointer is valid for the lifetime of the list handle.
/// Do NOT free the returned pointer with `potato_result_free`.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_list_result(
    list: *const potato_result_list,
    index: usize,
) -> *const potato_result {
    if list.is_null() {
        return ptr::null();
    }
    let list = &*list;
    if index >= list.entries.len() {
        return ptr::null();
    }
    match list.entries[index].result.as_ref() {
        Some(r) => std::ptr::from_ref::<potato_result>(r),
        None => ptr::null(),
    }
}

/// Returns the error message for the statement at `index`, or NULL if
/// the statement succeeded.
///
/// The returned pointer is valid for the lifetime of the list handle.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_list_error(
    list: *const potato_result_list,
    index: usize,
) -> *const c_char {
    if list.is_null() {
        return ptr::null();
    }
    let list = &*list;
    if index >= list.entries.len() {
        return ptr::null();
    }
    match list.entries[index].error.as_ref() {
        Some(e) => e.as_ptr(),
        None => ptr::null(),
    }
}

/// Frees a result list and all contained results.
///
/// # Safety
/// `list` must either be NULL or a valid pointer returned by this library
/// that has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_list_free(list: *mut potato_result_list) {
    if !list.is_null() {
        drop(Box::from_raw(list));
    }
}

// ---------------------------------------------------------------------------
// Recent queries / query log
// ---------------------------------------------------------------------------

/// Returns a handle to the recent query log.
///
/// The caller must free the result with `potato_query_log_free`.
/// Returns NULL if `db` is NULL.
///
/// # Safety
/// `db` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_recent_queries(db: *const potato_db) -> *mut potato_query_log {
    if db.is_null() {
        return ptr::null_mut();
    }
    let entries = (*db)
        .db
        .recent_queries()
        .into_iter()
        .map(|e| QueryLogCEntry {
            sql: CString::new(e.sql).unwrap_or_default(),
            duration_ms: e.duration.as_millis() as u64,
            rows: e.rows,
        })
        .collect();
    Box::into_raw(Box::new(potato_query_log { entries }))
}

/// Returns recent CDC events as a records result.
///
/// This is equivalent to executing `SELECT * FROM potatodb_cdc`.
///
/// # Safety
/// `db` must either be NULL or a valid mutable pointer returned by this
/// library that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_recent_cdc(db: *mut potato_db) -> *mut potato_result {
    if db.is_null() {
        return ptr::null_mut();
    }
    let db_ref = &mut *db;
    match db_ref
        .rt
        .block_on(db_ref.db.execute("SELECT * FROM potatodb_cdc"))
    {
        Ok(result) => {
            db_ref.last_error = None;
            query_result_to_handle(result)
        }
        Err(e) => {
            set_error(db_ref, e.to_string());
            ptr::null_mut()
        }
    }
}

/// Returns the number of entries in the query log.
///
/// # Safety
/// `log` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn potato_query_log_count(log: *const potato_query_log) -> usize {
    if log.is_null() {
        return 0;
    }
    (*log).entries.len()
}

/// Returns the SQL text of query log entry at `index`, or NULL.
///
/// The returned pointer is valid for the lifetime of the log handle.
///
/// # Safety
/// `log` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_query_log_sql(
    log: *const potato_query_log,
    index: usize,
) -> *const c_char {
    if log.is_null() {
        return ptr::null();
    }
    let log = &*log;
    if index >= log.entries.len() {
        return ptr::null();
    }
    log.entries[index].sql.as_ptr()
}

/// Returns the duration in milliseconds of query log entry at `index`.
///
/// Returns 0 if `log` is NULL or `index` is out of range.
///
/// # Safety
/// `log` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_query_log_duration_ms(
    log: *const potato_query_log,
    index: usize,
) -> u64 {
    if log.is_null() {
        return 0;
    }
    let log = &*log;
    if index >= log.entries.len() {
        return 0;
    }
    log.entries[index].duration_ms
}

/// Returns the row count of query log entry at `index`.
///
/// Returns 0 if `log` is NULL or `index` is out of range.
///
/// # Safety
/// `log` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_query_log_rows(
    log: *const potato_query_log,
    index: usize,
) -> usize {
    if log.is_null() {
        return 0;
    }
    let log = &*log;
    if index >= log.entries.len() {
        return 0;
    }
    log.entries[index].rows
}

/// Frees a query log handle.
///
/// # Safety
/// `log` must either be NULL or a valid pointer returned by this library
/// that has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_query_log_free(log: *mut potato_query_log) {
    if !log.is_null() {
        drop(Box::from_raw(log));
    }
}

// ---------------------------------------------------------------------------
// Backup / restore
// ---------------------------------------------------------------------------

/// Creates a compressed backup archive of the current local database.
///
/// Returns 0 on success, -1 on error (check `potato_last_error`).
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and
/// `archive_path` must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_backup(db: *mut potato_db, archive_path: *const c_char) -> i32 {
    if db.is_null() {
        return -1;
    }
    let db = &mut *db;
    db.last_error = None;

    let archive_path = if let Some(s) = cstr_to_str(archive_path) {
        s
    } else {
        set_error(db, "NULL archive_path pointer".into());
        return -1;
    };

    match db.rt.block_on(db.db.backup(archive_path)) {
        Ok(()) => 0,
        Err(e) => {
            set_error(db, e.to_string());
            -1
        }
    }
}

/// Restores a compressed backup archive into the current local database.
///
/// Returns 0 on success, -1 on error (check `potato_last_error`).
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and
/// `archive_path` must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_restore(db: *mut potato_db, archive_path: *const c_char) -> i32 {
    if db.is_null() {
        return -1;
    }
    let db = &mut *db;
    db.last_error = None;

    let archive_path = if let Some(s) = cstr_to_str(archive_path) {
        s
    } else {
        set_error(db, "NULL archive_path pointer".into());
        return -1;
    };

    match db.rt.block_on(db.db.restore(archive_path)) {
        Ok(()) => 0,
        Err(e) => {
            set_error(db, e.to_string());
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// Buffered write controls
// ---------------------------------------------------------------------------

/// Flushes all buffered inserts to Parquet files.
///
/// Returns 0 on success, -1 on error (check `potato_last_error`).
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_flush(db: *mut potato_db) -> i32 {
    if db.is_null() {
        return -1;
    }
    let db = &mut *db;
    db.last_error = None;
    match db.rt.block_on(db.db.execute("FLUSH;")) {
        Ok(_) => 0,
        Err(e) => {
            set_error(db, e.to_string());
            -1
        }
    }
}

/// Flushes buffered inserts for one table.
///
/// Returns 0 on success, -1 on error (check `potato_last_error`).
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and
/// `table_name` must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_flush_table(db: *mut potato_db, table_name: *const c_char) -> i32 {
    if db.is_null() {
        return -1;
    }
    let db = &mut *db;
    db.last_error = None;
    let table_name = if let Some(s) = cstr_to_str(table_name) {
        s.trim()
    } else {
        set_error(db, "NULL table_name pointer".into());
        return -1;
    };
    if table_name.is_empty() {
        set_error(db, "empty table_name".into());
        return -1;
    }
    let escaped_table_name = table_name.replace('"', "\"\"");
    let sql = format!("FLUSH TABLE \"{escaped_table_name}\";");
    match db.rt.block_on(db.db.execute(&sql)) {
        Ok(_) => 0,
        Err(e) => {
            set_error(db, e.to_string());
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// Table storage stats
// ---------------------------------------------------------------------------

/// Returns number of parquet files currently backing `table_name`.
///
/// Returns -1 on error (check `potato_last_error`).
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and
/// `table_name` must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_table_parquet_file_count(
    db: *mut potato_db,
    table_name: *const c_char,
) -> i64 {
    if db.is_null() {
        return -1;
    }
    let db = &mut *db;
    db.last_error = None;
    let table_name = if let Some(s) = cstr_to_str(table_name) {
        s.trim()
    } else {
        set_error(db, "NULL table_name pointer".into());
        return -1;
    };
    if table_name.is_empty() {
        set_error(db, "empty table_name".into());
        return -1;
    }
    match db.rt.block_on(db.db.parquet_file_count(table_name)) {
        Ok(count) => {
            if let Ok(v) = i64::try_from(count) {
                v
            } else {
                set_error(db, "parquet file count overflowed i64".into());
                -1
            }
        }
        Err(e) => {
            set_error(db, e.to_string());
            -1
        }
    }
}

/// Returns total parquet bytes currently backing `table_name`.
///
/// Returns -1 on error (check `potato_last_error`).
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and
/// `table_name` must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_table_total_bytes(
    db: *mut potato_db,
    table_name: *const c_char,
) -> i64 {
    if db.is_null() {
        return -1;
    }
    let db = &mut *db;
    db.last_error = None;
    let table_name = if let Some(s) = cstr_to_str(table_name) {
        s.trim()
    } else {
        set_error(db, "NULL table_name pointer".into());
        return -1;
    };
    if table_name.is_empty() {
        set_error(db, "empty table_name".into());
        return -1;
    }
    match db.rt.block_on(db.db.table_total_bytes(table_name)) {
        Ok(bytes) => {
            if let Ok(v) = i64::try_from(bytes) {
                v
            } else {
                set_error(db, "table total bytes overflowed i64".into());
                -1
            }
        }
        Err(e) => {
            set_error(db, e.to_string());
            -1
        }
    }
}

/// Returns age in seconds of the oldest parquet file for `table_name`.
///
/// Returns -1 on error (check `potato_last_error`).
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and
/// `table_name` must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_table_oldest_file_age_secs(
    db: *mut potato_db,
    table_name: *const c_char,
) -> i64 {
    if db.is_null() {
        return -1;
    }
    let db = &mut *db;
    db.last_error = None;
    let table_name = if let Some(s) = cstr_to_str(table_name) {
        s.trim()
    } else {
        set_error(db, "NULL table_name pointer".into());
        return -1;
    };
    if table_name.is_empty() {
        set_error(db, "empty table_name".into());
        return -1;
    }
    match db.rt.block_on(db.db.table_oldest_file_age_secs(table_name)) {
        Ok(age) => {
            if let Ok(v) = i64::try_from(age) {
                v
            } else {
                set_error(db, "oldest file age overflowed i64".into());
                -1
            }
        }
        Err(e) => {
            set_error(db, e.to_string());
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// Result inspection
// ---------------------------------------------------------------------------

/// Returns the result kind (records or message).
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn potato_result_get_kind(
    res: *const potato_result,
) -> potato_result_kind {
    if res.is_null() {
        return potato_result_kind::POTATO_RESULT_MESSAGE;
    }
    (*res).kind
}

/// Returns the message string for a message-type result, or `NULL`.
///
/// The returned pointer is valid for the lifetime of the result handle.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_message(res: *const potato_result) -> *const c_char {
    if res.is_null() {
        return ptr::null();
    }
    match (*res).message.as_ref() {
        Some(s) => s.as_ptr(),
        None => ptr::null(),
    }
}

/// Returns the total row count across all batches.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn potato_result_row_count(res: *const potato_result) -> usize {
    if res.is_null() {
        return 0;
    }
    (*res).row_count
}

/// Returns the number of columns in the result schema (0 for messages).
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_column_count(res: *const potato_result) -> usize {
    if res.is_null() {
        return 0;
    }
    (*res)
        .batches
        .first()
        .map_or(0, arrow::array::RecordBatch::num_columns)
}

/// Returns the name of column `col_idx`, or `NULL` if out of range.
///
/// The returned pointer is valid for the lifetime of the result handle.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_column_name(
    res: *const potato_result,
    col_idx: usize,
) -> *const c_char {
    if res.is_null() {
        return ptr::null();
    }
    let res = &*res;
    if col_idx >= res.column_names.len() {
        return ptr::null();
    }
    res.column_names[col_idx].as_ptr()
}

/// Returns the type tag of column `col_idx`.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_get_column_type(
    res: *const potato_result,
    col_idx: usize,
) -> potato_column_type {
    if res.is_null() {
        return potato_column_type::POTATO_TYPE_NULL;
    }
    let res = &*res;
    let schema = match res.batches.first() {
        Some(b) => b.schema(),
        None => return potato_column_type::POTATO_TYPE_NULL,
    };
    if col_idx >= schema.fields().len() {
        return potato_column_type::POTATO_TYPE_NULL;
    }
    let field = schema.field(col_idx);
    if field.metadata().get("potatodb.sql_type").is_some_and(|t| {
        let upper = t.to_uppercase();
        upper == "JSON" || upper == "JSONB"
    }) {
        return potato_column_type::POTATO_TYPE_JSON;
    }
    match field.data_type() {
        DataType::Boolean => potato_column_type::POTATO_TYPE_BOOL,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => potato_column_type::POTATO_TYPE_INT32,
        DataType::Int64 | DataType::UInt64 => potato_column_type::POTATO_TYPE_INT64,
        DataType::Float16 | DataType::Float32 => potato_column_type::POTATO_TYPE_FLOAT,
        DataType::Float64 => potato_column_type::POTATO_TYPE_DOUBLE,
        DataType::Utf8 | DataType::LargeUtf8 => potato_column_type::POTATO_TYPE_STRING,
        DataType::Date32 | DataType::Date64 => potato_column_type::POTATO_TYPE_DATE,
        DataType::Timestamp(_, _) => potato_column_type::POTATO_TYPE_TIMESTAMP,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            potato_column_type::POTATO_TYPE_DECIMAL
        }
        DataType::Duration(_) => potato_column_type::POTATO_TYPE_INTERVAL,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            potato_column_type::POTATO_TYPE_ARRAY
        }
        DataType::FixedSizeBinary(16) => potato_column_type::POTATO_TYPE_UUID,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            potato_column_type::POTATO_TYPE_BINARY
        }
        _ => potato_column_type::POTATO_TYPE_OTHER,
    }
}

/// Returns the formatted ASCII table for a records result.
///
/// The returned pointer is valid for the lifetime of the result handle.
/// Returns `NULL` for message-type results.
///
/// # Safety
/// `res` must either be NULL or a valid mutable pointer returned by this
/// library that has not been freed and is not concurrently aliased.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_display(res: *mut potato_result) -> *const c_char {
    if res.is_null() {
        return ptr::null();
    }
    let res = &mut *res;
    if res.batches.is_empty() {
        return ptr::null();
    }
    if res.display_cache.is_none() {
        let text = potatodb_display::format_batches(&res.batches);
        if let Ok(s) = CString::new(text) {
            res.display_cache = Some(s);
        }
    }
    res.display_cache
        .as_ref()
        .map_or(ptr::null(), |s| s.as_ptr())
}

// ---------------------------------------------------------------------------
// Row-level access
// ---------------------------------------------------------------------------

/// Returns whether the value at (`row`, `col`) is NULL.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_is_null(
    res: *const potato_result,
    row: usize,
    col: usize,
) -> bool {
    if res.is_null() {
        return true;
    }
    let (batch, local_row) = match resolve_row(&(*res).batches, row) {
        Some(v) => v,
        None => return true,
    };
    if col >= batch.num_columns() {
        return true;
    }
    batch.column(col).is_null(local_row)
}

/// Reads an `i64` value. Returns 0 if the cell is NULL or the column
/// is not an integer type.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_get_int(
    res: *const potato_result,
    row: usize,
    col: usize,
) -> i64 {
    if res.is_null() {
        return 0;
    }
    use arrow::array::{
        Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    let (batch, local_row) = match resolve_row(&(*res).batches, row) {
        Some(v) => v,
        None => return 0,
    };
    if col >= batch.num_columns() {
        return 0;
    }
    let arr = batch.column(col);
    if arr.is_null(local_row) {
        return 0;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return i64::from(a.value(local_row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(local_row);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
        return i64::from(a.value(local_row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
        return i64::from(a.value(local_row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
        return i64::from(a.value(local_row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
        return i64::from(a.value(local_row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
        return i64::from(a.value(local_row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
        return a.value(local_row) as i64;
    }
    0
}

/// Reads an `f64` value. Returns 0.0 if the cell is NULL or not a float type.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_get_double(
    res: *const potato_result,
    row: usize,
    col: usize,
) -> f64 {
    if res.is_null() {
        return 0.0;
    }
    use arrow::array::{Array, Float32Array, Float64Array};
    let (batch, local_row) = match resolve_row(&(*res).batches, row) {
        Some(v) => v,
        None => return 0.0,
    };
    if col >= batch.num_columns() {
        return 0.0;
    }
    let arr = batch.column(col);
    if arr.is_null(local_row) {
        return 0.0;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(local_row);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return f64::from(a.value(local_row));
    }
    0.0
}

/// Reads a boolean value. Returns `false` if the cell is NULL.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_get_bool(
    res: *const potato_result,
    row: usize,
    col: usize,
) -> bool {
    if res.is_null() {
        return false;
    }
    use arrow::array::BooleanArray;
    let (batch, local_row) = match resolve_row(&(*res).batches, row) {
        Some(v) => v,
        None => return false,
    };
    if col >= batch.num_columns() {
        return false;
    }
    let arr = batch.column(col);
    if arr.is_null(local_row) {
        return false;
    }
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .is_some_and(|a| a.value(local_row))
}

/// Reads a date value as epoch days (`Date32`) or epoch milliseconds
/// (`Date64`). Returns 0 if the cell is NULL or not a date type.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_get_date(
    res: *const potato_result,
    row: usize,
    col: usize,
) -> i64 {
    if res.is_null() {
        return 0;
    }
    use arrow::array::{Array, Date32Array, Date64Array};
    let (batch, local_row) = match resolve_row(&(*res).batches, row) {
        Some(v) => v,
        None => return 0,
    };
    if col >= batch.num_columns() {
        return 0;
    }
    let arr = batch.column(col);
    if arr.is_null(local_row) {
        return 0;
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return i64::from(a.value(local_row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date64Array>() {
        return a.value(local_row);
    }
    0
}

/// Reads a timestamp value as microseconds since epoch, normalizing
/// from the underlying `TimeUnit`. Returns 0 if the cell is NULL or
/// not a timestamp type.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_get_timestamp(
    res: *const potato_result,
    row: usize,
    col: usize,
) -> i64 {
    if res.is_null() {
        return 0;
    }
    use arrow::array::{
        Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    let (batch, local_row) = match resolve_row(&(*res).batches, row) {
        Some(v) => v,
        None => return 0,
    };
    if col >= batch.num_columns() {
        return 0;
    }
    let arr = batch.column(col);
    if arr.is_null(local_row) {
        return 0;
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return a.value(local_row);
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return a.value(local_row).saturating_mul(1_000);
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampSecondArray>() {
        return a.value(local_row).saturating_mul(1_000_000);
    }
    if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return a.value(local_row) / 1_000;
    }
    0
}

/// Reads a string value. For string columns this is a direct read;
/// for all other column types the value is converted to its display
/// representation (dates, timestamps, decimals, etc.).
///
/// Returns `NULL` if the cell is NULL. The caller must free the
/// returned pointer with `potato_string_free`.
///
/// # Safety
/// `res` must either be NULL or a valid pointer returned by this library that
/// has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_get_string(
    res: *const potato_result,
    row: usize,
    col: usize,
) -> *mut c_char {
    use arrow::array::{LargeStringArray, StringArray};
    if res.is_null() {
        return ptr::null_mut();
    }
    let (batch, local_row) = match resolve_row(&(*res).batches, row) {
        Some(v) => v,
        None => return ptr::null_mut(),
    };
    if col >= batch.num_columns() {
        return ptr::null_mut();
    }
    let arr = batch.column(col);
    if arr.is_null(local_row) {
        return ptr::null_mut();
    }

    // Fast path for native string arrays.
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return CString::new(a.value(local_row))
            .map(CString::into_raw)
            .unwrap_or(ptr::null_mut());
    }
    if let Some(a) = arr.as_any().downcast_ref::<LargeStringArray>() {
        return CString::new(a.value(local_row))
            .map(CString::into_raw)
            .unwrap_or(ptr::null_mut());
    }

    // Generic path: use Arrow display formatting for any other type.
    match arrow_value_to_string(arr.as_ref(), local_row) {
        Some(s) => CString::new(s)
            .map(CString::into_raw)
            .unwrap_or(ptr::null_mut()),
        None => ptr::null_mut(),
    }
}

/// Frees a string returned by `potato_result_get_string` or `potato_data_url`.
///
/// # Safety
/// `s` must either be NULL or a pointer returned by `CString::into_raw` from
/// this library (such as `potato_result_get_string`) that has not been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(CString::from_raw(s));
    }
}

/// Frees a query result.
///
/// # Safety
/// `res` must either be NULL or a pointer returned by this library that has
/// not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_result_free(res: *mut potato_result) {
    if !res.is_null() {
        drop(Box::from_raw(res));
    }
}

// ---------------------------------------------------------------------------
// Prepared statements
// ---------------------------------------------------------------------------

/// Prepares a named statement for later execution with parameters.
///
/// Returns 0 on success, -1 on error (check `potato_last_error`).
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and `name`
/// and `sql` must be non-NULL valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_prepare(
    db: *mut potato_db,
    name: *const c_char,
    sql: *const c_char,
) -> i32 {
    if db.is_null() {
        return -1;
    }
    let db = &mut *db;
    db.last_error = None;

    let name = if let Some(s) = cstr_to_str(name) {
        s
    } else {
        set_error(db, "NULL name pointer".into());
        return -1;
    };
    let sql = if let Some(s) = cstr_to_str(sql) {
        s
    } else {
        set_error(db, "NULL sql pointer".into());
        return -1;
    };

    let prepare_sql = format!("PREPARE {name} AS {sql}");
    match db.rt.block_on(db.db.execute(&prepare_sql)) {
        Ok(_) => 0,
        Err(e) => {
            set_error(db, e.to_string());
            -1
        }
    }
}

/// # Safety
/// Executes a previously prepared statement with the given parameters.
///
/// `params` is an array of `param_count` C strings.  Each is substituted
/// for `$1`, `$2`, … in the prepared SQL.
/// Returns a result handle on success, or NULL on error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_execute_prepared(
    db: *mut potato_db,
    name: *const c_char,
    params: *const *const c_char,
    param_count: usize,
) -> *mut potato_result {
    if db.is_null() {
        return ptr::null_mut();
    }
    let db = &mut *db;
    db.last_error = None;

    let name = if let Some(s) = cstr_to_str(name) {
        s
    } else {
        set_error(db, "NULL name pointer".into());
        return ptr::null_mut();
    };

    if param_count > 0 && params.is_null() {
        set_error(db, "NULL params pointer".into());
        return ptr::null_mut();
    }

    let mut param_strs = Vec::new();
    for i in 0..param_count {
        let p = *params.add(i);
        if let Some(s) = cstr_to_str(p) {
            param_strs.push(s.to_string());
        } else {
            set_error(db, format!("NULL parameter at index {i}"));
            return ptr::null_mut();
        }
    }

    let params_part = param_strs.join(", ");
    let exec_sql = format!("EXECUTE {name}({params_part})");
    match db.rt.block_on(db.db.execute(&exec_sql)) {
        Ok(result) => query_result_to_handle(result),
        Err(e) => {
            set_error(db, e.to_string());
            ptr::null_mut()
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming results
// ---------------------------------------------------------------------------

/// Executes a SQL statement and returns a streaming result handle.
///
/// For `SELECT` and similar read-only queries the result will be streamed
/// batch-by-batch via `potato_stream_next`. For DDL/DML statements the
/// result is a message that can be read with `potato_stream_message`.
///
/// Returns NULL on error (check `potato_last_error`).
/// The caller must free the stream with `potato_stream_free`.
///
/// **Important**: The `potato_db` handle must remain alive (not closed)
/// for the entire lifetime of the returned stream.
///
/// # Safety
/// `db` must be a valid mutable handle pointer from this library, and `sql`
/// must be a non-NULL valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_execute_stream(
    db: *mut potato_db,
    sql: *const c_char,
) -> *mut potato_stream {
    if db.is_null() {
        return ptr::null_mut();
    }
    let db = &mut *db;
    db.last_error = None;

    let sql = if let Some(s) = cstr_to_str(sql) {
        s
    } else {
        set_error(db, "NULL sql pointer".into());
        return ptr::null_mut();
    };

    match db.rt.block_on(db.db.execute_stream(sql)) {
        Ok(QueryResultStream::Stream(stream)) => {
            use futures::StreamExt as _;
            let boxed = Box::pin(
                stream.map(|r| r.map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e)))),
            );
            Box::into_raw(Box::new(potato_stream {
                rt: &raw const db.rt,
                inner: StreamInner::Stream(boxed),
            }))
        }
        Ok(QueryResultStream::Message(msg)) => {
            let c_msg = CString::new(msg).ok();
            Box::into_raw(Box::new(potato_stream {
                rt: &raw const db.rt,
                inner: StreamInner::Message(c_msg),
            }))
        }
        Err(e) => {
            set_error(db, e.to_string());
            ptr::null_mut()
        }
    }
}

/// Returns the next batch from a streaming result as a `potato_result`
/// containing a single batch.
///
/// Returns NULL when the stream is exhausted (no more batches) or if
/// the stream is a message-type result. The caller must free each
/// returned result with `potato_result_free`.
///
/// # Safety
/// `stream` must be a valid mutable pointer returned by
/// `potato_execute_stream` that has not been freed. The `potato_db`
/// that created the stream must still be alive.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_stream_next(stream: *mut potato_stream) -> *mut potato_result {
    if stream.is_null() {
        return ptr::null_mut();
    }
    let stream = &mut *stream;
    let rt = &*stream.rt;

    match &mut stream.inner {
        StreamInner::Stream(s) => {
            if let Some(Ok(batch)) = rt.block_on(s.next()) {
                let row_count = batch.num_rows();
                let column_names = collect_column_names(std::slice::from_ref(&batch));
                Box::into_raw(Box::new(potato_result {
                    kind: potato_result_kind::POTATO_RESULT_RECORDS,
                    batches: vec![batch],
                    message: None,
                    row_count,
                    column_names,
                    display_cache: None,
                }))
            } else {
                stream.inner = StreamInner::Exhausted;
                ptr::null_mut()
            }
        }
        StreamInner::Message(_) | StreamInner::Exhausted => ptr::null_mut(),
    }
}

/// Returns whether the stream holds a message (DDL/DML result) rather
/// than record batches.
///
/// # Safety
/// `stream` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn potato_stream_is_message(stream: *const potato_stream) -> bool {
    if stream.is_null() {
        return false;
    }
    matches!((*stream).inner, StreamInner::Message(_))
}

/// Returns the message string for a message-type stream, or NULL.
///
/// The returned pointer is valid for the lifetime of the stream handle.
///
/// # Safety
/// `stream` must either be NULL or a valid pointer returned by this library.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_stream_message(stream: *const potato_stream) -> *const c_char {
    if stream.is_null() {
        return ptr::null();
    }
    match &(*stream).inner {
        StreamInner::Message(Some(msg)) => msg.as_ptr(),
        _ => ptr::null(),
    }
}

/// Frees a streaming result handle.
///
/// # Safety
/// `stream` must either be NULL or a valid pointer returned by
/// `potato_execute_stream` that has not already been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn potato_stream_free(stream: *mut potato_stream) {
    if !stream.is_null() {
        drop(Box::from_raw(stream));
    }
}
