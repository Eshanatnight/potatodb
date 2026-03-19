/**
 * @file potatodb.h
 * @brief C API for the PotatoDB Parquet-backed SQL database engine.
 *
 * All functions are safe to call from any thread, but a single
 * `potato_db` handle must not be used concurrently from multiple
 * threads without external synchronisation.
 */

#ifndef POTATODB_H
#define POTATODB_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Opaque handles ──────────────────────────────────────────── */

/** Opaque database handle. */
typedef struct potato_db potato_db;

/** Opaque query-result handle. */
typedef struct potato_result potato_result;

/** Opaque string list handle (returned by metadata functions). */
typedef struct potato_string_list potato_string_list;

/** Opaque index list handle (returned by `potato_indexes`). */
typedef struct potato_index_list potato_index_list;

/** Opaque result list handle (returned by `potato_execute_file`). */
typedef struct potato_result_list potato_result_list;

/** Opaque query log handle (returned by `potato_recent_queries`). */
typedef struct potato_query_log potato_query_log;

/** Opaque streaming result handle (returned by `potato_execute_stream`). */
typedef struct potato_stream potato_stream;

/* ── Enums ───────────────────────────────────────────────────── */

/** Discriminant returned by `potato_result_kind`. */
typedef enum
{
    POTATO_RESULT_RECORDS = 0,
    POTATO_RESULT_MESSAGE = 1,
} potato_result_kind;

/** Column type tag returned by `potato_result_column_type`. */
typedef enum
{
    POTATO_TYPE_NULL      = 0,
    POTATO_TYPE_BOOL      = 1,
    POTATO_TYPE_INT32     = 2,
    POTATO_TYPE_INT64     = 3,
    POTATO_TYPE_FLOAT     = 4,
    POTATO_TYPE_DOUBLE    = 5,
    POTATO_TYPE_STRING    = 6,
    POTATO_TYPE_DATE      = 7,
    POTATO_TYPE_TIMESTAMP = 8,
    POTATO_TYPE_DECIMAL   = 9,
    POTATO_TYPE_BINARY    = 10,
    POTATO_TYPE_UUID      = 11,
    POTATO_TYPE_INTERVAL  = 12,
    POTATO_TYPE_ARRAY     = 13,
    POTATO_TYPE_JSON      = 14,
    POTATO_TYPE_OTHER     = 99,
} potato_column_type;

/* ── Database lifecycle ──────────────────────────────────────── */

/**
 * Opens a database.
 *
 * @param data_dir      Local path or `s3://bucket/prefix` URL (required).
 * @param s3_endpoint   S3-compatible endpoint URL, or NULL for AWS default.
 * @param s3_region     AWS region string, or NULL for default.
 * @param s3_allow_http Set to `true` to allow plain HTTP connections.
 * @param wal_dir       Local directory for write-ahead log files. or null for default.
 * @return Handle on success, NULL on failure.
 */
potato_db* potato_open(
    const char* data_dir, const char* s3_endpoint, const char* s3_region, bool s3_allow_http, const char* wal_dir);

/**
 * Opens a local database (convenience — no S3 parameters).
 *
 * @param data_dir Local filesystem path.
 * @return Handle on success, NULL on failure.
 */
potato_db* potato_open_local(const char* data_dir);

/**
 * Closes the database and frees all associated resources.
 *
 * @param db Handle returned by `potato_open*`. May be NULL (no-op).
 */
void potato_close(potato_db* db);

/**
 * Returns the last error message, or NULL if no error occurred.
 *
 * The returned pointer is valid until the next call on this handle.
 */
const char* potato_last_error(const potato_db* db);

/* ── Metadata introspection ──────────────────────────────────── */

/** Returns whether the database is inside a BEGIN transaction. */
bool potato_in_transaction(const potato_db* db);

/**
 * Returns the data directory / URL for the database.
 *
 * @return Heap-allocated string; free with `potato_string_free`.
 */
char* potato_data_url(const potato_db* db);

/**
 * Returns a list of all table names.
 *
 * @return String list handle; free with `potato_string_list_free`.
 */
potato_string_list* potato_table_names(const potato_db* db);

/**
 * Returns a list of column names for the given table.
 *
 * @param table_name Name of the table.
 * @return String list handle; free with `potato_string_list_free`.
 */
potato_string_list* potato_table_columns(const potato_db* db, const char* table_name);

/**
 * Returns a list of all view names.
 *
 * @return String list handle; free with `potato_string_list_free`.
 */
potato_string_list* potato_view_names(const potato_db* db);

/**
 * Returns a list of all user-defined SQL function names.
 *
 * @return String list handle; free with `potato_string_list_free`.
 */
potato_string_list* potato_function_names(const potato_db* db);

/**
 * Returns a list of all indexes as (name, table) pairs.
 *
 * @return Index list handle; free with `potato_index_list_free`.
 */
potato_index_list* potato_indexes(const potato_db* db);

/* ── String list accessors ───────────────────────────────────── */

/** Returns the number of strings in the list. */
size_t potato_string_list_count(const potato_string_list* list);

/**
 * Returns the string at `index`.
 *
 * Valid for the lifetime of the list handle.
 * Returns NULL if out of range.
 */
const char* potato_string_list_get(const potato_string_list* list, size_t index);

/** Frees a string list. */
void potato_string_list_free(potato_string_list* list);

/* ── Index list accessors ────────────────────────────────────── */

/** Returns the number of indexes in the list. */
size_t potato_index_list_count(const potato_index_list* list);

/**
 * Returns the index name at position `index`.
 *
 * Valid for the lifetime of the list handle.
 */
const char* potato_index_list_name(const potato_index_list* list, size_t index);

/**
 * Returns the table name for the index at position `index`.
 *
 * Valid for the lifetime of the list handle.
 */
const char* potato_index_list_table(const potato_index_list* list, size_t index);

/** Frees an index list. */
void potato_index_list_free(potato_index_list* list);

/* ── Query execution ─────────────────────────────────────────── */

/**
 * Executes a single SQL statement.
 *
 * @param db  Database handle.
 * @param sql NULL-terminated SQL string.
 * @return Result handle on success, NULL on error (check `potato_last_error`).
 *         Must be freed with `potato_result_free`.
 */
potato_result* potato_execute(potato_db* db, const char* sql);

/**
 * Executes a read-only SQL statement.
 *
 * This path is intended for read-only workloads and rejects mutating SQL.
 *
 * @param db  Database handle.
 * @param sql NULL-terminated SQL string.
 * @return Result handle on success, NULL on error (check `potato_last_error`).
 *         Must be freed with `potato_result_free`.
 */
potato_result* potato_execute_readonly(potato_db* db, const char* sql);

/* ── Execute file ────────────────────────────────────────────── */

/**
 * Executes all SQL statements from a `.sql` file.
 *
 * @param db               Database handle.
 * @param path             Path to the SQL file.
 * @param continue_on_error If true, continue executing after errors.
 * @return Result list handle; free with `potato_result_list_free`.
 *         Returns NULL on error (check `potato_last_error`).
 */
potato_result_list* potato_execute_file(potato_db* db, const char* path, bool continue_on_error);

/* ── Result list accessors ───────────────────────────────────── */

/** Returns the number of statements in the result list. */
size_t potato_result_list_count(const potato_result_list* list);

/**
 * Returns the SQL text of the statement at `index`.
 *
 * Valid for the lifetime of the list handle.
 */
const char* potato_result_list_sql(const potato_result_list* list, size_t index);

/**
 * Returns a borrowed pointer to the result at `index`, or NULL
 * if the statement failed.
 *
 * Do NOT free the returned pointer.
 */
const potato_result* potato_result_list_result(const potato_result_list* list, size_t index);

/**
 * Returns the error message for the statement at `index`, or NULL
 * if the statement succeeded.
 */
const char* potato_result_list_error(const potato_result_list* list, size_t index);

/** Frees a result list and all contained results. */
void potato_result_list_free(potato_result_list* list);

/* ── Result inspection ───────────────────────────────────────── */

/** Returns the kind of result (records or message). */
potato_result_kind potato_result_get_kind(const potato_result* res);

/** Returns the message string, or NULL for record results. */
const char* potato_result_message(const potato_result* res);

/** Returns the total number of rows across all batches. */
size_t potato_result_row_count(const potato_result* res);

/** Returns the number of columns (0 for message results). */
size_t potato_result_column_count(const potato_result* res);

/**
 * Returns the name of column `col_idx`.
 *
 * The returned pointer is valid for the lifetime of the result handle.
 * Returns NULL if `col_idx` is out of range.
 */
const char* potato_result_column_name(const potato_result* res, size_t col_idx);

/** Returns the type tag of column `col_idx`. */
potato_column_type potato_result_get_column_type(const potato_result* res, size_t col_idx);

/**
 * Returns a formatted ASCII table of the result.
 *
 * The returned pointer is valid for the lifetime of the result handle.
 * Returns NULL for message results.
 */
const char* potato_result_display(potato_result* res);

/* ── Row-level value access ──────────────────────────────────── */

/** Returns true if the value at (row, col) is NULL. */
bool potato_result_is_null(const potato_result* res, size_t row, size_t col);

/** Reads an integer value. Returns 0 for NULL or non-integer columns. */
long long potato_result_get_int(const potato_result* res, size_t row, size_t col);

/** Reads a double value. Returns 0.0 for NULL or non-float columns. */
double potato_result_get_double(const potato_result* res, size_t row, size_t col);

/** Reads a boolean value. Returns false for NULL. */
bool potato_result_get_bool(const potato_result* res, size_t row, size_t col);

/**
 * Reads a date value as epoch days (Date32) or milliseconds (Date64).
 *
 * @return Epoch days/ms, or 0 for NULL or non-date columns.
 */
long long potato_result_get_date(const potato_result* res, size_t row, size_t col);

/**
 * Reads a timestamp value as microseconds since epoch.
 *
 * @return Microseconds since epoch, or 0 for NULL or non-timestamp columns.
 */
long long potato_result_get_timestamp(const potato_result* res, size_t row, size_t col);

/**
 * Reads a string value.  For non-string columns the value is
 * converted to its display representation (dates, decimals, etc.).
 *
 * @return Heap-allocated string that the caller must free with
 *         `potato_string_free`, or NULL if the cell is NULL.
 */
char* potato_result_get_string(const potato_result* res, size_t row, size_t col);

/** Frees a string returned by `potato_result_get_string`. */
void potato_string_free(char* s);

/** Frees a query result. May be NULL (no-op). */
void potato_result_free(potato_result* res);

/* ── Prepared statements ─────────────────────────────────────── */

/**
 * Prepares a named statement for later execution.
 *
 * @param db   Database handle.
 * @param name Statement name (e.g. "find_user").
 * @param sql  SQL with $1, $2, ... parameter placeholders.
 * @return 0 on success, -1 on error (check `potato_last_error`).
 */
int potato_prepare(potato_db* db, const char* name, const char* sql);

/**
 * Executes a previously prepared statement with parameters.
 *
 * @param db          Database handle.
 * @param name        Statement name.
 * @param params      Array of NULL-terminated parameter strings.
 * @param param_count Number of parameters.
 * @return Result handle on success, NULL on error.
 */
potato_result* potato_execute_prepared(potato_db* db, const char* name, const char** params, size_t param_count);

/* ── Backup / restore ───────────────────────────────────────── */

/**
 * Creates a compressed backup archive of the current local database.
 *
 * @param db           Database handle.
 * @param archive_path Output archive path (e.g. "/tmp/potatodb.tar.gz").
 * @return 0 on success, -1 on error (check `potato_last_error`).
 */
int potato_backup(potato_db* db, const char* archive_path);

/**
 * Restores a compressed backup archive into the current local database.
 *
 * @param db           Database handle.
 * @param archive_path Input archive path (e.g. "/tmp/potatodb.tar.gz").
 * @return 0 on success, -1 on error (check `potato_last_error`).
 */
int potato_restore(potato_db* db, const char* archive_path);

/* ── Buffered write controls ─────────────────────────────────── */

/**
 * Flushes all buffered INSERT data to Parquet files.
 *
 * @param db Database handle.
 * @return 0 on success, -1 on error (check `potato_last_error`).
 */
int potato_flush(potato_db* db);

/**
 * Flushes buffered INSERT data for a single table.
 *
 * @param db         Database handle.
 * @param table_name Target table name.
 * @return 0 on success, -1 on error (check `potato_last_error`).
 */
int potato_flush_table(potato_db* db, const char* table_name);

/* ── Table storage stats ─────────────────────────────────────── */

/**
 * Returns number of Parquet files currently backing a table.
 *
 * @return File count, or -1 on error (check `potato_last_error`).
 */
long long potato_table_parquet_file_count(potato_db* db, const char* table_name);

/**
 * Returns total bytes across all Parquet files backing a table.
 *
 * @return Total bytes, or -1 on error (check `potato_last_error`).
 */
long long potato_table_total_bytes(potato_db* db, const char* table_name);

/**
 * Returns age (seconds) of the oldest Parquet file backing a table.
 *
 * @return Age in seconds, or -1 on error (check `potato_last_error`).
 */
long long potato_table_oldest_file_age_secs(potato_db* db, const char* table_name);

/* ── Recent queries / query log ──────────────────────────────── */

/**
 * Returns a handle to the recent query log.
 *
 * @return Query log handle; free with `potato_query_log_free`.
 */
potato_query_log* potato_recent_queries(const potato_db* db);

/**
 * Returns recent CDC events as a records result.
 *
 * Equivalent SQL: `SELECT * FROM potatodb_cdc`.
 *
 * @return Result handle; free with `potato_result_free`.
 */
potato_result* potato_recent_cdc(potato_db* db);

/** Returns the number of entries in the query log. */
size_t potato_query_log_count(const potato_query_log* log);

/**
 * Returns the SQL text of query log entry at `index`.
 *
 * Valid for the lifetime of the log handle.
 */
const char* potato_query_log_sql(const potato_query_log* log, size_t index);

/** Returns the duration in milliseconds of entry at `index`. */
uint64_t potato_query_log_duration_ms(const potato_query_log* log, size_t index);

/** Returns the row count of entry at `index`. */
size_t potato_query_log_rows(const potato_query_log* log, size_t index);

/** Frees a query log handle. */
void potato_query_log_free(potato_query_log* log);

/* ── Streaming results ───────────────────────────────────────── */

/**
 * Executes a SQL statement and returns a streaming result handle.
 *
 * For SELECT queries, call `potato_stream_next` repeatedly to
 * obtain results batch-by-batch. For DDL/DML, the stream holds
 * a message (check with `potato_stream_is_message`).
 *
 * The `potato_db` handle must remain alive while the stream exists.
 *
 * @return Stream handle; free with `potato_stream_free`.
 *         Returns NULL on error (check `potato_last_error`).
 */
potato_stream* potato_execute_stream(potato_db* db, const char* sql);

/**
 * Returns the next batch from a streaming result.
 *
 * @return Result handle for one batch (free with `potato_result_free`),
 *         or NULL when the stream is exhausted.
 */
potato_result* potato_stream_next(potato_stream* stream);

/** Returns whether the stream holds a DDL/DML message. */
bool potato_stream_is_message(const potato_stream* stream);

/**
 * Returns the message string for a message-type stream.
 *
 * Valid for the lifetime of the stream handle. Returns NULL for
 * record-type streams.
 */
const char* potato_stream_message(const potato_stream* stream);

/** Frees a streaming result handle. */
void potato_stream_free(potato_stream* stream);

#ifdef __cplusplus
}
#endif

#endif /* POTATODB_H */
