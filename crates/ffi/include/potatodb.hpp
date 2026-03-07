/**
 * @file potatodb.hpp
 * @brief Header-only C++ wrapper for the PotatoDB C API.
 *
 * Uses error-as-values instead of exceptions.  Fallible operations
 * return `potato::Expected<T>` which holds either a value or an error
 * string.  No exceptions are thrown; the header compiles cleanly with
 * `-fno-exceptions`.
 *
 * Example:
 * @code
 *   auto db = potato::Database::open("./data");
 *   if (!db) { std::cerr << db.error() << "\n"; return 1; }
 *
 *   auto res = db->execute("SELECT * FROM users;");
 *   if (!res) { std::cerr << res.error() << "\n"; return 1; }
 *
 *   for (size_t r = 0; r < res->row_count(); ++r)
 *       std::cout << res->get_string(r, 0) << "\n";
 * @endcode
 */

#ifndef POTATODB_HPP
#define POTATODB_HPP

#include "potatodb.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace potato {

// ── Expected<T> ───────────────────────────────────────────────

/// A lightweight result type: holds either a `T` or an error string.
///
/// Contextually convertible to `bool` (`true` = has value).
/// Access the value with `operator*` / `operator->` and the error
/// with `error()`.
template <typename T>
class Expected {
public:
    /// Constructs a successful result by copying `val` in.
    Expected(const T &val) : value_(val) {}

    /// Constructs a successful result by moving `val` in.
    Expected(T &&val) : value_(std::move(val)) {}

    /// Constructs an error result.
    static Expected err(std::string msg) {
        Expected e;
        e.error_ = std::move(msg);
        return e;
    }

    Expected() = default;
    ~Expected() = default;

    Expected(const Expected &) = default;
    Expected &operator=(const Expected &) = default;
    Expected(Expected &&) noexcept = default;
    Expected &operator=(Expected &&) noexcept = default;

    /// `true` when the result holds a value (not an error).
    explicit operator bool() const noexcept { return value_.has_value(); }

    /// Returns the error message (empty if `bool(*this)` is true).
    const std::string &error() const noexcept { return error_; }

    T &operator*() noexcept { return *value_; }
    const T &operator*() const noexcept { return *value_; }
    T *operator->() noexcept { return &(*value_); }
    const T *operator->() const noexcept { return &(*value_); }

private:
    std::optional<T> value_;
    std::string error_;
};

// ── Result ────────────────────────────────────────────────────

/// RAII wrapper around a `potato_result*`.
///
/// Move-only; automatically frees the underlying handle on destruction.
class Result {
public:
    Result() noexcept = default;

    explicit Result(potato_result *raw) noexcept : handle_(raw) {}

    ~Result() { potato_result_free(handle_); }

    Result(const Result &) = delete;
    Result &operator=(const Result &) = delete;

    Result(Result &&other) noexcept : handle_(other.handle_) {
        other.handle_ = nullptr;
    }

    Result &operator=(Result &&other) noexcept {
        if (this != &other) {
            potato_result_free(handle_);
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }

    /// `true` if this object holds a valid result.
    explicit operator bool() const noexcept { return handle_ != nullptr; }

    /// Whether this result contains row data (vs. a status message).
    bool is_records() const noexcept {
        return handle_ && ::potato_result_get_kind(handle_) == POTATO_RESULT_RECORDS;
    }

    /// Whether this result is a DDL/DML status message.
    bool is_message() const noexcept {
        return handle_ && ::potato_result_get_kind(handle_) == POTATO_RESULT_MESSAGE;
    }

    /// Returns the status message, or an empty string for record results.
    std::string message() const {
        const char *m = potato_result_message(handle_);
        return m ? std::string(m) : std::string();
    }

    /// Total number of rows.
    std::size_t row_count() const noexcept {
        return handle_ ? potato_result_row_count(handle_) : 0;
    }

    /// Number of columns.
    std::size_t column_count() const noexcept {
        return handle_ ? potato_result_column_count(handle_) : 0;
    }

    /// Column name at the given index.
    std::string column_name(std::size_t col) const {
        const char *n = potato_result_column_name(handle_, col);
        return n ? std::string(n) : std::string();
    }

    /// Column type tag at the given index.
    potato_column_type column_type(std::size_t col) const noexcept {
        return handle_ ? ::potato_result_get_column_type(handle_, col)
                       : POTATO_TYPE_NULL;
    }

    /// Returns a formatted ASCII table of the results.
    std::string display() const {
        const char *d = potato_result_display(handle_);
        return d ? std::string(d) : std::string("(0 rows)");
    }

    /// `true` if the cell at (row, col) is NULL.
    bool is_null(std::size_t row, std::size_t col) const noexcept {
        return potato_result_is_null(handle_, row, col);
    }

    /// Reads an integer value (returns 0 for NULL).
    int64_t get_int(std::size_t row, std::size_t col) const noexcept {
        return static_cast<int64_t>(potato_result_get_int(handle_, row, col));
    }

    /// Reads a double value (returns 0.0 for NULL).
    double get_double(std::size_t row, std::size_t col) const noexcept {
        return potato_result_get_double(handle_, row, col);
    }

    /// Reads a boolean value (returns `false` for NULL).
    bool get_bool(std::size_t row, std::size_t col) const noexcept {
        return potato_result_get_bool(handle_, row, col);
    }

    /// Reads a string value (returns `""` for NULL).
    /// For non-string columns, returns the display representation.
    std::string get_string(std::size_t row, std::size_t col) const {
        char *s = potato_result_get_string(handle_, row, col);
        if (!s) return {};
        std::string out(s);
        potato_string_free(s);
        return out;
    }

    /// Reads a date value as epoch days (Date32) or ms (Date64).
    int64_t get_date(std::size_t row, std::size_t col) const noexcept {
        return static_cast<int64_t>(potato_result_get_date(handle_, row, col));
    }

    /// Reads a timestamp as microseconds since epoch.
    int64_t get_timestamp(std::size_t row, std::size_t col) const noexcept {
        return static_cast<int64_t>(potato_result_get_timestamp(handle_, row, col));
    }

private:
    potato_result *handle_ = nullptr;
};

// ── Database ──────────────────────────────────────────────────

// Forward declarations for wrapper classes used by Database methods.
class StringList;
class IndexList;
class ResultList;
class QueryLog;
class Stream;

/// RAII wrapper around a `potato_string_list*`.
class StringList {
public:
    StringList() noexcept = default;
    explicit StringList(potato_string_list *raw) noexcept : handle_(raw) {}
    ~StringList() { potato_string_list_free(handle_); }

    StringList(const StringList &) = delete;
    StringList &operator=(const StringList &) = delete;
    StringList(StringList &&o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }
    StringList &operator=(StringList &&o) noexcept {
        if (this != &o) { potato_string_list_free(handle_); handle_ = o.handle_; o.handle_ = nullptr; }
        return *this;
    }

    explicit operator bool() const noexcept { return handle_ != nullptr; }
    std::size_t count() const noexcept { return potato_string_list_count(handle_); }
    std::string get(std::size_t i) const {
        const char *s = potato_string_list_get(handle_, i);
        return s ? std::string(s) : std::string();
    }

    /// Convenience: collect all strings into a vector.
    std::vector<std::string> to_vector() const {
        std::vector<std::string> v;
        std::size_t n = count();
        v.reserve(n);
        for (std::size_t i = 0; i < n; ++i) v.push_back(get(i));
        return v;
    }

private:
    potato_string_list *handle_ = nullptr;
};

/// RAII wrapper around a `potato_index_list*`.
class IndexList {
public:
    IndexList() noexcept = default;
    explicit IndexList(potato_index_list *raw) noexcept : handle_(raw) {}
    ~IndexList() { potato_index_list_free(handle_); }

    IndexList(const IndexList &) = delete;
    IndexList &operator=(const IndexList &) = delete;
    IndexList(IndexList &&o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }
    IndexList &operator=(IndexList &&o) noexcept {
        if (this != &o) { potato_index_list_free(handle_); handle_ = o.handle_; o.handle_ = nullptr; }
        return *this;
    }

    explicit operator bool() const noexcept { return handle_ != nullptr; }
    std::size_t count() const noexcept { return potato_index_list_count(handle_); }
    std::string name(std::size_t i) const {
        const char *s = potato_index_list_name(handle_, i);
        return s ? std::string(s) : std::string();
    }
    std::string table(std::size_t i) const {
        const char *s = potato_index_list_table(handle_, i);
        return s ? std::string(s) : std::string();
    }

private:
    potato_index_list *handle_ = nullptr;
};

/// RAII wrapper around a `potato_result_list*` (from `execute_file`).
class ResultList {
public:
    ResultList() noexcept = default;
    explicit ResultList(potato_result_list *raw) noexcept : handle_(raw) {}
    ~ResultList() { potato_result_list_free(handle_); }

    ResultList(const ResultList &) = delete;
    ResultList &operator=(const ResultList &) = delete;
    ResultList(ResultList &&o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }
    ResultList &operator=(ResultList &&o) noexcept {
        if (this != &o) { potato_result_list_free(handle_); handle_ = o.handle_; o.handle_ = nullptr; }
        return *this;
    }

    explicit operator bool() const noexcept { return handle_ != nullptr; }
    std::size_t count() const noexcept { return potato_result_list_count(handle_); }

    std::string sql(std::size_t i) const {
        const char *s = potato_result_list_sql(handle_, i);
        return s ? std::string(s) : std::string();
    }

    /// Returns a borrowed pointer (do NOT free). NULL if statement failed.
    const potato_result *result(std::size_t i) const {
        return potato_result_list_result(handle_, i);
    }

    std::string error(std::size_t i) const {
        const char *e = potato_result_list_error(handle_, i);
        return e ? std::string(e) : std::string();
    }

    bool has_error(std::size_t i) const {
        return potato_result_list_error(handle_, i) != nullptr;
    }

private:
    potato_result_list *handle_ = nullptr;
};

/// RAII wrapper around a `potato_query_log*`.
class QueryLog {
public:
    QueryLog() noexcept = default;
    explicit QueryLog(potato_query_log *raw) noexcept : handle_(raw) {}
    ~QueryLog() { potato_query_log_free(handle_); }

    QueryLog(const QueryLog &) = delete;
    QueryLog &operator=(const QueryLog &) = delete;
    QueryLog(QueryLog &&o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }
    QueryLog &operator=(QueryLog &&o) noexcept {
        if (this != &o) { potato_query_log_free(handle_); handle_ = o.handle_; o.handle_ = nullptr; }
        return *this;
    }

    explicit operator bool() const noexcept { return handle_ != nullptr; }
    std::size_t count() const noexcept { return potato_query_log_count(handle_); }

    std::string sql(std::size_t i) const {
        const char *s = potato_query_log_sql(handle_, i);
        return s ? std::string(s) : std::string();
    }
    uint64_t duration_ms(std::size_t i) const noexcept {
        return potato_query_log_duration_ms(handle_, i);
    }
    std::size_t rows(std::size_t i) const noexcept {
        return potato_query_log_rows(handle_, i);
    }

private:
    potato_query_log *handle_ = nullptr;
};

/// RAII wrapper around a `potato_stream*`.
class Stream {
public:
    Stream() noexcept = default;
    explicit Stream(potato_stream *raw) noexcept : handle_(raw) {}
    ~Stream() { potato_stream_free(handle_); }

    Stream(const Stream &) = delete;
    Stream &operator=(const Stream &) = delete;
    Stream(Stream &&o) noexcept : handle_(o.handle_) { o.handle_ = nullptr; }
    Stream &operator=(Stream &&o) noexcept {
        if (this != &o) { potato_stream_free(handle_); handle_ = o.handle_; o.handle_ = nullptr; }
        return *this;
    }

    explicit operator bool() const noexcept { return handle_ != nullptr; }

    /// Returns the next batch as a Result, or an empty Result when done.
    Result next() {
        return Result(potato_stream_next(handle_));
    }

    bool is_message() const noexcept {
        return potato_stream_is_message(handle_);
    }

    std::string message() const {
        const char *m = potato_stream_message(handle_);
        return m ? std::string(m) : std::string();
    }

private:
    potato_stream *handle_ = nullptr;
};

/// RAII wrapper around a `potato_db*`.
///
/// Constructed through the static `open` / `open_s3` factory methods
/// which return `Expected<Database>` instead of throwing.
class Database {
public:
    /// Opens a local database at the given path.
    ///
    /// Returns an error if the database cannot be opened.
    static Expected<Database> open(const std::string &data_dir) {
        potato_db *h = potato_open_local(data_dir.c_str());
        if (!h)
            return Expected<Database>::err(
                "Failed to open PotatoDB at: " + data_dir);
        return Expected<Database>(Database(h));
    }

    /// Opens a database with optional S3 configuration.
    ///
    /// Returns an error if the database cannot be opened.
    static Expected<Database> open_s3(const std::string &data_dir,
                                      const std::string &s3_endpoint,
                                      const std::string &s3_region,
                                      bool s3_allow_http = false) {
        potato_db *h = potato_open(
            data_dir.c_str(),
            s3_endpoint.empty() ? nullptr : s3_endpoint.c_str(),
            s3_region.empty()   ? nullptr : s3_region.c_str(),
            s3_allow_http);
        if (!h)
            return Expected<Database>::err(
                "Failed to open PotatoDB at: " + data_dir);
        return Expected<Database>(Database(h));
    }

    ~Database() { potato_close(handle_); }

    Database(const Database &) = delete;
    Database &operator=(const Database &) = delete;

    Database(Database &&other) noexcept : handle_(other.handle_) {
        other.handle_ = nullptr;
    }

    Database &operator=(Database &&other) noexcept {
        if (this != &other) {
            potato_close(handle_);
            handle_ = other.handle_;
            other.handle_ = nullptr;
        }
        return *this;
    }

    /// Executes a SQL statement.
    ///
    /// Returns `Expected<Result>` -- check with `if (!res)` before use.
    Expected<Result> execute(const std::string &sql) {
        potato_result *raw = potato_execute(handle_, sql.c_str());
        if (!raw) {
            const char *err = potato_last_error(handle_);
            return Expected<Result>::err(
                err ? std::string(err) : "Unknown query error");
        }
        return Expected<Result>(Result(raw));
    }

    /// Executes a read-only SQL statement.
    ///
    /// Returns `Expected<Result>` -- check with `if (!res)` before use.
    Expected<Result> execute_readonly(const std::string &sql) {
        potato_result *raw = potato_execute_readonly(handle_, sql.c_str());
        if (!raw) {
            const char *err = potato_last_error(handle_);
            return Expected<Result>::err(
                err ? std::string(err) : "Unknown query error");
        }
        return Expected<Result>(Result(raw));
    }

    /// Prepares a named statement for later execution with parameters.
    ///
    /// Returns true on success, false on error (check `last_error()`).
    bool prepare(const std::string &name, const std::string &sql) {
        return potato_prepare(handle_, name.c_str(), sql.c_str()) == 0;
    }

    /// Executes a previously prepared statement with the given parameters.
    Expected<Result> execute_prepared(
            const std::string &name,
            const std::vector<std::string> &params) {
        std::vector<const char *> c_params;
        c_params.reserve(params.size());
        for (auto &p : params) c_params.push_back(p.c_str());

        potato_result *raw = potato_execute_prepared(
            handle_, name.c_str(),
            c_params.data(), c_params.size());
        if (!raw) {
            const char *err = potato_last_error(handle_);
            return Expected<Result>::err(
                err ? std::string(err) : "Unknown query error");
        }
        return Expected<Result>(Result(raw));
    }

    /// Creates a compressed backup archive for the current local database.
    ///
    /// Returns true on success, false on error (check `last_error()`).
    bool backup(const std::string &archive_path) {
        return potato_backup(handle_, archive_path.c_str()) == 0;
    }

    /// Restores a compressed backup archive into the current local database.
    ///
    /// Returns true on success, false on error (check `last_error()`).
    bool restore(const std::string &archive_path) {
        return potato_restore(handle_, archive_path.c_str()) == 0;
    }

    /// Flushes all buffered INSERT data to Parquet files.
    ///
    /// Returns true on success, false on error (check `last_error()`).
    bool flush() {
        return potato_flush(handle_) == 0;
    }

    /// Flushes buffered INSERT data for a single table.
    ///
    /// Returns true on success, false on error (check `last_error()`).
    bool flush_table(const std::string &table_name) {
        return potato_flush_table(handle_, table_name.c_str()) == 0;
    }

    /// Returns number of Parquet files currently backing the table.
    Expected<int64_t> parquet_file_count(const std::string &table_name) {
        long long value = potato_table_parquet_file_count(handle_, table_name.c_str());
        if (value < 0) {
            const char *err = potato_last_error(handle_);
            return Expected<int64_t>::err(
                err ? std::string(err) : "Unknown table stats error");
        }
        return Expected<int64_t>(static_cast<int64_t>(value));
    }

    /// Returns total bytes of Parquet files currently backing the table.
    Expected<int64_t> table_total_bytes(const std::string &table_name) {
        long long value = potato_table_total_bytes(handle_, table_name.c_str());
        if (value < 0) {
            const char *err = potato_last_error(handle_);
            return Expected<int64_t>::err(
                err ? std::string(err) : "Unknown table stats error");
        }
        return Expected<int64_t>(static_cast<int64_t>(value));
    }

    /// Returns age (seconds) of the oldest Parquet file for the table.
    Expected<int64_t> table_oldest_file_age_secs(const std::string &table_name) {
        long long value = potato_table_oldest_file_age_secs(handle_, table_name.c_str());
        if (value < 0) {
            const char *err = potato_last_error(handle_);
            return Expected<int64_t>::err(
                err ? std::string(err) : "Unknown table stats error");
        }
        return Expected<int64_t>(static_cast<int64_t>(value));
    }

    // ── Metadata introspection ──────────────────────────────

    /// Returns whether the database is inside a BEGIN transaction.
    bool in_transaction() const noexcept {
        return potato_in_transaction(handle_);
    }

    /// Returns the data directory / URL for the database.
    std::string data_url() const {
        char *s = potato_data_url(handle_);
        if (!s) return {};
        std::string out(s);
        potato_string_free(s);
        return out;
    }

    /// Returns a list of all table names.
    StringList table_names() const {
        return StringList(potato_table_names(handle_));
    }

    /// Returns a list of column names for the given table.
    StringList table_columns(const std::string &table_name) const {
        return StringList(potato_table_columns(handle_, table_name.c_str()));
    }

    /// Returns a list of all view names.
    StringList view_names() const {
        return StringList(potato_view_names(handle_));
    }

    /// Returns a list of all user-defined SQL function names.
    StringList function_names() const {
        return StringList(potato_function_names(handle_));
    }

    /// Returns a list of all indexes as (name, table) pairs.
    IndexList indexes() const {
        return IndexList(potato_indexes(handle_));
    }

    // ── Execute file ────────────────────────────────────────

    /// Executes all SQL statements from a `.sql` file.
    Expected<ResultList> execute_file(const std::string &path,
                                      bool continue_on_error = true) {
        potato_result_list *raw = potato_execute_file(
            handle_, path.c_str(), continue_on_error);
        if (!raw) {
            const char *err = potato_last_error(handle_);
            return Expected<ResultList>::err(
                err ? std::string(err) : "Failed to execute file");
        }
        return Expected<ResultList>(ResultList(raw));
    }

    // ── Streaming ───────────────────────────────────────────

    /// Executes a SQL statement and returns a streaming result.
    Expected<Stream> execute_stream(const std::string &sql) {
        potato_stream *raw = potato_execute_stream(handle_, sql.c_str());
        if (!raw) {
            const char *err = potato_last_error(handle_);
            return Expected<Stream>::err(
                err ? std::string(err) : "Unknown streaming error");
        }
        return Expected<Stream>(Stream(raw));
    }

    // ── Recent queries ──────────────────────────────────────

    /// Returns a handle to the recent query log.
    QueryLog recent_queries() const {
        return QueryLog(potato_recent_queries(handle_));
    }

    /// Returns recent CDC events as a result set.
    Expected<Result> recent_cdc() {
        potato_result *raw = potato_recent_cdc(handle_);
        if (!raw) {
            const char *err = potato_last_error(handle_);
            return Expected<Result>::err(
                err ? std::string(err) : "Failed to read recent CDC events");
        }
        return Expected<Result>(Result(raw));
    }

    /// Returns the last error message, or an empty string.
    std::string last_error() const {
        const char *e = potato_last_error(handle_);
        return e ? std::string(e) : std::string();
    }

private:
    explicit Database(potato_db *h) noexcept : handle_(h) {}
    potato_db *handle_ = nullptr;
};

} // namespace potato

#endif /* POTATODB_HPP */
