"""Tests for the potatodb_python bindings."""

import os
import tempfile

import pytest

from potatodb_python import PotatoDB, PotatoStream


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "test_db")


@pytest.fixture()
def db(db_path):
    d = PotatoDB.open(db_path)
    yield d
    d.close()


@pytest.fixture()
def populated_db(db):
    """Database pre-loaded with a small ``items`` table."""
    db.execute(
        "CREATE TABLE items (id INT, name VARCHAR, price DOUBLE, active BOOLEAN);"
    )
    db.execute(
        """INSERT INTO items VALUES
            (1, 'Widget',  9.99,  true),
            (2, 'Gadget',  24.95, true),
            (3, 'Gizmo',   14.50, false);"""
    )
    return db


# ---------------------------------------------------------------------------
# Open / close
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_open_creates_database(self, db_path):
        d = PotatoDB.open(db_path)
        assert d is not None
        d.close()

    def test_close_is_idempotent(self, db_path):
        d = PotatoDB.open(db_path)
        d.close()
        d.close()  # should not raise

    def test_execute_after_close_raises(self, db_path):
        d = PotatoDB.open(db_path)
        d.close()
        with pytest.raises(RuntimeError, match="closed"):
            d.execute("SELECT 1;")

    def test_data_url(self, db, db_path):
        url = db.data_url()
        assert os.path.basename(url) == os.path.basename(db_path)


# ---------------------------------------------------------------------------
# Basic execute
# ---------------------------------------------------------------------------


class TestExecute:
    def test_ddl_returns_string(self, db):
        result = db.execute("CREATE TABLE t (x INT);")
        assert isinstance(result, str)

    def test_select_returns_list_of_dicts(self, populated_db):
        rows = populated_db.execute("SELECT * FROM items ORDER BY id;")
        assert isinstance(rows, list)
        assert len(rows) == 3
        assert rows[0]["id"] == 1
        assert rows[0]["name"] == "Widget"
        assert isinstance(rows[0]["price"], float)
        assert rows[0]["active"] is True

    def test_aggregate_query(self, populated_db):
        rows = populated_db.execute(
            "SELECT COUNT(*) AS cnt, ROUND(AVG(price), 2) AS avg FROM items;"
        )
        assert rows[0]["cnt"] == 3

    def test_insert_and_query(self, db):
        db.execute("CREATE TABLE t (v INT);")
        db.execute("INSERT INTO t VALUES (42);")
        rows = db.execute("SELECT v FROM t;")
        assert rows[0]["v"] == 42

    def test_invalid_sql_raises(self, db):
        with pytest.raises(RuntimeError):
            db.execute("NOT VALID SQL AT ALL;")


# ---------------------------------------------------------------------------
# Execute readonly
# ---------------------------------------------------------------------------


class TestExecuteReadonly:
    def test_select_works(self, populated_db):
        rows = populated_db.execute_readonly("SELECT * FROM items ORDER BY id;")
        assert len(rows) == 3

    def test_mutation_rejected(self, populated_db):
        with pytest.raises(RuntimeError, match="readonly"):
            populated_db.execute_readonly("INSERT INTO items VALUES (4, 'X', 1.0, true);")


# ---------------------------------------------------------------------------
# Execute file
# ---------------------------------------------------------------------------


class TestExecuteFile:
    def test_runs_sql_file(self, db, tmp_path):
        sql_file = tmp_path / "setup.sql"
        sql_file.write_text(
            "CREATE TABLE ef (id INT);\n"
            "INSERT INTO ef VALUES (1);\n"
            "INSERT INTO ef VALUES (2);\n"
            "SELECT * FROM ef ORDER BY id;\n"
        )
        results = db.execute_file(str(sql_file))
        assert isinstance(results, list)
        assert len(results) == 4
        for entry in results:
            assert "sql" in entry
            assert "result" in entry
            assert "error" in entry

        last = results[-1]
        assert last["error"] is None
        assert isinstance(last["result"], list)
        assert len(last["result"]) == 2

    def test_continue_on_error(self, db, tmp_path):
        sql_file = tmp_path / "partial.sql"
        sql_file.write_text(
            "CREATE TABLE ok1 (id INT);\n"
            "DEFINITELY NOT SQL;\n"
            "CREATE TABLE ok2 (id INT);\n"
        )
        results = db.execute_file(str(sql_file), continue_on_error=True)
        assert len(results) == 3
        assert results[0]["error"] is None
        assert results[1]["error"] is not None
        assert results[2]["error"] is None

    def test_stop_on_error_default(self, db, tmp_path):
        sql_file = tmp_path / "stop.sql"
        sql_file.write_text(
            "BAD SQL;\n"
            "CREATE TABLE should_not_run (id INT);\n"
        )
        results = db.execute_file(str(sql_file))
        assert len(results) == 1
        assert results[0]["error"] is not None

    def test_nonexistent_file_raises(self, db):
        with pytest.raises(RuntimeError):
            db.execute_file("/no/such/file.sql")


# ---------------------------------------------------------------------------
# Execute stream
# ---------------------------------------------------------------------------


class TestExecuteStream:
    def test_select_stream(self, populated_db):
        stream = populated_db.execute_stream("SELECT * FROM items ORDER BY id;")
        assert isinstance(stream, PotatoStream)
        assert not stream.is_message()
        assert stream.message() is None

        all_rows = []
        for batch in stream:
            assert isinstance(batch, list)
            all_rows.extend(batch)
        assert len(all_rows) == 3

    def test_ddl_stream(self, db):
        stream = db.execute_stream("CREATE TABLE st (x INT);")
        assert stream.is_message()
        msg = stream.message()
        assert isinstance(msg, str)
        assert stream.next_batch() is None

    def test_exhausted_stream_returns_none(self, populated_db):
        stream = populated_db.execute_stream("SELECT id FROM items;")
        while stream.next_batch() is not None:
            pass
        assert stream.next_batch() is None

    def test_stream_error_sql(self, db):
        with pytest.raises(RuntimeError):
            db.execute_stream("SELECT * FROM nonexistent_table;")


# ---------------------------------------------------------------------------
# Metadata introspection
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_table_names(self, populated_db):
        names = populated_db.table_names()
        assert "items" in names

    def test_table_columns(self, populated_db):
        cols = populated_db.table_columns("items")
        assert cols == ["id", "name", "price", "active"]

    def test_table_columns_nonexistent(self, db):
        assert db.table_columns("no_table") == []

    def test_view_names(self, populated_db):
        populated_db.execute(
            "CREATE VIEW expensive AS SELECT * FROM items WHERE price > 15;"
        )
        assert "expensive" in populated_db.view_names()

    def test_function_names(self, populated_db):
        populated_db.execute(
            "CREATE FUNCTION double_it(x INT) RETURNS INT AS x * 2;"
        )
        assert "double_it" in populated_db.function_names()

    def test_indexes(self, populated_db):
        populated_db.execute("CREATE INDEX idx_name ON items (name);")
        idxs = populated_db.indexes()
        assert isinstance(idxs, list)
        idx_names = [i["name"] for i in idxs]
        assert "idx_name" in idx_names
        idx = next(i for i in idxs if i["name"] == "idx_name")
        assert idx["table"] == "items"

    def test_in_transaction(self, db):
        assert db.in_transaction() is False
        db.execute("CREATE TABLE tx (id INT);")
        db.execute("BEGIN;")
        assert db.in_transaction() is True
        db.execute("COMMIT;")
        assert db.in_transaction() is False


# ---------------------------------------------------------------------------
# Prepared statements
# ---------------------------------------------------------------------------


class TestPrepared:
    def test_prepare_and_execute(self, populated_db):
        assert populated_db.prepare("find_item", "SELECT name FROM items WHERE id = $1")
        rows = populated_db.execute_prepared("find_item", ["1"])
        assert len(rows) == 1
        assert rows[0]["name"] == "Widget"

    def test_execute_prepared_multiple_params(self, populated_db):
        populated_db.prepare("price_range", "SELECT name FROM items WHERE price >= $1 AND price <= $2")
        rows = populated_db.execute_prepared("price_range", ["10", "25"])
        names = {r["name"] for r in rows}
        assert "Gadget" in names
        assert "Gizmo" in names
        assert "Widget" not in names

    def test_unknown_prepared_raises(self, db):
        with pytest.raises(RuntimeError):
            db.execute_prepared("nonexistent", ["1"])


# ---------------------------------------------------------------------------
# Flush & storage stats
# ---------------------------------------------------------------------------


class TestFlushAndStats:
    def test_flush(self, populated_db):
        result = populated_db.flush()
        assert isinstance(result, str)

    def test_flush_table(self, populated_db):
        result = populated_db.flush_table("items")
        assert isinstance(result, str)

    def test_parquet_file_count(self, populated_db):
        populated_db.flush()
        count = populated_db.table_parquet_file_count("items")
        assert count >= 1

    def test_total_bytes(self, populated_db):
        populated_db.flush()
        total = populated_db.table_total_bytes("items")
        assert total > 0

    def test_oldest_file_age(self, populated_db):
        populated_db.flush()
        age = populated_db.table_oldest_file_age_secs("items")
        assert age >= 0


# ---------------------------------------------------------------------------
# Query log
# ---------------------------------------------------------------------------


class TestQueryLog:
    def test_recent_queries(self, populated_db):
        populated_db.execute("SELECT 1 AS one;")
        log = populated_db.recent_queries()
        assert isinstance(log, list)
        assert len(log) > 0
        entry = log[-1]
        assert "sql" in entry
        assert "duration_ms" in entry
        assert "rows" in entry
        assert entry["duration_ms"] >= 0


# ---------------------------------------------------------------------------
# CDC
# ---------------------------------------------------------------------------


class TestCdc:
    def test_recent_cdc(self, populated_db):
        result = populated_db.recent_cdc()
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Backup / restore
# ---------------------------------------------------------------------------


class TestBackupRestore:
    def test_backup_and_restore(self, db, db_path, tmp_path):
        db.execute("CREATE TABLE br (val INT);")
        db.execute("INSERT INTO br VALUES (42);")
        db.flush()

        archive = str(tmp_path / "backup.tar.gz")
        assert db.backup(archive) is True
        assert os.path.isfile(archive)

        db.execute("DROP TABLE br;")
        assert "br" not in db.table_names()

        assert db.restore(archive) is True
        rows = db.execute("SELECT val FROM br;")
        assert rows[0]["val"] == 42

    def test_backup_invalid_path(self, db):
        db.execute("CREATE TABLE t (x INT);")
        db.flush()
        with pytest.raises(RuntimeError):
            db.backup("/nonexistent/deeply/nested/backup.tar.gz")


# ---------------------------------------------------------------------------
# Column type coverage
# ---------------------------------------------------------------------------


class TestColumnTypes:
    def test_null_values(self, db):
        db.execute("CREATE TABLE n (v INT);")
        db.execute("INSERT INTO n VALUES (NULL);")
        rows = db.execute("SELECT v FROM n;")
        assert rows[0]["v"] is None

    def test_boolean_values(self, db):
        db.execute("CREATE TABLE b (v BOOLEAN);")
        db.execute("INSERT INTO b VALUES (true), (false);")
        rows = db.execute("SELECT v FROM b ORDER BY v;")
        assert rows[0]["v"] is False
        assert rows[1]["v"] is True

    def test_varchar_values(self, db):
        db.execute("CREATE TABLE s (v VARCHAR);")
        db.execute("INSERT INTO s VALUES ('hello'), ('world');")
        rows = db.execute("SELECT v FROM s ORDER BY v;")
        assert rows[0]["v"] == "hello"
        assert rows[1]["v"] == "world"

    def test_double_values(self, db):
        db.execute("CREATE TABLE d (v DOUBLE);")
        db.execute("INSERT INTO d VALUES (3.14);")
        rows = db.execute("SELECT v FROM d;")
        assert abs(rows[0]["v"] - 3.14) < 1e-9

    def test_bigint_values(self, db):
        db.execute("CREATE TABLE bi (v BIGINT);")
        db.execute("INSERT INTO bi VALUES (9999999999);")
        rows = db.execute("SELECT v FROM bi;")
        assert rows[0]["v"] == 9999999999
