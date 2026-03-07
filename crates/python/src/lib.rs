use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    DurationMicrosecondArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
    LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use futures::StreamExt;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::IntoPyObjectExt;
use tokio::runtime::Runtime;

use potatodb_engine::{PotatoDB, QueryResult, QueryResultStream, S3Config};

#[pyclass(name = "PotatoDB")]
struct PyPotatoDB {
    rt: Arc<Runtime>,
    db: Option<PotatoDB>,
}

#[pyclass(name = "PotatoStream", unsendable)]
struct PyPotatoStream {
    rt: Arc<Runtime>,
    inner: Option<QueryResultStream>,
}

impl PyPotatoDB {
    fn db_mut(&mut self) -> PyResult<&mut PotatoDB> {
        self.db
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("database is closed"))
    }

    fn db_ref(&self) -> PyResult<&PotatoDB> {
        self.db
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("database is closed"))
    }
}

#[pymethods]
impl PyPotatoDB {
    #[staticmethod]
    #[pyo3(signature = (path, s3_endpoint=None, s3_region=None, s3_allow_http=false))]
    fn open(
        path: String,
        s3_endpoint: Option<String>,
        s3_region: Option<String>,
        s3_allow_http: bool,
    ) -> PyResult<Self> {
        let rt = Arc::new(Runtime::new().map_err(pyerr)?);
        let s3_config = path.starts_with("s3://").then_some(S3Config {
            endpoint: s3_endpoint,
            region: s3_region,
            allow_http: s3_allow_http,
        });
        let db = rt.block_on(PotatoDB::new(path, s3_config)).map_err(pyerr)?;
        Ok(Self { rt, db: Some(db) })
    }

    fn execute(&mut self, py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let result = rt.block_on(db.execute(sql)).map_err(pyerr)?;
        query_result_to_python(py, result)
    }

    fn execute_readonly(&mut self, py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let result = rt.block_on(db.execute_readonly(sql)).map_err(pyerr)?;
        query_result_to_python(py, result)
    }

    #[pyo3(signature = (path, continue_on_error=false))]
    fn execute_file(
        &mut self,
        py: Python<'_>,
        path: &str,
        continue_on_error: bool,
    ) -> PyResult<Py<PyAny>> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let entries = rt
            .block_on(db.execute_file(path, continue_on_error))
            .map_err(pyerr)?;

        let out = PyList::empty(py);
        for (sql, result) in entries {
            let item = PyDict::new(py);
            item.set_item("sql", sql)?;
            match result {
                Ok(query_result) => {
                    item.set_item("result", query_result_to_python(py, query_result)?)?;
                    item.set_item("error", py.None())?;
                }
                Err(err) => {
                    item.set_item("result", py.None())?;
                    item.set_item("error", err.to_string())?;
                }
            }
            out.append(item)?;
        }
        out.into_py_any(py)
    }

    fn execute_stream(&mut self, sql: &str) -> PyResult<PyPotatoStream> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let stream = rt.block_on(db.execute_stream(sql)).map_err(pyerr)?;
        Ok(PyPotatoStream {
            rt,
            inner: Some(stream),
        })
    }

    fn in_transaction(&self) -> PyResult<bool> {
        Ok(self.db_ref()?.in_transaction())
    }

    fn data_url(&self) -> PyResult<String> {
        Ok(self.db_ref()?.data_url().to_string())
    }

    fn table_names(&self) -> PyResult<Vec<String>> {
        Ok(self.db_ref()?.table_names())
    }

    fn table_columns(&self, table_name: &str) -> PyResult<Vec<String>> {
        Ok(self.db_ref()?.table_columns(table_name))
    }

    fn view_names(&self) -> PyResult<Vec<String>> {
        Ok(self.db_ref()?.view_names())
    }

    fn function_names(&self) -> PyResult<Vec<String>> {
        Ok(self.db_ref()?.function_names())
    }

    fn indexes(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let out = PyList::empty(py);
        for (name, table) in self.db_ref()?.indexes() {
            let row = PyDict::new(py);
            row.set_item("name", name)?;
            row.set_item("table", table)?;
            out.append(row)?;
        }
        out.into_py_any(py)
    }

    fn prepare(&mut self, name: &str, sql: &str) -> PyResult<bool> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let prepare_sql = format!("PREPARE {name} AS {sql}");
        rt.block_on(db.execute(&prepare_sql)).map_err(pyerr)?;
        Ok(true)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn execute_prepared(
        &mut self,
        py: Python<'_>,
        name: &str,
        params: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let params_part = params.join(", ");
        let sql = format!("EXECUTE {name}({params_part})");
        let result = rt.block_on(db.execute(&sql)).map_err(pyerr)?;
        query_result_to_python(py, result)
    }

    fn backup(&mut self, archive_path: &str) -> PyResult<bool> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        rt.block_on(db.backup(archive_path)).map_err(pyerr)?;
        Ok(true)
    }

    fn restore(&mut self, archive_path: &str) -> PyResult<bool> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        rt.block_on(db.restore(archive_path)).map_err(pyerr)?;
        Ok(true)
    }

    fn flush(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let result = rt.block_on(db.execute("FLUSH;")).map_err(pyerr)?;
        query_result_to_python(py, result)
    }

    fn flush_table(&mut self, py: Python<'_>, table_name: &str) -> PyResult<Py<PyAny>> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let escaped_table_name = table_name.replace('"', "\"\"");
        let sql = format!("FLUSH TABLE \"{escaped_table_name}\";");
        let result = rt.block_on(db.execute(&sql)).map_err(pyerr)?;
        query_result_to_python(py, result)
    }

    fn table_parquet_file_count(&self, table_name: &str) -> PyResult<usize> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_ref()?;
        rt.block_on(db.parquet_file_count(table_name))
            .map_err(pyerr)
    }

    fn table_total_bytes(&self, table_name: &str) -> PyResult<u64> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_ref()?;
        rt.block_on(db.table_total_bytes(table_name)).map_err(pyerr)
    }

    fn table_oldest_file_age_secs(&self, table_name: &str) -> PyResult<u64> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_ref()?;
        rt.block_on(db.table_oldest_file_age_secs(table_name))
            .map_err(pyerr)
    }

    fn recent_queries(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let out = PyList::empty(py);
        for entry in self.db_ref()?.recent_queries() {
            let row = PyDict::new(py);
            row.set_item("sql", entry.sql)?;
            row.set_item("duration_ms", entry.duration.as_millis())?;
            row.set_item("rows", entry.rows)?;
            out.append(row)?;
        }
        out.into_py_any(py)
    }

    fn recent_cdc(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let rt = Arc::clone(&self.rt);
        let db = self.db_mut()?;
        let result = rt
            .block_on(db.execute("SELECT * FROM potatodb_cdc"))
            .map_err(pyerr)?;
        query_result_to_python(py, result)
    }

    fn close(&mut self) {
        self.db = None;
    }
}

#[pymethods]
impl PyPotatoStream {
    #[allow(clippy::missing_const_for_fn)]
    fn is_message(&self) -> bool {
        matches!(self.inner, Some(QueryResultStream::Message(_)))
    }

    fn message(&self) -> Option<String> {
        match &self.inner {
            Some(QueryResultStream::Message(msg)) => Some(msg.clone()),
            _ => None,
        }
    }

    fn next_batch(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let rt = Arc::clone(&self.rt);
        let mut exhausted = false;

        let next = match self.inner.as_mut() {
            None | Some(QueryResultStream::Message(_)) => None,
            Some(QueryResultStream::Stream(stream)) => match rt.block_on(stream.next()) {
                Some(Ok(batch)) => Some(batches_to_python(py, std::slice::from_ref(&batch))?),
                Some(Err(err)) => return Err(pyerr(err)),
                None => {
                    exhausted = true;
                    None
                }
            },
        };

        if exhausted {
            self.inner = None;
        }
        Ok(next)
    }

    #[allow(clippy::missing_const_for_fn)]
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.next_batch(py)
    }
}

#[pymodule]
fn potatodb_python(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPotatoDB>()?;
    m.add_class::<PyPotatoStream>()?;
    Ok(())
}

fn query_result_to_python(py: Python<'_>, result: QueryResult) -> PyResult<Py<PyAny>> {
    match result {
        QueryResult::Message(msg) => msg.into_py_any(py),
        QueryResult::Records(batches) => batches_to_python(py, &batches),
    }
}

fn batches_to_python(
    py: Python<'_>,
    batches: &[arrow::record_batch::RecordBatch],
) -> PyResult<Py<PyAny>> {
    let rows = PyList::empty(py);
    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let obj = PyDict::new(py);
            for (idx, field) in schema.fields().iter().enumerate() {
                let value = array_value_to_py(py, batch.column(idx).as_ref(), row)?;
                obj.set_item(field.name(), value)?;
            }
            rows.append(obj)?;
        }
    }
    rows.into_py_any(py)
}

fn array_value_to_py(py: Python<'_>, array: &dyn Array, row: usize) -> PyResult<Py<PyAny>> {
    if array.is_null(row) {
        return Ok(py.None());
    }
    if let Some(a) = array.as_any().downcast_ref::<Int32Array>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<Int64Array>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<Float64Array>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<BooleanArray>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<StringArray>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeStringArray>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(PyBytes::new(py, a.value(row)).into_any().unbind());
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(PyBytes::new(py, a.value(row)).into_any().unbind());
    }
    if let Some(a) = array.as_any().downcast_ref::<DurationMicrosecondArray>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<Date32Array>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<Date64Array>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return a.value(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return a.value(row).saturating_mul(1_000).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        return a.value(row).saturating_mul(1_000_000).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return (a.value(row) / 1_000).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<Decimal128Array>() {
        return a.value_as_string(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<Decimal256Array>() {
        return a.value_as_string(row).into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        let value = a.value(row);
        if value.len() == 16 {
            let uuid = format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                value[0],
                value[1],
                value[2],
                value[3],
                value[4],
                value[5],
                value[6],
                value[7],
                value[8],
                value[9],
                value[10],
                value[11],
                value[12],
                value[13],
                value[14],
                value[15]
            );
            return uuid.into_py_any(py);
        }
        return Ok(PyBytes::new(py, value).into_any().unbind());
    }
    if let Some(a) = array.as_any().downcast_ref::<ListArray>() {
        let values = a.value(row);
        let out = PyList::empty(py);
        for i in 0..values.len() {
            out.append(array_value_to_py(py, values.as_ref(), i)?)?;
        }
        return out.into_py_any(py);
    }
    if let Some(a) = array.as_any().downcast_ref::<LargeListArray>() {
        let values = a.value(row);
        let out = PyList::empty(py);
        for i in 0..values.len() {
            out.append(array_value_to_py(py, values.as_ref(), i)?)?;
        }
        return out.into_py_any(py);
    }
    format!("{:?}", array.slice(row, 1)).into_py_any(py)
}

fn pyerr<E: std::fmt::Display>(err: E) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}
