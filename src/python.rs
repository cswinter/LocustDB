#![allow(clippy::too_many_arguments)]
#![allow(non_local_definitions)] // Try removing after PyO3 upgrade

use std::collections::HashMap;

use locustdb_compression_utils::xor_float;
use locustdb_serialization::api::{AnyVal, Column};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::logging_client::{BufferFullPolicy, LoggingClient};

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[pyclass]
struct Client {
    client: LoggingClient,
}

#[pymodule]
fn locustdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    env_logger::init();
    m.add_class::<Client>()?;
    Ok(())
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (url, max_buffer_size_bytes = 128 * (1 << 20), block_when_buffer_full = false, flush_interval_seconds = 1))]
    fn new(
        url: &str,
        max_buffer_size_bytes: usize,
        block_when_buffer_full: bool,
        flush_interval_seconds: u64,
    ) -> Self {
        let _guard = RT.enter();
        Self {
            client: LoggingClient::new(
                std::time::Duration::from_secs(flush_interval_seconds),
                url,
                max_buffer_size_bytes,
                if block_when_buffer_full {
                    BufferFullPolicy::Block
                } else {
                    BufferFullPolicy::Drop
                },
            ),
        }
    }

    fn log(&mut self, table: &str, metrics: HashMap<String, AnyValWrapper>) -> PyResult<()> {
        // Safe because AnyValWrapper is a transparent wrapper around AnyVal
        let metrics = unsafe {
            std::mem::transmute::<HashMap<String, AnyValWrapper>, HashMap<String, AnyVal>>(metrics)
        };
        self.client.log(table, metrics);
        Ok(())
    }

    fn multi_query(&self, py: Python, queries: Vec<String>) -> PyResult<PyObject> {
        let results = RT
            .block_on(self.client.multi_query(queries))
            .map_err(|e| PyErr::new::<PyException, _>(format!("{}", e)))?;
        let py_result = PyList::new_bound(
            py,
            results.into_iter().map(|result| {
                let columns = PyDict::new_bound(py);
                for (key, value) in result.columns {
                    columns.set_item(key, column_to_python(py, value)).unwrap();
                }
                columns
            }),
        );
        Ok(py_result.into_py(py))
    }

    fn query(&self, py: Python, query: String) -> PyResult<PyObject> {
        let result = RT
            .block_on(self.client.multi_query(vec![query]))
            .map_err(|e| PyErr::new::<PyException, _>(format!("{}", e)))?;
        assert_eq!(result.len(), 1);
        let columns = PyDict::new_bound(py);
        for (key, value) in result.into_iter().next().unwrap().columns {
            columns.set_item(key, column_to_python(py, value)).unwrap();
        }
        Ok(columns.into_py(py))
    }

    #[pyo3(signature = (table, pattern = None))]
    fn columns(&self, py: Python, table: String, pattern: Option<String>) -> PyResult<PyObject> {
        let response = RT
            .block_on(self.client.columns(table, pattern))
            .map_err(|e| PyErr::new::<PyException, _>(format!("{}", e)))?;
        Ok(response.columns.into_py(py))
    }
}

fn column_to_python(py: Python, column: Column) -> PyObject {
    match column {
        Column::Float(xs) => xs.into_py(py),
        Column::Int(xs) => xs.into_py(py),
        Column::String(xs) => xs.into_py(py),
        Column::Mixed(xs) => {
            PyList::new_bound(py, xs.iter().map(|x| any_to_python(py, x))).into_py(py)
        }
        Column::Null(n) => {
            PyList::new_bound(py, (0..n).map(|_| None::<()>.into_py(py))).into_py(py)
        }
        Column::Xor(xs) => xor_float::double::decode(&xs).unwrap().into_py(py),
    }
}

fn any_to_python(py: Python, value: &AnyVal) -> PyObject {
    match value {
        AnyVal::Int(i) => i.into_py(py),
        AnyVal::Float(f) => f.into_py(py),
        AnyVal::Str(s) => s.into_py(py),
        AnyVal::Null => None::<()>.into_py(py),
    }
}

#[repr(transparent)]
struct AnyValWrapper(AnyVal);

impl FromPyObject<'_> for AnyValWrapper {
    fn extract(ob: &PyAny) -> PyResult<Self> {
        if let Ok(i) = ob.extract::<i64>() {
            Ok(AnyValWrapper(AnyVal::Int(i)))
        } else if let Ok(f) = ob.extract::<f64>() {
            Ok(AnyValWrapper(AnyVal::Float(f)))
        } else if let Ok(s) = ob.extract::<String>() {
            Ok(AnyValWrapper(AnyVal::Str(s)))
        } else if ob.is_none() {
            Ok(AnyValWrapper(AnyVal::Null))
        } else {
            Err(PyErr::new::<PyException, _>("Invalid AnyVal"))
        }
    }
}
