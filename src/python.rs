#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use locustdb_compression_utils::column::{Column, Mixed};
use pyo3::exceptions::PyException;
use pyo3::types::{PyDict, PyList};
use pyo3::{prelude::*, wrap_pyfunction};

use crate::logging_client::LoggingClient;

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
    static ref DEFAULT_CLIENT: Arc<Mutex<LoggingClient>> = {
        let _guard = RT.enter();
        Arc::new(Mutex::new(LoggingClient::new(
            std::time::Duration::from_secs(1),
            "http://localhost:8080",
            128 * (1 << 20),
        )))
    };
}

#[pymodule]
fn locustdb(_py: Python, m: &PyModule) -> PyResult<()> {
    env_logger::init();
    m.add_function(wrap_pyfunction!(self::log, m)?).unwrap();
    m.add_function(wrap_pyfunction!(self::query, m)?).unwrap();
    Ok(())
}

#[pyfunction]
fn log(table: &str, metrics: HashMap<String, f64>) -> PyResult<()> {
    let mut client = DEFAULT_CLIENT.lock().unwrap();
    client.log(table, metrics);
    Ok(())
}

#[pyfunction]
fn query(py: Python, queries: Vec<String>) -> PyResult<PyObject> {
    let client = DEFAULT_CLIENT.lock().unwrap();
    let results = RT
        .block_on(client.multi_query(queries))
        .map_err(|e| PyErr::new::<PyException, _>(format!("{:?}", e)))?;
    let py_result = PyList::new(
        py,
        results.into_iter().map(|result| {
            let columns = PyDict::new(py);
            for (key, value) in result.columns {
                columns.set_item(key, column_to_python(py, value)).unwrap();
            }
            columns
        }),
    );
    Ok(py_result.into_py(py))
}

fn column_to_python(py: Python, column: Column) -> PyObject {
    match column {
        Column::Float(xs) => xs.into_py(py),
        Column::Int(xs) => xs.into_py(py),
        Column::String(xs) => xs.into_py(py),
        Column::Mixed(xs) => PyList::new(py, xs.iter().map(|x| mixed_to_python(py, x))).into_py(py),
        Column::Null(n) => n.into_py(py),
        Column::Xor(xs) => xs.into_py(py),
    }
}

fn mixed_to_python(py: Python, value: &Mixed) -> PyObject {
    match value {
        Mixed::Int(i) => i.into_py(py),
        Mixed::Float(f) => f.into_py(py),
        Mixed::Str(s) => s.into_py(py),
        Mixed::Null => None::<()>.into_py(py),
    }
}
