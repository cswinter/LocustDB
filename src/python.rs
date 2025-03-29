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
    #[pyo3(signature = (url, max_buffer_size_bytes = 128 * (1 << 20), block_when_buffer_full = false, flush_interval_seconds = 1, bearer_token = None))]
    fn new(
        url: &str,
        max_buffer_size_bytes: usize,
        block_when_buffer_full: bool,
        flush_interval_seconds: u64,
        bearer_token: Option<String>,
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
                bearer_token,
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

    fn log_batch(
        &mut self,
        table: &str,
        metrics: HashMap<String, Vec<AnyValWrapper>>,
    ) -> PyResult<()> {
        let len = metrics.values().next().map(|x| x.len()).unwrap_or(0);
        assert!(metrics.values().all(|x| x.len() == len));
        let mut batch = metrics
            .keys()
            .map(|k| (k.clone(), AnyVal::Null))
            .collect::<HashMap<_, _>>();
        for i in 0..len {
            for (k, v) in &metrics {
                *batch.get_mut(k).unwrap() = v[i].0.clone();
            }
            self.client.log(table, batch.clone());
        }
        Ok(())
    }

    fn multi_query<'a>(&self, py: Python<'a>, queries: Vec<String>) -> PyResult<Bound<'a, PyList>> {
        let results = RT
            .block_on(self.client.multi_query(queries))
            .map_err(|e| PyErr::new::<PyException, _>(format!("{}", e)))?;
        PyList::new(
            py,
            results.into_iter().map(|result| {
                let columns = PyDict::new(py);
                for (key, value) in result.columns {
                    columns.set_item(key, column_to_python(py, value)).unwrap();
                }
                columns
            }),
        )
    }

    fn query<'a>(&self, py: Python<'a>, query: String) -> PyResult<Bound<'a, PyDict>> {
        let result = RT
            .block_on(self.client.multi_query(vec![query]))
            .map_err(|e| PyErr::new::<PyException, _>(format!("{}", e)))?;
        assert_eq!(result.len(), 1);
        let columns = PyDict::new(py);
        for (key, value) in result.into_iter().next().unwrap().columns {
            columns.set_item(key, column_to_python(py, value)).unwrap();
        }
        Ok(columns)
    }

    #[pyo3(signature = (table, pattern = None))]
    fn columns<'a>(
        &self,
        py: Python<'a>,
        table: String,
        pattern: Option<String>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let response = RT
            .block_on(self.client.columns(table, pattern))
            .map_err(|e| PyErr::new::<PyException, _>(format!("{}", e)))?;
        response.columns.into_pyobject(py)
    }
}

fn column_to_python(py: Python, column: Column) -> PyObject {
    match column {
        Column::Float(xs) => xs.into_pyobject(py).unwrap().into(),
        Column::Int(xs) => xs.into_pyobject(py).unwrap().into(),
        Column::String(xs) => xs.into_pyobject(py).unwrap().into(),
        Column::Mixed(xs) => PyList::new(py, xs.iter().map(|x| any_to_python(py, x)))
            .unwrap()
            .into_pyobject(py)
            .unwrap()
            .into(),
        Column::Null(n) => PyList::new(py, (0..n).map(|_| None::<()>.into_pyobject(py).unwrap()))
            .unwrap()
            .into_pyobject(py)
            .unwrap()
            .into(),
        Column::Xor(xs) => xor_float::double::decode(&xs)
            .unwrap()
            .into_pyobject(py)
            .unwrap()
            .into(),
    }
}

fn any_to_python(py: Python, value: &AnyVal) -> PyObject {
    match value {
        AnyVal::Int(i) => i.into_pyobject(py).unwrap().into(),
        AnyVal::Float(f) => f.into_pyobject(py).unwrap().into(),
        AnyVal::Str(s) => s.into_pyobject(py).unwrap().into(),
        AnyVal::Null => None::<()>.into_pyobject(py).unwrap().into(),
    }
}

#[repr(transparent)]
struct AnyValWrapper(AnyVal);

impl<'py> FromPyObject<'py> for AnyValWrapper {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let val = if let Ok(i) = ob.extract::<i64>() {
            AnyVal::Int(i)
        } else if let Ok(f) = ob.extract::<f64>() {
            if f.is_nan() {
                AnyVal::Null
            } else if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                AnyVal::Int(f as i64)
            } else {
                AnyVal::Float(f)
            }
        } else if let Ok(s) = ob.extract::<String>() {
            AnyVal::Str(s)
        } else if ob.is_none() {
            AnyVal::Null
        } else {
            return Err(PyErr::new::<PyException, _>("Invalid AnyVal"));
        };
        Ok(AnyValWrapper(val))
    }
}
