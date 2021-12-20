#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;

use pyo3::{prelude::*, wrap_pyfunction};

use crate::logging_client::LoggingClient;

lazy_static! {
    static ref DEFAULT_CLIENT: LoggingClient =
        LoggingClient::new(std::time::Duration::from_secs(1), "http://localhost:8080");
}

/// Python API for the xprun experiment runner.
#[pymodule]
fn locustdb(_py: Python, m: &PyModule) -> PyResult<()> {
    env_logger::init();
    m.add_function(wrap_pyfunction!(log, m)?).unwrap();
    Ok(())
}

#[pyfunction]
fn log(metrics: HashMap<String, f64>) {
    println!("yay logging {:?}", metrics);
}
