#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    Ok(())
}

#[pyfunction]
fn log(table: &str, metrics: HashMap<String, f64>) -> PyResult<()> {
    let mut client = DEFAULT_CLIENT.lock().unwrap();
    client.log(table, metrics);
    Ok(())
}
