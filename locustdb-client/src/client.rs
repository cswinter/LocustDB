use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use locustdb_compression_utils::column;
use std::collections::HashMap;
use std::mem;

use super::log;

#[wasm_bindgen]
pub struct Client {
    client: reqwest::Client,
    url: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ColumnNameRequest {
    tables: Vec<String>,
    pattern: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ColumnNameResponse {
    columns: Vec<String>,
    offset: usize,
    len: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct MultiQueryRequest {
    queries: Vec<String>,
    encoding_opts: Option<EncodingOpts>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EncodingOpts {
    pub xor_float_compression: bool,
    pub mantissa: Option<u32>,
    pub full_precision_cols: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub columns: HashMap<String, column::Column>,
}

#[wasm_bindgen]
impl Client {
    pub fn new(url: &str) -> Client {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        Client {
            client: reqwest::Client::new(),
            url: url.to_string(),
        }
    }

    pub async fn request_columns(&self, table: &str) -> Result<JsValue, JsValue> {
        let request_body = ColumnNameRequest {
            tables: vec![table.to_string()],
            pattern: None,
            offset: None,
            limit: None,
        };
        //let start_time = Instant::now();
        let response = self
            .client
            .post(&format!("{}/columns", self.url))
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await
            .unwrap();
        let rsps: ColumnNameResponse = response.json().await.unwrap();
        Ok(serde_wasm_bindgen::to_value(&rsps.columns).unwrap())
    }

    pub async fn multi_query(&self, queries: Vec<String>, binary: bool, compress: bool, mantissa: u32, full_precision_cols: Vec<String>) -> Result<JsValue, JsValue> {
        let window = web_sys::window().expect("should have a window in this context");
        let performance = window
            .performance()
            .expect("performance should be available");

        let request_body = MultiQueryRequest {
            queries,
            encoding_opts: if binary {
                Some(EncodingOpts {
                    xor_float_compression: compress,
                    mantissa: Some(mantissa),
                    full_precision_cols,
                }) } else { None },
        };
        let request_start_ms = performance.now();
        let response = self
            .client
            .post(&format!("{}/multi_query_cols", self.url))
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await
            .unwrap();
        let first_response_ms = performance.now();
        let bytes = response.bytes().await.unwrap().to_vec();
        let download_finished_ms = performance.now();

        let rsps = if binary {
            let mut rsps: Vec<QueryResponse> = bincode::deserialize(&bytes).unwrap();
            rsps.iter_mut().for_each(|rsp| {
                rsp.columns.iter_mut().for_each(|(key, col)| {
                    let coltype = match col {
                        column::Column::Float(_) => "float",
                        column::Column::Int(_) => "int",
                        column::Column::String(_) => "string",
                        column::Column::Mixed(_) => "mixed",
                        column::Column::Null(_) => "null",
                        column::Column::Xor(_) => "xor",
                    };
                    log(&format!("decompressing column {key} of type {coltype}"));
                    *col = mem::replace(col, column::Column::Null(0)).decompress();
                });
            });
            log(&format!(
                "waiting on server {:.2?} ms, downloading {:.2?} ms, deserializing {:.2?} ms",
                first_response_ms - request_start_ms,
                download_finished_ms - first_response_ms,
                performance.now() - download_finished_ms,
            ));
            rsps
        } else {
            vec![]
        };
        Ok(serde_wasm_bindgen::to_value(&rsps).unwrap())
    }
}