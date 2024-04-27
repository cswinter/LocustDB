use locustdb_compression_utils::xor_float;
use locustdb_serialization::api::{
    AnyVal, Column, ColumnNameRequest, ColumnNameResponse, EncodingOpts, MultiQueryRequest,
    MultiQueryResponse,
};
use locustdb_serialization::event_buffer::{ColumnBuffer, ColumnData, EventBuffer, TableBuffer};
use reqwest::header::CONTENT_TYPE;
use std::collections::HashMap;
use std::sync::Once;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct Client {
    client: reqwest::Client,
    url: String,
    log_stats: bool,
}

static START: Once = Once::new();

#[wasm_bindgen]
impl Client {
    pub fn new(url: &str) -> Client {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();

        START.call_once(|| {
            wasm_logger::init(wasm_logger::Config::default());
        });

        Client {
            client: reqwest::Client::new(),
            url: url.to_string(),
            log_stats: true,
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
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?;
        let rsps: ColumnNameResponse = response
            .json()
            .await
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?;
        Ok(serde_wasm_bindgen::to_value(&rsps.columns).unwrap())
    }

    pub async fn multi_query(
        &self,
        queries: Vec<String>,
        binary: bool,
        compress: bool,
        mantissa: u32,
        full_precision_cols: Vec<String>,
    ) -> Result<JsValue, JsValue> {
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
                    full_precision_cols: full_precision_cols.into_iter().collect(),
                })
            } else {
                None
            },
        };
        let request_start_ms = performance.now();
        let response = self
            .client
            .post(&format!("{}/multi_query_cols", self.url))
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?
            .error_for_status()
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?;

        let first_response_ms = performance.now();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?
            .to_vec();
        let download_finished_ms = performance.now();

        let rsps = if binary {
            let mut rsps = MultiQueryResponse::deserialize(&bytes)
                .map_err(|e| JsValue::from_str(&format!("Failed to deserialize response: {}", e)))?
                .responses;
            rsps.iter_mut().for_each(|rsp| {
                rsp.columns.iter_mut().for_each(|(key, col)| {
                    let compressed_bytes = if self.log_stats { col.size_bytes() } else { 0 };
                    let coltype = match col {
                        Column::Float(_) => "float",
                        Column::Int(_) => "int",
                        Column::String(_) => "string",
                        Column::Mixed(_) => "mixed",
                        Column::Null(_) => "null",
                        Column::Xor(_) => "xor",
                    };
                    if let Column::Xor(compressed) = col {
                        *col = Column::Float(xor_float::double::decode(&compressed[..]).unwrap());
                    };
                    if self.log_stats {
                        log::info!(
                            "[{}; {}]  size: {}B  ratio: {: >2.2}x  {:2.2} B/row  {}",
                            coltype,
                            col.len(),
                            compressed_bytes,
                            col.size_bytes() as f64 / compressed_bytes as f64,
                            compressed_bytes as f64 / col.len().max(1) as f64,
                            key,
                        );
                    }
                });
            });
            log::info!(
                "waiting on server {:.2?} ms, downloading {:.2?} ms, deserializing {:.2?} ms",
                first_response_ms - request_start_ms,
                download_finished_ms - first_response_ms,
                performance.now() - download_finished_ms,
            );
            rsps
        } else {
            vec![]
        };
        Ok(serde_wasm_bindgen::to_value(&rsps).unwrap())
    }

    pub async fn insert(
        &self,
        table: &str,
        columns: Vec<String>,
        values: Vec<JsValue>,
    ) -> Result<(), JsValue> {
        let mut _columns = HashMap::new();
        for (name, value) in columns.into_iter().zip(values.iter()) {
            let val = js_value_to_any_val(value.clone())?;
            let mut buffer = ColumnBuffer {
                data: ColumnData::default(),
            };
            buffer.push(val, 0);
            _columns.insert(name, buffer);
        }
        let payload = EventBuffer {
            tables: [(table.to_string(), TableBuffer { len: 1, columns: _columns })]
                .iter()
                .cloned()
                .collect(),
        };
        let body = payload.serialize();
        self.client
            .post(&format!("{}/insert_bin", self.url))
            .body(body)
            .send()
            .await
            .map_err(|e| JsValue::from_str(&format!("{}", e)))?;
        Ok(())
    }
}

fn js_value_to_any_val(value: JsValue) -> Result<AnyVal, JsValue> {
    if let Some(value) = value.as_f64() {
        Ok(AnyVal::Float(value))
    } else if let Some(value) = value.as_string() {
        Ok(AnyVal::Str(value))
    } else {
        Err(JsValue::from_str(&format!(
            "unsupported type {:?} for value: {:?}",
            value.js_typeof(),
            value
        )))
    }
}
