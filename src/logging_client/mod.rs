use std::collections::HashMap;
use std::mem;
use std::os::unix::thread;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::fmt;

use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::server::{ColumnNameRequest, EncodingOpts, MultiQueryRequest, QueryResponse};
use locustdb_compression_utils::column;

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct EventBuffer {
    pub tables: HashMap<String, TableBuffer>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct TableBuffer {
    pub len: u64,
    pub columns: HashMap<String, ColumnBuffer>,
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct ColumnBuffer {
    pub data: ColumnData,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ColumnData {
    Dense(Vec<f64>),
    Sparse(Vec<(u64, f64)>),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnNameResponse {
    pub columns: Vec<String>,
    pub offset: usize,
    pub len: usize,
}

pub struct LoggingClient {
    // Table -> Rows
    events: Arc<Mutex<EventBuffer>>,
    shutdown: CancellationToken,
    flushed: Arc<(Mutex<bool>, Condvar)>,
    buffer_size: Arc<AtomicU64>,
    max_buffer_size_bytes: usize,
    pub total_events: u64,
    flush_interval: Duration,
    query_client: reqwest::Client,
    query_url: String,
    columns_url: String,
    buffer_full_policy: BufferFullPolicy,
}

struct BackgroundWorker {
    client: reqwest::Client,
    url: String,
    flush_interval: Duration,
    events: Arc<Mutex<EventBuffer>>,
    shutdown: CancellationToken,
    flushed: Arc<(Mutex<bool>, Condvar)>,
    buffer_size: Arc<AtomicU64>,
    request_data: Arc<Mutex<Option<Vec<u8>>>>,
}

#[derive(Debug)]
pub enum Error {
    Reqwest(reqwest::Error),
    Bincode(bincode::Error),
    Client { status_code: u16, msg: String },
}

pub enum BufferFullPolicy {
    Block,
    Drop,
}

impl LoggingClient {
    pub fn new(
        flush_interval: Duration,
        locustdb_url: &str,
        max_buffer_size_bytes: usize,
        buffer_full_policy: BufferFullPolicy,
    ) -> LoggingClient {
        let buffer: Arc<Mutex<EventBuffer>> = Arc::default();
        let shutdown = CancellationToken::new();
        let flushed = Arc::new((Mutex::new(false), Condvar::new()));
        let buffer_size = Arc::new(AtomicU64::new(0));
        let worker = BackgroundWorker {
            client: reqwest::Client::new(),
            flush_interval,
            url: format!("{locustdb_url}/insert_bin"),
            events: buffer.clone(),
            shutdown: shutdown.clone(),
            flushed: flushed.clone(),
            buffer_size: buffer_size.clone(),
            request_data: Arc::default(),
        };
        tokio::spawn(worker.run());

        LoggingClient {
            events: buffer,
            shutdown,
            flushed,
            total_events: 0,
            max_buffer_size_bytes,
            buffer_size,
            flush_interval,
            query_client: reqwest::Client::new(),
            query_url: format!("{locustdb_url}/multi_query_cols"),
            columns_url: format!("{locustdb_url}/columns"),
            buffer_full_policy,
        }
    }

    pub async fn multi_query(&self, queries: Vec<String>) -> Result<Vec<QueryResponse>, Error> {
        let request_body = MultiQueryRequest {
            queries,
            encoding_opts: Some(EncodingOpts {
                xor_float_compression: true,
                mantissa: None,
                full_precision_cols: Default::default(),
            }),
        };
        let response = self
            .query_client
            .post(&self.query_url)
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await?;
        if response.status().is_client_error() {
            let status_code = response.status().as_u16();
            let msg = response.text().await?;
            return Err(Error::Client { status_code, msg });
        }
        let bytes = response.bytes().await?.to_vec();
        let mut rsps: Vec<QueryResponse> = bincode::deserialize(&bytes).unwrap();
        rsps.iter_mut().for_each(|rsp| {
            rsp.columns.iter_mut().for_each(|(_, col)| {
                *col = mem::replace(col, column::Column::Null(0)).decompress();
            });
        });
        Ok(rsps)
    }

    pub fn log<Row: IntoIterator<Item = (String, f64)>>(&mut self, table: &str, row: Row) {
        let time_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64
            / 1000.0;
        let mut warncount = 0;
        loop {
            if self.buffer_size.load(std::sync::atomic::Ordering::SeqCst) as usize
                > self.max_buffer_size_bytes
            {
                match self.buffer_full_policy {
                    BufferFullPolicy::Block => {
                        if warncount % 10 == 0 {
                            log::warn!("Logging buffer full, waiting for flush");
                        }
                        warncount += 1;
                        std::thread::sleep(self.flush_interval);
                    }
                    BufferFullPolicy::Drop => {
                        log::warn!("Dropping event due to full buffer");
                        return;
                    }
                }
            } else {
                break;
            }
        }
        let mut events = self.events.lock().unwrap();
        let table = events.tables.entry(table.to_string()).or_default();
        for (column_name, value) in row {
            self.buffer_size
                .fetch_add(8, std::sync::atomic::Ordering::SeqCst);
            table
                .columns
                .entry(column_name.to_string())
                .or_default()
                .push(value, table.len);
        }
        table
            .columns
            .entry("timestamp".to_string())
            .or_default()
            .push(time_millis, table.len);
        table.len += 1;
        self.total_events += 1;
    }

    pub async fn columns(
        &self,
        table: String,
        pattern: Option<String>,
    ) -> Result<ColumnNameResponse, Error> {
        let request_body = ColumnNameRequest {
            tables: vec![table],
            pattern,
            offset: None,
            limit: None,
        };
        let response = self
            .query_client
            .post(&self.columns_url)
            .header(CONTENT_TYPE, "application/json")
            .json(&request_body)
            .send()
            .await?;
        if response.status().is_client_error() {
            let status_code = response.status().as_u16();
            let msg = response.text().await?;
            return Err(Error::Client { status_code, msg });
        }
        Ok(response.json::<ColumnNameResponse>().await?)
    }
}

impl BackgroundWorker {
    async fn run(self) {
        log::debug!("Starting log worker...");
        let mut interval = time::interval(self.flush_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        while !self.shutdown.is_cancelled() {
            select!(_ = interval.tick() => (), _ = self.shutdown.cancelled() => break);
            log::debug!("TICK!");
            self.flush().await;
        }
        loop {
            self.flush().await;
            if self.request_data.lock().unwrap().is_none() {
                break;
            }
        }
        self.flush().await;

        let (flushed, cvar) = &*self.flushed;
        *flushed.lock().unwrap() = true;
        cvar.notify_all();
    }

    fn create_request_data(&self) {
        let mut buffer = self.events.lock().unwrap();
        let mut request_data = self.request_data.lock().unwrap();
        if request_data.is_some() {
            return;
        }
        let serialized = bincode::serialize(&*buffer).unwrap();
        if buffer.tables.is_empty() {
            return;
        }
        log::info!(
            "Creating request data for {} events",
            buffer
                .tables
                .values()
                .map(|t| t.columns.values().next().map(|c| c.data.len()).unwrap_or(0))
                .sum::<usize>()
        );
        self.buffer_size
            .store(0, std::sync::atomic::Ordering::SeqCst);
        // TODO: keep all buffers on client, but don't send tables that are empty
        buffer.tables.clear();
        // for table in buffer.tables.values_mut() {
        //     for column in table.columns.values_mut() {
        //         column.clear();
        //     }
        // }
        *request_data = Some(serialized);
    }

    async fn flush(&self) {
        // TODO: not holding lock, could result in reordering of events (issue is that MutexGuard is not `Send` and can't be held across await point)
        self.create_request_data();
        let request_data = self.request_data.lock().unwrap().clone();
        if let Some(request_data) = request_data {
            let bytes = request_data.len();
            log::info!("Sending data ({} B)", bytes);
            let result = self.client.post(&self.url).body(request_data).send().await;
            match result {
                Err(err) => {
                    log::warn!("Failed to send data batch ({} B): {}", bytes, err);
                    let backoff = time::Duration::from_secs(1);
                    tokio::time::sleep(backoff).await;
                }
                Ok(response) => {
                    if let Err(err) = response.error_for_status_ref() {
                        log::warn!("Failed to send data batch ({} B): {}", bytes, err);
                        let backoff = time::Duration::from_secs(1);
                        tokio::time::sleep(backoff).await;
                    } else {
                        self.request_data.lock().unwrap().take();
                        log::info!("Succesfully sent data batch ({} B)", bytes);
                    }
                    log::debug!("{:?}", response);
                }
            }
        }
    }
}

impl ColumnBuffer {
    fn push(&mut self, value: f64, len: u64) {
        match &mut self.data {
            ColumnData::Dense(data) => {
                if data.len() as u64 == len {
                    data.push(value)
                } else {
                    let mut sparse_data: Vec<(u64, f64)> = data
                        .drain(..)
                        .enumerate()
                        .map(|(i, v)| (i as u64, v))
                        .collect();
                    sparse_data.push((len, value));
                    self.data = ColumnData::Sparse(sparse_data);
                }
            }
            ColumnData::Sparse(data) => data.push((len, value)),
        }
    }

    // fn clear(&mut self) {
    //     match &mut self.data {
    //         ColumnData::Dense(data) => data.clear(),
    //         ColumnData::Sparse(_) => {
    //             self.data = ColumnData::Dense(Vec::new());
    //         }
    //     }
    // }
}

impl Drop for LoggingClient {
    fn drop(&mut self) {
        self.shutdown.cancel();
        match self.buffer_full_policy {
            BufferFullPolicy::Block => {
                let (flushed, cvar) = &*self.flushed;
                let mut flushed = flushed.lock().unwrap();
                while !*flushed {
                    flushed = cvar.wait(flushed).unwrap();
                }
            }
            BufferFullPolicy::Drop => {
                // Wait for 1 minute for the buffer to flush
                let mut max_tries = 6;
                while max_tries > 0 {
                    max_tries -= 1;
                    std::thread::sleep(Duration::from_secs(10));
                    if *self.flushed.0.lock().unwrap() {
                        break;
                    }
                }
                log::warn!("Logging buffer not flushed, potentially dropping events");
            }
        }
        log::info!("Logging client dropped");
    }
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            ColumnData::Dense(data) => data.len(),
            ColumnData::Sparse(data) => data.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            ColumnData::Dense(data) => data.is_empty(),
            ColumnData::Sparse(data) => data.is_empty(),
        }
    }
}

impl Default for ColumnData {
    fn default() -> Self {
        ColumnData::Dense(Vec::new())
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::Reqwest(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Error::Bincode(err)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Reqwest(err) => write!(f, "Reqwest error: {}", err),
            Error::Bincode(err) => write!(f, "Bincode error: {}", err),
            Error::Client { status_code, msg } => {
                write!(f, "Client error ({}): {}", status_code, msg)
            }
        }
    }
}