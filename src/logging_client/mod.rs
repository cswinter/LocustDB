use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

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

pub struct LoggingClient {
    // Table -> Rows
    events: Arc<Mutex<EventBuffer>>,
    shutdown: CancellationToken,
    flushed: Arc<(Mutex<bool>, Condvar)>,
    buffer_size: Arc<AtomicU64>,
    max_buffer_size_bytes: usize,
    pub total_events: u64,
    flush_interval: Duration,
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

impl LoggingClient {
    pub fn new(flush_interval: Duration, locustdb_url: &str, max_buffer_size_bytes: usize) -> LoggingClient {
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
        }
    }

    pub fn log<Row: IntoIterator<Item = (String, f64)>>(&mut self, table: &str, row: Row) {
        let time_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64
            / 1000.0;
        let mut warncount = 0;
        loop {
            if self.buffer_size.load(std::sync::atomic::Ordering::SeqCst) as usize > self.max_buffer_size_bytes  {
                if warncount % 10 == 0 {
                    log::warn!("Logging buffer full, waiting for flush");
                }
                warncount += 1;
                std::thread::sleep(self.flush_interval);
            } else {
                break;
            }
        }
        let mut events = self.events.lock().unwrap();
        let table = events.tables.entry(table.to_string()).or_default();
        for (column_name, value) in row {
            self.buffer_size.fetch_add(8, std::sync::atomic::Ordering::SeqCst);
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
        self.buffer_size.store(0, std::sync::atomic::Ordering::SeqCst);
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
        let (flushed, cvar) = &*self.flushed;
        let mut flushed = flushed.lock().unwrap();
        while !*flushed {
            flushed = cvar.wait(flushed).unwrap();
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
