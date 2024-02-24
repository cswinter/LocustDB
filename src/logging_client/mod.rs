use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::time::{self, MissedTickBehavior};

#[derive(Default, Serialize, Deserialize)]
pub struct EventBuffer {
    pub tables: HashMap<String, TableBuffer>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct TableBuffer {
    pub len: u64,
    pub columns: HashMap<String, ColumnBuffer>,
}

#[derive(Default, Serialize, Deserialize)]
pub struct ColumnBuffer {
    pub column_name: String,
    pub data: ColumnData,
}

#[derive(Serialize, Deserialize)]
pub enum ColumnData {
    Dense(Vec<f64>),
    Sparse(Vec<(u64, f64)>),
}

pub struct LoggingClient {
    // Table -> Rows
    events: Arc<Mutex<EventBuffer>>,
    shutdown: Arc<AtomicBool>,
    flushed: Arc<AtomicBool>,
    pub total_events: u64,
}

struct BackgroundWorker {
    client: reqwest::Client,
    url: String,
    flush_interval: Duration,
    events: Arc<Mutex<EventBuffer>>,
    shutdown: Arc<AtomicBool>,
    flushed: Arc<AtomicBool>,
    request_data: Arc<Mutex<Option<Vec<u8>>>>,
}

impl LoggingClient {
    pub fn new(flush_interval: Duration, locustdb_url: &str) -> LoggingClient {
        let buffer: Arc<Mutex<EventBuffer>> = Arc::default();
        let shutdown = Arc::new(AtomicBool::new(false));
        let flushed = Arc::new(AtomicBool::new(false));
        let worker = BackgroundWorker {
            client: reqwest::Client::new(),
            flush_interval,
            url: format!("{locustdb_url}/insert_bin"),
            events: buffer.clone(),
            shutdown: shutdown.clone(),
            flushed: flushed.clone(),
            request_data: Arc::default(),
        };
        tokio::spawn(worker.run());

        LoggingClient {
            events: buffer,
            shutdown,
            flushed,
            total_events: 0,
        }
    }

    pub fn log<Row: IntoIterator<Item = (String, f64)>>(&mut self, table: &str, row: Row) {
        let time_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64
            / 1000.0;
        let mut events = self.events.lock().unwrap();
        let table = events.tables.entry(table.to_string()).or_default();
        for (column_name, value) in row {
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
        while !self.shutdown.load(std::sync::atomic::Ordering::SeqCst) {
            interval.tick().await;
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
        self.flushed
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn create_request_data(&self) {
        let mut buffer = self.events.lock().unwrap();
        let mut request_data = self.request_data.lock().unwrap();
        if request_data.is_some() {
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
        let serialized = bincode::serialize(&*buffer).unwrap();
        if buffer.tables.is_empty() {
            return;
        }
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
        // TODO: not holding lock, could result in reordering of events (issue is that MutexGuard is not sent and can't be held across await point)
        self.create_request_data();
        let request_data = self.request_data.lock().unwrap().clone();
        if let Some(request_data) = request_data {
            let bytes = request_data.len();
            let result = self.client.post(&self.url).body(request_data).send().await;
            match result {
                Err(err) => {
                    log::warn!("Failed to send data batch ({} B): {}", bytes, err);
                }
                Ok(response) => {
                    if let Err(err) = response.error_for_status_ref() {
                        log::warn!("Failed to send data batch ({} B): {}", bytes, err);
                    } else {
                        self.request_data.lock().unwrap().take();
                        log::info!("Sent data");
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
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        while !self.flushed.load(std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100));
        }
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
