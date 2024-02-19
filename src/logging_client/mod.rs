use std::collections::HashMap;
use std::mem;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::time::{self, MissedTickBehavior};

type EventBuffer = Arc<Mutex<HashMap<String, Vec<HashMap<String, f64>>>>>;

pub struct LoggingClient {
    // Table -> Rows
    events: EventBuffer,
    shutdown: Arc<AtomicBool>,
    flushed: Arc<AtomicBool>,
}

struct BackgroundWorker {
    client: reqwest::Client,
    url: String,
    flush_interval: Duration,
    events: EventBuffer,
    shutdown: Arc<AtomicBool>,
    flushed: Arc<AtomicBool>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DataBatch {
    pub table: String,
    pub rows: Vec<HashMap<String, f64>>,
}

impl LoggingClient {
    pub fn new(flush_interval: Duration, locustdb_url: &str) -> LoggingClient {
        let buffer = EventBuffer::default();
        let shutdown = Arc::new(AtomicBool::new(false));
        let flushed = Arc::new(AtomicBool::new(false));
        let worker = BackgroundWorker {
            client: reqwest::Client::new(),
            flush_interval,
            url: format!("{locustdb_url}/insert_bin"),
            events: buffer.clone(),
            shutdown: shutdown.clone(),
            flushed: flushed.clone(),
        };
        tokio::spawn(worker.run());

        LoggingClient {
            events: buffer,
            shutdown,
            flushed,
        }
    }

    pub fn log(&mut self, table: &str, mut row: HashMap<String, f64>) {
        let time_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64
            / 1000.0;
        row.insert("timestamp".to_string(), time_millis);
        let mut events = self.events.lock().unwrap();
        events.entry(table.to_string()).or_default().push(row);
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
        self.flush().await;
        self.flushed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    async fn flush(&self) {
        let buffer = mem::take(&mut *self.events.lock().unwrap());
        for (table, rows) in buffer.into_iter() {
            let nrows = rows.len();
            let data_batch = DataBatch { table: table.clone(), rows };
            let serialized = bincode::serialize(&data_batch).unwrap();
            log::debug!(
                "Pushing {} events to {} ({} bytes)",
                nrows,
                table,
                serialized.len(),
            );

            let result = self.client.post(&self.url).body(serialized).send().await;
            match result {
                Err(err) => {
                    log::warn!(
                        "Failed to send data batch for table {}, {} events dropped: {}",
                        data_batch.table,
                        data_batch.rows.len(),
                        err
                    )
                }
                Ok(response) => {
                    if let Err(err) = response.error_for_status_ref() {
                        log::warn!(
                            "Failed to send data batch for table {}, {} events dropped: {}",
                            data_batch.table,
                            data_batch.rows.len(),
                            err
                        )
                    }
                    log::debug!("{:?}", response);
                }
            }
        }
    }
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
