use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::time::{self, MissedTickBehavior};

type EventBuffer = Arc<Mutex<HashMap<String, Vec<HashMap<String, i64>>>>>;

pub struct LoggingClient {
    // Table -> Rows
    events: EventBuffer,
}

struct BackgroundWorker {
    client: reqwest::Client,
    url: String,
    flush_interval: Duration,
    events: EventBuffer,
}

#[derive(Serialize, Deserialize, Debug)]
struct DataBatch {
    pub table: String,
    pub rows: Vec<HashMap<String, i64>>,
}

impl LoggingClient {
    pub fn new(flush_interval: Duration, locustdb_url: &str) -> LoggingClient {
        let buffer = EventBuffer::default();
        let worker = BackgroundWorker {
            client: reqwest::Client::new(),
            flush_interval,
            url: format!("{}/insert", locustdb_url),
            events: buffer.clone(),
        };
        tokio::spawn(worker.run());

        LoggingClient { events: buffer }
    }

    pub fn log(&mut self, table: &str, mut row: HashMap<String, i64>) {
        let time_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
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
        loop {
            interval.tick().await;
            let buffer = mem::take(&mut *self.events.lock().unwrap());
            for (table, rows) in buffer.into_iter() {
                log::debug!("Pushing {} events to {}", rows.len(), table);
                let data_batch = DataBatch { table, rows };
                let result = self.client.post(&self.url).json(&data_batch).send().await;
                if let Err(err) = result {
                    log::warn!(
                        "Failed to send data batch for table {}, {} events dropped: {}",
                        data_batch.table,
                        data_batch.rows.len(),
                        err
                    );
                }
            }
        }
    }
}
