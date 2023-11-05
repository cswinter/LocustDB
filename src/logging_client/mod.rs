use std::collections::HashMap;
use std::mem;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::time::{self, MissedTickBehavior};

type EventBuffer = Arc<Mutex<HashMap<String, Vec<HashMap<String, f64>>>>>;

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
pub struct DataBatch {
    pub table: String,
    pub rows: Vec<HashMap<String, f64>>,
}

impl LoggingClient {
    pub fn new(flush_interval: Duration, locustdb_url: &str) -> LoggingClient {
        let buffer = EventBuffer::default();
        let worker = BackgroundWorker {
            client: reqwest::Client::new(),
            flush_interval,
            url: format!("{locustdb_url}/insert_bin"),
            events: buffer.clone(),
        };
        tokio::spawn(worker.run());

        LoggingClient { events: buffer }
    }

    pub fn log(&mut self, table: &str, mut row: HashMap<String, f64>) {
        let time_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as f64 / 1000.0;
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
            log::debug!("TICK!");
            let buffer = mem::take(&mut *self.events.lock().unwrap());
            for (table, rows) in buffer.into_iter() {
                fn bytes_in_row(row: &HashMap<String, f64>) -> usize {
                    row.iter().map(|(k, v)| k.len() + v.to_string().len()).sum::<usize>()
                }
                let bytes = rows.iter().map(bytes_in_row).sum::<usize>();
                log::debug!("Pushing {} events to {} ({} bytes)", rows.len(), table, bytes);

                let data_batch = DataBatch { table, rows };
                let body = bincode::serialize(&data_batch).unwrap();

                let mut s = String::new();
                for b in &body[0..64] {
                    s.push_str(&format!("{:02x}", *b));
                }
                log::info!("Inserting bytes: {} {}", s, body.len());
                bincode::deserialize::<DataBatch>(&body[..]).unwrap();

                let result = self.client.post(&self.url).body(body).send().await;
                match result {
                    Err(err) => {
                        log::warn!(
                            "Failed to send data batch for table {}, {} events dropped: {}",
                            data_batch.table,
                            data_batch.rows.len(),
                            err
                        )
                    },
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
}
