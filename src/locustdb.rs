use std::error::Error;
use std::path::PathBuf;
use std::str;
use std::sync::Arc;

use futures::channel::oneshot;
use locustdb_serialization::event_buffer::EventBuffer;

use crate::engine::query_task::QueryTask;
use crate::ingest::colgen::GenTable;
use crate::ingest::csv_loader::{CSVIngestionTask, Options as LoadOptions};
use crate::observability::{metrics, PerfCounter};
use crate::scheduler::*;
use crate::syntax::parser;
use crate::QueryError;
use crate::QueryResult;
use crate::{mem_store::*, BasicTypeColumn};

// Cannot implement Clone on LocustDB without changing Drop implementation.
pub struct LocustDB {
    inner_locustdb: Arc<InnerLocustDB>,
}

impl LocustDB {
    pub fn memory_only() -> LocustDB {
        LocustDB::new(&Options::default())
    }

    pub fn new(opts: &Options) -> LocustDB {
        opts.validate().expect("Invalid options");
        LocustDB {
            inner_locustdb: InnerLocustDB::new(opts),
        }
    }

    pub async fn run_query(
        &self,
        query: &str,
        explain: bool,
        rowformat: bool,
        show: Vec<usize>,
    ) -> QueryResult {
        let (sender, receiver) = oneshot::channel();
        metrics::QUERY_COUNT.inc();

        // PERF: perform compilation and table snapshot in asynchronous task?
        let query = match parser::parse_query(query) {
            Ok(query) => query,
            Err(err) => return Err(err),
        };

        let referenced_cols = query.find_referenced_cols();
        let colsvec;
        let all_cols = if referenced_cols.contains("*") {
            let colnames = self
                .inner_locustdb
                .schedule_query_column_names(&query.table)?
                .await?;
            match colnames {
                Ok(results) => match &results.columns[..] {
                    [(_, BasicTypeColumn::String(names))] => Some(names.to_vec()),
                    _ => {
                        return Err(fatal!(
                            "Expected string column when querying _meta_columns_{}, got {:?}",
                            query.table,
                            results
                        ))
                    }
                },
                Err(err) => return Err(err),
            }
        } else {
            None
        };
        let column_filter = if referenced_cols.contains("*") {
            None
        } else {
            colsvec = referenced_cols.into_iter().collect::<Vec<_>>();
            Some(&colsvec[..])
        };
        let data = match self.inner_locustdb.snapshot(&query.table, column_filter) {
            Some(data) => data,
            None => {
                return Err(QueryError::NotImplemented(format!(
                    "Table {} does not exist!",
                    &query.table
                )))
            }
        };

        let query_task = QueryTask::new(
            query,
            rowformat,
            explain,
            show,
            data,
            self.inner_locustdb.disk_read_scheduler().clone(),
            SharedSender::new(sender),
            self.inner_locustdb.opts().batch_size,
            all_cols,
        );

        match query_task {
            Ok(task) => {
                self.schedule(task);
                let result = receiver.await?;
                metrics::QUERY_OK_COUNT.inc();
                result
            }
            Err(err) => {
                metrics::QUERY_ERROR_COUNT.inc();
                Err(err)
            }
        }
    }

    pub async fn load_csv(&self, options: LoadOptions) -> Result<(), Box<dyn Error>> {
        let (sender, receiver) = oneshot::channel();
        let task = CSVIngestionTask::new(
            options,
            self.inner_locustdb.clone(),
            SharedSender::new(sender),
        );
        self.schedule(task);
        Ok(receiver.await??)
    }

    pub async fn ingest_efficient(&self, events: EventBuffer) {
        self.inner_locustdb.ingest_efficient(events);
    }

    pub async fn gen_table(&self, opts: GenTable) -> Result<(), oneshot::Canceled> {
        let mut receivers = Vec::new();
        let opts = Arc::new(opts);
        for partition in 0..opts.partitions {
            let opts = opts.clone();
            let inner = self.inner_locustdb.clone();
            let (task, receiver) =
                <dyn Task>::from_fn(move || inner.gen_partition(&opts, partition as u64));
            self.schedule(task);
            receivers.push(receiver);
        }
        for receiver in receivers {
            receiver.await?;
        }
        Ok(())
    }

    pub fn ast(&self, query: &str) -> String {
        match parser::parse_query(query) {
            Ok(query) => format!("{:#?}", query),
            Err(err) => format!("{:?}", err),
        }
    }

    pub async fn search_column_names(
        &self,
        table: &str,
        query: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let query = format!(
            "SELECT column_name FROM \"_meta_columns_{}\" WHERE regex(column_name, '{}');",
            table, query
        );
        let result = self.run_query(&query, false, false, vec![]).await?;
        match &result.columns[..] {
            [(_, BasicTypeColumn::String(column_names))] => Ok(column_names.to_vec()),
            _ => Err(Box::new(fatal!(
                "Expected string column, got {:?}",
                result.columns
            ))),
        }
    }

    pub fn recover(&self) {
        self.inner_locustdb.drop_pending_tasks();
        InnerLocustDB::start_worker_threads(&self.inner_locustdb);
    }

    pub async fn mem_tree(
        &self,
        depth: usize,
        table: Option<String>,
    ) -> Result<Vec<MemTreeTable>, oneshot::Canceled> {
        let inner = self.inner_locustdb.clone();
        let (task, receiver) = <dyn Task>::from_fn(move || inner.mem_tree(depth, table.clone()));
        self.schedule(task);
        receiver.await
    }

    pub async fn table_stats(&self) -> Result<Vec<TableStats>, oneshot::Canceled> {
        let inner = self.inner_locustdb.clone();
        let (task, receiver) = <dyn Task>::from_fn(move || inner.stats());
        self.schedule(task);
        receiver.await
    }

    pub fn schedule<T: Task + 'static>(&self, task: T) {
        self.inner_locustdb.schedule(task)
    }

    pub fn perf_counter(&self) -> &PerfCounter {
        self.inner_locustdb.perf_counter()
    }

    pub fn force_flush(&self) {
        let inner = self.inner_locustdb.clone();
        inner.trigger_wal_flush();
    }

    pub fn evict_cache(&self) -> usize {
        self.inner_locustdb.evict_cache()
    }
}

#[derive(Clone)]
pub struct Options {
    pub threads: usize,
    pub read_threads: usize,
    pub db_path: Option<PathBuf>,
    pub mem_size_limit_tables: usize,
    pub mem_lz4: bool,
    pub readahead: usize,
    /// Maximum size of WAL in bytes before triggering compaction
    pub max_wal_size_bytes: u64,
    /// Maximum size of partition
    pub max_partition_size_bytes: u64,
    /// Combine partitions when the size of every original partition is less than this factor of the combined partition size
    pub partition_combine_factor: u64,
    /// Maximum length of temporary buffer used in streaming stages during query execution
    pub batch_size: usize,
    /// Maximum number of rows in a partitions. Not implemented.
    pub max_partition_length: usize,
    /// Number of parallel threads used during WAL flush table batching and compacting
    pub wal_flush_compaction_threads: usize,
    /// Internal metrics collection interval in seconds
    pub metrics_interval: u64,
    /// Internal metrics table name
    pub metrics_table_name: Option<String>,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            threads: num_cpus::get(),
            read_threads: num_cpus::get(),
            db_path: None,
            mem_size_limit_tables: 8 * 1024 * 1024 * 1024, // 8 GiB
            mem_lz4: true,
            readahead: 256 * 1024 * 1024,              // 256 MiB
            max_wal_size_bytes: 64 * 1024 * 1024,      // 64 MiB
            max_partition_size_bytes: 8 * 1024 * 1024, // 8 MiB
            partition_combine_factor: 4,
            batch_size: 1024,
            max_partition_length: 1024 * 1024,
            wal_flush_compaction_threads: 1,
            metrics_interval: 15,
            metrics_table_name: Some("_metrics".to_string()),
        }
    }
}

impl Options {
    fn validate(&self) -> Result<(), String> {
        if self.threads == 0 {
            return Err("threads must be greater than 0".to_string());
        }
        if self.read_threads == 0 {
            return Err("read_threads must be greater than 0".to_string());
        }
        // if self.partition_combine_factor == 0 {
        //     return Err("partition_combine_factor must be greater than 0".to_string());
        // }
        if self.batch_size % 8 != 0 {
            return Err("batch_size must be a multiple of 8".to_string());
        }
        Ok(())
    }
}

impl Drop for LocustDB {
    fn drop(&mut self) {
        self.inner_locustdb.stop();
    }
}
