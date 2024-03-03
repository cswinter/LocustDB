use std::error::Error;
use std::path::PathBuf;
use std::str;
use std::sync::Arc;

use futures::channel::oneshot;

use crate::engine::query_task::QueryTask;
use crate::ingest::colgen::GenTable;
use crate::ingest::csv_loader::{CSVIngestionTask, Options as LoadOptions};
use crate::logging_client::EventBuffer;
use crate::mem_store::*;
use crate::perf_counter::PerfCounter;
use crate::scheduler::*;
use crate::syntax::parser;
use crate::QueryError;
use crate::QueryResult;

// Cannot implement Clone on LocustDB without changing Drop implementation.
pub struct LocustDB {
    inner_locustdb: Arc<InnerLocustDB>,
}

impl LocustDB {
    pub fn memory_only() -> LocustDB {
        LocustDB::new(&Options::default())
    }

    pub fn new(opts: &Options) -> LocustDB {
        let locustdb = Arc::new(InnerLocustDB::new(opts));
        InnerLocustDB::start_worker_threads(&locustdb);
        LocustDB {
            inner_locustdb: locustdb,
        }
    }

    pub async fn run_query(
        &self,
        query: &str,
        explain: bool,
        rowformat: bool,
        show: Vec<usize>,
    ) -> Result<QueryResult, oneshot::Canceled> {
        let (sender, receiver) = oneshot::channel();

        // PERF: perform compilation and table snapshot in asynchronous task?
        let query = match parser::parse_query(query) {
            Ok(query) => query,
            Err(err) => return Ok(Err(err)),
        };

        let mut data = match self.inner_locustdb.snapshot(&query.table) {
            Some(data) => data,
            None => {
                return Ok(Err(QueryError::NotImplemented(format!(
                    "Table {} does not exist!",
                    &query.table
                ))))
            }
        };

        if self.inner_locustdb.opts().seq_disk_read {
            self.inner_locustdb
                .disk_read_scheduler()
                .schedule_sequential_read(
                    &mut data,
                    &query.find_referenced_cols(),
                    self.inner_locustdb.opts().readahead,
                );
            let ldb = self.inner_locustdb.clone();
            let (read_data, _) =
                <dyn Task>::from_fn(move || ldb.disk_read_scheduler().service_reads(&ldb));
            self.inner_locustdb.schedule(read_data);
        }

        let query_task = QueryTask::new(
            query,
            rowformat,
            explain,
            show,
            data,
            self.inner_locustdb.disk_read_scheduler().clone(),
            SharedSender::new(sender),
        );

        match query_task {
            Ok(task) => {
                self.schedule(task);
                Ok(receiver.await?)
            }
            Err(err) => Ok(Err(err)),
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

    pub async fn bulk_load(&self) -> Result<Vec<MemTreeTable>, oneshot::Canceled> {
        for table in self.inner_locustdb.full_snapshot() {
            self.inner_locustdb
                .disk_read_scheduler()
                .schedule_bulk_load(table, self.inner_locustdb.opts().readahead);
        }
        let mut receivers = Vec::new();
        for _ in 0..self.inner_locustdb.opts().read_threads {
            let ldb = self.inner_locustdb.clone();
            let (read_data, receiver) =
                <dyn Task>::from_fn(move || ldb.disk_read_scheduler().service_reads(&ldb));
            self.inner_locustdb.schedule(read_data);
            receivers.push(receiver);
        }
        for receiver in receivers {
            receiver.await?;
        }
        self.mem_tree(2).await
    }

    pub fn recover(&self) {
        self.inner_locustdb.drop_pending_tasks();
        InnerLocustDB::start_worker_threads(&self.inner_locustdb);
    }

    pub async fn mem_tree(&self, depth: usize) -> Result<Vec<MemTreeTable>, oneshot::Canceled> {
        let inner = self.inner_locustdb.clone();
        let (task, receiver) = <dyn Task>::from_fn(move || inner.mem_tree(depth));
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
        self.inner_locustdb.wal_flush();
    }

    pub fn evict_cache(&self) -> usize {
        self.inner_locustdb.evict_cache()
    }
}

#[derive(Clone)]
pub struct Options {
    pub threads: usize,
    pub read_threads: usize,
    pub db_v2_path: Option<PathBuf>,
    pub mem_size_limit_tables: usize,
    pub mem_lz4: bool,
    pub readahead: usize,
    pub seq_disk_read: bool,
    /// Maximum size of WAL in bytes before triggering compaction
    pub max_wal_size_bytes: u64,
    /// Maximum size of partition
    pub max_partition_size_bytes: u64,
    /// Combine partitions when the size of every original partition is less than this factor of the combined partition size
    pub partition_combine_factor: u64,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            threads: num_cpus::get(),
            read_threads: num_cpus::get(),
            db_v2_path: None,
            mem_size_limit_tables: 8 * 1024 * 1024 * 1024, // 8 GiB
            mem_lz4: true,
            readahead: 256 * 1024 * 1024, // 256 MiB
            seq_disk_read: false,
            max_wal_size_bytes: 64 * 1024 * 1024, // 64 MiB
            max_partition_size_bytes: 8 * 1024 * 1024, // 8 MiB
            partition_combine_factor: 4,
        }
    }
}

impl Drop for LocustDB {
    fn drop(&mut self) {
        self.inner_locustdb.stop();
    }
}
