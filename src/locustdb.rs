use std::str;
use std::sync::Arc;
use std::error::Error;
use std::path::{Path, PathBuf};

use futures::channel::oneshot;
use num_cpus;

use crate::QueryError;
use crate::QueryResult;
use crate::disk_store::interface::*;
use crate::disk_store::noop_storage::NoopStorage;
use crate::engine::query_task::QueryTask;
use crate::ingest::colgen::GenTable;
use crate::ingest::csv_loader::{CSVIngestionTask, Options as LoadOptions};
use crate::mem_store::*;
use crate::scheduler::*;
use crate::syntax::parser;


pub struct LocustDB {
    inner_locustdb: Arc<InnerLocustDB>
}

impl LocustDB {
    pub fn memory_only() -> LocustDB {
        LocustDB::new(&Options::default())
    }

    pub fn new(opts: &Options) -> LocustDB {
        let disk_store = opts.db_path.as_ref()
            .map(|path| LocustDB::persistent_storage(path))
            .unwrap_or_else(|| Arc::new(NoopStorage));
        let locustdb = Arc::new(InnerLocustDB::new(disk_store, opts));
        InnerLocustDB::start_worker_threads(&locustdb);
        LocustDB { inner_locustdb: locustdb }
    }

    pub async fn run_query(&self, query: &str, explain: bool, show: Vec<usize>) -> Result<QueryResult, oneshot::Canceled> {
        let (sender, receiver) = oneshot::channel();

        // PERF: perform compilation and table snapshot in asynchronous task?
        let query = match parser::parse_query(query) {
            Ok(query) => query,
            Err(err) => return Ok(Err(err)),
        };

        let mut data = match self.inner_locustdb.snapshot(&query.table) {
            Some(data) => data,
            None => return Ok(Err(
                QueryError::NotImplemented(format!("Table {} does not exist!", &query.table)))),
        };

        if self.inner_locustdb.opts().seq_disk_read {
            self.inner_locustdb.disk_read_scheduler()
                .schedule_sequential_read(&mut data,
                                          &query.find_referenced_cols(),
                                          self.inner_locustdb.opts().readahead);
            let ldb = self.inner_locustdb.clone();
            let (read_data, _) = Task::from_fn(move || ldb.disk_read_scheduler().service_reads(&ldb));
            let _ = self.inner_locustdb.schedule(read_data);
        }

        let query_task = QueryTask::new(
            query, explain, show, data,
            self.inner_locustdb.disk_read_scheduler().clone(),
            SharedSender::new(sender)
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
            SharedSender::new(sender));
        let _ = self.schedule(task);
        Ok(receiver.await??)
    }

    pub async fn gen_table(&self, opts: GenTable) -> Result<(), oneshot::Canceled> {
        let mut receivers = Vec::new();
        let opts = Arc::new(opts);
        for partition in 0..opts.partitions {
            let opts = opts.clone();
            let inner = self.inner_locustdb.clone();
            let (task, receiver) = Task::from_fn(move || inner.gen_partition(&opts, partition as u64));
            let _ = self.schedule(task);
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
            self.inner_locustdb.disk_read_scheduler()
                .schedule_bulk_load(table,
                                    self.inner_locustdb.opts().readahead);
        }
        let mut receivers = Vec::new();
        for _ in 0..self.inner_locustdb.opts().read_threads {
            let ldb = self.inner_locustdb.clone();
            let (read_data, receiver) = Task::from_fn(move || ldb.disk_read_scheduler().service_reads(&ldb));
            let _ = self.inner_locustdb.schedule(read_data);
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
        let (task, receiver) = Task::from_fn(move || inner.mem_tree(depth));
        let _ = self.schedule(task);
        receiver.await
    }

    pub async fn table_stats(&self) -> Result<Vec<TableStats>, oneshot::Canceled> {
        let inner = self.inner_locustdb.clone();
        let (task, receiver) = Task::from_fn(move || inner.stats());
        let _ = self.schedule(task);
        receiver.await
    }

    pub fn schedule<T: Task + 'static>(&self, task: T) {
        self.inner_locustdb.schedule(task)
    }

    #[cfg(feature = "enable_rocksdb")]
    pub fn persistent_storage<P: AsRef<Path>>(db_path: P) -> Arc<dyn DiskStore> {
        use crate::disk_store::rocksdb;
        Arc::new(rocksdb::RocksDB::new(db_path))
    }

    #[cfg(not(feature = "enable_rocksdb"))]
    pub fn persistent_storage<P: AsRef<Path>>(_: P) -> Arc<dyn DiskStore> {
        panic!("RocksDB storage backend is not enabled in this build of LocustDB. Create db with `memory_only`, or set the `enable_rocksdb` feature.")
    }
}

impl Drop for LocustDB {
    fn drop(&mut self) {
        self.inner_locustdb.stop();
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
    pub seq_disk_read: bool,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            threads: num_cpus::get(),
            read_threads: num_cpus::get(),
            db_path: None,
            mem_size_limit_tables: 8 * 1024 * 1024 * 1024, // 8 GiB
            mem_lz4: true,
            readahead: 256 * 1024 * 1024, // 256 MiB
            seq_disk_read: false,
        }
    }
}

