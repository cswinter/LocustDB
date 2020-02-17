use std::str;
use std::sync::Arc;

use futures_channel::oneshot;
use futures_core::*;
use futures_util::FutureExt;
use futures_util;
use futures_executor::block_on;
use num_cpus;

use QueryError;
use QueryResult;
use disk_store::interface::*;
use disk_store::noop_storage::NoopStorage;
use engine::query_task::QueryTask;
use ingest::colgen::GenTable;
use ingest::csv_loader::{CSVIngestionTask, Options as LoadOptions};
use mem_store::*;
use scheduler::*;
use syntax::parser;
use trace::{Trace, TraceBuilder};


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

    pub fn run_query(&self, query: &str, explain: bool, show: Vec<usize>) -> Box<dyn Future<Item=(QueryResult, Trace), Error=oneshot::Canceled>> {
        let (sender, receiver) = oneshot::channel();

        // PERF: perform compilation and table snapshot in asynchronous task?
        let query = match parser::parse_query(query) {
            Ok(query) => query,
            Err(err) => {
                return Box::new(future::ok(
                    (Err(err),
                     TraceBuilder::new("empty".to_owned()).finalize())));
            }
        };

        let mut data = match self.inner_locustdb.snapshot(&query.table) {
            Some(data) => data,
            None => return Box::new(future::ok((
                Err(QueryError::NotImplemented(format!("Table {} does not exist!", &query.table))),
                TraceBuilder::new("empty".to_owned()).finalize()))),
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

        match QueryTask::new(
            query, explain, show, data,
            self.inner_locustdb.disk_read_scheduler().clone(),
            SharedSender::new(sender)) {
                Ok(task) => {
                    let trace_receiver = self.schedule(task);
                    Box::new(receiver.join(trace_receiver))
                }
                Err(err) => {
                    Box::new(future::ok((Err(err), TraceBuilder::new("empty".to_owned()).finalize())))
                }
            }
    }

    pub fn load_csv(&self, options: LoadOptions) -> impl Future<Item=Result<(), String>, Error=oneshot::Canceled> {
        let (sender, receiver) = oneshot::channel();
        let task = CSVIngestionTask::new(
            options,
            self.inner_locustdb.clone(),
            SharedSender::new(sender));
        self.schedule(task);
        receiver
    }

    pub fn gen_table(&self, opts: GenTable) -> impl Future<Item=(), Error=oneshot::Canceled> {
        let mut receivers = Vec::new();
        let opts = Arc::new(opts);
        for partition in 0..opts.partitions {
            let opts = opts.clone();
            let inner = self.inner_locustdb.clone();
            let (task, receiver) = Task::from_fn(move || inner.gen_partition(&opts, partition as u64));
            self.schedule(task);
            receivers.push(receiver);
        }
        futures_util::future::join_all(receivers).map(|_| ())
    }

    pub fn ast(&self, query: &str) -> String {
        match parser::parse_query(query) {
            Ok(query) => format!("{:#?}", query),
            Err(err) => format!("{:?}", err),
        }
    }

    pub fn bulk_load(&self) -> impl Future<Item=Vec<MemTreeTable>, Error=oneshot::Canceled> {
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
            block_on(receiver).unwrap();
        }
        self.mem_tree(2)
    }

    pub fn recover(&self) {
        self.inner_locustdb.drop_pending_tasks();
        InnerLocustDB::start_worker_threads(&self.inner_locustdb);
    }

    pub fn mem_tree(&self, depth: usize) -> impl Future<Item=Vec<MemTreeTable>, Error=oneshot::Canceled> {
        let inner = self.inner_locustdb.clone();
        let (task, receiver) = Task::from_fn(move || inner.mem_tree(depth));
        self.schedule(task);
        receiver
    }

    pub fn table_stats(&self) -> impl Future<Item=Vec<TableStats>, Error=oneshot::Canceled> {
        let inner = self.inner_locustdb.clone();
        let (task, receiver) = Task::from_fn(move || inner.stats());
        self.schedule(task);
        receiver
    }

    fn schedule<T: Task + 'static>(&self, task: T) -> impl Future<Item=Trace, Error=oneshot::Canceled> {
        self.inner_locustdb.schedule(task)
    }

    #[cfg(feature = "enable_rocksdb")]
    pub fn persistent_storage(db_path: &str) -> Arc<dyn DiskStore> {
        use disk_store::rocksdb;
        Arc::new(rocksdb::RocksDB::new(db_path))
    }

    #[cfg(not(feature = "enable_rocksdb"))]
    pub fn persistent_storage(_: &str) -> Arc<dyn DiskStore> {
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
    pub db_path: Option<String>,
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

