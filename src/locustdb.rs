use std::str;
use std::sync::Arc;

use futures_channel::oneshot;
use futures_core::*;
use futures_util::FutureExt;
use futures_executor::block_on;
use nom;
use num_cpus;

use QueryError;
use QueryResult;
use disk_store::interface::*;
use disk_store::noop_storage::NoopStorage;
use engine::query_task::QueryTask;
use ingest::csv_loader::{CSVIngestionTask, Options as LoadOptions};
use mem_store::*;
use scheduler::*;
use syntax::parser;
use trace::{Trace, TraceBuilder};


pub struct LocustDB {
    inner_locustdb: Arc<InnerLocustDB>
}

#[derive(Clone)]
pub struct Options {
    pub threads: usize,
    pub db_path: Option<String>,
    pub mem_size_limit_tables: usize,
}

impl LocustDB {
    pub fn memory_only() -> LocustDB {
        LocustDB::new(&Options::default())
    }

    pub fn new(opts: &Options) -> LocustDB {
        let disk_store = opts.db_path.as_ref()
            .map(|path| LocustDB::persistent_storage(path))
            .unwrap_or(Arc::new(NoopStorage));
        let locustdb = Arc::new(InnerLocustDB::new(disk_store, opts));
        InnerLocustDB::start_worker_threads(&locustdb);
        LocustDB { inner_locustdb: locustdb }
    }

    pub fn run_query(&self, query: &str, explain: bool, show: Vec<usize>) -> Box<Future<Item=(QueryResult, Trace), Error=oneshot::Canceled>> {
        let (sender, receiver) = oneshot::channel();

        // TODO(clemens): perform compilation and table snapshot in asynchronous task?
        let query = match parser::parse_query(query.as_bytes()) {
            nom::IResult::Done(remaining, query) => {
                if !remaining.is_empty() {
                    let error = match str::from_utf8(remaining) {
                        Ok(chars) => QueryError::SytaxErrorCharsRemaining(chars.to_owned()),
                        Err(_) => QueryError::SyntaxErrorBytesRemaining(remaining.to_vec()),
                    };
                    return Box::new(future::ok((Err(error), TraceBuilder::new("empty".to_owned()).finalize())));
                }
                query
            }
            nom::IResult::Error(err) => return Box::new(future::ok((
                Err(QueryError::ParseError(format!("{:?}", err))),
                TraceBuilder::new("empty".to_owned()).finalize()))),
            nom::IResult::Incomplete(needed) => return Box::new(future::ok((
                Err(QueryError::ParseError(format!("Incomplete. Needed: {:?}", needed))),
                TraceBuilder::new("empty".to_owned()).finalize()))),
        };

        // TODO(clemens): A table may not exist on all nodes, so querying empty table is valid and should return empty result.
        let data = match self.inner_locustdb.snapshot(&query.table) {
            Some(data) => data,
            None => return Box::new(future::ok((
                Err(QueryError::NotImplemented(format!("Table {} does not exist!", &query.table))),
                TraceBuilder::new("empty".to_owned()).finalize()))),
        };
        let task = QueryTask::new(
            query, explain, show, data, self.inner_locustdb.storage.clone(), SharedSender::new(sender));
        let trace_receiver = self.schedule(task);
        Box::new(receiver.join(trace_receiver))
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

    pub fn ast(&self, query: &str) -> String {
        match parser::parse_query(query.as_bytes()) {
            nom::IResult::Done(remaining, query) => {
                if !remaining.is_empty() {
                    match str::from_utf8(remaining) {
                        Ok(chars) => format!("Chars remaining: {}", chars),
                        Err(_) => format!("Bytes remaining: {:?}", &remaining),
                    }
                } else {
                    format!("{:#?}", query)
                }
            }
            nom::IResult::Error(err) => format!("Parse error: {:?}", err),
            nom::IResult::Incomplete(needed) => format!("Incomplete. Needed: {:?}", needed),
        }
    }

    pub fn bulk_load(&self) -> impl Future<Item=Vec<MemTreeTable>, Error=oneshot::Canceled> {
        let disk_store = &self.inner_locustdb.storage;
        let disk_store = disk_store.clone();
        let inner = self.inner_locustdb.clone();
        let (task, receiver) = Task::from_fn(
            move || disk_store.bulk_load(&inner));
        self.schedule(task);
        block_on(receiver).unwrap();
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
    pub fn persistent_storage(db_path: &str) -> Arc<DiskStore> {
        use disk_store::rocksdb;
        Arc::new(rocksdb::RocksDB::new(db_path))
    }

    #[cfg(not(feature = "enable_rocksdb"))]
    pub fn persistent_storage(_: &str) -> Arc<DiskStore> {
        panic!("RocksDB storage backend is not enabled in this build of LocustDB. Create db with `memory_only`, or set the `enable_rocksdb` feature.")
    }
}

impl Drop for LocustDB {
    fn drop(&mut self) {
        self.inner_locustdb.stop();
    }
}

impl Default for Options {
    fn default() -> Options {
        Options {
            threads: num_cpus::get(),
            db_path: None,
            mem_size_limit_tables: 1024 * 1024 * 1024 * 1024, // 1TB
        }
    }
}

