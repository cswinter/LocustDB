use std::str;
use std::sync::Arc;

// use rocksdb::{DB, Options, WriteBatch, IteratorMode, Direction};
// use tempdir::TempDir;
use disk_store::db::*;
use disk_store::noop_storage::NoopStorage;
use engine::query_task::{QueryResult, QueryTask};
use futures::*;
use futures_channel::oneshot;
use ingest::csv_loader::CSVIngestionTask;
use mem_store::table::TableStats;
use nom;
use scheduler::*;
use syntax::parser;
use trace::Trace;


pub struct Ruba {
    inner_ruba: Arc<InnerRuba>
}

impl Ruba {
    pub fn memory_only() -> Ruba {
        Ruba::new(Box::new(NoopStorage), false)
    }

    pub fn new(storage: Box<DB>, load_tabledata: bool) -> Ruba {
        let ruba = Arc::new(InnerRuba::new(storage, load_tabledata));
        InnerRuba::start_worker_threads(&ruba);
        Ruba { inner_ruba: ruba }
    }

    // TODO(clemens): proper error handling throughout query stack. panics! panics everywhere!
    pub fn run_query(&self, query: &str) -> impl Future<Item=(QueryResult, Trace), Error=oneshot::Canceled> {
        let (sender, receiver) = oneshot::channel();

        // TODO(clemens): perform compilation and table snapshot in asynchronous task?
        let query = match parser::parse_query(query.as_bytes()) {
            nom::IResult::Done(remaining, query) => {
                if !remaining.is_empty() {
                    panic!("Error parsing query. Bytes remaining: {:?}", &remaining);
                }
                query
            }
            err => panic!("Failed to parse query! {:?}", err),
        };

        // TODO(clemens): A table may not exist on all nodes, so querying empty table is valid and should return empty result.
        let data = self.inner_ruba.snapshot(&query.table)
            .expect(&format!("Table {} does not exist!", &query.table));
        let task = QueryTask::new(query, data, SharedSender::new(sender));
        let trace_receiver = self.schedule(task);
        receiver.join(trace_receiver)
    }

    pub fn load_csv(&self,
                    path: &str,
                    table_name: &str,
                    chunk_size: usize) -> impl Future<Item=(), Error=oneshot::Canceled> {
        let (sender, receiver) = oneshot::channel();
        let task = CSVIngestionTask::new(
            path.to_string(),
            table_name.to_string(),
            chunk_size,
            self.inner_ruba.clone(),
            SharedSender::new(sender));
        self.schedule(task);
        receiver
    }

    pub fn table_stats(&self) -> impl Future<Item=Vec<TableStats>, Error=oneshot::Canceled> {
        let inner = self.inner_ruba.clone();
        let (task, receiver) = Task::from_fn(move || inner.stats());
        self.schedule(task);
        receiver
    }

    fn schedule<T: Task + 'static>(&self, task: T) -> impl Future<Item=Trace, Error=oneshot::Canceled> {
        self.inner_ruba.schedule(task)
    }
}

impl Drop for Ruba {
    fn drop(&mut self) {
        self.inner_ruba.stop();
    }
}
