use std::str;
use std::sync::Arc;

// use rocksdb::{DB, Options, WriteBatch, IteratorMode, Direction};
// use tempdir::TempDir;
use disk_store::db::*;
use disk_store::noop_storage::NoopStorage;
use engine::query::{QueryResult, QueryTask};
use futures::*;
use futures_channel::oneshot;
use ingest::csv_loader::CSVIngestionTask;
use nom;
use parser::parser;
use scheduler::*;
use mem_store::table::TableStats;


pub struct Ruba {
    inner_ruba: Arc<InnerRuba>
}

impl Ruba {
    pub fn memory_only() -> Ruba {
        Ruba::new(Box::new(NoopStorage), false)
    }

    pub fn new(storage: Box<DB>, load_tabledata: bool) -> Ruba {
        let ruba = Arc::new(InnerRuba::new(storage, load_tabledata));
        InnerRuba::start_worker_threads(ruba.clone());
        Ruba { inner_ruba: ruba }
    }

    // TODO(clemens): proper error handling throughout query stack. panics! panics everywhere!
    pub fn run_query(&self, query: &str) -> impl Future<Item=QueryResult, Error=oneshot::Canceled> {
        let (sender, receiver) = oneshot::channel();

        // TODO(clemens): perform compilation and table snapshot in asynchronous task?
        let query = match parser::parse_query(query.as_bytes()) {
            nom::IResult::Done(remaining, query) => {
                if remaining.len() > 0 {
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
        self.schedule(task);
        receiver
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

    fn schedule<T: Task + 'static>(&self, task: T) {
        self.inner_ruba.schedule(Arc::new(task));
    }
}


/*#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage() {
        let tmpdir = TempDir::new("ruba_test_db").unwrap().into_path();
        {
            let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false*);
            for i in 0..10000 {
                ruba.ingest("default", vec![("timestamp".to_string(), mem_store::ingest::RawVal::Int(i))]);
            }
        }

        let ruba = Arc::new(Ruba::new(&tmpdir.to_str().unwrap(), false));
        let ruba_clone = ruba.clone();
        thread::spawn(move || { ruba_clone.load_table_data() });
        thread::sleep(stdtime::Duration::from_millis(1000));
        let result = ruba.run_query("select sum(timestamp) from default limit 10000;").unwrap();
        assert_eq!(result.stats.rows_scanned, 10000);
    }


    #[test]
    fn test_select_start_query() {
        let tmpdir = TempDir::new("ruba_test_db").unwrap().into_path();
        let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false);
        for i in 0..10000 {
            ruba.ingest("requests", vec![
                ("timestamp".to_string(), mem_store::ingest::RawVal::Int(i)),
                ("path".to_string(), mem_store::ingest::RawVal::Str("/test".to_string())),
            ]);
        }
        for i in 0..10000 {
            ruba.ingest("requests", vec![
                ("timestamp".to_string(), mem_store::ingest::RawVal::Int(i)),
                ("code".to_string(), mem_store::ingest::RawVal::Int(420)),
            ]);
        }

        let result = ruba.run_query("select * from requests limit 10;").unwrap();
        assert_eq!(result.colnames.len(), 3);
        assert_eq!(result.rows.len(), 10);
        assert_eq!(result.rows[0].len(), 3)
    }

    #[test]
    fn test_batch_before_query() {
        let tmpdir = TempDir::new("ruba_test_db").unwrap().into_path();
        let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false);
        for i in 0..100 {
            ruba.ingest("requests", vec![
                ("timestamp".to_string(), mem_store::ingest::RawVal::Int(i)),
                ("path".to_string(), mem_store::ingest::RawVal::Str("/test".to_string())),
            ]);
        }
        let result = ruba.run_query("select * from requests;").unwrap();
        assert_eq!(result.stats.rows_scanned, 100);
    }

    #[test]
    fn test_meta_tables() {
        let tmpdir = TempDir::new("ruba_test_db").unwrap().into_path();
        let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false);
        for i in 0..100 {
            ruba.ingest("requests", vec![
                ("timestamp".to_string(), mem_store::ingest::RawVal::Int(i)),
                ("path".to_string(), mem_store::ingest::RawVal::Str("/test".to_string())),
            ]);
        }
        let result = ruba.run_query("select * from _meta_tables;").unwrap();
        assert_eq!(result.stats.rows_scanned, 2);
    }
}*/
