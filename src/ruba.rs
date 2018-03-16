use std::str;
use std::sync::Arc;

// use rocksdb::{DB, Options, WriteBatch, IteratorMode, Direction};
// use tempdir::TempDir;
use disk_store::db::*;
use disk_store::noop_storage::NoopStorage;
use engine::query::QueryResult;
use futures::*;
use futures_channel::oneshot;
use ingest::csv_loader::CSVIngestionTask;
use scheduler::*;


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

    // TODO(clemens): make asynchronous
    pub fn run_query(&self, query: &str) -> Result<QueryResult, String> {
        self.inner_ruba.run_query(query)
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
            let ruba = Ruba::new(&tmpdir.to_str().unwrap(), false);
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
