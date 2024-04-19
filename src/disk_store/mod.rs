mod azure_writer;
mod file_writer;
mod gcs_writer;
pub mod meta_store;
pub mod noop_storage;
mod serialization;
pub mod storage;

lazy_static! {
    static ref RT: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

use crate::mem_store::column::Column;
use crate::perf_counter::QueryPerfCounter;
use crate::scheduler::inner_locustdb::InnerLocustDB;

pub trait ColumnLoader: Sync + Send + 'static {
    fn load_column(
        &self,
        table_name: &str,
        partition: PartitionID,
        column_name: &str,
        perf_counter: &QueryPerfCounter,
    ) -> Vec<Column>;
    fn load_column_range(
        &self,
        start: PartitionID,
        end: PartitionID,
        column_name: &str,
        ldb: &InnerLocustDB,
    );
}

pub type PartitionID = u64;
