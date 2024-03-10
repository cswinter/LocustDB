pub mod noop_storage;
pub mod storage;
mod file_writer;


use serde::{Deserialize, Serialize};

use crate::mem_store::column::Column;
use crate::perf_counter::QueryPerfCounter;
use crate::scheduler::inner_locustdb::InnerLocustDB;


pub trait ColumnLoader: Sync + Send + 'static {
    fn load_column(&self, table_name: &str, partition: PartitionID, column_name: &str, perf_counter: &QueryPerfCounter) -> Vec<Column>;
    fn load_column_range(&self, start: PartitionID, end: PartitionID, column_name: &str, ldb: &InnerLocustDB);
}

pub type PartitionID = u64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub len: usize,
    pub subpartitions: Vec<SubpartitionMetadata>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SubpartitionMetadata {
    pub column_names: Vec<String>,
    pub size_bytes: u64,
    pub subpartition_key: String,
}