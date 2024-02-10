use serde::{Deserialize, Serialize};

use crate::mem_store::column::Column;
use crate::scheduler::inner_locustdb::InnerLocustDB;


pub trait ColumnLoader: Sync + Send + 'static {
    fn load_column(&self, table_name: &str, partition: PartitionID, column_name: &str) -> Vec<Column>;
    fn load_column_range(&self, start: PartitionID, end: PartitionID, column_name: &str, ldb: &InnerLocustDB);
}

pub type PartitionID = u64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub len: usize,
    pub subpartitions: Vec<SubpartitionMeatadata>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SubpartitionMeatadata {
    pub column_names: Vec<String>,
    pub size_bytes: u64,
    pub subpartition_key: String,
}
