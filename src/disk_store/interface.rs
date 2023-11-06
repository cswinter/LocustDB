use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::mem_store::column::Column;
use crate::scheduler::inner_locustdb::InnerLocustDB;


pub trait DiskStore: Sync + Send + 'static {
    fn load_metadata(&self) -> Vec<PartitionMetadata>;
    fn load_column(&self, partition: PartitionID, column_name: &str) -> Column;
    fn load_column_range(&self, start: PartitionID, end: PartitionID, column_name: &str, ldb: &InnerLocustDB);
    fn bulk_load(&self, ldb: &InnerLocustDB);
    fn store_partition(&self, partition: PartitionID, tablename: &str, columns: &[Arc<Column>]);
}

pub type PartitionID = u64;

#[derive(Serialize, Deserialize, Clone)]
pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub len: usize,
    pub columns: Vec<ColumnMetadata>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub size_bytes: usize,
}
