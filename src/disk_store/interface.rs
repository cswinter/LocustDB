use std::sync::Arc;

use mem_store::column::Column;


pub trait DiskStore: Sync + Send + 'static {
    fn load_metadata(&self) -> Vec<PartitionMetadata>;
    fn load_column(&self, partition: PartitionID, column_name: &str) -> Column;
    fn bulk_load(&self, start: PartitionID, end: PartitionID) -> Vec<(PartitionID, Column)>;
    fn store_partition(&self, partition: PartitionID, tablename: &str, columns: &Vec<Arc<Column>>);
}

pub type PartitionID = u64;

pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub len: usize,
    pub columns: Vec<String>,
}

