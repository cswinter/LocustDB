use std::sync::Arc;

use mem_store::column::Column;


pub trait DiskStore: Sync + Send + 'static {
    fn load_metadata(&self) -> Vec<PartitionMetadata>;
    fn load_column_data(&self, partition: PartitionID, column_name: &str) -> Vec<u8>;
    fn store_partition(&self, partition: PartitionID, tablename: &str, columns: &Vec<Arc<Column>>);
}

pub type PartitionID = u64;

pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub columns: Vec<String>,
}

