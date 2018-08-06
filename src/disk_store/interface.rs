use std::sync::Arc;

use mem_store::column::Column;
use scheduler::inner_locustdb::InnerLocustDB;


pub trait DiskStore: Sync + Send + 'static {
    fn load_metadata(&self) -> Vec<PartitionMetadata>;
    fn load_column(&self, partition: PartitionID, column_name: &str) -> Column;
    fn bulk_load(&self, ldb: &InnerLocustDB);
    fn store_partition(&self, partition: PartitionID, tablename: &str, columns: &Vec<Arc<Column>>);
}

pub type PartitionID = u64;

pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub len: usize,
    pub columns: Vec<ColumnMetadata>,
}

pub struct ColumnMetadata {
    pub name: String,
    pub size_bytes: usize,
}
