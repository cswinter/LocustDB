use std::sync::Arc;

use mem_store::column::Column;
use disk_store::interface::*;
use scheduler::inner_locustdb::InnerLocustDB;

pub struct NoopStorage;

impl DiskStore for NoopStorage {
    fn load_metadata(&self) -> Vec<PartitionMetadata> { Vec::new() }
    fn load_column(&self, _: PartitionID, _: &str) -> Column { panic!("Can't load column from NoopStorage!") }
    fn bulk_load(&self, _: &InnerLocustDB) {}
    fn store_partition(&self, _: PartitionID, _: &str, _: &Vec<Arc<Column>>) {}
}
