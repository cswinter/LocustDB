use std::sync::Arc;

use mem_store::column::Column;
use disk_store::interface::*;

pub struct NoopStorage;

impl DiskStore for NoopStorage {
    fn load_metadata(&self) -> Vec<PartitionMetadata> { Vec::new() }
    fn load_column_data(&self, _: PartitionID, _: &str) -> Vec<u8> { Vec::new() }
    fn store_partition(&self, _: PartitionID, _: &str, _: &Vec<Arc<Column>>) {}
}
