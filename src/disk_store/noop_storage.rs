use crate::disk_store::*;

pub struct NoopStorage;

impl ColumnLoader for NoopStorage {
    fn load_column(&self, _: &str, _: PartitionID, _: &str, _: &QueryPerfCounter) -> Vec<Column> {
        panic!("Can't load column from NoopStorage!")
    }
    fn load_column_range(&self, _: PartitionID, _: PartitionID, _: &str, _: &InnerLocustDB) {}
}
