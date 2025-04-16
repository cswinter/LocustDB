use crate::disk_store::*;

pub struct NoopStorage;

impl ColumnLoader for NoopStorage {
    fn load_column(
        &self,
        _: &str,
        _: PartitionID,
        _: &str,
        _: &QueryPerfCounter,
    ) -> Option<Vec<Column>> {
        None
    }
    fn load_column_range(&self, _: PartitionID, _: PartitionID, _: &str, _: &InnerLocustDB) {}
    fn partition_has_been_loaded(&self, _: &str, _: PartitionID, _: &str) -> bool {
        true
    }
    fn mark_subpartition_as_loaded(&self, _: &str, _: PartitionID, _: &str) {}
}
