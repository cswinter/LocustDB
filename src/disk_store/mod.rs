mod file_writer;
pub mod noop_storage;
pub mod storage;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::mem_store::column::Column;
use crate::perf_counter::QueryPerfCounter;
use crate::scheduler::inner_locustdb::InnerLocustDB;

pub trait ColumnLoader: Sync + Send + 'static {
    fn load_column(
        &self,
        table_name: &str,
        partition: PartitionID,
        column_name: &str,
        perf_counter: &QueryPerfCounter,
    ) -> Vec<Column>;
    fn load_column_range(
        &self,
        start: PartitionID,
        end: PartitionID,
        column_name: &str,
        ldb: &InnerLocustDB,
    );
}

pub type PartitionID = u64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub offset: usize,
    pub len: usize,
    pub subpartitions: Vec<SubpartitionMetadata>,
    pub column_name_to_subpartition_index: HashMap<String, usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SubpartitionMetadata {
    // pub column_names: HashSet<String>,
    pub size_bytes: u64,
    pub subpartition_key: String,
}

impl PartitionMetadata {
    pub fn subpartition_key(&self, column_name: &str) -> String {
        let subpartition_index = self.column_name_to_subpartition_index[column_name];
        self.subpartitions[subpartition_index]
            .subpartition_key
            .clone()
    }
}
