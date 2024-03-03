use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use std::str;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

use itertools::Itertools;

use crate::disk_store::interface::*;
use crate::disk_store::v2::{Storage, StorageV2, WALSegment};
use crate::ingest::buffer::Buffer;
use crate::ingest::input_column::InputColumn;
use crate::ingest::raw_val::RawVal;
use crate::logging_client::ColumnData;
use crate::mem_store::partition::{ColumnLocator, Partition};
use crate::mem_store::*;

pub struct Table {
    name: String,
    partitions: RwLock<HashMap<PartitionID, Arc<Partition>>>,
    next_partition_id: AtomicU64,
    buffer: Mutex<Buffer>,
    /// LRU that keeps track of when each (table, partition, column) segment was last accessed.
    lru: Lru,
}

impl Table {
    pub fn new(name: &str, lru: Lru) -> Table {
        Table {
            name: name.to_string(),
            partitions: RwLock::new(HashMap::new()),
            next_partition_id: AtomicU64::new(0),
            buffer: Mutex::new(Buffer::default()),
            lru,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn snapshot(&self) -> Vec<Arc<Partition>> {
        let partitions = self.partitions.read().unwrap();
        let mut partitions: Vec<_> = partitions.values().cloned().collect();
        let buffer = self.buffer.lock().unwrap();
        if buffer.len() > 0 {
            partitions.push(Arc::new(
                Partition::from_buffer(self.name(), u64::MAX, buffer.clone(), self.lru.clone()).0,
            ));
        }
        partitions
    }

    pub fn snapshot_parts(&self, parts: &[PartitionID]) -> Vec<Arc<Partition>> {
        let partitions = self.partitions.read().unwrap();
        let mut partitions: Vec<_> = parts.iter().map(|id| partitions[id].clone()).collect();
        let buffer = self.buffer.lock().unwrap();
        if buffer.len() > 0 {
            partitions.push(Arc::new(
                Partition::from_buffer(self.name(), u64::MAX, buffer.clone(), self.lru.clone()).0,
            ));
        }
        partitions
    }

    pub fn restore_tables_from_disk(
        storage: &StorageV2,
        wal_segments: Vec<WALSegment>,
        lru: &Lru,
    ) -> HashMap<String, Table> {
        let mut tables = HashMap::new();
        for md in &storage.meta_store().lock().unwrap().partitions {
            let table = tables
                .entry(md.tablename.clone())
                .or_insert_with(|| Table::new(&md.tablename, lru.clone()));
            table.insert_nonresident_partition(md);
        }
        for wal_segment in wal_segments {
            for (table_name, table_data) in wal_segment.data.into_owned().tables {
                let table = tables
                    .entry(table_name.clone())
                    .or_insert_with(|| Table::new(&table_name, lru.clone()));
                let columns = table_data
                    .columns
                    .into_iter()
                    .map(|(k, v)| {
                        let col = match v.data {
                            ColumnData::Dense(data) => InputColumn::Float(data),
                            ColumnData::Sparse(_) => {
                                todo!("INGESTION OF SPARSE VALUES NOT IMPLEMENTED")
                            }
                        };
                        (k, col)
                    })
                    .collect();
                table.ingest_homogeneous(columns);
            }
        }
        tables
    }

    pub fn restore(&self, id: PartitionID, col: &Arc<Column>) {
        let partitions = self.partitions.read().unwrap();
        partitions[&id].restore(col);
    }

    pub fn evict(&self, key: &ColumnLocator) -> usize {
        let partitions = self.partitions.read().unwrap();
        partitions
            .get(&key.id)
            .map(|p| p.evict(&key.column))
            .unwrap_or(0)
    }

    pub fn insert_nonresident_partition(&self, md: &PartitionMetadata) {
        let partition = Arc::new(Partition::nonresident(
            self.name(),
            md.id,
            md.len,
            &md.subpartitions,
            self.lru.clone(),
        ));
        let mut partitions = self.partitions.write().unwrap();
        partitions.insert(md.id, partition);
        self.next_partition_id.fetch_max(md.id + 1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn ingest(&self, row: Vec<(String, RawVal)>) {
        log::debug!("Ingesting row: {:?}", row);
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push_row(row);
    }

    pub fn ingest_homogeneous(&self, columns: HashMap<String, InputColumn>) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push_typed_cols(columns);
    }

    pub fn ingest_heterogeneous(&self, columns: HashMap<String, Vec<RawVal>>) {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push_untyped_cols(columns);
    }

    pub fn load_partition(&self, partition: Partition) {
        let mut partitions = self.partitions.write().unwrap();
        partitions.insert(partition.id, Arc::new(partition));
    }

    pub(crate) fn batch(&self) -> Option<Arc<Partition>> {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.len() == 0 {
            return None;
        }
        let buffer = std::mem::take(buffer.deref_mut());
        let part_id = self.next_partition_id();
        let (new_partition, keys) =
            Partition::from_buffer(self.name(), part_id, buffer, self.lru.clone());
        let arc_partition;
        {
            let mut partitions = self.partitions.write().unwrap();
            arc_partition = Arc::new(new_partition);
            partitions.insert(part_id, arc_partition.clone());
        }
        for (id, column) in keys {
            self.lru.put(ColumnLocator::new(self.name(), id, &column));
        }
        Some(arc_partition)
    }

    /// Determines if partitions should be compacted. If so, returns the maximal list of partitions to compact.
    /// A subset of partitions is eligible for compaction if the size of each
    /// partition in the subset is at least `combine_factor` times the total size of all partitions in the subset.
    pub fn plan_compaction(&self, combine_factor: u64) -> Option<Vec<PartitionID>> {
        // TODO: max partition size
        let partitions = self.partitions.read().unwrap();
        let by_size_desc: Vec<Arc<Partition>> = partitions
            .values()
            .cloned()
            .sorted_by(|p1, p2| p2.total_size_bytes().cmp(&p1.total_size_bytes()));
        let cumulative = by_size_desc
            .iter()
            .rev()
            .scan(0, |acc, p| {
                *acc += p.total_size_bytes() as u64;
                Some(*acc)
            })
            .collect::<Vec<_>>();

        for (i, cum) in cumulative.iter().rev().enumerate() {
            if by_size_desc[i].total_size_bytes() as u64 * combine_factor < *cum {
                return Some(by_size_desc[i..].iter().map(|p| p.id).collect());
            }
        }

        None
    }

    pub fn compact(&self, id: PartitionID, columns: Vec<Arc<Column>>, old_partitions: &[PartitionID]) {
        let (partition, keys) = Partition::new(self.name(), id, columns, self.lru.clone());
        {
            let mut partitions = self.partitions.write().unwrap();
            for old_id in old_partitions {
                partitions.remove(old_id);
            }
            partitions.insert(id, Arc::new(partition));
        }
        for (id, column) in keys {
            self.lru.put(ColumnLocator::new(self.name(), id, &column));
        }
    }

    pub fn mem_tree(&self, depth: usize) -> MemTreeTable {
        assert!(depth > 0);
        let mut tree = MemTreeTable {
            name: self.name().to_string(),
            rows: 0,
            fully_resident: true,
            size_bytes: 0,
            columns: HashMap::default(),
        };
        let partitions = self.snapshot();
        for partition in partitions {
            partition.mem_tree(&mut tree.columns, if depth == 1 { 1 } else { depth - 1 });
            tree.rows += partition.len();
        }
        tree.aggregate();
        if depth == 1 {
            tree.columns = HashMap::default();
        }
        tree
    }

    pub fn stats(&self) -> TableStats {
        let partitions = self.snapshot();
        let size_per_column = Table::size_per_column(&partitions);
        let buffer = self.buffer.lock().unwrap();
        TableStats {
            name: self.name().to_string(),
            rows: partitions.iter().map(|p| p.len()).sum(),
            batches: partitions.len(),
            batches_bytes: partitions
                .iter()
                .map(|partition| partition.heap_size_of_children())
                .sum(),
            buffer_length: buffer.len(),
            buffer_bytes: buffer.heap_size_of_children(),
            size_per_column,
        }
    }

    pub fn heap_size_of_children(&self) -> usize {
        let batches_size: usize = {
            let batches = self.partitions.read().unwrap();
            batches
                .iter()
                .map(|(_, partition)| partition.heap_size_of_children())
                .sum()
        };
        let buffer_size = {
            let buffer = self.buffer.lock().unwrap();
            buffer.heap_size_of_children()
        };
        batches_size + buffer_size
    }

    fn size_per_column(partitions: &[Arc<Partition>]) -> Vec<(String, usize)> {
        let mut sizes: HashMap<String, usize> = HashMap::default();
        for partition in partitions {
            for (colname, size) in partition.heap_size_per_column() {
                *sizes.entry(colname).or_insert(0) += size;
            }
        }
        sizes
            .iter()
            .map(|(name, size)| (name.to_string(), *size))
            .collect()
    }

    pub fn column_names(&self, parts: &[u64]) -> Vec<String> {
        let partitions = self.partitions.read().unwrap();
        let mut columns = HashSet::new();
        for pid in parts {
            for col in partitions[pid].col_names() {
                columns.insert(col.clone());
            }
        }
        columns.into_iter().sorted()
    }

    pub fn next_partition_id(&self) -> u64 {
        self.next_partition_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Metadata {
    pub name: String,
    pub batch_count: u64,
}

#[derive(Debug)]
pub struct TableStats {
    pub name: String,
    pub rows: usize,
    pub batches: usize,
    pub batches_bytes: usize,
    pub buffer_length: usize,
    pub buffer_bytes: usize,
    pub size_per_column: Vec<(String, usize)>,
}
