use std::collections::{HashMap, HashSet};
use std::ops::{DerefMut, Range};
use std::str;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::sync::{Mutex, RwLock};

use datasize::DataSize;
use itertools::Itertools;

use crate::disk_store::storage::Storage;
use crate::disk_store::*;
use crate::ingest::buffer::Buffer;
use crate::ingest::input_column::InputColumn;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::partition::{ColumnLocator, Partition};
use crate::mem_store::*;

use self::meta_store::PartitionMetadata;

pub struct Table {
    name: String,
    // To prevent deadlocks, `frozen_buffer` lock has to be always acquired before `partitions` before `buffer`
    pub(crate) partitions: RwLock<HashMap<PartitionID, Arc<Partition>>>,
    next_partition_id: AtomicU64,
    next_partition_offset: AtomicUsize,
    buffer: Mutex<Buffer>,
    // When flushing WAL, buffer is swapped with `frozen_buffer` to quickly snapshot all buffer
    // data in existing WAL segments and unblock writing new WAL segments that will be flushed later.
    frozen_buffer: Mutex<Buffer>,
    /// LRU that keeps track of when each (table, partition, column) segment was last accessed.
    lru: Lru,

    // Set of every column name that is present in any partition
    column_names: RwLock<Option<HashSet<String>>>,
}

impl Table {
    pub fn new(name: &str, lru: Lru, column_names: Option<HashSet<String>>) -> Table {
        Table {
            name: name.to_string(),
            partitions: RwLock::new(HashMap::new()),
            next_partition_id: AtomicU64::new(0),
            next_partition_offset: AtomicUsize::new(0),
            buffer: Mutex::new(Buffer::default()),
            frozen_buffer: Mutex::new(Buffer::default()),
            lru,
            column_names: RwLock::new(if name.starts_with("_meta_columns_") {
                Some(HashSet::from(["column_names".to_string()]))
            } else if name.starts_with("_meta_tables") {
                Some(HashSet::from(["timestamp".to_string(), "name".to_string()]))
            } else {
                column_names
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn snapshot(&self, column_filter: Option<&[String]>) -> Vec<Arc<Partition>> {
        let frozen_buffer = self.frozen_buffer.lock().unwrap();
        let partitions = self.partitions.read().unwrap();
        let buffer = self.buffer.lock().unwrap();
        let mut partitions: Vec<_> = partitions.values().cloned().collect();
        let mut offset = partitions.iter().map(|p| p.len()).sum::<usize>();
        if frozen_buffer.len() > 0 {
            let buffer = match column_filter {
                Some(columns) => frozen_buffer.filter(columns),
                None => frozen_buffer.clone(),
            };
            partitions.push(Arc::new(
                Partition::from_buffer(
                    self.name(),
                    0xDEADBEEF_DEADBEEF,
                    buffer,
                    self.lru.clone(),
                    offset,
                )
                .0,
            ));
            offset += frozen_buffer.len();
        }
        if buffer.len() > 0 {
            let buffer = match column_filter {
                Some(columns) => buffer.filter(columns),
                None => buffer.clone(),
            };
            partitions.push(Arc::new(
                Partition::from_buffer(
                    self.name(),
                    0xDEADBEEF_DEADBEEF,
                    buffer,
                    self.lru.clone(),
                    offset,
                )
                .0,
            ));
        }
        partitions
    }

    pub fn init_column_names(&self, column_names: HashSet<String>) {
        let mut cns = self.column_names.write().unwrap();
        assert!(
            cns.is_none() || (*cns).as_ref() == Some(&column_names),
            "Inconsistent concurrent column name initialization for table {} ({:?} vs {:?})",
            self.name,
            cns.as_ref().unwrap(),
            column_names,
        );
        *cns = Some(column_names);
    }

    pub fn snapshot_parts(&self, parts: &[PartitionID]) -> Vec<Arc<Partition>> {
        let partitions = self.partitions.read().unwrap();
        parts.iter().map(|id| partitions[id].clone()).collect()
    }

    pub fn freeze_buffer(&self) {
        let mut frozen_buffer = self.frozen_buffer.lock().unwrap();
        let mut buffer = self.buffer.lock().unwrap();
        assert!(frozen_buffer.len() == 0, "Frozen buffer is not empty");
        std::mem::swap(&mut *buffer, &mut *frozen_buffer);
    }

    pub fn restore_tables_from_disk(storage: &Storage, lru: &Lru) -> HashMap<String, Arc<Table>> {
        let mut tables = HashMap::new();
        for md in storage.meta_store().read().unwrap().partitions() {
            let table = tables
                .entry(md.tablename.clone())
                .or_insert_with(|| Arc::new(Table::new(&md.tablename, lru.clone(), None)));
            table.insert_nonresident_partition(md);
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
        let partition = Arc::new(Partition::nonresident(md, self.lru.clone()));
        let mut partitions = self.partitions.write().unwrap();
        partitions.insert(md.id, partition);
        self.next_partition_id
            .fetch_max(md.id + 1, std::sync::atomic::Ordering::SeqCst);
        self.next_partition_offset
            .fetch_max(md.offset + md.len, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn ingest(&self, row: Vec<(String, RawVal)>) {
        log::debug!("Ingesting row: {:?}", row);
        let mut buffer = self.buffer.lock().unwrap();
        let mut column_names = self.column_names.write().unwrap();
        let column_names = column_names.as_mut().unwrap_or_else(|| {
            panic!(
                "Attmpting to ingest row in table {} but column names have not been initialized",
                self.name
            );
        });
        for (col, _) in &row {
            if !column_names.contains(col) {
                column_names.insert(col.clone());
            }
        }
        buffer.push_row(row);
    }

    pub fn ingest_homogeneous(&self, columns: HashMap<String, InputColumn>) {
        let mut buffer = self.buffer.lock().unwrap();
        let mut column_names = self.column_names.write().unwrap();
        let column_names = column_names.as_mut().unwrap_or_else(|| {
            panic!(
                "Attmpting to ingest row in table {} but column names have not been initialized",
                self.name
            );
        });
        for col in columns.keys() {
            if !column_names.contains(col) {
                column_names.insert(col.clone());
            }
        }
        buffer.push_typed_cols(columns);
    }

    pub fn ingest_heterogeneous(&self, columns: HashMap<String, Vec<RawVal>>) {
        let mut buffer = self.buffer.lock().unwrap();
        let mut column_names = self.column_names.write().unwrap();
        let column_names = column_names.as_mut().unwrap_or_else(|| {
            panic!(
                "Attmpting to ingest row in table {} but column names have not been initialized",
                self.name
            );
        });
        for col in columns.keys() {
            if !column_names.contains(col) {
                column_names.insert(col.clone());
            }
        }
        buffer.push_untyped_cols(columns);
    }

    /// Creates a new partition from current buffer and returns it.
    pub(crate) fn batch(&self) -> Option<Arc<Partition>> {
        let mut buffer = self.frozen_buffer.lock().unwrap();
        if buffer.len() == 0 {
            return None;
        }
        let buffer = std::mem::take(buffer.deref_mut());
        let part_id = self.next_partition_id();
        let partition_offset = self
            .next_partition_offset
            .fetch_add(buffer.len(), std::sync::atomic::Ordering::SeqCst);
        let (new_partition, keys) = Partition::from_buffer(
            self.name(),
            part_id,
            buffer,
            self.lru.clone(),
            partition_offset,
        );
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
    /// Additionally, partitions can only be compacted if they are contiguous.
    pub fn plan_compaction(&self, combine_factor: u64) -> Option<(Range<usize>, Vec<PartitionID>)> {
        // TODO: max partition size
        let partitions = self.partitions.read().unwrap();
        // let by_size_desc: Vec<Arc<Partition>> = partitions
        //     .values()
        //     .cloned()
        //     .sorted_by(|p1, p2| p2.total_size_bytes().cmp(&p1.total_size_bytes()));
        let by_offset: Vec<Arc<Partition>> = partitions
            .values()
            .cloned()
            .sorted_by(|p1, p2| p1.range().start.cmp(&p2.range().start))
            .collect();
        let cumulative = by_offset
            .iter()
            .rev()
            .scan(0, |acc, p| {
                *acc += p.total_size_bytes() as u64;
                Some(*acc)
            })
            .collect::<Vec<_>>();

        for (i, cum) in cumulative.iter().rev().enumerate() {
            if by_offset[i].total_size_bytes() as u64 * combine_factor < *cum {
                let range = by_offset[i].range().start..by_offset.last().unwrap().range().end;
                return Some((range, by_offset[i..].iter().map(|p| p.id).collect()));
            }
        }

        None
    }

    pub fn compact(
        &self,
        id: PartitionID,
        offset: usize,
        columns: Vec<Arc<Column>>,
        old_partitions: &[PartitionID],
    ) {
        let (partition, keys) =
            Partition::new(self.name(), id, columns, self.lru.clone(), false, offset);
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
        let partitions = self.snapshot(None);
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
        let partitions = self.snapshot(None);
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
            buffer_bytes: buffer.estimate_heap_size(),
            size_per_column,
        }
    }

    pub fn heap_size_of_children(&self) -> (usize, usize) {
        let batches_size: usize = {
            let batches = self.partitions.read().unwrap();
            batches
                .iter()
                .map(|(_, partition)| partition.heap_size_of_children())
                .sum()
        };
        let buffer_size = {
            let buffer = self.buffer.lock().unwrap();
            buffer.estimate_heap_size()
        };
        (batches_size, buffer_size)
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

    pub fn next_partition_id(&self) -> u64 {
        self.next_partition_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn columns_names_loaded(&self) -> bool {
        self.column_names.read().unwrap().is_some()
    }

    pub fn column_names(&self) -> HashSet<String> {
        self.column_names
            .read()
            .unwrap()
            .clone()
            .expect("Column names have not been initialized")
    }

    pub fn new_column_names<'a, I: Iterator<Item = &'a str>>(&self, columns: I) -> Vec<String> {
        let column_names = self.column_names();
        columns
            .filter(|col| !column_names.contains(*col))
            .map(|s| s.to_string())
            .collect()
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
