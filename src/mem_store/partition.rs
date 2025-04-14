use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use crate::disk_store::*;
use crate::ingest::buffer::Buffer;
use crate::mem_store::*;
use crate::observability::QueryPerfCounter;
use crate::scheduler::disk_read_scheduler::DiskReadScheduler;

use self::meta_store::PartitionMetadata;

// Table, Partition, Column
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub struct ColumnLocator {
    pub table: String,
    pub id: PartitionID,
    pub column: String,
}

pub struct Partition {
    pub id: PartitionID,
    table_name: String,
    range: Range<usize>,
    total_size_bytes: usize,
    // Column name -> ColumnHandle
    cols: RwLock<HashMap<String, Arc<ColumnHandle>>>,
    lru: Lru,
}

impl Partition {
    pub fn new(
        table: &str,
        id: PartitionID,
        cols: Vec<Arc<Column>>,
        lru: Lru,
        // Offset of this partition in the table
        offset: usize,
    ) -> (Partition, Vec<(u64, String)>) {
        // Can't put into lru directly, because then memory limit enforcer might try to evict column that is unreachable because this partition is not yet in the partition map.
        // Instead, we return the keys to be added to the lru after the partition is added to the partition map.
        let mut keys = Vec::with_capacity(cols.len());
        let len = cols[0].len();
        let total_size_bytes = cols.iter().map(|c| c.heap_size_of_children()).sum();
        let cols = cols
            .into_iter()
            .map(|c| {
                keys.push((id, c.name().to_string()));
                (
                    c.name().to_string(),
                    Arc::new(ColumnHandle::resident(table, id, c)),
                )
            })
            .collect();
        (
            Partition {
                id,
                table_name: table.to_string(),
                range: offset..(offset + len),
                total_size_bytes,
                cols: RwLock::new(cols),
                lru,
            },
            keys,
        )
    }

    pub fn nonresident(md: &PartitionMetadata, lru: Lru) -> Partition {
        let range = md.offset..(md.offset + md.len);
        let mut total_size_bytes = 0;
        for subpartition in &md.subpartitions {
            total_size_bytes += subpartition.size_bytes as usize;
        }
        Partition {
            id: md.id,
            table_name: md.tablename.clone(),
            range,
            cols: RwLock::new(HashMap::new()),
            lru,
            total_size_bytes,
        }
    }

    pub fn from_buffer(
        table: &str,
        id: PartitionID,
        buffer: Buffer,
        lru: Lru,
        offset: usize,
    ) -> (Partition, Vec<(u64, String)>) {
        Partition::new(
            table,
            id,
            buffer
                .buffer
                .into_iter()
                .map(|(name, raw_col)| raw_col.finalize(&name))
                .collect(),
            lru,
            offset,
        )
    }

    pub fn get_cols(
        &self,
        referenced_cols: &HashSet<String>,
        drs: &DiskReadScheduler,
        perf_counter: &QueryPerfCounter,
    ) -> HashMap<String, Arc<dyn DataSource>> {
        let mut columns = HashMap::<String, Arc<dyn DataSource>>::new();
        for colname in referenced_cols {
            let cols = self.cols.read().unwrap();
            let cols =
                if !cols.contains_key(colname) {
                    drop(cols);
                    let mut cols = self.cols.write().unwrap();
                    cols.entry(colname.to_string()).or_insert(Arc::new(
                        ColumnHandle::non_resident(&self.table_name, self.id, colname.to_string()),
                    ));
                    drop(cols);
                    self.cols.read().unwrap()
                } else {
                    cols
                };
            let handle = cols.get(colname).unwrap().clone();
            drop(cols);
            if let Some(column) = drs.get_or_load(&handle, &self.cols, perf_counter) {
                columns.insert(handle.name().to_string(), Arc::new(column));
            }
        }
        columns
    }

    // pub fn nonresidents_match(
    //     &self,
    //     nonresidents: &HashSet<String>,
    //     eligible: &HashSet<String>,
    // ) -> bool {
    //     for handle in self.col_handles() {
    //         if handle.is_resident() {
    //             if nonresidents.contains(handle.name()) {
    //                 return false;
    //             }
    //         } else if eligible.contains(handle.name()) && !nonresidents.contains(handle.name()) {
    //             return false;
    //         }
    //     }
    //     true
    // }

    pub fn restore(&self, col: &Arc<Column>) {
        let cols = self.cols.read().unwrap();
        if !cols.contains_key(col.name()) {
            drop(cols);
            let mut cols = self.cols.write().unwrap();
            cols.entry(col.name().to_string())
                .or_insert(Arc::new(ColumnHandle::resident(
                    &self.table_name,
                    self.id,
                    col.clone(),
                )));
        } else {
            let handle = cols.get(col.name()).unwrap();
            let mut maybe_column = handle.col.lock().unwrap();
            if maybe_column.is_none() {
                self.lru.put(handle.key.clone());
            }
            *maybe_column = Some(col.clone());
            handle.resident.store(true, Ordering::SeqCst);
            handle.load_scheduled.store(false, Ordering::SeqCst);
        }
    }

    pub fn evict(&self, col: &str) -> usize {
        let cols = self.cols.read().unwrap();
        let handle = cols.get(col).unwrap();
        let mem_size = handle.heap_size_of_children();
        let mut maybe_column = handle.col.lock().unwrap();
        handle.resident.store(false, Ordering::SeqCst);
        *maybe_column = None;
        self.lru.remove(&handle.key);
        mem_size
    }

    pub fn len(&self) -> usize {
        self.range.len()
    }

    pub fn range(&self) -> Range<usize> {
        self.range.clone()
    }

    pub fn mem_tree(&self, coltrees: &mut HashMap<String, MemTreeColumn>, depth: usize) {
        if depth == 0 {
            return;
        }
        let cols = self.cols.read().unwrap();
        for handle in cols.values() {
            let col = handle.col.lock().unwrap();
            let coltree = coltrees
                .entry(handle.name().to_string())
                .or_insert(MemTreeColumn {
                    name: handle.name().to_string(),
                    size_bytes: 0,
                    size_percentage: 0.0,
                    rows: 0,
                    rows_percentage: 0.0,
                    encodings: HashMap::default(),
                    fully_resident: false,
                });
            if let Some(ref col) = *col {
                col.mem_tree(coltree, depth);
            }
        }
    }

    pub fn heap_size_per_column(&self) -> Vec<(String, usize)> {
        let cols = self.cols.read().unwrap();
        cols.values()
            .map(|handle| {
                let c = handle.col.lock().unwrap();
                (
                    handle.name().to_string(),
                    match *c {
                        Some(ref x) => x.heap_size_of_children(),
                        None => 0,
                    },
                )
            })
            .collect()
    }

    pub fn heap_size_of_children(&self) -> usize {
        self.cols
            .read()
            .unwrap()
            .values()
            .map(|handle| {
                let c = handle.col.lock().unwrap();
                match *c {
                    Some(ref x) => x.heap_size_of_children(),
                    None => 0,
                }
            })
            .sum()
    }

    pub fn col_handle_count(&self) -> usize {
        self.cols.read().unwrap().len()
    }

    /// Total size of all columns in this partition, including non-resident columns.
    pub fn total_size_bytes(&self) -> usize {
        self.total_size_bytes
    }

    pub fn clone_column_handles(&self) -> Vec<Arc<ColumnHandle>> {
        self.cols.read().unwrap().values().cloned().collect()
    }
}

pub struct ColumnHandle {
    // Table, Partition, Subpartition
    key: ColumnLocator,
    name: String,
    size_bytes: AtomicUsize,
    resident: AtomicBool,
    load_scheduled: AtomicBool,
    // Column might not exist, in which case we insert a dummy column when queried to avoid re-reading the partition to check if it exists.
    empty: AtomicBool,
    col: Mutex<Option<Arc<Column>>>,
}

impl ColumnHandle {
    fn resident(table: &str, id: PartitionID, col: Arc<Column>) -> ColumnHandle {
        ColumnHandle {
            key: ColumnLocator {
                table: table.to_string(),
                id,
                column: col.name().to_string(),
            },
            name: col.name().to_string(),
            size_bytes: AtomicUsize::new(col.heap_size_of_children()),
            resident: AtomicBool::new(true),
            load_scheduled: AtomicBool::new(false),
            empty: AtomicBool::new(false),
            col: Mutex::new(Some(col)),
        }
    }

    pub fn non_resident(table: &str, id: PartitionID, name: String) -> ColumnHandle {
        ColumnHandle {
            key: ColumnLocator::new(table, id, &name),
            name,
            size_bytes: AtomicUsize::new(0),
            resident: AtomicBool::new(false),
            load_scheduled: AtomicBool::new(false),
            empty: AtomicBool::new(false),
            col: Mutex::new(None),
        }
    }

    pub fn set_empty(&self) {
        self.empty.store(true, Ordering::SeqCst);
    }

    pub fn is_empty(&self) -> bool {
        self.empty.load(Ordering::SeqCst)
    }

    pub fn set_resident(&self, size_bytes: usize) {
        self.resident.store(true, Ordering::SeqCst);
        self.size_bytes.store(size_bytes, Ordering::SeqCst);
    }

    pub fn is_resident(&self) -> bool {
        self.resident.load(Ordering::SeqCst)
    }

    pub fn is_load_scheduled(&self) -> bool {
        self.load_scheduled.load(Ordering::SeqCst)
    }

    pub fn key(&self) -> &ColumnLocator {
        &self.key
    }

    pub fn table(&self) -> &str {
        &self.key.table
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> PartitionID {
        self.key.id
    }

    pub fn try_get(&self) -> MutexGuard<Option<Arc<Column>>> {
        self.col.lock().unwrap()
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::SeqCst)
    }

    pub fn update_size_bytes(&self, size_bytes: usize) {
        self.size_bytes.store(size_bytes, Ordering::SeqCst)
    }

    pub fn heap_size_of_children(&self) -> usize {
        if self.is_resident() {
            self.size_bytes()
        } else {
            0
        }
    }
}

impl ColumnLocator {
    pub fn new(table: &str, id: PartitionID, column: &str) -> ColumnLocator {
        ColumnLocator {
            table: table.to_string(),
            id,
            column: column.to_string(),
        }
    }
}
