use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::disk_store::interface::*;
use crate::ingest::buffer::Buffer;
use crate::mem_store::*;
use crate::scheduler::disk_read_scheduler::DiskReadScheduler;

pub type ColumnKey = (PartitionID, String);

pub struct Partition {
    pub id: PartitionID,
    len: usize,
    pub(crate) cols: Vec<ColumnHandle>,
    lru: Lru,
}

impl Partition {
    pub fn new(id: PartitionID, cols: Vec<Arc<Column>>, lru: Lru) -> (Partition, Vec<ColumnKey>) {
        let mut keys = Vec::with_capacity(cols.len());
        (
            Partition {
                id,
                len: cols[0].len(),
                cols: cols
                    .into_iter()
                    .map(|c| {
                        let key = (id, c.name().to_string());
                        // Can't put into lru directly, because then memory limit enforcer might try to evict unreachable column.
                        keys.push(key);
                        ColumnHandle::resident(id, c)
                    })
                    .collect(),
                lru,
            },
            keys,
        )
    }

    pub fn nonresident(
        id: PartitionID,
        len: usize,
        cols: &[ColumnMetadata],
        lru: Lru,
    ) -> Partition {
        Partition {
            id,
            len,
            cols: cols
                .iter()
                .map(|c| ColumnHandle::non_resident(id, c.name.to_string(), c.size_bytes))
                .collect(),
            lru,
        }
    }

    pub fn from_buffer(id: PartitionID, buffer: Buffer, lru: Lru) -> (Partition, Vec<ColumnKey>) {
        Partition::new(
            id,
            buffer
                .buffer
                .into_iter()
                .map(|(name, raw_col)| raw_col.finalize(&name))
                .collect(),
            lru,
        )
    }

    pub fn get_cols(
        &self,
        referenced_cols: &HashSet<String>,
        drs: &DiskReadScheduler,
    ) -> HashMap<String, Arc<dyn DataSource>> {
        let mut columns = HashMap::<String, Arc<dyn DataSource>>::new();
        for handle in &self.cols {
            if referenced_cols.contains(handle.name()) {
                let column = drs.get_or_load(handle);
                columns.insert(handle.name().to_string(), Arc::new(column));
            }
        }
        columns
    }

    pub fn col_names(&self) -> Vec<&str> {
        let mut names = Vec::new();
        for handle in &self.cols {
            names.push(handle.name());
        }
        names
    }

    pub fn non_residents(&self, cols: &HashSet<String>) -> HashSet<String> {
        let mut non_residents = HashSet::new();
        for handle in &self.cols {
            if !handle.is_resident() && cols.contains(handle.name()) {
                non_residents.insert(handle.name().to_string());
            }
        }
        non_residents
    }

    pub fn nonresidents_match(
        &self,
        nonresidents: &HashSet<String>,
        eligible: &HashSet<String>,
    ) -> bool {
        for handle in &self.cols {
            if handle.is_resident() {
                if nonresidents.contains(handle.name()) {
                    return false;
                }
            } else if eligible.contains(handle.name()) && !nonresidents.contains(handle.name()) {
                return false;
            }
        }
        true
    }

    pub fn promise_load(&self, cols: &HashSet<String>) -> usize {
        let mut total_size = 0;
        for handle in &self.cols {
            if cols.contains(handle.name()) {
                handle.load_scheduled.store(true, Ordering::SeqCst);
                total_size += handle.size_bytes();
            }
        }
        total_size
    }

    pub fn restore(&self, col: &Arc<Column>) {
        for handle in &self.cols {
            if handle.name() == col.name() {
                let mut maybe_column = handle.col.lock().unwrap();
                if maybe_column.is_none() {
                    self.lru.put(handle.key.clone());
                }
                *maybe_column = Some(col.clone());
                handle.resident.store(true, Ordering::SeqCst);
                handle.load_scheduled.store(false, Ordering::SeqCst);
            }
        }
    }

    pub fn evict(&self, col: &str) -> usize {
        for handle in &self.cols {
            if handle.name() == col {
                let mut maybe_column = handle.col.lock().unwrap();
                let mem_size = handle.heap_size_of_children();
                handle.resident.store(false, Ordering::SeqCst);
                *maybe_column = None;
                self.lru.remove(&handle.key);
                return mem_size;
            }
        }
        0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn mem_tree(&self, coltrees: &mut HashMap<String, MemTreeColumn>, depth: usize) {
        if depth == 0 {
            return;
        }
        for handle in &self.cols {
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
        self.cols
            .iter()
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
            .iter()
            .map(|handle| {
                let c = handle.col.lock().unwrap();
                match *c {
                    Some(ref x) => x.heap_size_of_children(),
                    None => 0,
                }
            })
            .sum()
    }
}

pub struct ColumnHandle {
    key: (PartitionID, String),
    size_bytes: AtomicUsize,
    resident: AtomicBool,
    load_scheduled: AtomicBool,
    evictable: AtomicBool,
    col: Mutex<Option<Arc<Column>>>,
}

impl ColumnHandle {
    fn resident(id: PartitionID, col: Arc<Column>) -> ColumnHandle {
        ColumnHandle {
            key: (id, col.name().to_string()),
            size_bytes: AtomicUsize::new(col.heap_size_of_children()),
            resident: AtomicBool::new(true),
            load_scheduled: AtomicBool::new(false),
            evictable: AtomicBool::new(false),
            col: Mutex::new(Some(col)),
        }
    }

    fn non_resident(id: PartitionID, name: String, size_bytes: usize) -> ColumnHandle {
        ColumnHandle {
            key: (id, name),
            size_bytes: AtomicUsize::new(size_bytes),
            resident: AtomicBool::new(false),
            load_scheduled: AtomicBool::new(false),
            evictable: AtomicBool::new(true),
            col: Mutex::new(None),
        }
    }

    pub fn is_resident(&self) -> bool {
        self.resident.load(Ordering::SeqCst)
    }

    pub fn set_resident(&self) {
        self.resident.store(true, Ordering::SeqCst);
    }

    pub fn is_load_scheduled(&self) -> bool {
        self.load_scheduled.load(Ordering::SeqCst)
    }

    pub fn key(&self) -> &(PartitionID, String) {
        &self.key
    }

    pub fn id(&self) -> PartitionID {
        self.key.0
    }

    pub fn name(&self) -> &str {
        &self.key.1
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
