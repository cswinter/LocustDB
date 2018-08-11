use std::collections::HashSet;
use std::sync::{Arc, Mutex, Condvar};
use std::collections::VecDeque;

#[allow(unused_imports)]
use heapsize::HeapSizeOf;

use disk_store::interface::DiskStore;
use disk_store::interface::PartitionID;
use mem_store::*;
use mem_store::partition::ColumnHandle;
use mem_store::partition::Partition;
use scheduler::inner_locustdb::InnerLocustDB;


pub struct DiskReadScheduler {
    disk_store: Arc<DiskStore>,
    task_queue: Mutex<VecDeque<DiskRun>>,
    reader_token: Mutex<()>,
    lru: LRU,
    #[allow(dead_code)]
    lz4_decode: bool,

    background_load_wait_queue: Condvar,
    background_load_in_progress: Mutex<bool>,
}

#[derive(Default, Debug)]
struct DiskRun {
    start: PartitionID,
    end: PartitionID,
    columns: HashSet<String>,
    bytes: usize,
}

impl DiskReadScheduler {
    pub fn new(disk_store: Arc<DiskStore>, lru: LRU, lz4_decode: bool) -> DiskReadScheduler {
        DiskReadScheduler {
            disk_store,
            task_queue: Mutex::default(),
            reader_token: Mutex::default(),
            lru,
            lz4_decode,
            background_load_wait_queue: Condvar::default(),
            background_load_in_progress: Mutex::default(),
        }
    }

    pub fn schedule_sequential_read(&self,
                                    snapshot: &mut Vec<Arc<Partition>>,
                                    columns: &HashSet<String>,
                                    readahead: usize) {
        let mut task_queue = self.task_queue.lock().unwrap();
        snapshot.sort_unstable_by_key(|p| p.id());
        let mut current_run = DiskRun::default();
        let mut previous_partitionid = 0;
        for partition in snapshot {
            if current_run.bytes > readahead ||
                !partition.nonresidents_match(&current_run.columns, &columns) {
                if !current_run.columns.is_empty() {
                    current_run.end = previous_partitionid;
                    task_queue.push_back(current_run);
                }
                let columns = partition.non_residents(columns);
                current_run = DiskRun {
                    start: partition.id(),
                    end: 0,
                    bytes: partition.promise_load(&columns),
                    columns,
                };
            } else {
                current_run.bytes += partition.promise_load(&current_run.columns);
            }
            previous_partitionid = partition.id();
        }
        current_run.end = previous_partitionid;
        task_queue.push_back(current_run);
        debug!("Scheduled sequential reads. Queue: {:#?}", &*task_queue);
    }

    pub fn get_or_load(&self, handle: &ColumnHandle) -> Arc<Column> {
        loop {
            if handle.is_resident() {
                let mut maybe_column = handle.try_get();
                if let Some(ref mut column) = *maybe_column {
                    #[cfg(feature = "enable_lz4")] {
                        if self.lz4_decode {
                            Arc::get_mut(column).map(|c| c.lz4_decode());
                            handle.update_size_bytes(column.heap_size_of_children());
                        }
                    }
                    self.lru.touch(&handle.key());
                    return column.clone();
                } else {
                    debug!("{}.{} was not resident!", handle.name(), handle.id());
                }
            } else {
                if handle.is_load_scheduled() {
                    let mut is_load_in_progress =
                        self.background_load_in_progress.lock().unwrap();
                    while *is_load_in_progress && !handle.is_resident() && handle.is_load_scheduled() {
                        debug!("Queuing for {}.{}", handle.name(), handle.id());
                        is_load_in_progress = self.background_load_wait_queue
                            .wait(is_load_in_progress).unwrap();
                    }
                } else {
                    debug!("Point lookup for {}.{}", handle.name(), handle.id());
                    #[allow(unused_mut)]
                    let mut column = {
                        let _ = self.reader_token.lock().unwrap();
                        self.disk_store.load_column(handle.id(), handle.name())
                    };
                    // Need to hold lock when we put new value into lru
                    let mut maybe_column = handle.try_get();
                    self.lru.put(handle.key().clone());
                    #[cfg(feature = "enable_lz4")] {
                        if self.lz4_decode {
                            column.lz4_decode();
                            handle.update_size_bytes(column.heap_size_of_children());
                        }
                    }
                    let column = Arc::new(column);
                    *maybe_column = Some(column.clone());
                    handle.set_resident();
                    return column;
                }
            }
        }
    }

    pub fn service_reads(&self, ldb: &InnerLocustDB) {
        *self.background_load_in_progress.lock().unwrap() = true;
        debug!("Started servicing reads...");
        // TODO(clemens): Ensure only one thread enters this function at a time
        loop {
            let next_read = {
                let mut task_queue = self.task_queue.lock().unwrap();
                match task_queue.pop_front() {
                    Some(read) => read,
                    None => {
                        debug!("Stopped servicing reads...");
                        *self.background_load_in_progress.lock().unwrap() = false;
                        return;
                    }
                }
            };
            self.service_sequential_read(next_read, ldb);
            self.background_load_wait_queue.notify_all();
        }
    }

    fn service_sequential_read(&self, run: DiskRun, ldb: &InnerLocustDB) {
        let _ = self.reader_token.lock().unwrap();
        debug!("Servicing read: {:?}", &run);
        for col in &run.columns {
            self.disk_store.load_column_range(run.start, run.end, col, ldb);
        }
    }
}
