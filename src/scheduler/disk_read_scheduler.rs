// background_load_in_progress used with condition variable
#![allow(clippy::mutex_atomic)]

use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
use std_semaphore::Semaphore;

use crate::disk_store::*;
use crate::mem_store::partition::ColumnHandle;
use crate::mem_store::partition::Partition;
use crate::mem_store::*;
use crate::perf_counter::QueryPerfCounter;
use crate::scheduler::inner_locustdb::InnerLocustDB;

pub struct DiskReadScheduler {
    disk_store: Arc<dyn ColumnLoader>,
    task_queue: Mutex<VecDeque<DiskRun>>,
    reader_semaphore: Semaphore,
    lru: Lru,
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
    pub fn new(
        disk_store: Arc<dyn ColumnLoader>,
        lru: Lru,
        max_readers: usize,
        lz4_decode: bool,
    ) -> DiskReadScheduler {
        DiskReadScheduler {
            disk_store,
            task_queue: Mutex::default(),
            reader_semaphore: Semaphore::new(max_readers as isize),
            lru,
            lz4_decode,
            background_load_wait_queue: Condvar::default(),
            background_load_in_progress: Mutex::default(),
        }
    }

    pub fn schedule_sequential_read(
        &self,
        snapshot: &mut Vec<Arc<Partition>>,
        columns: &HashSet<String>,
        readahead: usize,
    ) {
        let mut task_queue = self.task_queue.lock().unwrap();
        snapshot.sort_unstable_by_key(|p| p.id);
        let mut current_run = DiskRun::default();
        let mut previous_partitionid = 0;
        for partition in snapshot {
            if current_run.bytes > readahead
                || !partition.nonresidents_match(&current_run.columns, columns)
            {
                if !current_run.columns.is_empty() {
                    current_run.end = previous_partitionid;
                    task_queue.push_back(current_run);
                }
                let columns = partition.non_residents(columns);
                current_run = DiskRun {
                    start: partition.id,
                    end: 0,
                    bytes: partition.promise_load(&columns),
                    columns,
                };
            } else {
                current_run.bytes += partition.promise_load(&current_run.columns);
            }
            previous_partitionid = partition.id;
        }
        current_run.end = previous_partitionid;
        task_queue.push_back(current_run);
        debug!("Scheduled sequential reads. Queue: {:#?}", &*task_queue);
    }

    pub fn schedule_bulk_load(&self, mut snapshot: Vec<Arc<Partition>>, chunk_size: usize) {
        let mut task_queue = self.task_queue.lock().unwrap();
        snapshot.sort_unstable_by_key(|p| p.id);
        let mut runs = HashMap::<&str, DiskRun>::default();
        for partition in &snapshot {
            for col in partition.col_names() {
                let reached_chunk_size = {
                    let run = runs.entry(col).or_insert(DiskRun {
                        start: partition.id,
                        end: partition.id,
                        bytes: 0,
                        columns: [col.to_string()].iter().cloned().collect(),
                    });
                    run.bytes += partition.promise_load(&run.columns);
                    run.end = partition.id;

                    run.bytes > chunk_size
                };
                if reached_chunk_size {
                    task_queue.push_back(runs.remove(&col.as_str()).unwrap());
                }
            }
        }
        for (_, run) in runs {
            task_queue.push_back(run);
        }
        debug!("Scheduled sequential reads. Queue: {:#?}", &*task_queue);
    }

    pub fn get_or_load(&self, handle: &ColumnHandle, cols: &HashMap<String, ColumnHandle>, perf_counter: &QueryPerfCounter) -> Arc<Column> {
        loop {
            if handle.is_resident() {
                let mut maybe_column = handle.try_get();
                if let Some(ref mut column) = *maybe_column {
                    if self.lz4_decode {
                        if let Some(c) = Arc::get_mut(column) {
                            c.lz4_decode()
                        };
                        handle.update_size_bytes(column.heap_size_of_children());
                    }
                    self.lru.touch(handle.key());
                    return column.clone();
                } else {
                    debug!("{}.{} was not resident!", handle.name(), handle.id());
                }
            } else if handle.is_load_scheduled() {
                let mut is_load_in_progress = self.background_load_in_progress.lock().unwrap();
                while *is_load_in_progress && !handle.is_resident() && handle.is_load_scheduled() {
                    debug!("Queuing for {}.{}", handle.name(), handle.id());
                    is_load_in_progress = self
                        .background_load_wait_queue
                        .wait(is_load_in_progress)
                        .unwrap();
                }
            } else {
                debug!("Point lookup for {}.{}", handle.name(), handle.id());
                let columns = {
                    let _token = self.reader_semaphore.access();
                    self.disk_store.load_column(&handle.key().table, handle.id(), handle.name(), perf_counter)
                };
                let mut result = None;
                #[allow(unused_mut)]
                for mut column in columns {
                    let _handle = cols.get(column.name()).unwrap();
                    // Need to hold lock when we put new value into lru
                    let mut maybe_column = _handle.try_get();
                    // TODO: if not main handle, put it at back of lru
                    self.lru.put(_handle.key().clone());
                    if self.lz4_decode {
                        column.lz4_decode();
                        _handle.update_size_bytes(column.heap_size_of_children());
                    }
                    let column = Arc::new(column);
                    *maybe_column = Some(column.clone());
                    _handle.set_resident(column.heap_size_of_children());
                    if column.name() == handle.name() {
                        result = Some(column);
                    }
                }
                return result.unwrap()
            }
        }
    }

    pub fn service_reads(&self, ldb: &InnerLocustDB) {
        debug!("Waiting to service reads...");
        *self.background_load_in_progress.lock().unwrap() = true;
        debug!("Started servicing reads...");
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
            self.service_sequential_read(&next_read, ldb);
            self.background_load_wait_queue.notify_all();
        }
    }

    fn service_sequential_read(&self, run: &DiskRun, ldb: &InnerLocustDB) {
        let _token = self.reader_semaphore.access();
        debug!("Servicing read: {:?}", &run);
        for col in &run.columns {
            self.disk_store
                .load_column_range(run.start, run.end, col, ldb);
        }
    }
}
