// background_load_in_progress used with condition variable
#![allow(clippy::mutex_atomic)]

use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std_semaphore::Semaphore;

use crate::disk_store::*;
use crate::mem_store::partition::ColumnHandle;
use crate::mem_store::*;
use crate::observability::QueryPerfCounter;
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

    /// Returns the column if it's already loaded.
    /// If not, loads the relevant partition and also populates all other columns in the same subpartition.
    pub fn get_or_load(
        &self,
        handle: &ColumnHandle,
        cols: &RwLock<HashMap<String, Arc<ColumnHandle>>>,
        perf_counter: &QueryPerfCounter,
    ) -> Option<Arc<Column>> {
        loop {
            // Empty marker
            if handle.is_empty() {
                return None;
            // Handle already loaded! Return data.
            } else if handle.is_resident() {
                let mut maybe_column = handle.try_get();
                if let Some(ref mut column) = *maybe_column {
                    if self.lz4_decode {
                        if let Some(c) = Arc::get_mut(column) {
                            c.lz4_or_pco_decode()
                        };
                        handle.update_size_bytes(column.heap_size_of_children());
                    }
                    self.lru.touch(handle.key());
                    return Some(column.clone());
                } else {
                    debug!("{}.{} was not resident!", handle.name(), handle.id());
                }
            // Load for column is already scheduled, wait for it to complete.
            // TODO: this doesn't do anything currently, was only used by sequential disk reads. should check whether relevant subpartition is currently being loaded.
            } else if handle.is_load_scheduled() {
                let mut is_load_in_progress = self.background_load_in_progress.lock().unwrap();
                while *is_load_in_progress && !handle.is_resident() && handle.is_load_scheduled() {
                    debug!("Queuing for {}.{}", handle.name(), handle.id());
                    is_load_in_progress = self
                        .background_load_wait_queue
                        .wait(is_load_in_progress)
                        .unwrap();
                }
            // Load for column is not scheduled, load all columns in the same subpartition..
            } else {
                // TODO: ensure same partition isn't being loaded multiple times
                debug!("Point lookup for {}.{}", handle.name(), handle.id());
                let columns = {
                    let _token = self.reader_semaphore.access();
                    self.disk_store.load_column(
                        &handle.key().table,
                        handle.id(),
                        handle.name(),
                        perf_counter,
                    )
                };
                let mut result = None;
                #[allow(unused_mut)]
                let mut cols = cols.write().unwrap();
                for mut column in columns {
                    let _handle = cols.entry(column.name().to_string()).or_insert(Arc::new(
                        ColumnHandle::non_resident(
                            handle.table(),
                            handle.id(),
                            column.name().to_string(),
                        ),
                    ));
                    // Need to hold lock when we put new value into lru
                    let mut maybe_column = _handle.try_get();
                    // TODO: if not main handle, put it at back of lru
                    self.lru.put(_handle.key().clone());
                    if self.lz4_decode {
                        column.lz4_or_pco_decode();
                        _handle.update_size_bytes(column.heap_size_of_children());
                    }
                    let column = Arc::new(column);
                    *maybe_column = Some(column.clone());
                    _handle.set_resident(column.heap_size_of_children());
                    if column.name() == handle.name() {
                        result = Some(column);
                    }
                }
                match result {
                    Some(column) => return Some(column),
                    None => handle.set_empty(),
                }
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
