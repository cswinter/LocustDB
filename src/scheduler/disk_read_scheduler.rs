// background_load_in_progress used with condition variable
#![allow(clippy::mutex_atomic)]

use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
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
    // Maps (TableName, PartitionID) to whether a load is scheduled for that partition.
    load_scheduled: RwLock<HashMap<(String, PartitionID), AtomicBool>>,

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
            load_scheduled: RwLock::default(),
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
        let partition_handle = (handle.table().to_string(), handle.id());
        if !self
            .load_scheduled
            .read()
            .unwrap()
            .contains_key(&partition_handle)
        {
            self.load_scheduled
                .write()
                .unwrap()
                .insert(partition_handle.clone(), AtomicBool::new(false));
        }

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
            } else if self.is_load_scheduled(&partition_handle) {
                let mut is_load_in_progress = self.background_load_in_progress.lock().unwrap();
                while *is_load_in_progress
                    && !handle.is_resident()
                    && self.is_load_scheduled(&partition_handle)
                {
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
                    {
                        let mut load_scheduled = self.load_scheduled.write().unwrap();
                        if load_scheduled
                            .get(&partition_handle)
                            .unwrap()
                            .load(Ordering::SeqCst)
                        {
                            continue;
                        } else {
                            load_scheduled
                                .get_mut(&partition_handle)
                                .unwrap()
                                .store(true, Ordering::SeqCst);
                        }
                    }

                    let _token = self.reader_semaphore.access();
                    match self.disk_store.load_column(
                        &handle.key().table,
                        handle.id(),
                        handle.name(),
                        perf_counter,
                    ) {
                        Some(columns) => columns,
                        None => {
                            handle.set_empty();
                            return None;
                        }
                    }
                };
                if handle.is_resident() {
                    log::warn!(
                        "Loaded partition for column which was already resident: {}",
                        handle.name()
                    );
                }
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
                self.disk_store.mark_subpartition_as_loaded(
                    handle.table(),
                    handle.id(),
                    handle.name(),
                );
                self.load_scheduled.read().unwrap().get(&partition_handle).unwrap().store(false, Ordering::SeqCst);
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

    pub fn is_load_scheduled(&self, partition_key: &(String, PartitionID)) -> bool {
        self.load_scheduled
            .read()
            .unwrap()
            .get(partition_key)
            .unwrap()
            .load(Ordering::SeqCst)
    }

    fn service_sequential_read(&self, run: &DiskRun, ldb: &InnerLocustDB) {
        let _token = self.reader_semaphore.access();
        debug!("Servicing read: {:?}", &run);
        for col in &run.columns {
            self.disk_store
                .load_column_range(run.start, run.end, col, ldb);
        }
    }

    pub fn partition_has_been_loaded(
        &self,
        table: &str,
        partition: PartitionID,
        column: &str,
    ) -> bool {
        self.disk_store
            .partition_has_been_loaded(table, partition, column)
    }
}
