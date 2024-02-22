use std::sync::atomic::{AtomicU64, Ordering};

const ORDERING: Ordering = Ordering::SeqCst;

#[derive(Debug, Default)]
pub struct PerfCounter {
    disk_write_wal_bytes: AtomicU64,
    disk_write_new_partition_bytes: AtomicU64,
    disk_write_compaction_bytes: AtomicU64,
    disk_write_meta_store_bytes: AtomicU64,

    disk_read_partition_bytes: AtomicU64,
    disk_read_meta_store_bytes: AtomicU64,
    disk_read_wal_bytes: AtomicU64,

    files_created_wal: AtomicU64,
    files_created_meta_store: AtomicU64,
    files_created_new_partition: AtomicU64,

    file_accessed_partition: AtomicU64,

    ingestion_requests: AtomicU64,
    network_read_ingestion_bytes: AtomicU64,
}

impl PerfCounter {
    pub fn new() -> PerfCounter {
        PerfCounter::default()
    }

    pub fn disk_write_wal(&self, bytes: u64) {
        self.files_created_wal.fetch_add(1, ORDERING);
        self.disk_write_wal_bytes.fetch_add(bytes, ORDERING);
    }

    pub fn disk_write_new_partition(&self, bytes: u64) {
        self.disk_write_new_partition_bytes
            .fetch_add(bytes, ORDERING);
    }

    pub fn new_wal_file_write(&self, bytes: u64) {
        self.files_created_wal.fetch_add(1, ORDERING);
        self.disk_write_wal_bytes.fetch_add(bytes, ORDERING);
    }

    pub fn new_partition_file_write(&self, bytes: u64) {
        self.files_created_new_partition.fetch_add(1, ORDERING);
        self.disk_write_new_partition_bytes
            .fetch_add(bytes, ORDERING);
    }

    pub fn disk_write_compaction(&self, bytes: u64) {
        self.disk_write_compaction_bytes.fetch_add(bytes, ORDERING);
    }

    pub fn disk_write_meta_store(&self, bytes: u64) {
        self.files_created_meta_store.fetch_add(1, ORDERING);
        self.disk_write_meta_store_bytes.fetch_add(bytes, ORDERING);
    }

    pub fn disk_read_partition(&self, bytes: u64) {
        self.file_accessed_partition.fetch_add(1, ORDERING);
        self.disk_read_partition_bytes.fetch_add(bytes, ORDERING);
    }

    pub fn disk_read_meta_store(&self, bytes: u64) {
        self.disk_read_meta_store_bytes.fetch_add(bytes, ORDERING);
    }

    pub fn disk_read_wal(&self, bytes: u64) {
        self.disk_read_wal_bytes.fetch_add(bytes, ORDERING);
    }

    pub fn file_created_wal(&self) {
        self.files_created_wal.fetch_add(1, ORDERING);
    }

    pub fn file_created_new_partition(&self) {
        self.files_created_new_partition.fetch_add(1, ORDERING);
    }

    pub fn network_read_ingestion(&self, bytes: u64) {
        self.ingestion_requests.fetch_add(1, ORDERING);
        self.network_read_ingestion_bytes.fetch_add(bytes, ORDERING);
    }

    pub fn disk_write_bytes(&self) -> u64 {
        self.disk_write_wal_bytes.load(ORDERING)
            + self.disk_write_new_partition_bytes.load(ORDERING)
            + self.disk_write_compaction_bytes.load(ORDERING)
            + self.disk_write_meta_store_bytes.load(ORDERING)
    }

    pub fn disk_write_wal_bytes(&self) -> u64 {
        self.disk_write_wal_bytes.load(ORDERING)
    }

    pub fn disk_write_new_partition_bytes(&self) -> u64 {
        self.disk_write_new_partition_bytes.load(ORDERING)
    }

    pub fn disk_write_compaction_bytes(&self) -> u64 {
        self.disk_write_compaction_bytes.load(ORDERING)
    }

    pub fn disk_write_meta_store_bytes(&self) -> u64 {
        self.disk_write_meta_store_bytes.load(ORDERING)
    }

    pub fn files_created(&self) -> u64 {
        self.files_created_wal.load(ORDERING)
            + self.files_created_new_partition.load(ORDERING)
            + self.files_created_meta_store.load(ORDERING)
    }

    pub fn files_created_wal(&self) -> u64 {
        self.files_created_wal.load(ORDERING)
    }

    pub fn files_created_new_partition(&self) -> u64 {
        self.files_created_new_partition.load(ORDERING)
    }

    pub fn files_created_meta_store(&self) -> u64 {
        self.files_created_meta_store.load(ORDERING)
    }

    pub fn network_read_ingestion_bytes(&self) -> u64 {
        self.network_read_ingestion_bytes.load(ORDERING)
    }

    pub fn ingestion_requests(&self) -> u64 {
        self.ingestion_requests.load(ORDERING)
    }
}
