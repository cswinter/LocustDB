use prometheus::{register_counter, register_gauge};
use prometheus::{Counter, Gauge};

lazy_static! {
    pub static ref QUERY_COUNT: Counter =
        register_counter!("query_count", "Number of queries executed").unwrap();
    pub static ref QUERY_OK_COUNT: Counter =
        register_counter!("query_ok_count", "Number of queries executed successfully").unwrap();
    pub static ref QUERY_ERROR_COUNT: Counter =
        register_counter!("query_error_count", "Number of queries executed with errors").unwrap();
    pub static ref INGESTION_EVENT_COUNT: Counter =
        register_counter!("ingestion_event_count", "Number of ingestion events").unwrap();
    pub static ref WAL_SIZE_BYTES: Gauge =
        register_gauge!("wal_size_bytes", "Size of the WAL").unwrap();
    pub static ref WAL_UTILIZATION: Gauge =
        register_gauge!("wal_utilization", "Utilization of the WAL").unwrap();
    pub static ref COLUMN_CACHE_BYTES: Gauge =
        register_gauge!("column_cache_bytes", "In-memory size of the column cache").unwrap();
    pub static ref COLUMN_CACHE_UTILIZATION: Gauge =
        register_gauge!("column_cache_utilization", "Utilization of the column cache").unwrap();
    pub static ref META_STORE_BYTES: Gauge =
        register_gauge!("meta_store_bytes", "In-memory size of the meta store").unwrap();
    pub static ref TABLE_COUNT: Gauge =
        register_gauge!("table_count", "Number of tables").unwrap();
    pub static ref ROW_COUNT: Gauge =
        register_gauge!("row_count", "Number of rows in the database").unwrap();
    pub static ref PARTITION_COUNT: Gauge =
        register_gauge!("partition_count", "Number of partitions in the database").unwrap();
    pub static ref PARTITION_COLUMN_COUNT: Gauge =
        register_gauge!("partition_column_count", "Sum of columns over all partitions").unwrap();
    pub static ref PARTITION_VALUES: Gauge =
        register_gauge!("value_count", "Number of values in the partitions in the database").unwrap();
    pub static ref DATABASE_SIZE_BYTES: Gauge =
        register_gauge!("database_size_bytes", "Size of the database").unwrap();
}
