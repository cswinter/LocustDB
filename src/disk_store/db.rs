use ingest::buffer::Buffer;
use mem_store::table::Metadata;


pub trait DB: Sync + Send + 'static {
    fn metadata(&self) -> Vec<&Metadata>;
    fn data(&self, table_name: &str) -> Vec<Buffer>;
}

