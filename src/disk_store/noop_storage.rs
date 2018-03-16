use disk_store::db::*;
use ingest::buffer::Buffer;
use mem_store::table::Metadata;

pub struct NoopStorage;

impl DB for NoopStorage {
    fn metadata(&self) -> Vec<&Metadata> { Vec::new() }
    fn data(&self, _: &str) -> Vec<Buffer> { Vec::new() }
}
