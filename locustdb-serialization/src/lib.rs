pub mod dbmeta_capnp;
pub mod partition_segment_capnp;
pub mod wal_segment_capnp;
pub mod api_capnp;
pub mod api;
pub mod event_buffer;


pub fn default_reader_options() -> capnp::message::ReaderOptions {
    let mut options = capnp::message::ReaderOptions::new();
    // Allow messages up to 8 GiB
    options.traversal_limit_in_words(Some(1024 * 1024 * 1024));
    options
}