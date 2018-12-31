#![feature(fn_traits, integer_atomics, refcell_replace_swap, specialization, trait_alias, core_intrinsics, box_patterns, proc_macro_hygiene)]
extern crate aliasmethod;
extern crate byteorder;
#[cfg(feature = "enable_rocksdb")]
extern crate capnp;
extern crate chrono;
extern crate crypto;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate fnv;
extern crate futures_channel;
extern crate futures_core;
extern crate futures_executor;
extern crate futures_util;
extern crate heapsize;
#[macro_use]
extern crate heapsize_derive;
extern crate hex;
extern crate itertools;
#[macro_use]
extern crate lazy_static;
extern crate locustdb_derive;
#[macro_use]
extern crate log;
extern crate lru;
extern crate num;
extern crate num_cpus;
extern crate rand;
extern crate regex;
extern crate seahash;
extern crate sqlparser;
extern crate std_semaphore;
extern crate tempdir;
extern crate time;


pub use disk_store::noop_storage::NoopStorage;
pub use engine::query_task::QueryOutput;
pub use errors::QueryError;
pub use ingest::colgen;
pub use ingest::csv_loader::Options as LoadOptions;
pub use ingest::extractor;
pub use ingest::nyc_taxi_data;
pub use ingest::raw_val::RawVal as Value;
pub use ingest::raw_val::syntax as value_syntax;
pub use locustdb::LocustDB as LocustDB;
pub use locustdb::Options as Options;
pub use mem_store::table::TableStats;
#[doc(hidden)]
pub use trace::_replace;
#[doc(hidden)]
pub use trace::_start;

#[macro_use]
mod trace;
#[macro_use]
mod errors;
mod syntax;
mod mem_store;
mod ingest;
mod engine;
mod scheduler;
mod locustdb;
mod disk_store;
mod stringpack;
mod bitvec;
pub mod unit_fmt;

pub type QueryResult = Result<QueryOutput, QueryError>;

#[allow(warnings)]
#[cfg(feature = "enable_rocksdb")]
pub(crate) mod storage_format_capnp {
    include!(concat!(env!("OUT_DIR"), "/storage_format_capnp.rs"));
}

