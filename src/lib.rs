#![feature(fn_traits, integer_atomics, refcell_replace_swap, specialization, trait_alias, core_intrinsics, box_patterns)]
#[macro_use]
extern crate nom;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
extern crate bincode;
extern crate chrono;
extern crate failure;
extern crate futures_core;
extern crate futures_util;
extern crate futures_channel;
extern crate futures_executor;
#[macro_use]
extern crate heapsize_derive;
extern crate heapsize;
extern crate itertools;
extern crate num;
extern crate num_cpus;
extern crate regex;
extern crate seahash;
extern crate serde;
extern crate time;
extern crate tempdir;
extern crate fnv;
extern crate byteorder;
extern crate lru;
extern crate crypto;


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
pub mod unit_fmt;

pub use engine::query_task::QueryOutput;
pub use errors::QueryError;
pub use ingest::csv_loader::IngestFile;
pub use ingest::extractor;
pub use ingest::nyc_taxi_data;
pub use ingest::raw_val::RawVal as Value;
pub use locustdb::LocustDB as LocustDB;
pub use locustdb::Options as Options;
pub use mem_store::table::TableStats;
pub use disk_store::noop_storage::NoopStorage;

pub type QueryResult = Result<QueryOutput, QueryError>;

#[doc(hidden)]
pub use trace::_replace;
#[doc(hidden)]
pub use trace::_start;
