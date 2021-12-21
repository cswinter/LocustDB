// TODO: migrate off incomplete/unsound specialization feature
#![allow(incomplete_features)]
#![feature(
    fn_traits,
    integer_atomics,
    specialization,
    trait_alias,
    core_intrinsics,
    box_patterns,
    proc_macro_hygiene
)]
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

pub use crate::disk_store::noop_storage::NoopStorage;
pub use crate::engine::query_task::QueryOutput;
pub use crate::errors::QueryError;
pub use crate::ingest::colgen;
pub use crate::ingest::csv_loader::Options as LoadOptions;
pub use crate::ingest::extractor;
pub use crate::ingest::float::FloatOrd;
pub use crate::ingest::nyc_taxi_data;
pub use crate::ingest::raw_val::syntax as value_syntax;
pub use crate::ingest::raw_val::RawVal as Value;
pub use crate::locustdb::LocustDB;
pub use crate::locustdb::Options;
pub use crate::mem_store::table::TableStats;

#[macro_use]
mod errors;
mod bitvec;
mod disk_store;
mod engine;
mod ingest;
mod locustdb;
pub mod logging_client;
mod mem_store;
mod scheduler;
pub mod server;
mod stringpack;
mod syntax;
pub mod unit_fmt;

#[cfg(feature = "python")]
pub mod python;

pub type QueryResult = Result<QueryOutput, QueryError>;

#[allow(warnings)]
#[cfg(feature = "enable_rocksdb")]
pub(crate) mod storage_format_capnp {
    include!(concat!(env!("OUT_DIR"), "/storage_format_capnp.rs"));
}
