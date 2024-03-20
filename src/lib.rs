// TODO: migrate off incomplete/unsound specialization feature
#![allow(incomplete_features)]
#![feature(
    fn_traits,
    specialization,
    trait_alias,
    core_intrinsics,
    box_patterns,
    proc_macro_hygiene,
    let_chains,
    trait_upcasting,
)]
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
pub use crate::disk_store::noop_storage::NoopStorage;

pub use crate::engine::query_task::{QueryOutput, BasicTypeColumn};
pub use crate::errors::QueryError;
pub use crate::ingest::colgen;
pub use crate::ingest::csv_loader::Options as LoadOptions;
pub use crate::ingest::extractor;
pub use crate::ingest::nyc_taxi_data;
pub use crate::ingest::raw_val::syntax as value_syntax;
pub use crate::ingest::raw_val::RawVal as Value;
pub use crate::locustdb::LocustDB;
pub use crate::locustdb::Options;
pub use crate::mem_store::table::TableStats;

#[macro_use]
mod errors;
mod bitvec;
pub mod disk_store;
mod engine;
mod ingest;
mod locustdb;
pub mod logging_client;
mod mem_store;
pub mod perf_counter;
mod scheduler;
pub mod server;
mod stringpack;
mod syntax;
pub mod unit_fmt;

#[cfg(feature = "python")]
pub mod python;

pub type QueryResult = Result<QueryOutput, QueryError>;
