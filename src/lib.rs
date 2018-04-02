#![feature(conservative_impl_trait, fn_traits, integer_atomics, refcell_replace_swap, trait_alias)]
#[macro_use]
extern crate nom;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
extern crate bincode;
extern crate bit_vec;
extern crate failure;
extern crate futures;
extern crate futures_channel;
extern crate heapsize;
extern crate itertools;
extern crate num;
extern crate num_cpus;
extern crate regex;
extern crate seahash;
extern crate serde;
extern crate time;
// extern crate tempdir;

#[macro_use]
mod trace;
#[macro_use]
mod errors;
mod syntax;
mod mem_store;
mod ingest;
mod engine;
mod scheduler;
mod ruba;
mod disk_store;

pub use ingest::raw_val::RawVal as Value;
pub use ruba::Ruba as Ruba;
pub use engine::query_task::QueryOutput;
pub use mem_store::table::TableStats;
pub use ingest::extractor;
pub use errors::QueryError;

pub type QueryResult = Result<QueryOutput, QueryError>;

#[doc(hidden)]
pub use trace::_replace;
#[doc(hidden)]
pub use trace::_start;
