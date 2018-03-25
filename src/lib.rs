#![feature(conservative_impl_trait, fn_traits, integer_atomics, refcell_replace_swap)]
#[macro_use]
extern crate nom;
#[macro_use]
extern crate serde_derive;

extern crate bincode;
extern crate heapsize;
extern crate itertools;
extern crate num;
extern crate regex;
extern crate time;
extern crate seahash;
extern crate serde;
extern crate bit_vec;
extern crate num_cpus;
extern crate futures;
extern crate futures_channel;
// extern crate tempdir;

#[macro_use]
mod trace;
mod syntax;
mod mem_store;
mod ingest;
mod engine;
mod scheduler;
mod ruba;
mod disk_store;

pub use ingest::raw_val::RawVal as Value;
pub use ruba::Ruba as Ruba;
pub use engine::query_task::QueryResult;
pub use mem_store::table::TableStats;