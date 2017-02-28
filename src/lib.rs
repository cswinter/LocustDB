extern crate serde_json;
#[macro_use]
extern crate nom;
extern crate heapsize;
extern crate itertools;
extern crate num;
extern crate regex;
extern crate csv;
extern crate time;

pub mod parser;
pub mod columns;
pub mod query_engine;

mod util;
mod value;
mod expression;
mod aggregator;
mod limit;

use value::ValueType;
use columns::{auto_ingest, Batch};

use std::env;
use std::panic;
use std::collections::HashMap;

pub fn ingest_file(filename: &str, chunk_size: usize) -> Vec<Batch> {
    let mut reader = csv::Reader::from_file(filename)
        .unwrap()
        .has_headers(true);
    let headers = reader.headers().unwrap();
    auto_ingest(reader.records().map(|r| r.unwrap()), headers, chunk_size)
}
