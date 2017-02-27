extern crate serde_json;
extern crate time;
#[macro_use]
extern crate nom;
extern crate heapsize;
extern crate rustyline;
extern crate itertools;
extern crate num;
extern crate regex;
extern crate csv;

mod util;
mod value;
mod expression;
mod aggregator;
mod limit;
mod columns;
mod query_engine;
mod parser;
use value::ValueType;
use columns::{auto_ingest, Batch};

use std::env;
use std::panic;
use std::collections::HashMap;
use heapsize::HeapSizeOf;
use time::precise_time_s;

const LOAD_CHUNK_SIZE: usize = 200_000;

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = &args[1];
    let mut reader = csv::Reader::from_file(filename)
        .unwrap()
        .has_headers(true);
    let headers = reader.headers().unwrap();
    println!("{:?}", headers);
    let columnarization_start_time = precise_time_s();
    let batches = auto_ingest(reader.records().map(|r| r.unwrap()), headers, LOAD_CHUNK_SIZE);
    print_ingestion_stats(&batches, columnarization_start_time);

    repl(&batches);
}

fn print_ingestion_stats(batches: &Vec<Batch>, starttime: f64) {
    let bytes_in_ram: usize = batches.iter().map(|batch| batch.cols.heap_size_of_children()).sum();
    println!("Loaded data into {:.2} MiB in RAM in {} chunk(s) in {:.1} seconds.",
             bytes_in_ram as f64 / 1024f64 / 1024f64,
             batches.len(),
             precise_time_s() - starttime);

    println!("\n# Breakdown by column #");
    let mut column_sizes = HashMap::new();
    for batch in batches {
        for col in &batch.cols {
            let heapsize = col.heap_size_of_children();
            if let Some(size) = column_sizes.get_mut(col.get_name()) {
                *size += heapsize;
            }
            if !column_sizes.contains_key(col.get_name()) {
                column_sizes.insert(col.get_name().to_string(), heapsize);
            }
        }
    }
    for (columname, heapsize) in column_sizes {
        println!("{}: {:.2}MiB", columname, heapsize as f64 / 1024. / 1024.);
    }
}

fn repl(datasource: &Vec<Batch>) {
    let mut rl = rustyline::Editor::<()>::new();
    rl.load_history(".ruba_history").ok();
    while let Ok(mut s) = rl.readline("ruba> ") {
        if let Some('\n')=s.chars().next_back() {
            s.pop();
        }
        if let Some('\r')=s.chars().next_back() {
            s.pop();
        }
        if s == "exit" { break }
        if s.chars().next_back() != Some(';') {
            s.push(';');
        }
        rl.add_history_entry(&s);
        match parser::parse_query(s.as_bytes()) {
            nom::IResult::Done(remaining, query) => {
                panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut compiled_query = query.compile(datasource);
                    let result = compiled_query.run();
                    query_engine::print_query_result(&result);
                }));
            },
            err => {
                println!("Failed to parse query! {:?}", err);
                println!("Example for supported query:");
                println!("select url, count(1), app_name, sum(events) where and( >(timestamp, 1000), =(version, \"1.5.3\") )\n");
            },
        }
    }
    rl.save_history(".ruba_history").ok();
}

/*
fn read_data<'a>(filename: &str) -> Vec<RecordType<'a>> {
    let file = BufReader::new(File::open(filename).unwrap());
    let json = serde_json::from_reader(file).unwrap();
    if let Value::Array(data) = json {
        data.into_iter().map(|v| json_to_record(v)).collect()
    } else {
        panic!("Unexpected JSON contents.")
    }
}

fn json_to_record<'a>(json: Value) -> RecordType<'a> {
    if let Value::Object(object) = json {
        object.into_iter().map(|(k, v)| (k, json_to_value(v))).collect()
    } else {
        panic!("Non-record object: {:?}", json)
    }
}

fn json_to_value(json: Value) -> ValueType {
    match json {
        Value::Null => ValueType::Null,
        Value::Bool(b) => ValueType::Integer(b as i64),
        Value::Number(n) => n.as_i64().map(ValueType::Integer)
                            .or(n.as_f64().map(|f| ValueType::Integer((1000.0 * f) as i64)))
                            .unwrap(),
        Value::String(s) => ValueType::Str(Rc::new(s)),
        Value::Array(arr) => ValueType::Set(Rc::new(arr.into_iter()
                                                    .map(|v| match v { Value::String(s) => s, _ => panic!("Expected list of strings") })
                                                    .collect())),
        o => panic!("Objects not supported: {:?}", o)
    }
}
*/
