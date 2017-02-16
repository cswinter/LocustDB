extern crate serde_json;
extern crate time;
#[macro_use]
extern crate nom;
extern crate heapsize;
extern crate rustyline;

mod util;
mod value;
mod expression;
mod aggregator;
mod columns;
mod query_engine;
mod csv_loader;
mod parser;
use value::{RecordType, ValueType};
use columns::columnarize;
use columns::Column;

use std::fs::File;
use serde_json::Value;
use std::io::{BufReader};
use std::env;
use std::rc::Rc;
use heapsize::HeapSizeOf;
use time::precise_time_s;

fn main() {
    let args: Vec<String> = env::args().collect();
    let data = csv_loader::load_csv_file(&args[1]);
    let columnarization_start_time = precise_time_s();
    let cols = columnarize(data);
    let bytes_in_ram = cols.heap_size_of_children();
    println!("Loaded data into {:.2} MiB in RAM in {:.1} seconds.",
             bytes_in_ram as f64 / 1024f64 / 1024f64,
             precise_time_s() - columnarization_start_time);
    repl(&cols);
}

fn repl(datasource: &Vec<Box<Column>>) {
    use std::io::{stdin,stdout,Write};
    let mut rl = rustyline::Editor::<()>::new();
    rl.load_history(".ruba_history");
    loop {
        let mut s = rl.readline("ruba> ").expect("Did not enter a correct string");
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
                println!("{:?}, {:?}\n", query, remaining);
                let result = query.run(datasource);
                query_engine::print_query_result(&result);
            },
            err => println!("Failed to parse query! {:?}", err),
        }
    }
    rl.save_history(".ruba_history");
}

fn read_data(filename: &str) -> Vec<RecordType> {
    let file = BufReader::new(File::open(filename).unwrap());
    let json = serde_json::from_reader(file).unwrap();
    if let Value::Array(data) = json {
        data.into_iter().map(|v| json_to_record(v)).collect()
    } else {
        panic!("Unexpected JSON contents.")
    }
}

fn json_to_record(json: Value) -> RecordType {
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
