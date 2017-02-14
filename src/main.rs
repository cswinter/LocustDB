extern crate serde_json;

mod value;
mod expression;
mod aggregator;
mod query_engine;
use value::{RecordType, ValueType};

use std::fs::File;
use serde_json::Value;
use std::io::{BufReader};
use std::env;
use std::rc::Rc;

fn main() {
    query_engine::test();
    let args: Vec<String> = env::args().collect();
    let data = read_data(&args[1]);
    println!("{:?}", data);
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
