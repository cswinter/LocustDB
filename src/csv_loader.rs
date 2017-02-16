use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::rc::Rc;
use std::iter;

use value::ValueType;
use value::RecordType;


pub fn load_csv_file(filename: &str) -> Vec<RecordType> {
    let mut file = BufReader::new(File::open(filename).unwrap());
    let mut lines_iter = file.lines();

    let first_line = lines_iter.next().unwrap().unwrap();
    let headers: Vec<&str> = first_line.split(",").collect();

    lines_iter.map(|line| {
        let l = line.unwrap();
        let record: RecordType = l.split(",")
            .zip(headers.iter())
            .map(|(val, col)| parse_value(col, val))
            .collect();
        record
    }).collect()
}

fn parse_value(colname: &str, value: &str) -> (String, ValueType) {
    let val = if value == "" {
        ValueType::Null
    } else {
        match value.parse::<i64>() {
            Ok(int) => ValueType::Integer(int),
            Err(_) => ValueType::Str(Rc::new(value.to_string())),
        }
    };
    (colname.to_string(), val)
}
