use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::rc::Rc;
use std::iter;
use std::boxed::Box;

use value::ValueType;
use value::RecordType;

pub struct CSVIter<'a> {
    iter: Box<Iterator<Item=RecordType> + 'a>
}

impl<'a> Iterator for CSVIter<'a> {
    type Item = RecordType;

    fn next(&mut self) -> Option<RecordType> {
        self.iter.next()
    }
}

pub fn load_csv_file(filename: &str) -> CSVIter {
    let mut file = BufReader::new(File::open(filename).unwrap());
    let mut lines_iter = file.lines();

    let first_line = lines_iter.next().unwrap().unwrap();
    let headers: Vec<String> = first_line.split(",").map(|s| s.to_owned()).collect();

    let iter = lines_iter.map(move |line| {
        let l = line.unwrap();
        let record: RecordType = l.split(",")
            .zip(headers.iter())
            .map(|(val, col)| parse_value(col, val))
            .collect();
        record
    });
    CSVIter { iter: Box::new(iter) }
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
