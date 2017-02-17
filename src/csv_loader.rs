use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::rc::Rc;
use std::iter;
use std::boxed::Box;

use value::InpVal;
use value::InpRecordType;

pub struct CSVIter<'a> {
    iter: Box<Iterator<Item=InpRecordType<'a>> + 'a>,
}

impl<'a> Iterator for CSVIter<'a> {
    type Item = InpRecordType<'a>;

    fn next(&mut self) -> Option<InpRecordType<'a>> {
        self.iter.next()
    }
}

pub fn load_headers(filename: &str) -> Vec<String> {
    let mut file = BufReader::new(File::open(filename).unwrap());
    let mut lines_iter = file.lines();
    let first_line = lines_iter.next().unwrap().unwrap();
    first_line.split(",").map(|s| s.to_owned()).collect()
}

pub fn load_csv_file<'a>(filename: &str, headers: &'a Vec<String>) -> CSVIter<'a> {
    let mut file = BufReader::new(File::open(filename).unwrap());
    let mut lines_iter = file.lines();

    let first_line = lines_iter.next().unwrap().unwrap();

    let iter = lines_iter.map(move |line| {
        line.unwrap().split(",")
            .zip(headers.iter())
            .map(|(val, col)| parse_value(col, val))
            .collect()
    });

    CSVIter { iter: Box::new(iter) }
}

fn parse_value<'a>(colname: &'a str, value: &str) -> (&'a str, InpVal) {
    let val = if value == "" {
        InpVal::Null
    } else {
        match value.parse::<i64>() {
            Ok(int) => InpVal::Integer(int),
            Err(_) => InpVal::Str(Rc::new(value.to_string())),
        }
    };
    (colname, val)
}
