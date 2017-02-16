use std::fs::File;
use std::io::BufReader;
use std::io::BufRead;
use std::rc::Rc;
use std::iter;
use std::boxed::Box;

use value::InpVal;
use value::InpRecordType;

pub struct CSVIter<'a> {
    iter: Box<Iterator<Item=InpRecordType> + 'a>,
}

impl<'a> Iterator for CSVIter<'a> {
    type Item = InpRecordType;

    fn next(&mut self) -> Option<InpRecordType> {
        self.iter.next()
    }
}

pub fn load_csv_file(filename: &str) -> CSVIter {
    let mut file = BufReader::new(File::open(filename).unwrap());
    let mut lines_iter = file.lines();

    let first_line = lines_iter.next().unwrap().unwrap();
    let headers: Vec<String> = first_line.split(",").map(|s| s.to_owned()).collect();

    let iter = lines_iter.map(move |line| {
        let record = line.unwrap().split(",")
            .zip(headers.iter())
            .map(|(val, col)| parse_value(col, val))
            .collect();
        record
    });
    CSVIter { iter: Box::new(iter) }
}

fn parse_value(colname: &str, value: &str) -> (String, InpVal) {
    let val = if value == "" {
        InpVal::Null
    } else {
        match value.parse::<i64>() {
            Ok(int) => InpVal::Integer(int),
            Err(_) => InpVal::Str(Rc::new(value.to_string())),
        }
    };
    (colname.to_string(), val)
}
