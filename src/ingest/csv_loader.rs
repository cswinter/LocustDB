extern crate csv;

use mem_store::batch::Batch;
use mem_store::column::*;
use mem_store::column_builder::*;
use mem_store::null_column::NullColumn;
use std::boxed::Box;
use std::ops::BitOr;
use std::sync::Arc;
use scheduler::*;


pub fn ingest_file(filename: &str, chunk_size: usize) -> Vec<Batch> {
    let mut reader = csv::Reader::from_file(filename)
        .unwrap()
        .has_headers(true);
    let headers = reader.headers().unwrap();
    auto_ingest(reader.records().map(|r| r.unwrap()), &headers, chunk_size)
}

fn auto_ingest<T: Iterator<Item=Vec<String>>>(records: T,
                                              colnames: &[String],
                                              batch_size: usize)
                                              -> Vec<Batch> {
    let num_columns = colnames.len();
    let mut batches = Vec::new();

    let mut raw_cols = (0..num_columns).map(|_| RawCol::new()).collect::<Vec<_>>();
    let mut row_num = 0usize;
    for row in records {
        for (i, val) in row.into_iter().enumerate() {
            raw_cols[i].push(val);
        }

        if row_num % batch_size == batch_size - 1 {
            batches.push(create_batch(raw_cols, colnames));
            raw_cols = (0..num_columns).map(|_| RawCol::new()).collect::<Vec<_>>();
        }
        row_num += 1;
    }

    if row_num % batch_size != 0 {
        batches.push(create_batch(raw_cols, colnames));
    }

    batches
}

fn create_batch(cols: Vec<RawCol>, colnames: &[String]) -> Batch {
    let mut mem_store = Vec::new();
    for (i, col) in cols.into_iter().enumerate() {
        mem_store.push(Column::new(colnames[i].clone(), col.finalize()));
    }
    Batch::from(mem_store)
}

pub struct CSVIngestionTask {
    filename: String,
    table: String,
    chunk_size: usize,
    ruba: Arc<InnerRuba>,
    sender: SharedSender<()>,
}

impl CSVIngestionTask {
    pub fn new(filename: String,
               table: String,
               chunk_size: usize,
               ruba: Arc<InnerRuba>,
               sender: SharedSender<()>) -> CSVIngestionTask {
        CSVIngestionTask {
            filename,
            table,
            chunk_size,
            ruba,
            sender,
        }
    }
}

impl Task for CSVIngestionTask {
    fn execute(&self) {
        let batches = ingest_file(&self.filename, self.chunk_size);
        self.ruba.load_batches(&self.table, batches);
        self.sender.send(());
    }
    fn completed(&self) -> bool { false }
    fn multithreaded(&self) -> bool { false }
}


struct RawCol {
    types: ColType,
    data: Vec<String>,
}

impl RawCol {
    fn new() -> RawCol {
        RawCol {
            types: ColType::nothing(),
            data: Vec::new(),
        }
    }

    fn push(&mut self, elem: String) {
        self.types = self.types | ColType::determine(&elem);
        self.data.push(elem);
    }

    fn finalize(self) -> Box<ColumnData> {
        if self.types.contains_string {
            let mut builder = StringColBuilder::new();
            for s in self.data {
                builder.push(&s);
            }
            builder.finalize()
        } else if self.types.contains_int {
            let mut builder = IntColBuilder::new();
            for s in self.data {
                let int = if s.is_empty() {
                    0
                } else if let Ok(int) = s.parse::<i64>() {
                    int
                } else if let Ok(float) = s.parse::<f64>() {
                    (float * 10000.) as i64
                } else {
                    unreachable!("{} should be parseable as int or float", s)
                };
                builder.push(&int);
            }
            builder.finalize()
        } else {
            Box::new(NullColumn::new(self.data.len()))
        }
    }
}


#[derive(Copy, Clone)]
struct ColType {
    contains_string: bool,
    contains_int: bool,
    contains_null: bool,
}

impl ColType {
    fn new(string: bool, int: bool, null: bool) -> ColType {
        ColType { contains_string: string, contains_int: int, contains_null: null }
    }

    fn string() -> ColType {
        ColType::new(true, false, false)
    }

    fn int() -> ColType {
        ColType::new(false, true, false)
    }

    fn null() -> ColType {
        ColType::new(false, false, true)
    }

    fn nothing() -> ColType {
        ColType::new(false, false, false)
    }

    fn determine(s: &str) -> ColType {
        if s.is_empty() {
            ColType::null()
        } else if s.parse::<i64>().is_ok() || s.parse::<f64>().is_ok() {
            ColType::int()
        } else {
            ColType::string()
        }
    }
}

impl BitOr for ColType {
    type Output = Self;
    fn bitor(self, rhs: ColType) -> Self::Output {
        ColType {
            contains_string: self.contains_string | rhs.contains_string,
            contains_int: self.contains_int | rhs.contains_int,
            contains_null: self.contains_null | rhs.contains_null,
        }
    }
}
