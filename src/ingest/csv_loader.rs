extern crate csv;
extern crate flate2;

use mem_store::column::*;
use mem_store::column_builder::*;
use mem_store::strings::fast_build_string_column;
use scheduler::*;
use self::flate2::read::GzDecoder;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::ops::BitOr;
use std::str;
use std::sync::Arc;
use super::extractor;
use stringpack::*;

type IngestionTransform = HashMap<String, extractor::Extractor>;

pub struct Options {
    filename: String,
    tablename: String,
    partition_size: usize,
    colnames: Option<Vec<String>>,
    extractors: IngestionTransform,
    ignore_cols: HashSet<String>,
    always_string: HashSet<String>,
    unzip: bool,
}

impl Options {
    pub fn new(filename: &str, tablename: &str) -> Options {
        Options {
            filename: filename.to_owned(),
            tablename: tablename.to_owned(),
            partition_size: 1 << 16,
            colnames: None,
            extractors: HashMap::new(),
            ignore_cols: HashSet::new(),
            always_string: HashSet::new(),
            unzip: filename.ends_with(".gz"),
        }
    }

    pub fn with_partition_size(mut self, chunk_size: usize) -> Options {
        self.partition_size = chunk_size;
        self
    }

    pub fn with_column_names(mut self, col_names: Vec<String>) -> Options {
        self.colnames = Some(col_names);
        self
    }

    pub fn with_extractors(mut self, extractors: &[(&str, extractor::Extractor)]) -> Options {
        self.extractors = extractors.iter().map(|&(col, extractor)| (col.to_owned(), extractor)).collect();
        self
    }

    pub fn with_ignore_cols(mut self, ignore: &[String]) -> Options {
        self.ignore_cols = ignore.into_iter().map(|x| x.to_owned()).collect();
        self
    }

    pub fn with_always_string(mut self, always_string: &[&str]) -> Options {
        self.always_string = always_string.into_iter().map(|&x| x.to_owned()).collect();
        self
    }
}

pub fn ingest_file(ldb: &InnerLocustDB, opts: &Options) -> Result<(), String> {
    // Can't combine these two branches because csv::Reader takes a type param which differs for creating from Reader/File
    if opts.unzip {
        let f = File::open(&opts.filename).map_err(|x| x.to_string())?;
        let decoded = GzDecoder::new(f);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(opts.colnames.is_none())
            .from_reader(decoded);
        let headers = match opts.colnames {
            Some(ref colnames) => colnames.clone(),
            None => reader.headers().unwrap().iter().map(str::to_owned).collect()
        };
        auto_ingest(ldb, reader.records().map(|r| r.unwrap()), &headers, opts)
    } else {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(opts.colnames.is_none())
            .from_path(&opts.filename)
            .map_err(|x| x.to_string())?;
        let headers = match opts.colnames {
            Some(ref colnames) => colnames.clone(),
            None => reader.headers().unwrap().iter().map(str::to_owned).collect()
        };
        auto_ingest(ldb, reader.records().map(|r| r.unwrap()), &headers, opts)
    }
}

fn auto_ingest<T>(ldb: &InnerLocustDB, records: T, colnames: &[String], opts: &Options) -> Result<(), String>
    where T: Iterator<Item=csv::StringRecord> {
    let ignore = colnames.iter().map(|x| opts.ignore_cols.contains(x)).collect::<Vec<_>>();
    let string = colnames.iter().map(|x| opts.always_string.contains(x)).collect::<Vec<_>>();
    let mut raw_cols = (0..colnames.len()).map(|_| RawCol::new()).collect::<Vec<_>>();
    let mut row_num = 0usize;
    for row in records {
        for (i, val) in row.iter().enumerate() {
            if !ignore[i] {
                raw_cols[i].push(val);
            }
        }

        if row_num % opts.partition_size == opts.partition_size - 1 {
            let partition = create_batch(&mut raw_cols, colnames, &opts.extractors, &ignore, &string);
            ldb.store_partition(&opts.tablename, partition);
        }
        row_num += 1;
    }

    if row_num % opts.partition_size != 0 {
        let partition = create_batch(&mut raw_cols, colnames, &opts.extractors, &ignore, &string);
        ldb.store_partition(&opts.tablename, partition);
    }
    Ok(())
}

fn create_batch(cols: &mut [RawCol], colnames: &[String], extractors: &IngestionTransform, ignore: &[bool], string: &[bool]) -> Vec<Arc<Column>> {
    let mut mem_store = Vec::new();
    for (i, col) in cols.iter_mut().enumerate() {
        if !ignore[i] {
            let new_column = match extractors.get(&colnames[i]) {
                Some(extractor) => col.extract(&colnames[i], extractor),
                None => col.finalize(&colnames[i], string[i]),
            };
            mem_store.push(new_column);
        }
    }
    mem_store
}

pub struct CSVIngestionTask {
    options: Options,
    locustdb: Arc<InnerLocustDB>,
    sender: SharedSender<Result<(), String>>,
}

impl CSVIngestionTask {
    pub fn new(options: Options,
               locustdb: Arc<InnerLocustDB>,
               sender: SharedSender<Result<(), String>>) -> CSVIngestionTask {
        CSVIngestionTask {
            options,
            locustdb,
            sender,
        }
    }
}

impl Task for CSVIngestionTask {
    fn execute(&self) {
        self.sender.send(ingest_file(&self.locustdb, &self.options))
    }
    fn completed(&self) -> bool { false }
    fn multithreaded(&self) -> bool { false }
}


struct RawCol {
    types: ColType,
    values: IndexedPackedStrings,
    lhex: bool,
    uhex: bool,
    string_bytes: usize,
}

impl RawCol {
    fn new() -> RawCol {
        RawCol {
            types: ColType::nothing(),
            values: IndexedPackedStrings::default(),
            lhex: true,
            uhex: true,
            string_bytes: 0,
        }
    }

    fn push(&mut self, elem: &str) {
        self.types = self.types | ColType::determine(elem);
        self.lhex = self.lhex && is_lowercase_hex(elem);
        self.uhex = self.uhex && is_uppercase_hex(elem);
        self.string_bytes += elem.as_bytes().len();
        self.values.push(elem);
    }

    fn finalize(&mut self, name: &str, string: bool) -> Arc<Column> {
        let result = if self.types.contains_string || string {
            fast_build_string_column(name, self.values.iter(), self.values.len(),
                                     self.lhex, self.uhex, self.string_bytes)
        } else if self.types.contains_int {
            let mut builder = IntColBuilder::new();
            for s in self.values.iter() {
                let int = if s.is_empty() {
                    0
                } else if let Ok(int) = s.parse::<i64>() {
                    int
                } else if let Ok(float) = s.parse::<f64>() {
                    float as i64
                } else {
                    unreachable!("{} should be parseable as int or float. {} {:?}", s, name, self.types)
                };
                builder.push(&int);
            }
            builder.finalize(name)
        } else {
            Arc::new(Column::null(name, self.values.len()))
        };
        self.clear();
        result
    }

    fn extract(&mut self, name: &str, extractor: &extractor::Extractor) -> Arc<Column> {
        let mut builder = IntColBuilder::new();
        for s in self.values.iter() {
            builder.push(&extractor(s));
        }
        self.clear();
        builder.finalize(name)
    }

    fn clear(&mut self) {
        self.types = ColType::nothing();
        self.values.clear();
    }
}

fn is_lowercase_hex(string: &str) -> bool {
    string.len() & 1 == 0 && string.chars().all(|c| {
        c == '0' || c == '1' || c == '2' || c == '3' ||
            c == '4' || c == '5' || c == '6' || c == '7' ||
            c == '8' || c == '9' || c == 'a' || c == 'b' ||
            c == 'c' || c == 'd' || c == 'e' || c == 'f'
    })
}

fn is_uppercase_hex(string: &str) -> bool {
    string.len() & 1 == 0 && string.chars().all(|c| {
        c == '0' || c == '1' || c == '2' || c == '3' ||
            c == '4' || c == '5' || c == '6' || c == '7' ||
            c == '8' || c == '9' || c == 'A' || c == 'B' ||
            c == 'C' || c == 'D' || c == 'E' || c == 'F'
    })
}


#[derive(Copy, Clone, Debug)]
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

