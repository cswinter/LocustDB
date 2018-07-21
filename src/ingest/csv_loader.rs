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

type IngestionTransform = HashMap<String, extractor::Extractor>;

pub struct IngestFile {
    filename: String,
    tablename: String,
    chunk_size: usize,
    col_names: Option<Vec<String>>,
    extractors: IngestionTransform,
    ignore_cols: HashSet<String>,
    always_string: HashSet<String>,
    unzip: bool,
}

impl IngestFile {
    pub fn new(filename: &str, tablename: &str) -> IngestFile {
        IngestFile {
            filename: filename.to_owned(),
            tablename: tablename.to_owned(),
            chunk_size: 1 << 16,
            col_names: None,
            extractors: HashMap::new(),
            ignore_cols: HashSet::new(),
            always_string: HashSet::new(),
            unzip: filename.ends_with(".gz"),
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> IngestFile {
        self.chunk_size = chunk_size;
        self
    }

    pub fn with_col_names(mut self, col_names: Vec<String>) -> IngestFile {
        self.col_names = Some(col_names);
        self
    }

    pub fn with_extractors(mut self, extractors: &[(&str, extractor::Extractor)]) -> IngestFile {
        self.extractors = extractors.iter().map(|&(col, extractor)| (col.to_owned(), extractor)).collect();
        self
    }

    pub fn with_ignore_cols(mut self, ignore: &[String]) -> IngestFile {
        self.ignore_cols = ignore.into_iter().map(|x| x.to_owned()).collect();
        self
    }

    pub fn with_always_string(mut self, always_string: &[&str]) -> IngestFile {
        self.always_string = always_string.into_iter().map(|&x| x.to_owned()).collect();
        self
    }
}

pub fn ingest_file(filename: &str,
                   colnames: Option<Vec<String>>,
                   chunk_size: usize,
                   extractors: &IngestionTransform,
                   unzip: bool,
                   ignore_cols: &HashSet<String>,
                   always_string: &HashSet<String>) -> Result<Vec<Vec<Arc<Column>>>, String> {
    // Can't combine these two branches because csv::Reader takes a type param which differs for creating from Reader/File
    if unzip {
        let f = File::open(filename).map_err(|x| x.to_string())?;
        let decoded = GzDecoder::new(f);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(colnames.is_none())
            .from_reader(decoded);
        let headers = match colnames {
            Some(colnames) => colnames,
            None => reader.headers().unwrap().iter().map(str::to_owned).collect()
        };
        Ok(auto_ingest(reader.records().map(|r| r.unwrap()), &headers, chunk_size, extractors, ignore_cols, always_string))
    } else {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(colnames.is_none())
            .from_path(filename)
            .map_err(|x| x.to_string())?;
        let headers = match colnames {
            Some(colnames) => colnames,
            None => reader.headers().unwrap().iter().map(str::to_owned).collect()
        };
        Ok(auto_ingest(reader.records().map(|r| r.unwrap()), &headers, chunk_size, extractors, ignore_cols, always_string))
    }
}

fn auto_ingest<T: Iterator<Item=csv::StringRecord>>(records: T,
                                                    colnames: &[String],
                                                    batch_size: usize,
                                                    extractors: &IngestionTransform,
                                                    ignore_cols: &HashSet<String>,
                                                    always_string: &HashSet<String>) -> Vec<Vec<Arc<Column>>> {
    let num_columns = colnames.len();
    let mut batches = Vec::new();
    let ignore = colnames.iter().map(|x| ignore_cols.contains(x)).collect::<Vec<_>>();
    let string = colnames.iter().map(|x| always_string.contains(x)).collect::<Vec<_>>();
    let mut raw_cols = (0..num_columns).map(|_| RawCol::new()).collect::<Vec<_>>();
    let mut row_num = 0usize;
    for row in records {
        for (i, val) in row.iter().enumerate() {
            if !ignore[i] {
                raw_cols[i].push(val);
            }
        }

        if row_num % batch_size == batch_size - 1 {
            batches.push(create_batch(&mut raw_cols, colnames, extractors, &ignore, &string));
        }
        row_num += 1;
    }

    if row_num % batch_size != 0 {
        batches.push(create_batch(&mut raw_cols, colnames, extractors, &ignore, &string));
    }

    batches
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
    options: IngestFile,
    locustdb: Arc<InnerLocustDB>,
    sender: SharedSender<Result<(), String>>,
}

impl CSVIngestionTask {
    pub fn new(options: IngestFile,
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
        let batches = ingest_file(
            &self.options.filename,
            self.options.col_names.clone(),
            self.options.chunk_size,
            &self.options.extractors,
            self.options.unzip,
            &self.options.ignore_cols,
            &self.options.always_string);
        match batches {
            Ok(batches) => {
                self.locustdb.store_partitions(&self.options.tablename, batches);
                self.sender.send(Ok(()));
            }
            Err(msg) => self.sender.send(Err(msg))
        }
    }
    fn completed(&self) -> bool { false }
    fn multithreaded(&self) -> bool { false }
}


struct RawCol {
    types: ColType,
    values: IndexedPackedStrings,
}

impl RawCol {
    fn new() -> RawCol {
        RawCol {
            types: ColType::nothing(),
            values: IndexedPackedStrings::default(),
        }
    }

    fn push(&mut self, elem: &str) {
        self.types = self.types | ColType::determine(elem);
        self.values.push(elem);
    }

    fn finalize(&mut self, name: &str, string: bool) -> Arc<Column> {
        let result = if self.types.contains_string || string {
            fast_build_string_column(name, self.values.iter(), self.values.len())
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
            Column::plain(name, self.values.len(), None)
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

#[derive(Default)]
pub struct IndexedPackedStrings {
    data: Vec<(u32, u32)>,
    backing_store: Vec<u8>,
}

impl IndexedPackedStrings {
    pub fn push(&mut self, elem: &str) {
        let bytes = elem.as_bytes();
        self.data.push((self.backing_store.len() as u32, bytes.len() as u32));
        self.backing_store.extend_from_slice(bytes);
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.backing_store.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item=&str> + Clone {
        self.data.iter().map(move |&(i, len)| unsafe {
            str::from_utf8_unchecked(&self.backing_store[i as usize..(i + len) as usize])
        })
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

