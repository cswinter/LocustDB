extern crate csv;
extern crate flate2;

use ordered_float::OrderedFloat;

use crate::bitvec::*;
use crate::ingest::raw_val::RawVal;
use crate::ingest::schema::*;
use crate::scheduler::*;
use crate::stringpack::*;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::ops::BitOr;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use super::extractor;

use self::flate2::read::GzDecoder;

type IngestionTransform = HashMap<usize, extractor::Extractor>;

#[derive(Debug)]
pub struct Options {
    filename: PathBuf,
    tablename: String,
    partition_size: usize,
    colnames: Option<Vec<String>>,
    extractors: IngestionTransform,
    ignore_cols: HashSet<usize>,
    always_string: HashSet<usize>,
    allow_nulls: HashSet<usize>,
    allow_nulls_all_columns: bool,
    unzip: bool,
}

impl Options {
    pub fn new<P: AsRef<Path>>(filename: P, tablename: &str) -> Options {
        Options {
            filename: filename.as_ref().to_path_buf(),
            tablename: tablename.to_owned(),
            partition_size: 1 << 16,
            colnames: None,
            extractors: HashMap::new(),
            ignore_cols: HashSet::new(),
            always_string: HashSet::new(),
            allow_nulls: HashSet::new(),
            allow_nulls_all_columns: false,
            unzip: filename.as_ref().to_string_lossy().ends_with(".gz"),
        }
    }

    #[must_use]
    pub fn with_schema(mut self, schema: &str) -> Options {
        let schema = Schema::parse(schema).unwrap();
        self.colnames = schema.column_names;
        let mut extractors = HashMap::new();
        let mut always_string = HashSet::new();
        let mut allow_nulls = HashSet::new();
        let mut ignore_cols = HashSet::new();
        for (i, colschema) in schema.column_schemas.iter().enumerate() {
            if let Some(ref x) = colschema.transformation {
                let transform = match x {
                    ColumnTransformation::Multiply100 => extractor::multiply_by_100,
                    ColumnTransformation::Multiply1000 => extractor::multiply_by_1000,
                    ColumnTransformation::Date => extractor::date_time,
                };
                extractors.insert(i, transform);
            } else if colschema.types == ColumnType::Integer
                || colschema.types == ColumnType::NullableInteger
            {
                extractors.insert(i, extractor::int);
            }
            if colschema.types == ColumnType::String
                || colschema.types == ColumnType::NullableString
            {
                always_string.insert(i);
            }
            if colschema.types == ColumnType::NullableInteger
                || colschema.types == ColumnType::NullableString
            {
                allow_nulls.insert(i);
            }
            if colschema.types == ColumnType::Drop {
                ignore_cols.insert(i);
            }
        }
        self.extractors = extractors;
        self.always_string = always_string;
        self.allow_nulls = allow_nulls;
        self.ignore_cols = ignore_cols;
        self
    }

    #[must_use]
    pub fn with_partition_size(mut self, chunk_size: usize) -> Options {
        self.partition_size = chunk_size;
        self
    }

    #[must_use]
    pub fn with_column_names(mut self, col_names: Vec<String>) -> Options {
        self.colnames = Some(col_names);
        self
    }

    #[must_use]
    pub fn with_extractors(mut self, extractors: &[(usize, extractor::Extractor)]) -> Options {
        self.extractors = extractors.iter().cloned().collect();
        self
    }

    #[must_use]
    pub fn with_ignore_cols(mut self, ignore: &[usize]) -> Options {
        self.ignore_cols = ignore.iter().cloned().collect();
        self
    }

    #[must_use]
    pub fn with_always_string(mut self, always_string: &[usize]) -> Options {
        self.always_string = always_string.iter().cloned().collect();
        self
    }

    #[must_use]
    pub fn allow_nulls(mut self, allow_nulls: &[usize]) -> Options {
        self.allow_nulls = allow_nulls.iter().cloned().collect();
        self
    }

    #[must_use]
    pub fn allow_nulls_all_columns(mut self) -> Options {
        self.allow_nulls_all_columns = true;
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
            None => reader
                .headers()
                .unwrap()
                .iter()
                .map(str::to_owned)
                .collect(),
        };
        auto_ingest(ldb, reader.records().map(|r| r.unwrap()), &headers, opts)
    } else {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(opts.colnames.is_none())
            .from_path(&opts.filename)
            .map_err(|x| x.to_string())?;
        let headers = match opts.colnames {
            Some(ref colnames) => colnames.clone(),
            None => reader
                .headers()
                .unwrap()
                .iter()
                .map(str::to_owned)
                .collect(),
        };
        auto_ingest(ldb, reader.records().map(|r| r.unwrap()), &headers, opts)
    }
}

fn auto_ingest<T>(
    ldb: &InnerLocustDB,
    records: T,
    colnames: &[String],
    opts: &Options,
) -> Result<(), String>
where
    T: Iterator<Item = csv::StringRecord>,
{
    let ignore = (0..colnames.len())
        .map(|x| opts.ignore_cols.contains(&x))
        .collect::<Vec<_>>();
    let string = (0..colnames.len())
        .map(|x| opts.always_string.contains(&x))
        .collect::<Vec<_>>();
    let mut raw_cols = (0..colnames.len())
        .map(|x| RawCol::new(opts.allow_nulls_all_columns || opts.allow_nulls.contains(&x)))
        .collect::<Vec<_>>();
    let mut row_num = 0usize;
    for row in records {
        for (i, val) in row.iter().enumerate() {
            if !ignore[i] {
                raw_cols[i].push(val);
            }
        }

        if row_num % opts.partition_size == opts.partition_size - 1 {
            let cols = create_batch(&mut raw_cols, colnames, &opts.extractors, &ignore, &string);
            ldb.ingest_heterogeneous(&opts.tablename, cols);
            ldb.wal_flush();
        }
        row_num += 1;
    }

    if row_num % opts.partition_size != 0 {
        let cols = create_batch(&mut raw_cols, colnames, &opts.extractors, &ignore, &string);
        ldb.ingest_heterogeneous(&opts.tablename, cols);
    }
    Ok(())
}

fn create_batch(
    cols: &mut [RawCol],
    colnames: &[String],
    extractors: &IngestionTransform,
    ignore: &[bool],
    string: &[bool],
) -> HashMap<String, Vec<RawVal>> {
    let mut mem_store = HashMap::new();
    for (i, col) in cols.iter_mut().enumerate() {
        if !ignore[i] {
            let new_column = match extractors.get(&i) {
                Some(extractor) => col.extract(*extractor),
                None => col.finalize(&colnames[i], string[i]),
            };
            mem_store.insert(colnames[i].to_string(), new_column);
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
    pub fn new(
        options: Options,
        locustdb: Arc<InnerLocustDB>,
        sender: SharedSender<Result<(), String>>,
    ) -> CSVIngestionTask {
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
    fn completed(&self) -> bool {
        false
    }
    fn multithreaded(&self) -> bool {
        false
    }
}

struct RawCol {
    types: ColType,
    values: IndexedPackedStrings,
    lhex: bool,
    uhex: bool,
    string_bytes: usize,
    allow_null: bool,
    present: Vec<u8>,
    any_null: bool,
}

impl RawCol {
    fn new(allow_null: bool) -> RawCol {
        RawCol {
            types: ColType::nothing(),
            values: IndexedPackedStrings::default(),
            lhex: true,
            uhex: true,
            string_bytes: 0,
            allow_null,
            present: Vec::new(),
            any_null: false,
        }
    }

    fn push(&mut self, elem: &str) {
        self.types = self.types | ColType::determine(elem);
        self.lhex = self.lhex && is_lowercase_hex(elem);
        self.uhex = self.uhex && is_uppercase_hex(elem);
        self.string_bytes += elem.as_bytes().len();
        if self.allow_null {
            if elem.is_empty() {
                self.any_null = true;
            } else {
                self.present.set(self.values.len())
            }
        }
        self.values.push(elem);
    }

    fn finalize(&mut self, name: &str, string: bool) -> Vec<RawVal> {
        let result = if self.types.contains_string || string {
            self.values
                .iter()
                .map(|s| {
                    if self.allow_null && s.is_empty() {
                        RawVal::Null
                    } else {
                        RawVal::Str(s.to_string())
                    }
                })
                .collect()
        } else if self.types.contains_float {
            let mut result = Vec::with_capacity(self.values.len());
            for s in self.values.iter() {
                if s.is_empty() {
                    if self.allow_null {
                        result.push(RawVal::Null);
                    } else {
                        result.push(RawVal::Float(OrderedFloat(0.0)));
                    }
                } else if let Ok(float) = s.parse::<f64>() {
                    result.push(RawVal::Float(OrderedFloat(float)));
                } else {
                    unreachable!(
                        "{} should be parseable as float. {} {:?}",
                        s, name, self.types
                    )
                }
            }
            result
        } else if self.types.contains_int {
            let mut result = Vec::with_capacity(self.values.len());
            for s in self.values.iter() {
                if s.is_empty() {
                    if self.allow_null {
                        result.push(RawVal::Null);
                    } else {
                        result.push(RawVal::Int(0));
                    }
                } else if let Ok(int) = s.parse::<i64>() {
                    result.push(RawVal::Int(int));
                } else if let Ok(float) = s.parse::<f64>() {
                    result.push(RawVal::Int(float as i64));
                } else {
                    unreachable!(
                        "{} should be parseable as int or float. {} {:?}",
                        s, name, self.types
                    )
                };
            }
            result
        } else {
            vec![RawVal::Null; self.values.len()]
        };
        self.clear();
        result
    }

    fn extract(&mut self, extractor: extractor::Extractor) -> Vec<RawVal> {
        let mut builder = Vec::new();
        for s in self.values.iter() {
            if self.allow_null {
                if s.is_empty() {
                    self.any_null = true;
                    builder.push(RawVal::Null);
                } else {
                    self.present.set(self.values.len());
                    builder.push(RawVal::Int(extractor(s)));
                }
            } else {
                builder.push(RawVal::Int(extractor(s)))
            }
        }
        self.clear();
        builder
    }

    fn clear(&mut self) {
        self.types = ColType::nothing();
        self.values.clear();
        self.present.clear();
    }
}

fn is_lowercase_hex(string: &str) -> bool {
    string.len() & 1 == 0
        && string.chars().all(|c| {
            c == '0'
                || c == '1'
                || c == '2'
                || c == '3'
                || c == '4'
                || c == '5'
                || c == '6'
                || c == '7'
                || c == '8'
                || c == '9'
                || c == 'a'
                || c == 'b'
                || c == 'c'
                || c == 'd'
                || c == 'e'
                || c == 'f'
        })
}

fn is_uppercase_hex(string: &str) -> bool {
    string.len() & 1 == 0
        && string.chars().all(|c| {
            c == '0'
                || c == '1'
                || c == '2'
                || c == '3'
                || c == '4'
                || c == '5'
                || c == '6'
                || c == '7'
                || c == '8'
                || c == '9'
                || c == 'A'
                || c == 'B'
                || c == 'C'
                || c == 'D'
                || c == 'E'
                || c == 'F'
        })
}

#[derive(Copy, Clone, Debug)]
struct ColType {
    contains_string: bool,
    contains_int: bool,
    contains_float: bool,
    contains_null: bool,
}

impl ColType {
    fn new(string: bool, int: bool, float: bool, null: bool) -> ColType {
        ColType {
            contains_string: string,
            contains_int: int,
            contains_float: float,
            contains_null: null,
        }
    }

    fn string() -> ColType {
        ColType::new(true, false, false, false)
    }

    fn int() -> ColType {
        ColType::new(false, true, false, false)
    }

    fn float() -> ColType {
        ColType::new(false, false, true, false)
    }

    fn null() -> ColType {
        ColType::new(false, false, false, true)
    }

    fn nothing() -> ColType {
        ColType::new(false, false, false, false)
    }

    fn determine(s: &str) -> ColType {
        if s.is_empty() {
            ColType::null()
        } else if s.parse::<i64>().is_ok() {
            ColType::int()
        } else if s.parse::<f64>().is_ok() {
            ColType::float()
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
            contains_float: self.contains_float | rhs.contains_float,
            contains_null: self.contains_null | rhs.contains_null,
        }
    }
}
