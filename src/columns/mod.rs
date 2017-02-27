mod integers;
mod strings;
use value::{ValueType, InpVal, InpRecordType};
use std::boxed::Box;
use std::collections::HashMap;
use std::collections::hash_set::HashSet;
use std::collections::hash_map::Entry;
use std::rc::Rc;
use std::iter;
use std::cmp;
use heapsize::HeapSizeOf;
use self::integers::IntegerColumn;
use self::strings::{build_string_column, MAX_UNIQUE_STRINGS};
use std::i64;
use std::hash::Hash;

pub struct Batch {
    pub cols: Vec<Column>,
}

pub struct Column {
    name: String,
    data: Box<ColumnData>,
}

impl Column {
    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn iter<'a>(&'a self) -> ColIter<'a> {
        self.data.iter()
    }

    fn new(name: String, data: Box<ColumnData>) -> Column {
        Column { name: name, data: data }
    }
}

impl HeapSizeOf for Column {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.data.heap_size_of_children()
    }
}

pub trait ColumnData : HeapSizeOf {
    fn iter<'a>(&'a self) -> ColIter<'a>;
}

pub struct ColIter<'a> {
    iter: Box<Iterator<Item=ValueType<'a>> + 'a>
}

impl<'a> Iterator for ColIter<'a> {
    type Item = ValueType<'a>;

    fn next(&mut self) -> Option<ValueType<'a>> {
        self.iter.next()
    }
}

struct NullColumn {
    length: usize,
}

impl NullColumn {
    fn new(length: usize) -> NullColumn {
        NullColumn {
            length: length,
        }
    }
}

impl ColumnData for NullColumn {
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = iter::repeat(ValueType::Null).take(self.length);
        ColIter{iter: Box::new(iter)}
    }
}


struct TimestampColumn {
    values: Vec<u64>
}

impl TimestampColumn {
    fn new(values: Vec<u64>) -> TimestampColumn {
        TimestampColumn {
            values: values
        }
    }
}

impl ColumnData for TimestampColumn {
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.values.iter().map(|&t| ValueType::Timestamp(t));
        ColIter{iter: Box::new(iter)}
    }
}

struct SetColumn {
    values: Vec<Vec<String>>
}

impl SetColumn {
    fn new(values: Vec<Vec<String>>) -> SetColumn {
        SetColumn {
            values: values
        }
    }
}

impl ColumnData for SetColumn {
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.values.iter().map(|s| ValueType::Set(Rc::new(s.clone())));
        ColIter{iter: Box::new(iter)}
    }
}

struct MixedColumn {
    values: Vec<InpVal>,
}

impl MixedColumn {
    fn new(mut values: Vec<InpVal>) -> MixedColumn {
        values.shrink_to_fit();
        MixedColumn {
            values: values,
        }
    }
}

impl ColumnData for MixedColumn {
    fn iter(&self) -> ColIter {
        let iter = self.values.iter().map(|val| val.to_val());
        ColIter{iter: Box::new(iter)}
    }
}


impl HeapSizeOf for Batch {
    fn heap_size_of_children(&self) -> usize {
        self.cols.heap_size_of_children()
    }
}

impl HeapSizeOf for NullColumn {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}

impl HeapSizeOf for TimestampColumn {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for SetColumn {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for MixedColumn {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}

pub struct UniqueValues<T> {
    max_count: usize,
    values: HashSet<T>,
}

impl<T: cmp::Eq + Hash> UniqueValues<T> {
    fn new(max_count: usize) -> UniqueValues<T> {
        UniqueValues { max_count: max_count, values: HashSet::new() }
    }

    fn new_with(values: Vec<T>, max_count: usize) -> UniqueValues<T> {
        // Intended for a small number of elements and does not obey max_count
        UniqueValues { max_count: max_count, values: values.into_iter().collect() }
    }

    fn insert(&mut self, value: T) {
        if self.values.len() < self.max_count {
            self.values.insert(value);
        }
    }

    pub fn get_values(self) -> Option<HashSet<T>> {
        if self.values.len() < self.max_count {
            Some(self.values)
        } else {
            None
        }
    }
}

enum VecType {
    NullVec(usize),
    TimestampVec(Vec<u64>),
    IntegerVec(Vec<i64>, i64, i64),
    StringVec(Vec<Option<Rc<String>>>, UniqueValues<Option<Rc<String>>>),
    SetVec(Vec<Vec<String>>),
    MixedVec(Vec<InpVal>),
}

impl VecType {
    fn new() -> VecType {
        VecType::NullVec(0)
    }

    fn push_null(&mut self) -> Option<VecType> {
        use self::VecType::*;
        match self {
            &mut NullVec(ref mut n) => *n += 1,
            vt@&mut TimestampVec(_) => return vt.push_int(0), // TODO: properly handle null timestamps
            vt@&mut IntegerVec(..) => return vt.push_int(0), // TODO: properly handle null integers
            &mut StringVec(ref mut v, ref mut u) => { v.push(None); u.insert(None); },
            &mut SetVec(ref mut v) => v.push(Vec::new()), // TODO: properly handle null sets
            &mut MixedVec(ref mut v) => v.push(InpVal::Null),
        }
        None
    }

    fn push_int(&mut self, i: i64) -> Option<VecType> {
        use self::VecType::*;
        match self {
            &mut NullVec(ref mut n) => {
                let mut int_vec = iter::repeat(0i64).take(*n).collect(); // TODO: properly handle null integers
                let mut integer_vec = IntegerVec(int_vec, 0, 0);
                return integer_vec.push_int(i).or(Some(integer_vec))
            }
            &mut StringVec(ref mut v, ref mut u) => {
                let s = Rc::new(format!("{}", i));
                v.push(Some(s.clone()));
                u.insert(Some(s.clone()));
            },
            &mut IntegerVec(ref mut v, ref mut min, ref mut max) => {
                *min = cmp::min(i, *min);
                *max = cmp::max(i, *max);
                v.push(i)
            },
            _ => return self.push_mixed(InpVal::Integer(i)),
        }
        None
    }

    fn push_str(&mut self, s: String) -> Option<VecType> {
        use self::VecType::*;
        match self {
            &mut NullVec(ref n) => {
                let r = Rc::new(s);
                let mut string_vec: Vec<Option<Rc<String>>> = iter::repeat(None).take(*n).collect();
                string_vec.push(Some(r.clone()));
                return Some(VecType::StringVec(string_vec, UniqueValues::new_with(vec![None, Some(r.clone())], MAX_UNIQUE_STRINGS)))
            },
            &mut IntegerVec(ref mut v, ref mut min, ref mut max) => {
                let mut string_vec: Vec<Option<Rc<String>>> = v.iter().map(|i| Some(Rc::new(format!("{}", i)))).collect();
                string_vec.push(Some(Rc::new(s)));
                return Some(VecType::StringVec(string_vec.clone(), UniqueValues::new_with(string_vec, MAX_UNIQUE_STRINGS)))
            },
            &mut StringVec(ref mut v, ref mut u) =>  {
                let r = Rc::new(s);
                v.push(Some(r.clone()));
                u.insert(Some(r));
            }
            _ => return self.push_mixed(InpVal::Str(Rc::new(s))),
        }
        None
    }

    fn push_mixed(&mut self, value: InpVal) -> Option<VecType> {
        use self::VecType::*;
        match self {
            &mut MixedVec(ref mut v) => {
                v.push(value.clone());
                None
            },
            _ =>  {
                let mut new_vec = self.to_mixed();
                new_vec.push_mixed(value);
                Some(new_vec)
            }
        }
    }

    fn to_mixed(&self) -> VecType {
        use self::VecType::*;
        match self {
            &NullVec(ref n)      => VecType::MixedVec(iter::repeat(InpVal::Null).take(*n).collect()),
            &TimestampVec(ref v) => VecType::MixedVec(v.iter().map(|t| InpVal::Timestamp(*t)).collect()),
            &IntegerVec(ref v, ..)   => VecType::MixedVec(v.iter().map(|i| InpVal::Integer(*i)).collect()),
            &StringVec(ref v, ..)    => VecType::MixedVec(v.iter().map(|s| match s {
                &Some(ref string) => InpVal::Str(string.clone()),
                &None => InpVal::Null,
            }).collect()),
            &SetVec(ref v)       => VecType::MixedVec(v.iter().map(|s| InpVal::Set(Rc::new(s.clone()))).collect()),
            &MixedVec(_) => panic!("Trying to convert mixed columns to mixed column!"),
        }
    }

    fn to_column_data(self) -> Box<ColumnData> {
        use self::VecType::*;
        match self {
            NullVec(n)      => Box::new(NullColumn::new(n)),
            TimestampVec(v) => Box::new(TimestampColumn::new(v)),
            IntegerVec(v, min, max) => IntegerColumn::new(v, min, max),
            StringVec(v, u)    => build_string_column(v, u),
            SetVec(v)       => Box::new(SetColumn::new(v)),
            MixedVec(v)     => Box::new(MixedColumn::new(v)),
        }
    }
}

pub fn auto_ingest<T: Iterator<Item=Vec<String>>>(records: T, colnames: Vec<String>, batch_size: usize) -> Vec<Batch> {
    let row_len = colnames.len();
    let mut batches = Vec::new();
    let mut partial_columns = (0..row_len).map(|_| VecType::new()).collect::<Vec<_>>();

    let mut row_num =0usize;
    for row in records {
        for (i, val) in row.into_iter().enumerate() {
            if let Some(new_col) = {
                if val.is_empty() {
                    partial_columns[i].push_null()
                } else {
                    match val.parse::<i64>() {
                        Ok(int) => partial_columns[i].push_int(int),
                        Err(_) => match val.parse::<f64>() {
                            Ok(float) => partial_columns[i].push_int((float * 10000.) as i64),
                            Err(_) => partial_columns[i].push_str(val),
                        }
                    }
                }
            } {
                partial_columns[i] = new_col;
            }
        }

        if row_num % batch_size == batch_size - 1 {
            batches.push(create_batch(partial_columns, &colnames));
            partial_columns = (0..row_len).map(|_| VecType::new()).collect::<Vec<_>>();
        }
        row_num += 1;
    }

    if row_num % batch_size != batch_size - 1 {
        batches.push(create_batch(partial_columns, &colnames));
    }

    batches
}

fn create_batch(cols: Vec<VecType>, colnames: &Vec<String>) -> Batch {
    let mut columns = Vec::new();
    for (i, col) in cols.into_iter().enumerate() {
        columns.push(Column::new(colnames[i].clone(), col.to_column_data()));
    }
    Batch { cols: columns}
}
