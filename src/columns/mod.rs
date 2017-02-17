mod packed_strings;
use value::{ValueType, InpVal, InpRecordType};
use std::boxed::Box;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::rc::Rc;
use std::iter;
use std::{u8, u16, u32, i64};
use std::cmp;
use heapsize::HeapSizeOf;
use self::packed_strings::StringPacker;

pub struct Batch {
    pub cols: Vec<Box<Column>>,
}

pub trait Column : HeapSizeOf {
    fn get_name(&self) -> &str;
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
    name: String,
    length: usize,
}

impl NullColumn {
    fn new(name: String, length: usize) -> NullColumn {
        NullColumn {
            name: name,
            length: length,
        }
    }
}

impl Column for NullColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = iter::repeat(ValueType::Null).take(self.length);
        ColIter{iter: Box::new(iter)}
    }
}


struct TimestampColumn {
    name: String,
    values: Vec<u64>
}

impl TimestampColumn {
    fn new(name: String, values: Vec<u64>) -> TimestampColumn {
        TimestampColumn {
            name: name,
            values: values
        }
    }
}

impl Column for TimestampColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.values.iter().map(|&t| ValueType::Timestamp(t));
        ColIter{iter: Box::new(iter)}
    }
}

struct IntegerColumn {
    name: String,
    values: Vec<i64>
}

impl IntegerColumn {
    fn new(name: String, mut values: Vec<i64>, min: i64, max: i64) -> Box<Column> {
        println!("{}: {} - {} = {}", name, max, min, max - min);
        if max - min <= u8::MAX as i64 {
            Box::new(IntegerColumn1B::new(name, values, max - min))
        } else if max - min <= u16::MAX as i64 {
            Box::new(IntegerColumn2B::new(name, values, max - min))
        } else if max - min <= u32::MAX as i64 {
            Box::new(IntegerColumn4B::new(name, values, max - min))
        } else {
            values.shrink_to_fit();
            Box::new(IntegerColumn {
                name: name,
                values: values,
            })
        }
    }
}

impl Column for IntegerColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.values.iter().map(|&i| ValueType::Integer(i));
        ColIter{iter: Box::new(iter)}
    }
}


struct IntegerColumn4B { name: String, values: Vec<u32>, offset: i64, }
struct IntegerColumn2B { name: String, values: Vec<u16>, offset: i64, }
struct IntegerColumn1B { name: String, values: Vec<u8>, offset: i64, }

impl IntegerColumn4B {
    fn new(name: String, values: Vec<i64>, offset: i64) -> IntegerColumn4B {
        println!("Created 4b for {}", name);
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push((v - offset) as u32);
        }
        IntegerColumn4B { name: name, values: encoded_vals, offset: offset, }
    }
}

impl IntegerColumn2B {
    fn new(name: String, values: Vec<i64>, offset: i64) -> IntegerColumn2B {
        println!("Created 2b for {}", name);
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push((v - offset) as u16);
        }
        IntegerColumn2B { name: name, values: encoded_vals, offset: offset, }
    }
}

impl IntegerColumn1B {
    fn new(name: String, values: Vec<i64>, offset: i64) -> IntegerColumn1B {
        println!("Created 1b for {}", name);
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push((v - offset) as u8);
        }
        IntegerColumn1B { name: name, values: encoded_vals, offset: offset, }
    }
}

impl Column for IntegerColumn4B {
    fn get_name(&self) -> &str { &self.name }
    
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let offset = self.offset;
        let iter = self.values.iter().map(move |i| ValueType::Integer(*i as i64 + offset));
        ColIter { iter: Box::new(iter) }
    }
}

impl Column for IntegerColumn2B {
    fn get_name(&self) -> &str { &self.name }
    
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let offset = self.offset;
        let iter = self.values.iter().map(move |i| ValueType::Integer(*i as i64 + offset));
        ColIter { iter: Box::new(iter) }
    }
}

impl Column for IntegerColumn1B {
    fn get_name(&self) -> &str { &self.name }
    
    fn iter<'a>(&'a self) -> ColIter<'a> {
        let offset = self.offset;
        let iter = self.values.iter().map(move |i| ValueType::Integer(*i as i64 + offset));
        ColIter { iter: Box::new(iter) }
    }
}

struct StringColumn {
    name: String,
    values: StringPacker,
}

impl StringColumn {
    fn new(name: String, values: Vec<Option<Rc<String>>>) -> StringColumn {
        StringColumn {
            name: name,
            values: StringPacker::from_strings(&values),
        }
    }
}

impl Column for StringColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.values.iter().map(|s| ValueType::Str(s));
        ColIter{iter: Box::new(iter)}
    }
}

struct SetColumn {
    name: String,
    values: Vec<Vec<String>>
}

impl SetColumn {
    fn new(name: String, values: Vec<Vec<String>>) -> SetColumn {
        SetColumn {
            name: name,
            values: values
        }
    }
}

impl Column for SetColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

    fn iter<'a>(&'a self) -> ColIter<'a> {
        let iter = self.values.iter().map(|s| ValueType::Set(Rc::new(s.clone())));
        ColIter{iter: Box::new(iter)}
    }
}

struct MixedColumn {
    name: String,
    values: Vec<InpVal>,
}

impl MixedColumn {
    fn new(name: String, mut values: Vec<InpVal>) -> MixedColumn {
        values.shrink_to_fit();
        MixedColumn {
            name: name,
            values: values,
        }
    }
}

impl Column for MixedColumn {
    fn get_name(&self) -> &str {
        &self.name
    }

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
        self.name.heap_size_of_children()
    }
}

impl HeapSizeOf for IntegerColumn {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for IntegerColumn4B {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for IntegerColumn2B {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for IntegerColumn1B {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for TimestampColumn {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for StringColumn {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for SetColumn {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}

impl HeapSizeOf for MixedColumn {
    fn heap_size_of_children(&self) -> usize {
        self.name.heap_size_of_children() + self.values.heap_size_of_children()
    }
}


enum VecType {
    NullVec(usize),
    TimestampVec(Vec<u64>),
    IntegerVec(Vec<i64>, i64, i64),
    StringVec(Vec<Option<Rc<String>>>),
    SetVec(Vec<Vec<String>>),
    MixedVec(Vec<InpVal>),
}

impl VecType {
    fn new_with_value(value: InpVal) -> VecType {
        use self::VecType::*;
        match value {
            InpVal::Null => NullVec(1),
            InpVal::Timestamp(t) => TimestampVec(vec![t]),
            InpVal::Integer(i) => IntegerVec(vec![i], i64::MAX, i64::MIN),
            InpVal::Str(s) => StringVec(vec![Some(s)]),
            InpVal::Set(s) => SetVec(vec![Rc::try_unwrap(s).unwrap()]),
        }
    }

    fn push(&mut self, value: InpVal) -> Option<VecType> {
        match (self, value) {
            (&mut VecType::NullVec(ref mut n), InpVal::Null) => *n += 1,
            (&mut VecType::NullVec(ref n), InpVal::Str(ref s)) => {
                let mut string_vec = iter::repeat(None).take(*n).collect::<Vec<Option<Rc<String>>>>();
                string_vec.push(Some(s.clone()));
                return Some(VecType::StringVec(string_vec))
            },
            (&mut VecType::TimestampVec(ref mut v), InpVal::Timestamp(t)) => v.push(t),
            (&mut VecType::IntegerVec(ref mut v, ref mut min, ref mut max), InpVal::Integer(i)) => {
                *min = cmp::min(i, *min);
                *max = cmp::max(i, *max);
                v.push(i)
            },
            (&mut VecType::StringVec(ref mut v), InpVal::Str(ref s)) => v.push(Some(s.clone())),
            (&mut VecType::StringVec(ref mut v), InpVal::Null) => v.push(None),
            (&mut VecType::SetVec(ref mut v), InpVal::Set(ref s)) => v.push(Rc::try_unwrap(s.clone()).unwrap()),
            (&mut VecType::MixedVec(ref mut v), ref anyval) => v.push(anyval.clone()),
            (slf, anyval) => {
                let mut new_vec = slf.to_mixed();
                new_vec.push(anyval.clone());
                return Some(new_vec)
            },
        }
        None
    }

    fn to_mixed(&self) -> VecType {
        match self {
            &VecType::NullVec(ref n)      => VecType::MixedVec(iter::repeat(InpVal::Null).take(*n).collect()),
            &VecType::TimestampVec(ref v) => VecType::MixedVec(v.iter().map(|t| InpVal::Timestamp(*t)).collect()),
            &VecType::IntegerVec(ref v, ..)   => VecType::MixedVec(v.iter().map(|i| InpVal::Integer(*i)).collect()),
            &VecType::StringVec(ref v)    => VecType::MixedVec(v.iter().map(|s| match s {
                &Some(ref string) => InpVal::Str(string.clone()),
                &None => InpVal::Null,
            }).collect()),
            &VecType::SetVec(ref v)       => VecType::MixedVec(v.iter().map(|s| InpVal::Set(Rc::new(s.clone()))).collect()),
            &VecType::MixedVec(_) => panic!("Trying to convert mixed columns to mixed column!"),
        }
    }

    fn to_column(self, name: String) -> Box<Column> {
        match self {
            VecType::NullVec(n)      => Box::new(NullColumn::new(name, n)),
            VecType::TimestampVec(v) => Box::new(TimestampColumn::new(name, v)),
            VecType::IntegerVec(v, min, max) => IntegerColumn::new(name, v, min, max),
            VecType::StringVec(v)    => Box::new(StringColumn::new(name, v)),
            VecType::SetVec(v)       => Box::new(SetColumn::new(name, v)),
            VecType::MixedVec(v)     => Box::new(MixedColumn::new(name, v)),
        }
    }
}

pub fn columnarize(records: Vec<InpRecordType>) -> Batch {
    let mut field_map: BTreeMap<&str, VecType> = BTreeMap::new();
    for record in records {
        for (name, value) in record {
            let to_insert = match field_map.entry(name) {
                Entry::Vacant(e) => {
                    e.insert(VecType::new_with_value(value));
                    None
                },
                Entry::Occupied(mut e) => {
                    if let Some(new_vec) = e.get_mut().push(value) {
                        let (name, _) = e.remove_entry();
                        Some((name, new_vec))
                    } else {
                        None
                    }
                },
            };

            if let Some((name, vec)) = to_insert {
                field_map.insert(name, vec);
            }
        }
    }

    let mut columns = Vec::new();
    for (name, values) in field_map {
        columns.push(values.to_column(name.to_string()))
    }

    Batch { cols: columns }
}
