use std::collections::HashMap;
use std::collections::hash_set::HashSet;
use std::fmt;
use std::hash::BuildHasherDefault;
use std::marker::PhantomData;
use std::rc::Rc;
use std::str;
use std::{u8, u16};

use heapsize::HeapSizeOf;
use num::PrimInt;
use seahash::SeaHasher;

use engine::typed_vec::*;
use engine::types::*;
use ingest::raw_val::RawVal;
use mem_store::*;
use mem_store::column_builder::UniqueValues;


type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;
type HashSetSea<K> = HashSet<K, BuildHasherDefault<SeaHasher>>;

// TODO(clemens): this should depend on the batch size and element length
pub const MAX_UNIQUE_STRINGS: usize = 10000;

pub fn fast_build_string_column<'a, T: Iterator<Item=&'a str> + Clone>(name: &str, strings: T, len: usize) -> Box<Column> {
    let mut unique_values = HashSetSea::default();
    for s in strings.clone() {
        unique_values.insert(s);
        if unique_values.len() == MAX_UNIQUE_STRINGS {
            return Box::new(StringPacker::from_iterator(name, strings, len));
        }
    }
    let dict_size = unique_values.len();
    let mut mapping: Vec<String> = unique_values.into_iter().map(str::to_owned).collect::<Vec<_>>();
    mapping.sort();
    if dict_size <= From::from(u8::MAX) {
        let indices: Vec<u8> = {
            let mut dictionary: HashMapSea<&str, u8> = HashMapSea::default();
            for (i, s) in mapping.iter().enumerate() {
                dictionary.insert(&s, i as u8);
            }
            strings.map(|s| *dictionary.get(s).unwrap()).collect()
        };
        Column::encoded(name,
                        indices,
                        DictionaryEncoding::<u8> { mapping, t: PhantomData },
                        Some((0, dict_size as i64)))
    } else {
        let indices: Vec<u16> = {
            let mut dictionary: HashMapSea<&str, u16> = HashMapSea::default();
            for (i, s) in mapping.iter().enumerate() {
                dictionary.insert(&s, i as u16);
            }
            strings.map(|s| *dictionary.get(s).unwrap()).collect()
        };
        Column::encoded(name,
                        indices,
                        DictionaryEncoding::<u16> { mapping, t: PhantomData },
                        Some((0, dict_size as i64)))
    }
}

pub fn build_string_column(name: &str,
                           values: &[Option<Rc<String>>],
                           unique_values: UniqueValues<Option<Rc<String>>>)
                           -> Box<Column> {
    if let Some(u) = unique_values.get_values() {
        let range = Some((0, u.len() as i64));
        // TODO(clemens): constant column when there is only one value
        if u.len() <= From::from(u8::MAX) {
            let (indices, dictionary) = DictEncodedStrings::construct_dictionary::<u8>(values, u);
            Column::encoded(name, indices, DictionaryEncoding::<u8> { mapping: dictionary, t: PhantomData }, range)
        } else {
            let (indices, dictionary) = DictEncodedStrings::construct_dictionary::<u16>(values, u);
            Column::encoded(name, indices, DictionaryEncoding::<u16> { mapping: dictionary, t: PhantomData }, range)
        }
    } else {
        Box::new(StringPacker::from_nullable_strings(name.to_owned(), values))
    }
}

struct StringPacker {
    name: String,
    count: usize,
    data: Vec<u8>,
}

// TODO(clemens): encode using variable size length + special value to represent null
impl StringPacker {
    pub fn from_nullable_strings(name: String, strings: &[Option<Rc<String>>]) -> StringPacker {
        let mut sp = StringPacker { name, count: strings.len(), data: Vec::new() };
        for string in strings {
            match *string {
                Some(ref string) => sp.push(string),
                None => sp.push(""),
            }
        }
        sp.shrink_to_fit();
        sp
    }

    pub fn from_iterator<'a>(name: &str, strings: impl Iterator<Item=&'a str>, len: usize) -> StringPacker {
        let mut sp = StringPacker { name: name.to_owned(), count: len, data: Vec::new() };
        for string in strings {
            sp.push(string);
        }
        sp.shrink_to_fit();
        sp
    }

    pub fn push(&mut self, string: &str) {
        for &byte in string.as_bytes().iter() {
            self.data.push(byte);
        }
        self.data.push(0);
    }

    pub fn shrink_to_fit(&mut self) {
        self.data.shrink_to_fit();
    }

    pub fn iter(&self) -> StringPackerIterator {
        StringPackerIterator {
            data: &self.data,
            curr_index: 0,
        }
    }
}

impl Column for StringPacker {
    fn name(&self) -> &str { &self.name }
    fn len(&self) -> usize { self.count }
    fn get_encoded(&self, _: usize, _: usize) -> Option<BoxedVec> { None }
    fn decode(&self) -> BoxedVec { TypedVec::owned(self.iter().collect()) }
    fn codec(&self) -> Option<Codec> { None }
    fn encoding_type(&self) -> EncodingType { EncodingType::U8 }
    fn basic_type(&self) -> BasicType { BasicType::String }
    fn range(&self) -> Option<(i64, i64)> { None }
}

impl fmt::Debug for StringPacker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<{}, StringPacked>", &self.name)
    }
}

impl HeapSizeOf for StringPacker {
    fn heap_size_of_children(&self) -> usize {
        self.data.heap_size_of_children()
    }
}

pub struct StringPackerIterator<'a> {
    data: &'a Vec<u8>,
    curr_index: usize,
}

impl<'a> Iterator for StringPackerIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<&'a str> {
        if self.curr_index >= self.data.len() {
            return None;
        }

        let mut index = self.curr_index;
        while self.data[index] != 0 {
            index += 1;
        }
        let result = unsafe { str::from_utf8_unchecked(&self.data[self.curr_index..index]) };
        self.curr_index = index + 1;
        Some(result)
    }
}

struct DictEncodedStrings;

impl DictEncodedStrings {
    pub fn construct_dictionary<T: PrimInt>(strings: &[Option<Rc<String>>],
                                            unique_values: HashSet<Option<Rc<String>>>)
                                            -> (Vec<T>, Vec<String>) {
        // TODO(clemens): handle null values
        let mut mapping: Vec<String> =
            unique_values.into_iter().map(|o| o.unwrap().to_string()).collect();
        mapping.sort();
        let encoded_values: Vec<T> = {
            let mut reverse_mapping: HashMap<&String, T> = HashMap::default();
            let mut index = T::zero();
            for string in &mapping {
                reverse_mapping.insert(string, index);
                index = index + T::one();
            }
            strings.iter().map(|o| reverse_mapping[o.clone().unwrap().as_ref()]).collect()
        };

        (encoded_values, mapping)
    }
}

struct DictionaryEncoding<T> {
    mapping: Vec<String>,
    t: PhantomData<T>,
}

impl<'a, T: IntVecType<T>> ColumnCodec<'a> for &'a DictionaryEncoding<T> {
    fn unwrap_decode<'b>(&self, data: &TypedVec<'b>, buffer: &mut TypedVec<'b>) where 'a: 'b {
        let data = T::unwrap(data);
        let result = <&str>::unwrap_mut(buffer);
        for encoded_value in data {
            result.push(self.mapping[encoded_value.cast_usize()].as_ref());
        }
    }

    fn encode_str(&self, s: &str) -> RawVal {
        // TODO(clemens): use binary search!
        for (i, val) in self.mapping.iter().enumerate() {
            if val == s {
                return RawVal::Int(i as i64);
            }
        }
        RawVal::Int(-1)
    }

    fn is_summation_preserving(&self) -> bool { false }
    fn is_order_preserving(&self) -> bool { true }
    fn is_positive_integer(&self) -> bool { true }
    fn encoding_type(&self) -> EncodingType { T::t() }
    fn decoded_type(&self) -> BasicType { BasicType::String }
    fn decode_range(&self, _: (i64, i64)) -> Option<(i64, i64)> { None }
}

impl<T> fmt::Debug for DictionaryEncoding<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StringDictionary({})", self.mapping.len())
    }
}

impl<T> HeapSizeOf for DictionaryEncoding<T> {
    fn heap_size_of_children(&self) -> usize {
        self.mapping.heap_size_of_children()
    }
}
