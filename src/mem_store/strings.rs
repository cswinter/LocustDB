use std::collections::HashMap;
use std::collections::hash_set::HashSet;
use std::hash::BuildHasherDefault;
use std::rc::Rc;
use std::str;
use std::sync::Arc;
use std::{u8, u16};

use num::PrimInt;
use seahash::SeaHasher;

use engine::types::*;
use mem_store::*;
use mem_store::column_builder::UniqueValues;


type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;
type HashSetSea<K> = HashSet<K, BuildHasherDefault<SeaHasher>>;

// TODO(clemens): this should depend on the batch size and element length
pub const MAX_UNIQUE_STRINGS: usize = 10000;

pub fn fast_build_string_column<'a, T: Iterator<Item=&'a str> + Clone>(name: &str, strings: T, len: usize) -> Arc<Column> {
    let mut unique_values = HashSetSea::default();
    for s in strings.clone() {
        unique_values.insert(s);
        if unique_values.len() == MAX_UNIQUE_STRINGS {
            let packed = StringPacker::from_iterator(strings);
            return Arc::new(Column::new(
                name,
                len,
                None,
                string_pack_codec(),
                vec![DataSection::U8(packed.data)],
            ));
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
        Arc::new(Column::new(
            name,
            indices.len(),
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U8),
            vec![DataSection::U8(indices), DataSection::String(mapping)]))
    } else {
        let indices: Vec<u16> = {
            let mut dictionary: HashMapSea<&str, u16> = HashMapSea::default();
            for (i, s) in mapping.iter().enumerate() {
                dictionary.insert(&s, i as u16);
            }
            strings.map(|s| *dictionary.get(s).unwrap()).collect()
        };
        Arc::new(Column::new(
            name,
            indices.len(),
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U16),
            vec![DataSection::U16(indices), DataSection::String(mapping)]))
    }
}

pub fn build_string_column(name: &str,
                           values: &[Option<Rc<String>>],
                           unique_values: UniqueValues<Option<Rc<String>>>)
                           -> Arc<Column> {
    if let Some(u) = unique_values.get_values() {
        // TODO(clemens): constant column when there is only one value
        if u.len() <= From::from(u8::MAX) {
            let (indices, dictionary) = DictEncodedStrings::construct_dictionary::<u8>(values, u);
            Arc::new(Column::new(
                name,
                indices.len(),
                Some((0, dictionary.len() as i64)),
                dict_codec(EncodingType::U8),
                vec![DataSection::U8(indices), DataSection::String(dictionary)]))
        } else {
            let (indices, dictionary) = DictEncodedStrings::construct_dictionary::<u16>(values, u);
            Arc::new(Column::new(
                name,
                indices.len(),
                Some((0, dictionary.len() as i64)),
                dict_codec(EncodingType::U16),
                vec![DataSection::U16(indices), DataSection::String(dictionary)]))
        }
    } else {
        let packed = StringPacker::from_nullable_strings(values);
        Arc::new(Column::new(
            name,
            values.len(),
            None,
            string_pack_codec(),
            vec![DataSection::U8(packed.data)]))
    }
}

struct StringPacker {
    data: Vec<u8>,
}

// TODO(clemens): encode using variable size length + special value to represent null
impl StringPacker {
    pub fn from_nullable_strings(strings: &[Option<Rc<String>>]) -> StringPacker {
        let mut sp = StringPacker { data: Vec::new() };
        for string in strings {
            match *string {
                Some(ref string) => sp.push(string),
                None => sp.push(""),
            }
        }
        sp.shrink_to_fit();
        sp
    }

    pub fn from_iterator<'a>(strings: impl Iterator<Item=&'a str>) -> StringPacker {
        let mut sp = StringPacker { data: Vec::new() };
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

pub fn dict_codec(index_type: EncodingType) -> Codec {
    Codec::new(vec![
        CodecOp::PushDataSection(1),
        CodecOp::DictLookup(index_type),
    ])
}

pub fn string_pack_codec() -> Codec {
    Codec::new(vec![CodecOp::UnpackStrings])
}