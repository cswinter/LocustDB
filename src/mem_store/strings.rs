use std::collections::HashMap;
use std::collections::hash_set::HashSet;
use std::hash::BuildHasherDefault;
use std::rc::Rc;
use std::str;
use std::sync::Arc;
use std::{u8, u16};

use num::PrimInt;
use seahash::SeaHasher;
use hex;

use stringpack::*;
use engine::types::*;
use mem_store::*;
use mem_store::column_builder::UniqueValues;


type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;
type HashSetSea<K> = HashSet<K, BuildHasherDefault<SeaHasher>>;

const DICTIONARY_RATIO: usize = 2;

pub fn fast_build_string_column<'a, T>(name: &str,
                                       strings: T,
                                       len: usize,
                                       lhex: bool,
                                       uhex: bool,
                                       total_bytes: usize)
                                       -> Arc<Column> where T: Iterator<Item=&'a str> + Clone {
    let mut unique_values = HashSetSea::default();
    for s in strings.clone() {
        unique_values.insert(s);
        // TODO(clemens): is 2 the right constant? and should probably also depend on the length of the strings
        // TODO(clemens): len > 1000 || name == "string_packed" is a hack to make tests use dictionary encoding. Remove once we are able to group by string packed columns.
        if unique_values.len() == len / DICTIONARY_RATIO && (len > 1000 || name == "string_packed") {
            let (codec, data) = if (lhex || uhex) && total_bytes / len > 5 {
                let packed = PackedBytes::from_iterator(strings.map(|s| hex::decode(s).unwrap()));
                (vec![CodecOp::UnhexpackStrings(uhex, total_bytes)], DataSection::U8(packed.into_vec()))
            } else {
                let packed = PackedStrings::from_iterator(strings);
                (string_pack_codec(), DataSection::U8(packed.into_vec()))
            };
            let mut column = Column::new(
                name,
                len,
                None,
                codec,
                vec![data],
            );
            column.lz4_encode();
            return Arc::new(column);
        }
    }
    let dict_size = unique_values.len();
    let mut mapping = unique_values.into_iter().collect::<Vec<_>>();
    mapping.sort();
    let mut packed_mapping = IndexedPackedStrings::default();
    for s in mapping {
        packed_mapping.push(s);
    }
    let mut column = if dict_size <= From::from(u8::MAX) {
        let indices: Vec<u8> = {
            let mut dictionary: HashMapSea<&str, u8> = HashMapSea::default();
            for (i, s) in packed_mapping.iter().enumerate() {
                dictionary.insert(&s, i as u8);
            }
            strings.map(|s| dictionary[s]).collect()
        };
        let (dictionary_indices, dictionary_data) = packed_mapping.into_parts();
        Column::new(
            name,
            indices.len(),
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U8),
            vec![DataSection::U8(indices),
                 DataSection::U64(dictionary_indices),
                 DataSection::U8(dictionary_data)])
    } else {
        let indices: Vec<u16> = {
            let mut dictionary: HashMapSea<&str, u16> = HashMapSea::default();
            for (i, s) in packed_mapping.iter().enumerate() {
                dictionary.insert(&s, i as u16);
            }
            strings.map(|s| dictionary[s]).collect()
        };
        let (dictionary_indices, dictionary_data) = packed_mapping.into_parts();
        Column::new(
            name,
            indices.len(),
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U16),
            vec![DataSection::U16(indices),
                 DataSection::U64(dictionary_indices),
                 DataSection::U8(dictionary_data)])
    };
    column.lz4_encode();
    Arc::new(column)
}

pub fn build_string_column(name: &str,
                           values: &[Option<Rc<String>>],
                           unique_values: UniqueValues<Option<Rc<String>>>)
                           -> Arc<Column> {
    if let Some(u) = unique_values.get_values() {
// TODO(clemens): constant column when there is only one value
        if u.len() <= From::from(u8::MAX) {
            let (indices, dictionary_indices, dictionary_data) = dictionary_compress::<u8>(values, u);
            Arc::new(Column::new(
                name,
                indices.len(),
                Some((0, dictionary_indices.len() as i64)),
                dict_codec(EncodingType::U8),
                vec![DataSection::U8(indices),
                     DataSection::U64(dictionary_indices),
                     DataSection::U8(dictionary_data)]))
        } else {
            let (indices, dictionary_indices, dictionary_data) = dictionary_compress::<u16>(values, u);
            Arc::new(Column::new(
                name,
                indices.len(),
                Some((0, dictionary_indices.len() as i64)),
                dict_codec(EncodingType::U16),
                vec![DataSection::U16(indices),
                     DataSection::U64(dictionary_indices),
                     DataSection::U8(dictionary_data)]))
        }
    } else {
        let packed = PackedStrings::from_nullable_strings(values);
        Arc::new(Column::new(
            name,
            values.len(),
            None,
            string_pack_codec(),
            vec![DataSection::U8(packed.into_vec())]))
    }
}

pub fn dictionary_compress<T: PrimInt>(strings: &[Option<Rc<String>>],
                                       unique_values: HashSet<Option<Rc<String>>>)
                                       -> (Vec<T>, Vec<u64>, Vec<u8>) {
// TODO(clemens): handle null values
    let mut mapping = unique_values.into_iter().map(|o| o.unwrap()).collect::<Vec<_>>();
    mapping.sort();
    let mut packed_mapping = IndexedPackedStrings::default();
    for s in mapping {
        packed_mapping.push(&s);
    }
    let encoded_values: Vec<T> = {
        let mut reverse_mapping: HashMap<&str, T> = HashMap::default();
        let mut index = T::zero();
        for string in packed_mapping.iter() {
            reverse_mapping.insert(string, index);
            index = index + T::one();
        }
        strings.iter().map(|o| reverse_mapping[o.clone().unwrap().as_ref().as_str()]).collect()
    };
    let (dictionary_indices, dictionary_data) = packed_mapping.into_parts();
    (encoded_values, dictionary_indices, dictionary_data)
}

pub fn dict_codec(index_type: EncodingType) -> Vec<CodecOp> {
    vec![
        CodecOp::PushDataSection(1),
        CodecOp::PushDataSection(2),
        CodecOp::DictLookup(index_type),
    ]
}

pub fn string_pack_codec() -> Vec<CodecOp> {
    vec![CodecOp::UnpackStrings]
}