use std::collections::HashMap;
use std::collections::hash_set::HashSet;
use std::hash::BuildHasherDefault;
use std::rc::Rc;
use std::str;
use std::sync::Arc;
use std::{u8, u16};

use num::PrimInt;
use seahash::SeaHasher;

use stringpack::*;
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
            let packed = PackedStrings::from_iterator(strings);
            return Arc::new(Column::new(
                name,
                len,
                None,
                string_pack_codec(),
                vec![DataSection::U8(packed.into_vec())],
            ));
        }
    }
    let dict_size = unique_values.len();
    let mut mapping = unique_values.into_iter().collect::<Vec<_>>();
    mapping.sort();
    let mut packed_mapping = IndexedPackedStrings::default();
    for s in mapping {
        packed_mapping.push(s);
    }
    if dict_size <= From::from(u8::MAX) {
        let indices: Vec<u8> = {
            let mut dictionary: HashMapSea<&str, u8> = HashMapSea::default();
            for (i, s) in packed_mapping.iter().enumerate() {
                dictionary.insert(&s, i as u8);
            }
            strings.map(|s| *dictionary.get(s).unwrap()).collect()
        };
        let (dictionary_indices, dictionary_data) = packed_mapping.into_parts();
        Arc::new(Column::new(
            name,
            indices.len(),
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U8),
            vec![DataSection::U8(indices),
                 DataSection::U64(dictionary_indices),
                 DataSection::U8(dictionary_data)]))
    } else {
        let indices: Vec<u16> = {
            let mut dictionary: HashMapSea<&str, u16> = HashMapSea::default();
            for (i, s) in packed_mapping.iter().enumerate() {
                dictionary.insert(&s, i as u16);
            }
            strings.map(|s| *dictionary.get(s).unwrap()).collect()
        };
        let (dictionary_indices, dictionary_data) = packed_mapping.into_parts();
        Arc::new(Column::new(
            name,
            indices.len(),
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U16),
            vec![DataSection::U16(indices),
                 DataSection::U64(dictionary_indices),
                 DataSection::U8(dictionary_data)]))
    }
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

pub fn dict_codec(index_type: EncodingType) -> Codec {
    Codec::new(vec![
        CodecOp::PushDataSection(1),
        CodecOp::PushDataSection(2),
        CodecOp::DictLookup(index_type),
    ])
}

pub fn string_pack_codec() -> Codec {
    Codec::new(vec![CodecOp::UnpackStrings])
}