use hex;
use seahash::SeaHasher;

use crate::engine::data_types::*;
use crate::mem_store::*;
use crate::stringpack::*;
use std::collections::hash_set::HashSet;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::str;
use std::sync::Arc;
use std::{u16, u32, u8};

type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;
type HashSetSea<K> = HashSet<K, BuildHasherDefault<SeaHasher>>;

const DICTIONARY_RATIO: usize = 2;

pub fn fast_build_string_column<'a, T>(
    name: &str,
    strings: T,
    len: usize,
    lhex: bool,
    uhex: bool,
    total_bytes: usize,
    present: Option<Vec<u8>>,
) -> Arc<Column>
where
    T: Iterator<Item = &'a str> + Clone,
{
    let mut unique_values = HashSetSea::default();
    for s in strings.clone() {
        unique_values.insert(s);
        // PERF: is 2 the right constant? and should probably also depend on the length of the strings
        // TODO(#103): len > 1000 || name == "string_packed" is a hack to make tests use dictionary encoding. Remove once we are able to group by string packed columns.
        if unique_values.len() == len / DICTIONARY_RATIO {
            let (mut codec, data) = if (lhex || uhex) && total_bytes / len > 5 {
                let packed = PackedBytes::from_iterator(strings.map(|s| hex::decode(s).unwrap()));
                (
                    vec![CodecOp::UnhexpackStrings(uhex, total_bytes)],
                    DataSection::U8(packed.into_vec()),
                )
            } else {
                let packed = PackedStrings::from_iterator(strings);
                (string_pack_codec(), DataSection::U8(packed.into_vec()))
            };
            let mut column = if let Some(present) = present {
                codec.push(CodecOp::PushDataSection(1));
                codec.push(CodecOp::Nullable);
                Column::new(name, len, None, codec, vec![data, DataSection::Bitvec(present)])
            } else {
                Column::new(name, len, None, codec, vec![data])
            };
            column.lz4_encode();
            return Arc::new(column);
        }
    }

    let dict_size = unique_values.len();
    let mut mapping = unique_values.into_iter().collect::<Vec<_>>();
    mapping.sort_unstable();
    let mut packed_mapping = IndexedPackedStrings::default();
    for s in mapping {
        packed_mapping.push(s);
    }

    let (range, mut codec, mut data_sections) = if dict_size <= Into::<usize>::into(u8::MAX) {
        let indices: Vec<u8> = {
            let mut dictionary: HashMapSea<&str, u8> = HashMapSea::default();
            for (i, s) in packed_mapping.iter().enumerate() {
                dictionary.insert(s, i as u8);
            }
            strings.map(|s| dictionary[s]).collect()
        };
        let (dictionary_indices, dictionary_data) = packed_mapping.into_parts();
        (
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U8),
            vec![
                DataSection::U8(indices),
                DataSection::U64(dictionary_indices),
                DataSection::U8(dictionary_data),
            ],
        )
    } else if dict_size <= Into::<usize>::into(u16::MAX) {
        let indices: Vec<u16> = {
            let mut dictionary: HashMapSea<&str, u16> = HashMapSea::default();
            for (i, s) in packed_mapping.iter().enumerate() {
                dictionary.insert(s, i as u16);
            }
            strings.map(|s| dictionary[s]).collect()
        };
        let (dictionary_indices, dictionary_data) = packed_mapping.into_parts();
        (
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U16),
            vec![
                DataSection::U16(indices),
                DataSection::U64(dictionary_indices),
                DataSection::U8(dictionary_data),
            ],
        )
    } else {
        let indices: Vec<u32> = {
            let mut dictionary: HashMapSea<&str, u32> = HashMapSea::default();
            for (i, s) in packed_mapping.iter().enumerate() {
                dictionary.insert(s, i as u32);
            }
            strings.map(|s| dictionary[s]).collect()
        };
        let (dictionary_indices, dictionary_data) = packed_mapping.into_parts();
        (
            Some((0, dict_size as i64)),
            dict_codec(EncodingType::U32),
            vec![
                DataSection::U32(indices),
                DataSection::U64(dictionary_indices),
                DataSection::U8(dictionary_data),
            ],
        )
    };
    if let Some(present) = present {
        codec.insert(0, CodecOp::PushDataSection(3));
        codec.insert(1, CodecOp::Nullable);
        data_sections.push(DataSection::Bitvec(present));
    }
    let mut column = Column::new(name, len, range, codec, data_sections);
    column.lz4_encode();
    Arc::new(column)
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
