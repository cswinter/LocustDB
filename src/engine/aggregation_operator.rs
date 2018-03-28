use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::hash::Hash;

use engine::typed_vec::TypedVec;
use engine::types::*;
use engine::vector_op::VecOperator;
use num::PrimInt;
use seahash::SeaHasher;

type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;

pub fn grouping(grouping_key: TypedVec) -> (TypedVec, usize, TypedVec) {
    // TODO(clemens): refine criterion
    let max_cardinality = grouping_key.max_cardinality();
    if max_cardinality < 1 << 12 && grouping_key.is_positive_integer() {
        match grouping_key.get_type() {
            EncodingType::U8 => {
                let raw_groups = {
                    let (data, encoding) = grouping_key.cast_ref_u8();
                    (unique(data, max_cardinality), encoding).into()
                };
                (grouping_key, max_cardinality, raw_groups)
            }
            EncodingType::U16 => {
                let raw_groups = {
                    let (data, encoding) = grouping_key.cast_ref_u16();
                    (unique(data, max_cardinality), encoding).into()
                };
                (grouping_key, max_cardinality, raw_groups)
            }
            t => panic!("vec grouping not implemented for type {:?}", t)
        }
    } else {
        match grouping_key.get_type() {
            EncodingType::U16 => {
                let (data, encoding) = grouping_key.cast_ref_u16();
                let (grouping, max_index, raw_groups) = ht_grouping(data);
                ((grouping, encoding).into(), max_index, (raw_groups, encoding).into())
            }
            t => panic!("ht grouping not implemented for type {:?}", t)
        }
    }
}

fn unique<T: PrimInt + 'static>(data: &[T], max_index: usize) -> Vec<T> {
    let mut seen_before = vec![false; max_index + 1];
    let mut result = Vec::new();
    for &i in data {
        let index = i.to_usize().unwrap();
        if !seen_before[index] {
            result.push(i);
            seen_before[index] = true;
        }
    }
    result.sort();
    result
}

fn ht_grouping<T: PrimInt + Hash>(grouping_key: &[T]) -> (Vec<T>, usize, Vec<T>) {
    let mut count = T::zero();
    let mut grouping = Vec::with_capacity(grouping_key.len());
    let mut groups = Vec::new();
    let mut map = HashMapSea::default();
    for i in grouping_key {
        grouping.push(*map.entry(i).or_insert_with(|| {
            groups.push(*i);
            let old = count;
            count = count + T::one();
            old
        }));
    }
    (grouping, count.to_usize().unwrap(), groups)
}

pub struct VecCount<'b, T: PrimInt + 'static> {
    grouping: &'b [T],
    max_index: usize,
    dense_grouping: bool,
}

impl<'b, T: PrimInt> VecCount<'b, T> {
    pub fn new(grouping: &'b [T], max_index: usize, dense_grouping: bool) -> VecCount<'b, T> {
        VecCount {
            grouping,
            max_index,
            dense_grouping,
        }
    }
}

impl<'a, 'b, T: PrimInt + 'static> VecOperator<'a> for VecCount<'b, T> {
    fn execute(&mut self) -> TypedVec<'a> {
        let mut result = vec![0; self.max_index + 1];
        for i in self.grouping {
            result[i.to_usize().unwrap()] += 1;
        }
        if !self.dense_grouping {
            // Remove 0 counts for all entries that weren't present in grouping
            let mut j = 0;
            for i in 0..result.len() {
                if result[i] > 0 {
                    result[j] = result[i];
                    j += 1;
                }
            }
            result.truncate(j);
        }
        TypedVec::Integer(result)
    }
}

