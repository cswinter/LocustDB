use std::collections::HashMap;
use std::hash::Hash;
use engine::query::QueryStats;
use engine::typed_vec::TypedVec;
use engine::vector_operator::VecOperator;
use engine::types::*;
use seahash::SeaHasher;
use std::hash::BuildHasherDefault;


type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;

pub fn grouping<'a>(grouping_key: TypedVec<'a>) -> (Vec<usize>, usize, TypedVec<'a>) {
    match grouping_key.get_type() {
        EncodingType::U8 => {
            let (data, encoding) = grouping_key.cast_ref_u8();
            let (grouping, max_index, raw_groups) = ht_grouping(data);
            (grouping, max_index, (raw_groups, encoding).into())
        }
        EncodingType::U16 => {
            let (data, encoding) = grouping_key.cast_ref_u16();
            let (grouping, max_index, raw_groups) = ht_grouping(data);
            (grouping, max_index, (raw_groups, encoding).into())
        }
        t => panic!(" grouping not implemented for type {:?}", t)
    }
}

fn ht_grouping<T: Number>(grouping_key: &[T]) -> (Vec<usize>, usize, Vec<T>) {
    let mut count = 0;
    let mut grouping = Vec::with_capacity(grouping_key.len());
    let mut groups = Vec::new();
    let mut map = HashMapSea::default();
    for i in grouping_key {
        grouping.push(*map.entry(i).or_insert_with(|| {
            groups.push(*i);
            let old = count;
            count += 1;
            old
        }));
    }
    (grouping, count, groups)
}

pub struct HTSummationCi64<'b> {
    grouping: &'b Vec<usize>,
    max_index: usize,
    constant: i64,
}

impl<'b> HTSummationCi64<'b> {
    pub fn new(grouping: &'b Vec<usize>, max_index: usize, constant: i64) -> HTSummationCi64 {
        HTSummationCi64 {
            grouping: grouping,
            max_index: max_index,
            constant: constant,
        }
    }
}

impl<'a, 'b> VecOperator<'a> for HTSummationCi64<'b> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        stats.start();
        let mut result = vec![0; self.max_index];
        for i in self.grouping {
            result[*i] += self.constant;
        }
        stats.record(&"ht_summation_ci64");
        stats.ops += result.len() + self.grouping.len();
        TypedVec::Integer(result)
    }
}

pub trait Number: Hash + Eq + Copy + 'static {}

impl Number for u32 {}

impl Number for u16 {}

impl Number for u8 {}
