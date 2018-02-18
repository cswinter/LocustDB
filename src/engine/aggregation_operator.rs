use std::collections::HashMap;
use std::hash::Hash;
use query_engine::QueryStats;
use engine::typed_vec::TypedVec;
use engine::vector_operator::VecOperator;
use mem_store::point_codec::PointCodec;
use seahash::SeaHasher;
use std::hash::BuildHasherDefault;


type HashMapSea<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;


pub trait AggregationOperator<'a>: VecOperator<'a> {
    fn execute_all(&mut self, stats: &mut QueryStats) -> (TypedVec<'a>, TypedVec<'a>);
}

pub struct HTSummationCi64<'a, 'b, T: Number> {
    grouping: &'b [T],
    codec: &'a PointCodec<T>,
    constant: i64,
}

impl<'a, 'b, T: Number> HTSummationCi64<'a, 'b, T> {
    pub fn new(grouping: (&'b [T], &'a PointCodec<T>), constant: i64) -> HTSummationCi64<'a, 'b, T> {
        HTSummationCi64 {
            grouping: grouping.0,
            codec: grouping.1,
            constant: constant,
        }
    }
}

impl<'a, 'b, T: Number> VecOperator<'a> for HTSummationCi64<'a, 'b, T> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        stats.start();
        let mut map = HashMapSea::default();
        for i in self.grouping {
            *map.entry(i).or_insert(0) += self.constant;
        }
        let result = map.values().map(|i| *i).collect::<Vec<_>>();
        stats.record(&"ht_summation_ci64");
        stats.ops += result.len() + self.grouping.len();
        TypedVec::Integer(result)
    }
}

impl<'a, 'b, T: Number> AggregationOperator<'a> for HTSummationCi64<'a, 'b, T> where
    (Vec<T>, &'a PointCodec<T>): Into<TypedVec<'a>> {
    fn execute_all(&mut self, stats: &mut QueryStats) -> (TypedVec<'a>, TypedVec<'a>) {
        stats.start();
        let mut map = HashMapSea::default();
        for &i in self.grouping {
            *map.entry(i).or_insert(0) += self.constant;
        }
        let count = map.len();
        let mut result = Vec::with_capacity(count);
        let mut grouping_result = Vec::with_capacity(count);
        for (k, v) in map.into_iter() {
            result.push(v);
            grouping_result.push(k);
        }
        stats.record(&"ht_summation_ci64_all");
        stats.ops += count + self.grouping.len();
        ((grouping_result, self.codec).into(), TypedVec::Integer(result))
    }
}


pub trait Number: Hash + Eq  + Copy + 'static {}

impl Number for u32 {}

impl Number for u16 {}

impl Number for u8 {}
