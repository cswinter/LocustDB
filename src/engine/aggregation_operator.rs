use std::collections::HashMap;
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

pub struct HTu16SummationCi64<'a, 'b> {
    grouping: &'b [u16],
    codec: &'a PointCodec<u16>,
    constant: i64,
}

impl<'a, 'b> HTu16SummationCi64<'a, 'b> {
    pub fn new(grouping: (&'b [u16], &'a PointCodec<u16>), constant: i64) -> HTu16SummationCi64<'a, 'b> {
        HTu16SummationCi64 {
            grouping: grouping.0,
            codec: grouping.1,
            constant: constant,
        }
    }
}

impl<'a, 'b> VecOperator<'a> for HTu16SummationCi64<'a, 'b> {
    fn execute(&mut self, stats: &mut QueryStats) -> TypedVec<'a> {
        stats.start();
        let mut map = HashMapSea::default();
        for i in self.grouping {
            *map.entry(i).or_insert(0) += self.constant;
        }
        let result = map.values().map(|i| *i).collect::<Vec<_>>();
        stats.record(&"ht_u16_summation_ci61");
        stats.ops += result.len() + self.grouping.len();
        TypedVec::Integer(result)
    }
}

impl<'a, 'b> AggregationOperator<'a> for HTu16SummationCi64<'a, 'b> {
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
        stats.record(&"ht_u16_summation_ci61_all");
        stats.ops += count + self.grouping.len();
        (TypedVec::EncodedU16(grouping_result, self.codec), TypedVec::Integer(result))
    }
}

