use std::hash::Hash;

use fnv::FnvHashMap;

use engine::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct HashMapGrouping<T: VecData<T> + Hash> {
    input: BufferRef<T>,
    unique_out: BufferRef<T>,
    grouping_key_out: BufferRef<u32>,
    cardinality_out: BufferRef<Scalar<i64>>,
    map: FnvHashMap<T, u32>,
}

impl<'a, T: VecData<T> + Hash + 'a> HashMapGrouping<T> {
    pub fn boxed(input: BufferRef<T>,
                 unique_out: BufferRef<T>,
                 grouping_key_out: BufferRef<u32>,
                 cardinality_out: BufferRef<Scalar<i64>>,
                 _max_index: usize) -> BoxedOperator<'a> {
        Box::new(HashMapGrouping::<T> {
            input,
            unique_out,
            grouping_key_out,
            cardinality_out,
            map: FnvHashMap::default(),
        })
    }
}

impl<'a, T: VecData<T> + Hash + 'a> VecOperator<'a> for HashMapGrouping<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let count = {
            let raw_grouping_key = scratchpad.get(self.input);
            let mut grouping = scratchpad.get_mut(self.grouping_key_out);
            let mut unique = scratchpad.get_mut(self.unique_out);
            if stream { grouping.clear() }
            for i in raw_grouping_key.iter() {
                grouping.push(*self.map.entry(*i).or_insert_with(|| {
                    unique.push(*i);
                    unique.len() as u32 - 1
                }));
            }
            RawVal::Int(unique.len() as i64)
        };
        scratchpad.set_any(self.cardinality_out.any(), Data::constant(count));
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.unique_out, Vec::new());
        scratchpad.set(self.grouping_key_out, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.unique_out.any(), self.grouping_key_out.any(), self.cardinality_out.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, output: usize) -> bool { output != self.unique_out.i }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("hashmap_grouping({})", self.input)
    }
}
