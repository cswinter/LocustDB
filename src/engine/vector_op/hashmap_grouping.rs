use fnv::FnvHashMap;

use engine::typed_vec::TypedVec;
use engine::vector_op::*;
use engine::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct HashMapGrouping<T: IntVecType<T>> {
    input: BufferRef,
    unique_out: BufferRef,
    grouping_key_out: BufferRef,
    cardinality_out: BufferRef,
    map: FnvHashMap<T, T>,
}

impl<T: IntVecType<T> + IntoUsize> HashMapGrouping<T> {
    pub fn boxed<'a>(input: BufferRef,
                     unique_out: BufferRef,
                     grouping_key_out: BufferRef,
                     cardinality_out: BufferRef,
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

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for HashMapGrouping<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let count = {
            let raw_grouping_key = scratchpad.get::<T>(self.input);
            let mut grouping = scratchpad.get_mut::<T>(self.grouping_key_out);
            let mut unique = scratchpad.get_mut::<T>(self.unique_out);
            if stream { grouping.clear() }
            for i in raw_grouping_key.iter() {
                grouping.push(*self.map.entry(*i).or_insert_with(|| {
                    unique.push(*i);
                    T::from(unique.len()).unwrap() - T::one()
                }));
            }
            RawVal::Int(unique.len() as i64)
        };
        scratchpad.set(self.cardinality_out, TypedVec::constant(count));
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): Estimate capacities for unique + map?
        scratchpad.set(self.unique_out, TypedVec::owned(Vec::<T>::new()));
        scratchpad.set(self.grouping_key_out, TypedVec::owned(Vec::<T>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.unique_out, self.grouping_key_out, self.cardinality_out] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self, output: BufferRef) -> bool { output != self.unique_out }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("hashmap_grouping({})", self.input)
    }
}

