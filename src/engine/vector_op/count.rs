use std::marker::PhantomData;

use engine::typed_vec::TypedVec;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct VecCount<T> {
    grouping: BufferRef,
    output: BufferRef,
    max_index: BufferRef,
    dense_grouping: bool,
    t: PhantomData<T>,
}

impl<T> VecCount<T> {
    pub fn new(grouping: BufferRef, output: BufferRef, max_index: BufferRef, dense_grouping: bool) -> VecCount<T> {
        VecCount {
            grouping,
            output,
            max_index,
            dense_grouping,
            t: PhantomData,
        }
    }
}

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for VecCount<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut result = scratchpad.get_mut::<u32>(self.output);
        let grouping = scratchpad.get::<T>(self.grouping);
        for i in grouping.iter() {
            result[i.cast_usize()] += 1;
        }
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        if !self.dense_grouping {
            let mut result = scratchpad.get_mut::<u32>(self.output);
            // Remove 0 counts for all entries that weren't present in grouping
            let mut j = 0;
            for i in 0..result.len() {
                if result[i] > 0 {
                    result[j] = result[i];
                    j += 1;
                }
            }
            result.truncate(j);
            trace!("Vec count: {:?}", &result);
        }
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let max_index = scratchpad.get_const::<i64>(self.max_index) as usize;
        scratchpad.set(self.output, TypedVec::owned(vec![0u32; max_index + 1]));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.grouping, self.max_index] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }
}
