use std::marker::PhantomData;

use engine::typed_vec::TypedVec;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct VecSum<T, U> {
    input: BufferRef,
    grouping: BufferRef,
    output: BufferRef,
    max_index: BufferRef,
    dense_grouping: bool,
    modified: Vec<bool>,
    t: PhantomData<T>,
    u: PhantomData<U>,
}

impl<T, U> VecSum<T, U> where
    T: IntVecType<T>, U: IntVecType<U> + IntoUsize {
    pub fn boxed<'a>(input: BufferRef, grouping: BufferRef, output: BufferRef, max_index: BufferRef, dense_grouping: bool) -> BoxedOperator<'a> {
        Box::new(VecSum::<T, U> {
            input,
            grouping,
            output,
            max_index,
            dense_grouping,
            modified: Vec::with_capacity(0),
            t: PhantomData,
            u: PhantomData,
        })
    }
}

impl<'a, T, U> VecOperator<'a> for VecSum<T, U> where
    T: IntVecType<T>, U: IntVecType<U> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): this is already computed in unique function, we should just reuse
        let nums = scratchpad.get::<T>(self.input);
        let grouping = scratchpad.get::<U>(self.grouping);
        let mut sums = scratchpad.get_mut::<i64>(self.output);
        for (i, n) in grouping.iter().zip(nums.iter()) {
            sums[i.cast_usize()] += Into::<i64>::into(*n);
            self.modified[i.cast_usize()] = true;
        }
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        if !self.dense_grouping {
            let mut sums = scratchpad.get_mut::<i64>(self.output);
            // Remove all unmodified entries
            let mut j = 0;
            for i in 0..sums.len() {
                if self.modified[i] {
                    sums[j] = sums[i];
                    j += 1;
                }
            }
            sums.truncate(j);
        }
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let len = scratchpad.get_const::<i64>(self.max_index) as usize + 1;
        self.modified = vec![false; len];
        scratchpad.set(self.output, TypedVec::owned(vec![0 as i64; len]));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.grouping, self.input, self.max_index] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }
}
