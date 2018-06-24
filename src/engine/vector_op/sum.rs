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
    t: PhantomData<T>,
    u: PhantomData<U>,
}

impl<T, U> VecSum<T, U> where
    T: IntVecType<T>, U: IntVecType<U> + IntoUsize {
    pub fn boxed<'a>(input: BufferRef, grouping: BufferRef, output: BufferRef, max_index: BufferRef) -> BoxedOperator<'a> {
        Box::new(VecSum::<T, U> {
            input,
            grouping,
            output,
            max_index,
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
        }
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let len = scratchpad.get_const::<i64>(self.max_index) as usize + 1;
        scratchpad.set(self.output, TypedVec::owned(vec![0 as i64; len]));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.grouping, self.input, self.max_index] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}
