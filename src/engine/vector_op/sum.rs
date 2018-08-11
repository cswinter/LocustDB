use std::marker::PhantomData;

use engine::typed_vec::AnyVec;
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
    T: GenericIntVec<T> + Into<i64>, U: GenericIntVec<U> + CastUsize {
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
    T: GenericIntVec<T> + Into<i64>, U: GenericIntVec<U> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let nums = scratchpad.get::<T>(self.input);
        let grouping = scratchpad.get::<U>(self.grouping);
        let mut sums = scratchpad.get_mut::<i64>(self.output);

        let len = scratchpad.get_const::<i64>(self.max_index) as usize + 1;
        if len > sums.len() {
            sums.resize(len, 0);
        }

        for (i, n) in grouping.iter().zip(nums.iter()) {
            sums[i.cast_usize()] += Into::<i64>::into(*n);
        }
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, AnyVec::owned(Vec::<i64>::with_capacity(0)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.grouping, self.input, self.max_index] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, input: BufferRef) -> bool { input != self.max_index }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}
