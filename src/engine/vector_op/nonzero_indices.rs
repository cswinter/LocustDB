use std::marker::PhantomData;

use engine::typed_vec::TypedVec;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct NonzeroIndices<T, U> {
    input: BufferRef,
    output: BufferRef,
    t: PhantomData<T>,
    u: PhantomData<U>,
}

impl<T: IntVecType<T> + IntoUsize, U: IntVecType<U>> NonzeroIndices<T, U> {
    pub fn boxed<'a>(input: BufferRef, output: BufferRef) -> BoxedOperator<'a> {
        Box::new(NonzeroIndices::<T, U> { input, output, t: PhantomData, u: PhantomData })
    }
}

impl<'a, T: IntVecType<T> + IntoUsize, U: IntVecType<U>> VecOperator<'a> for NonzeroIndices<T, U> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let exists = scratchpad.get::<T>(self.input);
        let mut unique = scratchpad.get_mut::<U>(self.output);
        for (index, &n) in exists.iter().enumerate() {
            if n > T::zero() {
                unique.push(U::from(index).unwrap());
            }
        }
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): output size estimate?
        scratchpad.set(self.output, TypedVec::owned(Vec::<U>::new()));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("nonzero_indices({})", self.input)
    }
}

