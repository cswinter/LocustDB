use std::marker::PhantomData;

use engine::typed_vec::TypedVec;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct Exists<T> {
    input: BufferRef,
    output: BufferRef,
    max_index: BufferRef,
    t: PhantomData<T>,
}

impl<T: IntVecType<T> + IntoUsize> Exists<T> {
    pub fn boxed<'a>(input: BufferRef, output: BufferRef, max_index: BufferRef) -> BoxedOperator<'a> {
        Box::new(Exists::<T> { input, output, max_index, t: PhantomData })
    }
}

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for Exists<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<T>(self.input);
        let mut exists = scratchpad.get_mut::<u8>(self.output);
        for &i in data.iter() {
            let index = i.cast_usize();
            exists[index] = 1;
        }
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let len = scratchpad.get_const::<i64>(self.max_index) as usize + 1;
        scratchpad.set(self.output, TypedVec::owned(vec![0u8; len]));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] = 1", self.output, self.input)
    }
}

