use std::marker::PhantomData;

use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct Select<T> {
    pub input: BufferRef,
    pub indices: BufferRef,
    pub output: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: 'a> VecOperator<'a> for Select<T> where T: VecType<T> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<T>(self.input);
            let indices = scratchpad.get::<usize>(self.indices);
            let mut output = indices.iter().map(|i| data[*i]).collect();
            TypedVec::owned(output)
        };
        scratchpad.set(self.output, result);
    }
}

