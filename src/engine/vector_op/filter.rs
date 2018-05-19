use std::marker::PhantomData;

use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct Filter<T> {
    pub input: BufferRef,
    pub filter: BufferRef,
    pub output: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: 'a> VecOperator<'a> for Filter<T> where T: VecType<T> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<T>(self.input);
            let filter = scratchpad.get_bit_vec(self.filter);
            let mut output = Vec::with_capacity(filter.len());
            for (d, select) in data.iter().zip(filter.iter()) {
                if select {
                    output.push(*d);
                }
            }
            TypedVec::owned(output)
        };
        scratchpad.set(self.output, result);
    }
}

