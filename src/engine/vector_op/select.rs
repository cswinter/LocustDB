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

impl<'a, T: 'a> VecOperator<'a> for Select<T> where T: GenericVec<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<T>(self.input);
        let indices = scratchpad.get::<usize>(self.indices);
        let mut output = scratchpad.get_mut::<T>(self.output);
        if stream { output.clear(); }
        for i in indices.iter() {
            output.push(data[*i]);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, AnyVec::owned(Vec::<T>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input, self.indices] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    // TODO(clemens): need to add functionality to read from block input (sort indices) in streaming fashion
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.indices)
    }
}

