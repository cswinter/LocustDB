use std::marker::PhantomData;

use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct DictLookup<'a, T> {
    pub indices: BufferRef,
    pub output: BufferRef,
    pub dictionary: &'a Vec<String>,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for DictLookup<'a, T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let indices = scratchpad.get::<T>(self.indices);
        let mut output = scratchpad.get_mut::<&str>(self.output);
        if stream { output.clear(); }
        for i in indices.iter() {
            output.push(self.dictionary[i.cast_usize()].as_ref());
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Box::new(Vec::<&str>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.indices] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("dictionary[{}]", self.indices)
    }
}
