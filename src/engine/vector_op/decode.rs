use std::marker::PhantomData;

use engine::*;
use engine::vector_op::vector_operator::*;
use mem_store::*;


#[derive(Debug)]
pub struct Decode<'a, T> {
    pub input: BufferRef,
    pub output: BufferRef,
    pub codec: Codec<'a>,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericVec<T> + 'a> VecOperator<'a> for Decode<'a, T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let encoded = scratchpad.get_any(self.input);
        let mut decoded = scratchpad.get_mut::<T>(self.output);
        if stream { decoded.clear(); }
        self.codec.unwrap_decode(&*encoded, &mut *decoded);
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Box::new(Vec::<T>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("decode({}; {:?})", self.input, self.codec)
    }
}

