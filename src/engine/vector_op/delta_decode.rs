use std::marker::PhantomData;

use engine::*;
use engine::vector_op::vector_operator::*;

#[derive(Debug)]
pub struct DeltaDecode<T> {
    pub encoded: BufferRef,
    pub decoded: BufferRef,
    pub previous: i64,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for DeltaDecode<T> {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) {
        let encoded = scratchpad.get::<T>(self.encoded);
        let mut decoded = scratchpad.get_mut::<i64>(self.decoded);
        if streaming { decoded.clear(); }
        let mut previous = self.previous;
        for e in encoded.iter() {
            let current = e.to_i64().unwrap() + previous;
            decoded.push(current);
            previous = current;
        }
        self.previous = previous;
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.decoded, Box::new(Vec::<i64>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.encoded] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.decoded] }
    fn can_stream_input(&self, _: BufferRef) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("delta_decode({})", self.encoded)
    }
}

