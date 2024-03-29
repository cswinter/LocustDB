use crate::engine::*;

#[derive(Debug)]
pub struct DeltaDecode<T> {
    pub encoded: BufferRef<T>,
    pub decoded: BufferRef<i64>,
    pub previous: i64,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for DeltaDecode<T> {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let encoded = scratchpad.get(self.encoded);
        let mut decoded = scratchpad.get_mut(self.decoded);
        if streaming { decoded.clear(); }
        let mut previous = self.previous;
        for e in encoded.iter() {
            let current = e.to_i64().unwrap() + previous;
            decoded.push(current);
            previous = current;
        }
        self.previous = previous;
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.decoded, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.encoded.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.encoded.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.decoded.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("delta_decode({})", self.encoded)
    }
}

