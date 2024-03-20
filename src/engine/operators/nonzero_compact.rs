use crate::engine::*;

#[derive(Debug)]
pub struct NonzeroCompact<T> {
    pub data: BufferRef<T>,
    pub compacted: BufferRef<T>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for NonzeroCompact<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let mut data = scratchpad.get_mut(self.data);
        // Remove all unmodified entries
        let mut j = 0;
        for i in 0..data.len() {
            if data[i] > T::zero() {
                data[j] = data[i];
                j += 1;
            }
        }
        data.truncate(j);
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.alias(self.data, self.compacted);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.data.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.data.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.compacted.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn mutates(&self, i: usize) -> bool { i == self.data.i }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{} > 0]", self.data, self.data)
    }
}

