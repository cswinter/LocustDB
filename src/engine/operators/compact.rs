use crate::engine::*;

#[derive(Debug)]
pub struct Compact<T, U> {
    pub data: BufferRef<T>,
    pub select: BufferRef<U>,
    pub compacted: BufferRef<T>,
}

impl<'a, T: VecData<T> + 'a, U: GenericIntVec<U>> VecOperator<'a> for Compact<T, U> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let mut data = scratchpad.get_mut(self.data);
        let select = scratchpad.get(self.select);
        // Remove all unmodified entries
        let mut j = 0;
        for (i, &s) in select.iter().take(data.len()).enumerate() {
            if s > U::zero() {
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

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.data.any(), self.select.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.data.i, &mut self.select.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.compacted.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn mutates(&self, i: usize) -> bool { i == self.data.i }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{} > 0]", self.data, self.select)
    }
}

