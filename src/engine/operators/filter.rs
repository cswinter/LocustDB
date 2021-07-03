use crate::bitvec::BitVec;
use crate::engine::*;

pub struct Filter<T> {
    pub input: BufferRef<T>,
    pub filter: BufferRef<u8>,
    pub output: BufferRef<T>,
}

impl<'a, T: 'a> VecOperator<'a> for Filter<T>
where
    T: VecData<T>,
{
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let data = scratchpad.get(self.input);
        let filter = scratchpad.get(self.filter);
        let mut filtered = scratchpad.get_mut(self.output);
        if stream {
            filtered.clear();
        }
        for (d, &select) in data.iter().zip(filter.iter()) {
            if select > 0 {
                filtered.push(*d);
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.input.any(), self.filter.any()]
    }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        true
    }
    fn can_stream_output(&self, _: usize) -> bool {
        true
    }
    fn allocates(&self) -> bool {
        true
    }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.filter)
    }
}

pub struct NullableFilter<T> {
    pub input: BufferRef<T>,
    pub filter: BufferRef<Nullable<u8>>,
    pub output: BufferRef<T>,
}

impl<'a, T: 'a> VecOperator<'a> for NullableFilter<T>
where
    T: VecData<T>,
{
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let data = scratchpad.get(self.input);
        let (filter, present) = scratchpad.get_nullable(self.filter);
        let mut filtered = scratchpad.get_mut(self.output);
        if stream {
            filtered.clear();
        }
        for i in 0..data.len() {
            if filter[i] > 0 && (&*present).is_set(i) {
                filtered.push(data[i]);
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.input.any(), self.filter.any()]
    }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        true
    }
    fn can_stream_output(&self, _: usize) -> bool {
        true
    }
    fn allocates(&self) -> bool {
        true
    }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.filter)
    }
}
