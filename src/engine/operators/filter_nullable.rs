use crate::bitvec::*;
use crate::engine::*;

/// Selects all elements in nullable `input` where the corresponding element in `filter` is non-zero.
pub struct FilterNullable<T> {
    pub input: BufferRef<Nullable<T>>,
    pub filter: BufferRef<u8>,
    pub output: BufferRef<Nullable<T>>,
}

impl<'a, T: 'a> VecOperator<'a> for FilterNullable<T>
where
    T: VecData<T>,
{
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (data, present) = scratchpad.get_nullable(self.input);
        let filter = scratchpad.get(self.filter);
        let (mut filtered, mut filtered_present) = scratchpad.get_mut_nullable(self.output);
        if stream {
            filtered.clear();
            for p in filtered_present.iter_mut() {
                *p = 0;
            }
        }
        for (i, (d, &select)) in data.iter().zip(filter.iter()).enumerate() {
            if select > 0 {
                if BitVec::is_set(&&*present, i) {
                    filtered_present.set(filtered.len());
                }
                filtered.push(*d);
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_nullable(self.output, Vec::with_capacity(batch_size), Vec::with_capacity((batch_size + 7) / 8));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.input.any(), self.filter.any()]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i, &mut self.filter.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        true
    }
    fn can_stream_output(&self, _: usize) -> bool {
        true
    }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool {
        true
    }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.filter)
    }
}

/// Selects all elements in nullable `input` where the corresponding element in `filter` is non-zero and non-null.
pub struct NullableFilterNullable<T> {
    pub input: BufferRef<Nullable<T>>,
    pub filter: BufferRef<Nullable<u8>>,
    pub output: BufferRef<Nullable<T>>,
}

impl<'a, T: 'a> VecOperator<'a> for NullableFilterNullable<T>
where
    T: VecData<T>,
{
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (data, input_present) = scratchpad.get_nullable(self.input);
        let (filter, filter_present) = scratchpad.get_nullable(self.filter);
        let (mut filtered, mut filtered_present) = scratchpad.get_mut_nullable(self.output);
        if stream {
            filtered.clear();
            for p in filtered_present.iter_mut() {
                *p = 0;
            }
        }
        for i in 0..data.len() {
            if filter[i] > 0 && (&*filter_present).is_set(i) {
                if BitVec::is_set(&&*input_present, i) {
                    filtered_present.set(filtered.len());
                }
                filtered.push(data[i]);
            }
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_nullable(self.output, Vec::with_capacity(batch_size), Vec::with_capacity((batch_size + 7) / 8));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.input.any(), self.filter.any()]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i, &mut self.filter.i] }
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
