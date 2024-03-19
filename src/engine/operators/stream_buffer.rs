use std::cmp::min;

use crate::engine::*;


pub struct StreamBuffer<T> {
    pub input: BufferRef<T>,
    pub output: BufferRef<T>,

    pub is_bitvec: bool,
    pub current_index: usize,
    pub batch_size: usize,
    pub has_more: bool,
}

impl<'a, T: 'a> VecOperator<'a> for StreamBuffer<T>
where
    T: VecData<T>,
{
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let input = scratchpad.get_pinned(self.input);
        if self.is_bitvec {
            // Basic sanity check, this will panic if the data is not u8
            input.cast_ref_u8();
            assert!(
                self.current_index & 7 == 0,
                "Bitvec read must be aligned to byte boundary"
            );
        }
        let (from, to) = if streaming {
            if self.is_bitvec {
                (
                    (self.current_index + 7) / 8,
                    (self.current_index + self.batch_size + 7) / 8,
                )
            } else {
                (self.current_index, self.current_index + self.batch_size)
            }
        } else {
            (0, input.len())
        };

        let to = min(to, input.len());
        let result = Box::new(&input[from..to]);
        scratchpad.set_any(self.output.any(), result);
        self.current_index += self.batch_size;
        self.has_more = to < input.len();
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, _: &mut Scratchpad<'a>) {
        self.batch_size = batch_size
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        // Safety relies on query code behaving well.
        // If any of the final result columns are referenced streamed buffers, they will not be pinned and may be deallocated before their values are read out.
        // This should not happen because output columns cannot be streamed.
        unsafe {
            scratchpad.unpin(self.input.any());
        }
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }
    fn has_more(&self) -> bool { self.has_more }

    fn display_op(&self, _: bool) -> String {
        if self.is_bitvec {
            format!("stream_bitvec({})", self.input)
        } else {
            format!("stream({})", self.input)
        }
    }
}


pub struct StreamBufferNullable<T> {
    pub input: BufferRef<Nullable<T>>,

    pub output_data: BufferRef<Any>,
    pub output_present: BufferRef<u8>,
    pub output: BufferRef<Nullable<T>>,

    pub current_index: usize,
    pub batch_size: usize,
    pub has_more: bool,
}

impl<'a, T: 'a> VecOperator<'a> for StreamBufferNullable<T>
where
    T: VecData<T>,
{
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (input, present) = scratchpad.get_pinned_nullable(self.input);
        let (from, to) = if streaming {
            (self.current_index, self.current_index + self.batch_size)
        } else {
            (0, input.len())
        };
        let to = min(to, input.len());
        let result_data = Box::new(&input[from..to]);
        scratchpad.set_any(self.output_data.any(), result_data);
        let result_present = Box::new(&present[(from / 8)..(to + 7) / 8]);
        scratchpad.set_any(self.output_present.any(), result_present);
        self.current_index += self.batch_size;
        self.has_more = to < input.len();
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.assemble_nullable_any(self.output_data, self.output_present, self.output.nullable_any());
        self.batch_size = batch_size
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        // Safety relies on query code behaving well.
        // If any of the final result columns are referenced streamed buffers, they will not be pinned and may be deallocated before their values are read out.
        // This should not happen because output columns cannot be streamed.
        unsafe {
            scratchpad.unpin(self.input.any());
        }
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }
    fn has_more(&self) -> bool { self.has_more }
    fn display_op(&self, _: bool) -> String { format!("stream({})", self.input) }
}

pub struct StreamNullVec {
    pub input: BufferRef<Any>,
    pub output: BufferRef<Any>,

    pub current_index: usize,
    pub batch_size: usize,
    pub has_more: bool,
}

impl<'a> VecOperator<'a> for StreamNullVec {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let len = scratchpad.get_any(self.input).len();
        let count = if streaming {
            assert!(
                len > self.current_index,
                "StreamNullVec: index out of bounds len={} current_index={} batch_size={} has_more={}",
                len, self.current_index, self.batch_size, self.has_more,
            );
            min(self.batch_size, len - self.current_index)
        } else {
            len
        };
        let mut output = scratchpad.get_any_mut(self.output);
        *output.cast_ref_mut_null() = count;
        self.current_index += self.batch_size;
        self.has_more = self.current_index < len;
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, _: &mut Scratchpad<'a>) { self.batch_size = batch_size }
    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String { format!("stream({})", self.input) }
    fn has_more(&self) -> bool { self.has_more }
}

