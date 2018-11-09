use std::slice;
use std::mem;

use engine::*;


#[derive(Debug)]
pub struct SlicePackInt<T> {
    pub input: BufferRef<T>,
    pub output: BufferRef<Any>,
    pub stride: usize,
    pub offset: usize,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for SlicePackInt<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get_pinned(self.input);
        let mut packed_any = scratchpad.get_any_mut(self.output);
        let packed = packed_any.cast_ref_mut_byte_slices();
        for (i, datum) in data.iter().enumerate() {
            packed.data[i * self.stride + self.offset] = bytes(datum);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        if scratchpad.get_any(self.output).len() == 0 {
            scratchpad.set_any(self.output, Box::new(ByteSlices {
                row_len: self.stride,
                data: vec![&[]; batch_size * self.stride],
            }));
        }
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}, {}, ...] = {}", self.output, self.offset, self.offset + self.stride, self.input)
    }
}

#[derive(Debug)]
pub struct SlicePackString<'a> {
    pub input: BufferRef<&'a str>,
    pub output: BufferRef<Any>,
    pub stride: usize,
    pub offset: usize,
}

impl<'a> VecOperator<'a> for SlicePackString<'a> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get(self.input);
        let mut packed_any = scratchpad.get_any_mut(self.output);
        let packed = packed_any.cast_ref_mut_byte_slices();
        for (i, datum) in data.iter().enumerate() {
            packed.data[i * self.stride + self.offset] = datum.as_bytes();
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        if scratchpad.get_any(self.output).len() == 0 {
            scratchpad.set_any(self.output, Box::new(ByteSlices {
                row_len: self.stride,
                data: vec![&[]; batch_size * self.stride],
            }));
        }
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}, {}, ...] = {}", self.output, self.offset, self.offset + self.stride, self.input)
    }
}

fn bytes<T>(t: &T) -> &[u8] {
    let p: *const T = t;
    let p: *const u8 = p as *const u8;
    unsafe {
        slice::from_raw_parts(p, mem::size_of::<T>())
    }
}