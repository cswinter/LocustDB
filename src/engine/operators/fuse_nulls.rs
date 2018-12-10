use std::i64;

use engine::*;
use bitvec::BitVec;


pub struct FuseNullsI64 {
    pub input: BufferRef<Nullable<i64>>,
    pub fused: BufferRef<i64>,
}

impl<'a> VecOperator<'a> for FuseNullsI64 {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let (input, present) = scratchpad.get_nullable(self.input);
        let mut fused = scratchpad.get_mut(self.fused);
        if stream { fused.clear(); }
        for i in 0..input.len() {
            if (&*present).is_set(i) {
                fused.push(input[i]);
            } else {
                fused.push(i64::MIN);
            }
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.fused, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.fused.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("FuseNullsI64({})", self.fused)
    }
}

pub struct FuseNullsStr<'a> {
    pub input: BufferRef<Nullable<&'a str>>,
    pub fused: BufferRef<Option<&'a str>>,
}

impl<'a> VecOperator<'a> for FuseNullsStr<'a> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let (input, present) = scratchpad.get_nullable(self.input);
        let mut fused = scratchpad.get_mut(self.fused);
        if stream { fused.clear(); }
        for i in 0..input.len() {
            if (&*present).is_set(i) {
                fused.push(Some(input[i]));
            } else {
                fused.push(None);
            }
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.fused, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.fused.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("FuseNullsStr({})", self.fused)
    }
}
