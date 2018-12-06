use engine::*;

#[derive(Debug)]
pub struct CombineNullMaps {
    pub lhs: BufferRef<Nullable<Any>>,
    pub rhs: BufferRef<Nullable<Any>>,
    pub output: BufferRef<u8>,
}

impl<'a> VecOperator<'a> for CombineNullMaps {
    fn execute(&mut self, _streaming: bool, scratchpad: &mut Scratchpad<'a>) {
        let lhs = scratchpad.get_null_map(self.lhs);
        let rhs = scratchpad.get_null_map(self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        for (out, (l, r)) in output.iter_mut().zip(lhs.iter().zip(rhs.iter())) {
            *out = l & r;
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        let output = vec![0u8; batch_size / 8 + 1];
        scratchpad.set(self.output, output);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn display_op(&self, _: bool) -> String { format!("combine_null_maps({}, {})", self.lhs, self.rhs) }
}

