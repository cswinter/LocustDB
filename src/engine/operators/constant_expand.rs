use engine::*;

#[derive(Debug)]
pub struct ConstantExpand<T> {
    pub val: T,
    pub output: BufferRef<T>,

    pub current_index: usize,
    pub len: usize,
    pub batch_size: usize,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for ConstantExpand<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        if self.current_index + self.batch_size > self.len {
            let mut output = scratchpad.get_mut(self.output);
            output.truncate(self.len - self.current_index);
        }
        self.current_index += self.batch_size;
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        self.batch_size = batch_size;
        scratchpad.set(self.output, vec![self.val; batch_size]);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn is_streaming_producer(&self) -> bool { true }
    fn has_more(&self) -> bool { self.current_index < self.len }

    fn display_op(&self, _: bool) -> String {
        "ConstantExpand".to_string()
    }
}

