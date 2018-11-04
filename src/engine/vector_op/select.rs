use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct Select<T> {
    pub input: BufferRef<T>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<T>,
}

impl<'a, T: 'a> VecOperator<'a> for Select<T> where T: GenericVec<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get(self.input);
        let indices = scratchpad.get(self.indices);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        for i in indices.iter() {
            output.push(data[*i]);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any(), self.indices.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    // TODO(clemens): need to add functionality to read from block input (sort indices) in streaming fashion
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.indices)
    }
}

