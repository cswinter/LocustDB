use bitvec::*;
use engine::*;


#[derive(Debug)]
pub struct Select<T> {
    pub input: BufferRef<T>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<T>,
}

impl<'a, T: 'a> VecOperator<'a> for Select<T> where T: VecData<T> {
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
    // TODO(#98): need to add functionality to read from block input (sort indices) in streaming fashion
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.indices)
    }
}


#[derive(Debug)]
pub struct SelectNullable<T> {
    pub input: BufferRef<Nullable<T>>,
    pub indices: BufferRef<usize>,
    pub output: BufferRef<Nullable<T>>,
}

impl<'a, T: 'a> VecOperator<'a> for SelectNullable<T> where T: VecData<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let (data, present) = scratchpad.get_nullable(self.input);
        let indices = scratchpad.get(self.indices);
        let (mut data_out, mut present_out) = scratchpad.get_mut_nullable(self.output);
        if stream {
            data_out.clear();
            present_out.clear();
        }
        for (i, &index) in indices.iter().enumerate() {
            data_out.push(data[index]);
            if (&*present).is_set(index) { present_out.set(i) }
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_nullable(self.output, Vec::with_capacity(batch_size), Vec::with_capacity(batch_size / 8));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any(), self.indices.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    // TODO(#98): need to add functionality to read from block input (sort indices) in streaming fashion
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.indices)
    }
}
