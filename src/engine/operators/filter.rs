use engine::*;


#[derive(Debug)]
pub struct Filter<T> {
    pub input: BufferRef<T>,
    pub filter: BufferRef<u8>,
    pub output: BufferRef<T>,
}

impl<'a, T: 'a> VecOperator<'a> for Filter<T> where T: GenericVec<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get(self.input);
        let filter = scratchpad.get(self.filter);
        let mut filtered = scratchpad.get_mut(self.output);
        if stream { filtered.clear(); }
        for (d, &select) in data.iter().zip(filter.iter()) {
            if select > 0 {
                filtered.push(d.clone());
            }
        }
        trace!("filter: {:?}", filter);
        trace!("data: {:?}", data);
        trace!("filtered: {:?}", filtered);
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any(), self.filter.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.filter)
    }
}

