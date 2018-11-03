use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct DivideVS {
    pub lhs: BufferRef<i64>,
    pub rhs: BufferRef<i64>,
    pub output: BufferRef<i64>,
}

impl<'a> VecOperator<'a> for DivideVS {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut output = scratchpad.get_mut::<i64>(self.output);
        if stream { output.clear(); }
        let data = scratchpad.get::<i64>(self.lhs);
        let c = scratchpad.get_const::<i64>(&self.rhs);
        for d in data.iter() {
            output.push(d / c);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} / {}", self.lhs, self.rhs)
    }
}

