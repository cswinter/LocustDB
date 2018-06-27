use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct DivideVS {
    pub lhs: BufferRef,
    pub rhs: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for DivideVS {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut output = scratchpad.get_mut::<i64>(self.output);
        if stream { output.clear(); }
        let data = scratchpad.get::<i64>(self.lhs);
        let c = scratchpad.get_const::<i64>(self.rhs);
        for d in data.iter() {
            output.push(d / c);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Box::new(Vec::<i64>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.lhs, self.rhs] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} / {}", self.lhs, self.rhs)
    }
}

