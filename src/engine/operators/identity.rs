use engine::vector_op::vector_operator::*;


#[derive(Debug)]
struct Identity {
    input: BufferRef,
    output: BufferRef,
}

impl Identity {
    pub fn new(input: BufferRef, output: BufferRef) -> Identity {
        Identity { input, output }
    }
}

impl<'a> VecOperator<'a> for Identity {
    fn execute(&mut self, _scratchpad: &mut Scratchpad<'a>) {}
}
