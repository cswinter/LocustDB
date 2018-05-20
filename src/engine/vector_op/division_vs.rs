use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct DivideVS {
    pub lhs: BufferRef,
    pub rhs: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for DivideVS {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<i64>(self.lhs);
            let c = scratchpad.get_const::<i64>(self.rhs);
            let mut output = Vec::with_capacity(data.len());
            for d in data.iter() {
                output.push(d / c);
            }
            output
        };
        scratchpad.set(self.output, Box::new(result));
    }
}

