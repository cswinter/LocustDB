use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct BitUnpackOperator {
    input: BufferRef,
    output: BufferRef,
    shift: u8,
    width: u8,
}

impl BitUnpackOperator {
    pub fn new(input: BufferRef, output: BufferRef, shift: u8, width: u8) -> BitUnpackOperator {
        BitUnpackOperator {
            input,
            output,
            shift,
            width,
        }
    }
}

impl<'a> VecOperator<'a> for BitUnpackOperator {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<i64>(self.input);
            let mask = (1 << self.width) - 1;
            let mut output = Vec::with_capacity(data.len());
            for d in data.iter() {
                output.push((d >> self.shift) & mask);
            }
            TypedVec::owned(output)
        };
        scratchpad.set(self.output, result);
    }
}
