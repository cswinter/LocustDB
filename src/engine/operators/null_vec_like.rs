use crate::engine::*;
use crate::bitvec::BitVec;

#[derive(Debug)]
pub struct NullVecLike {
    pub input: BufferRef<Any>,
    pub output: BufferRef<Any>,
    // 0: use input length, 1: non-zero elements in u8 input, 2: non-zero non-null elements in nullalb u8 input
    pub source_type: u8,
    pub count: usize,
}

impl<'a> VecOperator<'a> for NullVecLike {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        if streaming { self.count = 0 };
        self.count += match self.source_type {
            0 => scratchpad.get_any(self.input).len(),
            1 => scratchpad.get(self.input.u8()).iter().filter(|&&x| x != 0).count(),
            2 => {
                let mut count = 0;
                let (data, present) = scratchpad.get_nullable(self.input.nullable_u8());
                for (i, d) in data.iter().enumerate() {
                    if *d != 0 && BitVec::is_set(&&*present, i) {
                        count += 1;
                    }
                }
                count
            },
            _ => unreachable!(),
        };
        let mut output = scratchpad.get_any_mut(self.output);
        *output.cast_ref_mut_null() = self.count;
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, _: &mut Scratchpad<'a>) { }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String {
        format!("NullVecLike({})", self.input)
    }
}
