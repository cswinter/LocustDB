use crate::engine::*;
use crate::bitvec::BitVec;

#[derive(Debug)]
pub enum LengthSource {
    InputLength,
    NonZeroU8ElementCount,
    NonNullElementCount,
}

#[derive(Debug)]
pub struct NullVecLike {
    pub input: BufferRef<Any>,
    pub output: BufferRef<Any>,
    pub source_type: LengthSource,
    pub count: usize,
}

impl<'a> VecOperator<'a> for NullVecLike {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        if streaming { self.count = 0 };
        self.count += match self.source_type {
            LengthSource::InputLength => scratchpad.get_any(self.input).len(),
            LengthSource::NonZeroU8ElementCount => scratchpad.get(self.input.u8()).iter().filter(|&&x| x != 0).count(),
            LengthSource::NonNullElementCount => {
                let mut count = 0;
                let (data, present) = scratchpad.get_nullable(self.input.nullable_u8());
                for (i, d) in data.iter().enumerate() {
                    if *d != 0 && BitVec::is_set(&&*present, i) {
                        count += 1;
                    }
                }
                count
            },
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
