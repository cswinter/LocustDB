use engine::vector_op::vector_operator::*;
use engine::typed_vec::AnyVec;


#[derive(Debug)]
pub struct SortIndices {
    pub input: BufferRef,
    pub output: BufferRef,
    pub descending: bool,
}

impl<'a> VecOperator<'a> for SortIndices {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let input = scratchpad.get_any(self.input);
            let mut result = (0..input.len()).collect();
            if self.descending {
                input.sort_indices_desc(&mut result);
            } else {
                input.sort_indices_asc(&mut result);
            }
            AnyVec::owned(result)
        };
        scratchpad.set(self.output, result);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("sort_indices({}; desc={})", self.input, self.descending)
    }
}
