use engine::vector_op::vector_operator::*;
use engine::typed_vec::TypedVec;


#[derive(Debug)]
pub struct SortIndices {
    pub input: BufferRef,
    pub output: BufferRef,
    pub descending: bool,
}

impl<'a> VecOperator<'a> for SortIndices {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let input = scratchpad.get_any(self.input);
            let mut result = (0..input.len()).collect();
            if self.descending {
                input.sort_indices_desc(&mut result);
            } else {
                input.sort_indices_asc(&mut result);
            }
            TypedVec::owned(result)
        };
        scratchpad.set(self.output, result);
    }
}
