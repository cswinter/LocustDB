use crate::bitvec::BitVec;
use crate::engine::*;

// Returns the indices of all nonzero elements in the input vector.
#[derive(Debug)]
pub struct NonzeroIndices<T, U> {
    pub input: BufferRef<T>,
    pub output: BufferRef<U>,

    pub offset: usize,
}

impl<'a, T: GenericIntVec<T> + CastUsize, U: GenericIntVec<U>> VecOperator<'a> for NonzeroIndices<T, U> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let exists = scratchpad.get(self.input);
        let mut unique = scratchpad.get_mut(self.output);
        if stream { unique.clear(); }
        for (index, &n) in exists.iter().enumerate() {
            if n > T::zero() {
                unique.push(U::from(index).unwrap() + U::from(self.offset).unwrap());
            }
        }
        self.offset += exists.len();
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::new());
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn display_op(&self, _: bool) -> String { format!("nonzero_indices({})", self.input) }
}


// Returns the indices of all nonzero and non-null elements in the input vector.
#[derive(Debug)]
pub struct NonzeroNonnullIndices<T, U> {
    pub input: BufferRef<Nullable<T>>,
    pub output: BufferRef<U>,

    pub offset: usize,
}

impl<'a, T: GenericIntVec<T> + CastUsize, U: GenericIntVec<U>> VecOperator<'a> for NonzeroNonnullIndices<T, U> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (input, input_present)  = scratchpad.get_nullable(self.input);
        let mut unique = scratchpad.get_mut(self.output);
        if stream { unique.clear(); }
        for (index, &n) in input.iter().enumerate() {
            if n > T::zero() && (&*input_present).is_set(index) {
                unique.push(U::from(index).unwrap() + U::from(self.offset).unwrap());
            }
        }
        self.offset += input.len();
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::new());
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn can_block_output(&self) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn display_op(&self, _: bool) -> String { format!("nonzero_indices({})", self.input) }
}

