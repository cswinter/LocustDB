use crate::engine::*;

// Take a null count and expands it into a nullable vec of the same length with arbitrary type and all values set to null
#[derive(Debug)]
pub struct NullToVec<T> {
    pub input: BufferRef<Any>,
    pub output: BufferRef<Nullable<T>>,

    pub batch_size: usize,
}

impl<'a, T: 'a> VecOperator<'a> for NullToVec<T>
where
    T: VecData<T> + Copy + Default,
{
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let len = scratchpad.get_any(self.input).len();
        if self.batch_size > len {
            let (mut output, mut present) = scratchpad.get_mut_nullable(self.output);
            output.truncate(len);
            present.truncate((len + 7) / 8);
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        self.batch_size = batch_size;
        scratchpad.set_nullable(
            self.output,
            vec![T::default(); batch_size],
            vec![0u8; (batch_size + 7) / 8],
        );
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.input.any()]
    }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        true
    }
    fn can_stream_output(&self, _: usize) -> bool {
        true
    }
    fn allocates(&self) -> bool {
        true
    }

    fn display_op(&self, _: bool) -> String {
        format!("{} expand as Nullable<{:?}>", self.input, T::t())
    }
}
