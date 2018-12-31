use engine::*;


#[derive(Debug)]
pub struct Exists<T> {
    pub input: BufferRef<T>,
    pub output: BufferRef<u8>,
    pub max_index: BufferRef<Scalar<i64>>,
}

impl<'a, T: GenericIntVec<T> + CastUsize> VecOperator<'a> for Exists<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError>{
        let data = scratchpad.get(self.input);
        let mut exists = scratchpad.get_mut(self.output);

        let len = scratchpad.get_scalar(&self.max_index) as usize + 1;
        if len > exists.len() {
            exists.resize(len, 0);
        }

        for &i in data.iter() {
            let index = i.cast_usize();
            exists[index] = 1;
        }
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_output(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] = 1 {}", self.output, self.input, self.max_index)
    }
}

