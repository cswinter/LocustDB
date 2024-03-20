use crate::engine::*;


pub struct BufferStream<T> {
    pub input: BufferRef<T>,
    pub output: BufferRef<T>,
}

impl<'a, T: 'a> VecOperator<'a> for BufferStream<T>
where
    T: VecData<T>,
{
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let data = scratchpad.get(self.input);
        let mut output = scratchpad.get_mut(self.output);
        output.extend(data.iter());
        Ok(())
    }

    fn init(&mut self, total_count: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(total_count));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }
    fn display_op(&self, _: bool) -> String { format!("buffer({})", self.input) }
}


pub struct BufferStreamNullable<T> {
    pub input: BufferRef<Nullable<T>>,
    pub output: BufferRef<Nullable<T>>,
}

impl<'a, T: 'a> VecOperator<'a> for BufferStreamNullable<T>
where
    T: VecData<T>,
{
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let (data, present) = scratchpad.get_nullable(self.input);
        let (mut output, mut output_present) = scratchpad.get_mut_nullable(self.output);
        output.extend(data.iter());
        output_present.extend(present.iter());
        Ok(())
    }

    fn init(&mut self, total_count: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set_nullable(self.output, Vec::with_capacity(total_count), Vec::with_capacity((total_count + 7) / 8));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.input.any()]
    }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> {
        vec![self.output.any()]
    }
    fn can_stream_input(&self, _: usize) -> bool {
        true
    }
    fn can_stream_output(&self, _: usize) -> bool {
        false
    }
    fn allocates(&self) -> bool {
        true
    }

    fn display_op(&self, _: bool) -> String {
        format!("buffer({})", self.input)
    }
}


#[derive(Debug)]
pub struct BufferStreamNull {
    pub input: BufferRef<Any>,
    pub output: BufferRef<Any>,
    pub count: usize,
}

impl<'a> VecOperator<'a> for BufferStreamNull {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        self.count += scratchpad.get_any(self.input).len();
        let mut output = scratchpad.get_any_mut(self.output);
        *output.cast_ref_mut_null() = self.count;
        Ok(())
    }

    fn init(&mut self, _: usize, _: usize, _: &mut Scratchpad<'a>) { }
    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.input.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String { format!("BufferStreamNull({})", self.input) }
}
