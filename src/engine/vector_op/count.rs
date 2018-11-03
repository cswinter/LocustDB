use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct VecCount<T> {
    pub grouping: BufferRef<T>,
    pub output: BufferRef<u32>,
    pub max_index: BufferRef<i64>,
}

impl<'a, T: GenericIntVec<T> + CastUsize> VecOperator<'a> for VecCount<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut result = scratchpad.get_mut::<u32>(self.output);
        let grouping = scratchpad.get::<T>(self.grouping);

        let len = scratchpad.get_const::<i64>(&self.max_index) as usize + 1;
        if len > result.len() {
            result.resize(len, 0);
        }

        for i in grouping.iter() {
            result[i.cast_usize()] += 1;
        }
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.grouping.any(), self.max_index.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += 1", self.output, self.grouping)
    }
    fn display_output(&self) -> bool { false }
}
