use engine::*;


#[derive(Debug)]
pub struct VecSum<T, U> {
    pub input: BufferRef<T>,
    pub grouping: BufferRef<U>,
    pub output: BufferRef<i64>,
    pub max_index: BufferRef<Scalar<i64>>,
}

impl<'a, T, U> VecOperator<'a> for VecSum<T, U> where
    T: GenericIntVec<T> + Into<i64>, U: GenericIntVec<U> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let nums = scratchpad.get(self.input);
        let grouping = scratchpad.get(self.grouping);
        let mut sums = scratchpad.get_mut(self.output);

        let len = scratchpad.get_scalar(&self.max_index) as usize + 1;
        if len > sums.len() {
            sums.resize(len, 0);
        }

        for (i, n) in grouping.iter().zip(nums.iter()) {
            sums[i.cast_usize()] += Into::<i64>::into(*n);
        }
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(0));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.grouping.any(), self.input.any(), self.max_index.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] += {}", self.output, self.grouping, self.input)
    }
    fn display_output(&self) -> bool { false }
}
