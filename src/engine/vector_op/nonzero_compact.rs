use std::marker::PhantomData;

use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct NonzeroCompact<T> {
    data: BufferRef,
    t: PhantomData<T>,
}

impl<'a, T: IntVecType<T>> NonzeroCompact<T> {
    pub fn boxed(data: BufferRef) -> BoxedOperator<'a> {
        Box::new(NonzeroCompact::<T> { data, t: PhantomData })
    }
}

impl<'a, T: IntVecType<T>> VecOperator<'a> for NonzeroCompact<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut data = scratchpad.get_mut::<T>(self.data);
        // Remove all unmodified entries
        let mut j = 0;
        for i in 0..data.len() {
            if data[i] > T::zero() {
                data[j] = data[i];
                j += 1;
            }
        }
        data.truncate(j);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.data] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.data] }
    fn can_stream_input(&self) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{} > 0]", self.data, self.data)
    }
}

