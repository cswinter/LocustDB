use std::marker::PhantomData;

use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct Filter<T> {
    pub input: BufferRef,
    pub filter: BufferRef,
    pub output: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: 'a> VecOperator<'a> for Filter<T> where T: GenericVec<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<T>(self.input);
        let filter = scratchpad.get::<u8>(self.filter);
        let mut filtered = scratchpad.get_mut::<T>(self.output);
        if stream { filtered.clear(); }
        for (d, &select) in data.iter().zip(filter.iter()) {
            if select > 0 {
                filtered.push(*d);
            }
        }
        trace!("filter: {:?}", filter);
        trace!("data: {:?}", data);
        trace!("filtered: {:?}", filtered);
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, AnyVec::owned(Vec::<T>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input, self.filter] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.input, self.filter)
    }
}

