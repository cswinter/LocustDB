use std::marker::PhantomData;

use engine::typed_vec::TypedVec;
use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct Unique<T> {
    input: BufferRef,
    output: BufferRef,
    max_index: usize,
    seen_before: Vec<bool>,
    t: PhantomData<T>,
}

impl<T: IntVecType<T> + IntoUsize> Unique<T> {
    pub fn boxed<'a>(input: BufferRef, output: BufferRef, max_index: usize) -> BoxedOperator<'a> {
        Box::new(Unique::<T> { input, output, max_index, seen_before: Vec::new(), t: PhantomData })
    }
}

impl<'a, T: IntVecType<T> + IntoUsize> VecOperator<'a> for Unique<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<T>(self.input);
        let mut unique = scratchpad.get_mut::<T>(self.output);
        for &i in data.iter() {
            let index = i.cast_usize();
            if !self.seen_before[index] {
                unique.push(i);
                self.seen_before[index] = true;
            }
        }
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let mut unique = scratchpad.get_mut::<T>(self.output);
        unique.sort();
        trace!("unique: {:?}", &unique);
    }

    fn init(&mut self, _: usize, _: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        self.seen_before = vec![false; self.max_index + 1];
        // TODO(clemens): output size estimate?
        scratchpad.set(self.output, TypedVec::owned(Vec::<T>::new()));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self) -> bool { false }
    fn allocates(&self) -> bool { true }
}

