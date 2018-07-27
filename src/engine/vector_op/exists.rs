use engine::*;
use engine::typed_vec::AnyVec;
use engine::vector_op::*;
use std::marker::PhantomData;


#[derive(Debug)]
pub struct Exists<T> {
    input: BufferRef,
    output: BufferRef,
    max_index: BufferRef,
    t: PhantomData<T>,
}

impl<T: GenericIntVec<T> + CastUsize> Exists<T> {
    pub fn boxed<'a>(input: BufferRef, output: BufferRef, max_index: BufferRef) -> BoxedOperator<'a> {
        Box::new(Exists::<T> { input, output, max_index, t: PhantomData })
    }
}

impl<'a, T: GenericIntVec<T> + CastUsize> VecOperator<'a> for Exists<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<T>(self.input);
        let mut exists = scratchpad.get_mut::<u8>(self.output);

        let len = scratchpad.get_const::<i64>(self.max_index) as usize + 1;
        if len > exists.len() {
            exists.resize(len, 0);
        }

        for &i in data.iter() {
            let index = i.cast_usize();
            exists[index] = 1;
        }
    }

    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, AnyVec::owned(Vec::<u8>::with_capacity(0)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_output(&self) -> bool { false }
    fn display_op(&self, _: bool) -> String {
        format!("{}[{}] = 1 {}", self.output, self.input, self.max_index)
    }
}

