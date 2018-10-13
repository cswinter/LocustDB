use std::marker::PhantomData;

use engine::vector_op::*;
use engine::*;


#[derive(Debug)]
pub struct Compact<T, U> {
    data: BufferRef,
    select: BufferRef,
    t: PhantomData<T>,
    u: PhantomData<U>,
}

impl<'a, T: GenericVec<T> + 'a, U: GenericIntVec<U>> Compact<T, U> {
    pub fn boxed(data: BufferRef, select: BufferRef) -> BoxedOperator<'a> {
        Box::new(Compact::<T, U> { data, select, t: PhantomData, u: PhantomData })
    }
}

impl<'a, T: GenericVec<T> + 'a, U: GenericIntVec<U>> VecOperator<'a> for Compact<T, U> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut data = scratchpad.get_mut::<T>(self.data);
        let select = scratchpad.get::<U>(self.select);
        // Remove all unmodified entries
        let mut j = 0;
        for (i, &s) in select.iter().take(data.len()).enumerate() {
            if s > U::zero() {
                data[j] = data[i].clone();
                j += 1;
            }
        }
        data.truncate(j);
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.data, self.select] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.data] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{} > 0]", self.data, self.select)
    }
}

