use engine::*;


#[derive(Debug)]
pub struct Compact<T, U> {
    data: BufferRef<T>,
    select: BufferRef<U>,
}

impl<'a, T: GenericVec<T> + 'a, U: GenericIntVec<U>> Compact<T, U> {
    pub fn boxed(data: BufferRef<T>, select: BufferRef<U>) -> BoxedOperator<'a> {
        Box::new(Compact { data, select })
    }
}

impl<'a, T: GenericVec<T> + 'a, U: GenericIntVec<U>> VecOperator<'a> for Compact<T, U> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut data = scratchpad.get_mut(self.data);
        let select = scratchpad.get(self.select);
        // Remove all unmodified entries
        let mut j = 0;
        for (i, &s) in select.iter().take(data.len()).enumerate() {
            if s > U::zero() {
                data[j] = data[i];
                j += 1;
            }
        }
        data.truncate(j);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.data.any(), self.select.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.data.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{} > 0]", self.data, self.select)
    }
}

