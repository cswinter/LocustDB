use engine::*;
use bitvec::*;


pub struct IsNull {
    pub input: BufferRef<Nullable<Any>>,
    pub is_null: BufferRef<u8>,
}

impl<'a> VecOperator<'a> for IsNull {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let len = scratchpad.get_any(self.input.any()).len();
        let present = scratchpad.get_null_map(self.input);
        let mut is_null = scratchpad.get_mut(self.is_null);
        if stream { is_null.clear(); }
        for i in 0..len {
            if (&*present).is_set(i) {
                is_null.push(false as u8);
            } else {
                is_null.push(true as u8);
            }
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.is_null, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.is_null.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("IsNull({})", self.input)
    }
}

pub struct IsNotNull {
    pub input: BufferRef<Nullable<Any>>,
    pub is_not_null: BufferRef<u8>,
}

impl<'a> VecOperator<'a> for IsNotNull {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let len = scratchpad.get_any(self.input.any()).len();
        let present = scratchpad.get_null_map(self.input);
        let mut is_not_null = scratchpad.get_mut(self.is_not_null);
        if stream { is_not_null.clear(); }
        for i in 0..len {
            if (&*present).is_set(i) {
                is_not_null.push(true as u8);
            } else {
                is_not_null.push(false as u8);
            }
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.is_not_null, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.is_not_null.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("IsNotNull({})", self.input)
    }
}

