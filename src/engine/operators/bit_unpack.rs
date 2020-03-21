use crate::engine::*;


#[derive(Debug)]
pub struct BitUnpackOperator {
    pub input: BufferRef<i64>,
    pub output: BufferRef<i64>,
    pub shift: u8,
    pub width: u8,
}

impl<'a> VecOperator<'a> for BitUnpackOperator {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError>{
        let data = scratchpad.get(self.input);
        let mut unpacked = scratchpad.get_mut(self.output);
        if stream { unpacked.clear(); }
        let mask = (1 << self.width) - 1;
        for d in data.iter() {
            unpacked.push((d >> self.shift) & mask);
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::<i64>::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, alternate: bool) -> String {
        if alternate {
            let mask = (1 << self.width) - 1;
            format!("({} >> {}) & {:x}", self.input, self.shift, mask)
        } else {
            format!("({} >> $shift) & $mask", self.input)
        }
    }
}
