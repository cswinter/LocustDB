use engine::*;
use mem_store::value::Val;


#[derive(Debug)]
pub struct ValRowsPack<'a> {
    pub input: BufferRef<Val<'a>>,
    pub output: BufferRef<ValRows<'a>>,
    pub stride: usize,
    pub offset: usize,
}

impl<'a> VecOperator<'a> for ValRowsPack<'a> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError>{
        let data = scratchpad.get(self.input);
        let mut val_rows = scratchpad.get_mut_val_rows(self.output.clone());
        for (i, datum) in data.iter().enumerate() {
            val_rows.data[i * self.stride + self.offset] = *datum;
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        if scratchpad.get_any(self.output.any()).len() == 0 {
            scratchpad.set_any(self.output.any(), Box::new(ValRows {
                row_len: self.stride,
                data: vec![Val::Null; batch_size * self.stride],
            }));
        }
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}, {}, ...] = {}", self.output, self.offset, self.offset + self.stride, self.input)
    }
}
