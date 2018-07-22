use chrono::{NaiveDateTime, Datelike};

use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct ToYear {
    pub input: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for ToYear {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let timestamps = scratchpad.get::<i64>(self.input);
        let mut years = scratchpad.get_mut::<i64>(self.output);
        if stream { years.clear() }
        for ts in timestamps.iter() {
            years.push(NaiveDateTime::from_timestamp(*ts, 0).year() as i64);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Box::new(Vec::<i64>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.input] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("to_year({})", self.input)
    }
}

