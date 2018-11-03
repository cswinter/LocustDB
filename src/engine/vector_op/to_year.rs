use std::i64;

use chrono::{NaiveDateTime, Datelike};

use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct ToYear {
    pub input: BufferRef<i64>,
    pub output: BufferRef<i64>,
}

impl<'a> VecOperator<'a> for ToYear {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let timestamps = scratchpad.get::<i64>(self.input);
        let mut years = scratchpad.get_mut::<i64>(self.output);
        if stream { years.clear() }
        for ts in timestamps.iter() {
            years.push(i64::from(NaiveDateTime::from_timestamp(*ts, 0).year()));
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("to_year({})", self.input)
    }
}

