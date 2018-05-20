use chrono::{NaiveDateTime, Datelike};

use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct ToYear {
    pub input: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for ToYear {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let encoded = scratchpad.get::<i64>(self.input);
            encoded.iter().map(|t| NaiveDateTime::from_timestamp(*t, 0).year() as i64).collect::<Vec<_>>()
        };
        scratchpad.set(self.output, Box::new(result));
    }
}

