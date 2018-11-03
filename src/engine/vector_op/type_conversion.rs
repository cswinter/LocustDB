use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct TypeConversionOperator<T, U> {
    pub input: BufferRef<T>,
    pub output: BufferRef<U>,
}

impl<'a, T: 'a, U: 'a> VecOperator<'a> for TypeConversionOperator<T, U> where
    T: GenericVec<T> + Copy, U: GenericVec<U>, T: Cast<U> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<T>(self.input);
        let mut output = scratchpad.get_mut::<U>(self.output);
        if stream { output.clear() }
        for d in data.iter() {
            output.push(d.cast());
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
        format!("{} as {:?}", self.input, U::t())
    }
}


pub trait Cast<T> {
    fn cast(self) -> T;
}

impl Cast<u8> for u16 { fn cast(self) -> u8 { self as u8 } }

impl Cast<u8> for u32 { fn cast(self) -> u8 { self as u8 } }

impl Cast<u8> for i64 { fn cast(self) -> u8 { self as u8 } }

impl Cast<u16> for u8 { fn cast(self) -> u16 { u16::from(self) } }

impl Cast<u16> for u32 { fn cast(self) -> u16 { self as u16 } }

impl Cast<u16> for i64 { fn cast(self) -> u16 { self as u16 } }

impl Cast<u32> for u8 { fn cast(self) -> u32 { u32::from(self) } }

impl Cast<u32> for u16 { fn cast(self) -> u32 { u32::from(self) } }

impl Cast<u32> for i64 { fn cast(self) -> u32 { self as u32 } }

impl Cast<i64> for u8 { fn cast(self) -> i64 { i64::from(self) } }

impl Cast<i64> for u16 { fn cast(self) -> i64 { i64::from(self) } }

impl Cast<i64> for u32 { fn cast(self) -> i64 { i64::from(self) } }
