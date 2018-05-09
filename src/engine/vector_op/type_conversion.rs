use std::marker::PhantomData;

use engine::*;
use engine::vector_op::vector_operator::*;


#[derive(Debug)]
pub struct TypeConversionOperator<T, U> {
    input: BufferRef,
    output: BufferRef,
    t: PhantomData<T>,
    s: PhantomData<U>,
}

impl<T, U> TypeConversionOperator<T, U> {
    pub fn new(input: BufferRef, output: BufferRef) -> TypeConversionOperator<T, U> {
        TypeConversionOperator {
            input,
            output,
            t: PhantomData,
            s: PhantomData,
        }
    }
}

impl<'a, T: 'a, U: 'a> VecOperator<'a> for TypeConversionOperator<T, U> where
    T: VecType<T> + Copy, U: VecType<U>, T: Cast<U> {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<T>(self.input);
            let mut output = Vec::with_capacity(data.len());
            for d in data.iter() {
                output.push(d.cast());
            }
            TypedVec::owned(output)
        };
        scratchpad.set(self.output, result);
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
