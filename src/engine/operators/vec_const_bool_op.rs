use engine::*;
use std::fmt;
use std::marker::PhantomData;


#[derive(Debug)]
pub struct VecConstBoolOperator<T, U, Op> {
    pub lhs: BufferRef<T>,
    pub rhs: BufferRef<U>,
    pub output: BufferRef<u8>,
    pub op: PhantomData<Op>,
}

impl<'a, T: 'a, U, Op> VecOperator<'a> for VecConstBoolOperator<T, U, Op> where
    T: GenericVec<T>, U: ConstType<U> + fmt::Debug, Op: BoolOperation<T, U> + fmt::Debug {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get(self.lhs);
        let c = &scratchpad.get_const::<U>(&self.rhs);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear(); }
        for d in data.iter() {
            output.push(Op::perform(d, &c));
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.lhs.any(), self.rhs.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{} {} {}", self.lhs, Op::symbol(), self.rhs)
    }
}

pub trait BoolOperation<T, U> {
    fn perform(lhs: &T, rhs: &U) -> u8;
    fn symbol() -> &'static str;
}

#[derive(Debug)]
pub struct LessThanInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for LessThanInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> u8 { (Into::<i64>::into(*l) < *r) as u8 }
    fn symbol() -> &'static str { "<" }
}

#[derive(Debug)]
pub struct EqualsInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for EqualsInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> u8 { (Into::<i64>::into(*l) == *r) as u8 }
    fn symbol() -> &'static str { "==" }
}

#[derive(Debug)]
pub struct EqualsString;

impl<'a> BoolOperation<&'a str, String> for EqualsString {
    #[inline]
    fn perform(l: &&'a str, r: &String) -> u8 { (l == r) as u8 }
    fn symbol() -> &'static str { "==" }
}


#[derive(Debug)]
pub struct NotEqualsInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for NotEqualsInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> u8 { (Into::<i64>::into(*l) != *r) as u8 }
    fn symbol() -> &'static str { "<>" }
}

#[derive(Debug)]
pub struct NotEqualsString;

impl<'a> BoolOperation<&'a str, String> for NotEqualsString {
    #[inline]
    fn perform(l: &&'a str, r: &String) -> u8 { (l != r) as u8 }
    fn symbol() -> &'static str { "<>" }
}
