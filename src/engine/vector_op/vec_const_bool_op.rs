use engine::*;
use engine::vector_op::vector_operator::*;
use std::fmt;
use std::marker::PhantomData;


#[derive(Debug)]
pub struct VecConstBoolOperator<T, U, Op> {
    lhs: BufferRef,
    rhs: BufferRef,
    output: BufferRef,
    t: PhantomData<T>,
    u: PhantomData<U>,
    op: PhantomData<Op>,
}

impl<'a, T: 'a, U, Op> VecConstBoolOperator<T, U, Op> where
    T: GenericVec<T>, U: ConstType<U> + fmt::Debug, Op: BoolOperation<T, U> {
    pub fn new(lhs: BufferRef, rhs: BufferRef, output: BufferRef) -> VecConstBoolOperator<T, U, Op> {
        VecConstBoolOperator {
            lhs,
            rhs,
            output,
            t: PhantomData,
            u: PhantomData,
            op: PhantomData,
        }
    }
}

impl<'a, T: 'a, U, Op> VecOperator<'a> for VecConstBoolOperator<T, U, Op> where
    T: GenericVec<T>, U: ConstType<U> + fmt::Debug, Op: BoolOperation<T, U> + fmt::Debug {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let data = scratchpad.get::<T>(self.lhs);
        let c = &scratchpad.get_const::<U>(self.rhs);
        let mut output = scratchpad.get_mut::<u8>(self.output);
        if stream { output.clear(); }
        for d in data.iter() {
            output.push(Op::perform(d, &c));
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, AnyVec::owned(Vec::<u8>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.lhs, self.rhs] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self) -> bool { true }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
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
pub struct Equals<T> { t: PhantomData<T> }

impl<T: PartialEq> BoolOperation<T, T> for Equals<T> {
    #[inline]
    fn perform(l: &T, r: &T) -> u8 { (l == r) as u8 }
    fn symbol() -> &'static str { "==" }
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
pub struct NotEquals<T> { t: PhantomData<T> }

impl<T: PartialEq> BoolOperation<T, T> for NotEquals<T> {
    #[inline]
    fn perform(l: &T, r: &T) -> u8 { (l != r) as u8 }
    fn symbol() -> &'static str { "<>" }
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
