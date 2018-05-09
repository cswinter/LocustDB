use std::fmt;
use std::marker::PhantomData;

use bit_vec::BitVec;

use engine::*;
use engine::vector_op::vector_operator::*;


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
    T: VecType<T>, U: ConstType<U> + fmt::Debug, Op: BoolOperation<T, U> {
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
    T: VecType<T>, U: ConstType<U> + fmt::Debug, Op: BoolOperation<T, U> + fmt::Debug {
    fn execute(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let data = scratchpad.get::<T>(self.lhs);
            let c = &scratchpad.get_const::<U>(self.rhs);
            let mut output = BitVec::with_capacity(data.len());
            for d in data.iter() {
                output.push(Op::perform(d, &c));
            }
            TypedVec::bit_vec(output)
        };
        scratchpad.set(self.output, result);
    }
}

pub trait BoolOperation<T, U> {
    fn perform(lhs: &T, rhs: &U) -> bool;
}

#[derive(Debug)]
pub struct LessThanInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for LessThanInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> bool { Into::<i64>::into(*l) < *r }
}

#[derive(Debug)]
pub struct Equals<T> { t: PhantomData<T> }

impl<T: PartialEq> BoolOperation<T, T> for Equals<T> {
    #[inline]
    fn perform(l: &T, r: &T) -> bool { l == r }
}

#[derive(Debug)]
pub struct EqualsInt<T> { t: PhantomData<T> }

impl<T: Into<i64> + Copy> BoolOperation<T, i64> for EqualsInt<T> {
    #[inline]
    fn perform(l: &T, r: &i64) -> bool { Into::<i64>::into(*l) == *r }
}

#[derive(Debug)]
pub struct EqualsString;

impl<'a> BoolOperation<&'a str, String> for EqualsString {
    #[inline]
    fn perform(l: &&'a str, r: &String) -> bool { l == r }
}

