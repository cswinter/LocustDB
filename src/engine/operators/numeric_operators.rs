use std::marker::PhantomData;

use num::PrimInt;

use super::binary_operator::*;


pub struct Addition<LHS, RHS> {
    lhs: PhantomData<LHS>,
    rhs: PhantomData<RHS>,
}

pub struct Subtraction<LHS, RHS> {
    lhs: PhantomData<LHS>,
    rhs: PhantomData<RHS>,
}

pub struct Multiplication<LHS, RHS> {
    lhs: PhantomData<LHS>,
    rhs: PhantomData<RHS>,
}

pub struct Division<LHS, RHS> {
    lhs: PhantomData<LHS>,
    rhs: PhantomData<RHS>,
}

pub struct Modulo<LHS, RHS> {
    lhs: PhantomData<LHS>,
    rhs: PhantomData<RHS>,
}


impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Addition<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() + rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "+" }
}

impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Subtraction<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() - rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "-" }
}

impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Multiplication<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() * rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "*" }
}

impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Division<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() / rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "/" }
}

impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Modulo<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() % rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "%" }
}
