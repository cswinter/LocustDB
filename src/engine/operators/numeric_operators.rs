use num::PrimInt;

use std::marker::PhantomData;

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

impl<LHS: PrimInt, RHS: PrimInt> CheckedBinaryOp<LHS, RHS, i64> for Addition<LHS, RHS> {
    #[inline]
    fn perform_checked(lhs: LHS, rhs: RHS) -> (i64, bool) {
        lhs.to_i64().unwrap().overflowing_add(rhs.to_i64().unwrap())
    }
}

impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Subtraction<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() - rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "-" }
}

impl<LHS: PrimInt, RHS: PrimInt> CheckedBinaryOp<LHS, RHS, i64> for Subtraction<LHS, RHS> {
    #[inline]
    fn perform_checked(lhs: LHS, rhs: RHS) -> (i64, bool) {
        lhs.to_i64().unwrap().overflowing_sub(rhs.to_i64().unwrap())
    }
}

impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Multiplication<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() * rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "*" }
}

impl<LHS: PrimInt, RHS: PrimInt> CheckedBinaryOp<LHS, RHS, i64> for Multiplication<LHS, RHS> {
    #[inline]
    fn perform_checked(lhs: LHS, rhs: RHS) -> (i64, bool) {
        lhs.to_i64().unwrap().overflowing_mul(rhs.to_i64().unwrap())
    }
}

impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Division<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() / rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "/" }
}

impl<LHS: PrimInt, RHS: PrimInt> CheckedBinaryOp<LHS, RHS, i64> for Division<LHS, RHS> {
    #[inline]
    fn perform_checked(lhs: LHS, rhs: RHS) -> (i64, bool) {
        if rhs.to_i64().unwrap() == 0 {
            (1, true)
        } else {
            (lhs.to_i64().unwrap() / rhs.to_i64().unwrap(), false)
        }
    }
}

impl<LHS: PrimInt, RHS: PrimInt> BinaryOp<LHS, RHS, i64> for Modulo<LHS, RHS> {
    #[inline]
    fn perform(lhs: LHS, rhs: RHS) -> i64 {
        lhs.to_i64().unwrap() % rhs.to_i64().unwrap()
    }

    fn symbol() -> &'static str { "%" }
}

impl<LHS: PrimInt, RHS: PrimInt> CheckedBinaryOp<LHS, RHS, i64> for Modulo<LHS, RHS> {
    #[inline]
    fn perform_checked(lhs: LHS, rhs: RHS) -> (i64, bool) {
        if rhs.to_i64().unwrap() == 0 {
            (1, true)
        } else {
            (lhs.to_i64().unwrap() % rhs.to_i64().unwrap(), false)
        }
    }
}
