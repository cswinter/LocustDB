use super::binary_operator::*;

use num::PrimInt;

use crate::engine::data_types::GenericIntVec;


pub struct LessThan;

pub struct LessThanEquals;

pub struct NotEquals;

pub struct Equals;


impl<T, U, V> BinaryOp<T, U, u8> for LessThan
    where T: Widen<U, Join=V>, V: PrimInt, T: GenericIntVec<T> {
    fn perform(t: T, u: U) -> u8 {
        let (t, u) = t.widen(u);
        (t < u) as u8
    }
    fn symbol() -> &'static str { "<" }
}

impl<'a> BinaryOp<&'a str, &'a str, u8> for LessThan {
    #[inline]
    fn perform(l: &'a str, r: &'a str) -> u8 { (l < r) as u8 }
    fn symbol() -> &'static str { "<" }
}


impl<T, U, V> BinaryOp<T, U, u8> for LessThanEquals
    where T: Widen<U, Join=V>, V: PrimInt, T: GenericIntVec<T> {
    fn perform(t: T, u: U) -> u8 {
        let (t, u) = t.widen(u);
        (t <= u) as u8
    }
    fn symbol() -> &'static str { "<=" }
}

impl<'a> BinaryOp<&'a str, &'a str, u8> for LessThanEquals {
    #[inline]
    fn perform(l: &'a str, r: &'a str) -> u8 { (l <= r) as u8 }
    fn symbol() -> &'static str { "<=" }
}

impl<T, U, V> BinaryOp<T, U, u8> for Equals
    where T: Widen<U, Join=V>, V: PrimInt, T: GenericIntVec<T> {
    fn perform(t: T, u: U) -> u8 {
        let (t, u) = t.widen(u);
        (t == u) as u8
    }
    fn symbol() -> &'static str { "=" }
}

impl<'a> BinaryOp<&'a str, &'a str, u8> for Equals {
    #[inline]
    fn perform(l: &'a str, r: &'a str) -> u8 { (l == r) as u8 }
    fn symbol() -> &'static str { "=" }
}


impl<T, U, V> BinaryOp<T, U, u8> for NotEquals
    where T: Widen<U, Join=V>, V: PrimInt, T: GenericIntVec<T> {
    fn perform(t: T, u: U) -> u8 {
        let (t, u) = t.widen(u);
        (t != u) as u8
    }
    fn symbol() -> &'static str { "<>" }
}

impl<'a> BinaryOp<&'a str, &'a str, u8> for NotEquals {
    #[inline]
    fn perform(l: &'a str, r: &'a str) -> u8 { (l != r) as u8 }
    fn symbol() -> &'static str { "<>" }
}


pub trait Widen<T> {
    type Join: PrimInt;
    fn widen(self, u: T) -> (Self::Join, Self::Join);
}

impl<T: PrimInt> Widen<T> for T {
    type Join = T;
    fn widen(self, u: T) -> (T, T) { (self, u) }
}

impl Widen<u16> for u8 {
    type Join = u16;
    fn widen(self, u: u16) -> (u16, u16) { (self as u16, u) }
}

impl Widen<u32> for u8 {
    type Join = u32;
    fn widen(self, u: u32) -> (u32, u32) { (self as u32, u) }
}

impl Widen<i64> for u8 {
    type Join = i64;
    fn widen(self, u: i64) -> (i64, i64) { (self as i64, u) }
}

impl Widen<u8> for u16 {
    type Join = u16;
    fn widen(self, u: u8) -> (u16, u16) { (self, u as u16) }
}

impl Widen<u32> for u16 {
    type Join = u32;
    fn widen(self, u: u32) -> (u32, u32) { (self as u32, u) }
}

impl Widen<i64> for u16 {
    type Join = i64;
    fn widen(self, u: i64) -> (i64, i64) { (self as i64, u) }
}

impl Widen<u8> for u32 {
    type Join = u32;
    fn widen(self, u: u8) -> (u32, u32) { (self, u as u32) }
}

impl Widen<u16> for u32 {
    type Join = u32;
    fn widen(self, u: u16) -> (u32, u32) { (self, u as u32) }
}

impl Widen<i64> for u32 {
    type Join = i64;
    fn widen(self, u: i64) -> (i64, i64) { (self as i64, u) }
}

impl Widen<u8> for i64 {
    type Join = i64;
    fn widen(self, u: u8) -> (i64, i64) { (self, u as i64) }
}

impl Widen<u16> for i64 {
    type Join = i64;
    fn widen(self, u: u16) -> (i64, i64) { (self, u as i64) }
}

impl Widen<u32> for i64 {
    type Join = i64;
    fn widen(self, u: u32) -> (i64, i64) { (self, u as i64) }
}

