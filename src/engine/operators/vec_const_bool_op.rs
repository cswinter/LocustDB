use super::binary_operator::*;


pub struct LessThan;

pub struct NotEquals;

pub struct Equals;


impl<T: Into<i64> + Copy> BinaryOp<T, i64, u8> for LessThan {
    #[inline]
    fn perform(l: T, r: i64) -> u8 { (Into::<i64>::into(l) < r) as u8 }
    fn symbol() -> &'static str { "<" }
}

impl<'a> BinaryOp<&'a str, &'a str, u8> for LessThan {
    #[inline]
    fn perform(l: &'a str, r: &'a str) -> u8 { (l < r) as u8 }
    fn symbol() -> &'static str { "=" }
}


impl<T: Into<i64> + Copy> BinaryOp<T, i64, u8> for Equals {
    #[inline]
    fn perform(l: T, r: i64) -> u8 { (Into::<i64>::into(l) == r) as u8 }
    fn symbol() -> &'static str { "=" }
}

impl<'a> BinaryOp<&'a str, &'a str, u8> for Equals {
    #[inline]
    fn perform(l: &'a str, r: &'a str) -> u8 { (l == r) as u8 }
    fn symbol() -> &'static str { "=" }
}


impl<T: Into<i64> + Copy> BinaryOp<T, i64, u8> for NotEquals {
    #[inline]
    fn perform(l: T, r: i64) -> u8 { (Into::<i64>::into(l) != r) as u8 }
    fn symbol() -> &'static str { "<>" }
}

impl<'a> BinaryOp<&'a str, &'a str, u8> for NotEquals {
    #[inline]
    fn perform(l: &'a str, r: &'a str) -> u8 { (l != r) as u8 }
    fn symbol() -> &'static str { "<>" }
}
