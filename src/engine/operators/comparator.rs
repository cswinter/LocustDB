use ordered_float::OrderedFloat;

use crate::mem_store::Val;

pub trait Comparator<T> {
    fn cmp(left: T, right: T) -> bool;
    fn cmp_eq(left: T, right: T) -> bool;
    fn is_less_than() -> bool;
}


#[derive(Debug)]
pub struct CmpLessThan;

// Blanket implementation of Asc for PrimInt doesn't work because of trait coherence rules (upstream could implement `PrimInt` for &str).
impl Comparator<u8> for CmpLessThan {
    fn cmp(left: u8, right: u8) -> bool { left < right }
    fn cmp_eq(left: u8, right: u8) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

impl Comparator<u16> for CmpLessThan {
    fn cmp(left: u16, right: u16) -> bool { left < right }
    fn cmp_eq(left: u16, right: u16) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

impl Comparator<u32> for CmpLessThan {
    fn cmp(left: u32, right: u32) -> bool { left < right }
    fn cmp_eq(left: u32, right: u32) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

impl Comparator<i64> for CmpLessThan {
    fn cmp(left: i64, right: i64) -> bool { left < right }
    fn cmp_eq(left: i64, right: i64) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

impl Comparator<u64> for CmpLessThan {
    fn cmp(left: u64, right: u64) -> bool { left < right }
    fn cmp_eq(left: u64, right: u64) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

impl Comparator<OrderedFloat<f64>> for CmpLessThan {
    fn cmp(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> bool { left < right }
    fn cmp_eq(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

impl Comparator<Option<OrderedFloat<f64>> > for CmpLessThan {
    fn cmp(left: Option<OrderedFloat<f64>>, right: Option<OrderedFloat<f64>>) -> bool { left < right }
    fn cmp_eq(left: Option<OrderedFloat<f64>>, right: Option<OrderedFloat<f64>>) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

impl<'a> Comparator<&'a str> for CmpLessThan {
    fn cmp(left: &str, right: &str) -> bool { left < right }
    fn cmp_eq(left: &str, right: &str) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

impl<'a> Comparator<Option<&'a str>> for CmpLessThan {
    fn cmp(left: Option<&str>, right: Option<&str>) -> bool { left < right }
    fn cmp_eq(left: Option<&str>, right: Option<&str>) -> bool { left <= right }
    fn is_less_than() -> bool { true }
}

// Null < Bool < Integer < Str < Float
impl<'a> Comparator<Val<'a>> for CmpLessThan {
    fn cmp(left: Val<'a>, right: Val<'a>) -> bool {
        match (left, right) {
            (Val::Null, _) => true,
            (_, Val::Null) => false,
            (Val::Bool(l), Val::Bool(r)) => !l & r,
            (Val::Bool(_), _) => true,
            (_, Val::Bool(_)) => false,
            (Val::Integer(l), Val::Integer(r)) => l < r,
            (Val::Integer(_), _) => true,
            (_, Val::Integer(_)) => false,
            (Val::Str(l), Val::Str(r)) => l < r,
            (Val::Str(_), _) => true,
            (_, Val::Str(_)) => false,
            (Val::Float(l), Val::Float(r)) => l < r,
        }
    }

    fn cmp_eq(left: Val<'a>, right: Val<'a>) -> bool {
        match (left, right) {
            (Val::Null, Val::Null) => true,
            (Val::Bool(l), Val::Bool(r)) => l == r,
            (Val::Integer(l), Val::Integer(r)) => l == r,
            (Val::Str(l), Val::Str(r)) => l == r,
            (Val::Float(l), Val::Float(r)) => l == r,
            _ => false,
        }
    }

    fn is_less_than() -> bool { true }
}


#[derive(Debug)]
pub struct CmpGreaterThan;

impl Comparator<u8> for CmpGreaterThan {
    fn cmp(left: u8, right: u8) -> bool { left > right }
    fn cmp_eq(left: u8, right: u8) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl Comparator<u16> for CmpGreaterThan {
    fn cmp(left: u16, right: u16) -> bool { left > right }
    fn cmp_eq(left: u16, right: u16) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl Comparator<u32> for CmpGreaterThan {
    fn cmp(left: u32, right: u32) -> bool { left > right }
    fn cmp_eq(left: u32, right: u32) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl Comparator<u64> for CmpGreaterThan {
    fn cmp(left: u64, right: u64) -> bool { left > right }
    fn cmp_eq(left: u64, right: u64) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl Comparator<i64> for CmpGreaterThan {
    fn cmp(left: i64, right: i64) -> bool { left > right }
    fn cmp_eq(left: i64, right: i64) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl Comparator<OrderedFloat<f64>> for CmpGreaterThan {
    fn cmp(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> bool { left > right }
    fn cmp_eq(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl Comparator<Option<OrderedFloat<f64>> > for CmpGreaterThan {
    fn cmp(left: Option<OrderedFloat<f64>>, right: Option<OrderedFloat<f64>>) -> bool { left > right }
    fn cmp_eq(left: Option<OrderedFloat<f64>>, right: Option<OrderedFloat<f64>>) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl<'a> Comparator<&'a str> for CmpGreaterThan {
    fn cmp(left: &str, right: &str) -> bool { left > right }
    fn cmp_eq(left: &str, right: &str) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl<'a> Comparator<Option<&'a str>> for CmpGreaterThan {
    fn cmp(left: Option<&str>, right: Option<&str>) -> bool { left > right }
    fn cmp_eq(left: Option<&str>, right: Option<&str>) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl<'a> Comparator<Val<'a>> for CmpGreaterThan {
    fn cmp(left: Val<'a>, right: Val<'a>) -> bool {
        match (left, right) {
            (Val::Null, _) => false,
            (_, Val::Null) => true,
            (Val::Bool(l), Val::Bool(r)) => l & !r,
            (Val::Bool(_), _) => false,
            (_, Val::Bool(_)) => true,
            (Val::Integer(l), Val::Integer(r)) => l > r,
            (Val::Integer(_), _) => false,
            (_, Val::Integer(_)) => true,
            (Val::Str(l), Val::Str(r)) => l > r,
            (Val::Str(_), _) => false,
            (_, Val::Str(_)) => true,
            (Val::Float(l), Val::Float(r)) => l > r,
        }
    }

    fn cmp_eq(left: Val<'a>, right: Val<'a>) -> bool {
        match (left, right) {
            (Val::Null, Val::Null) => true,
            (Val::Bool(l), Val::Bool(r)) => l >= r,
            (Val::Integer(l), Val::Integer(r)) => l >= r,
            (Val::Str(l), Val::Str(r)) => l >= r,
            (Val::Float(l), Val::Float(r)) => l >= r,
            _ => false,
        }
    }

    fn is_less_than() -> bool { false }
}