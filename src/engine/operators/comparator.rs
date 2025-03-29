use std::cmp::Ordering;

use ordered_float::OrderedFloat;

use crate::mem_store::Val;

pub trait Comparator<T> {
    // Compare two values. If values are equal, return false. Otherwise, total order is defined by the return value.
    fn cmp(left: T, right: T) -> bool;
    // Compare two values. If values are equal, return true. Otherwise, total order is defined by the return value.
    fn cmp_eq(left: T, right: T) -> bool;
    fn ordering(left: T, right: T) -> Ordering;
    fn is_less_than() -> bool;
}

#[derive(Debug)]
pub struct CmpLessThan;

// Blanket implementation of Asc for PrimInt doesn't work because of trait coherence rules (upstream could implement `PrimInt` for &str).
impl Comparator<u8> for CmpLessThan {
    fn cmp(left: u8, right: u8) -> bool {
        left < right
    }
    fn cmp_eq(left: u8, right: u8) -> bool {
        left <= right
    }
    fn ordering(left: u8, right: u8) -> Ordering {
        left.cmp(&right)
    }
    fn is_less_than() -> bool {
        true
    }
}

impl Comparator<u16> for CmpLessThan {
    fn cmp(left: u16, right: u16) -> bool {
        left < right
    }
    fn cmp_eq(left: u16, right: u16) -> bool {
        left <= right
    } 
    fn ordering(left: u16, right: u16) -> Ordering {
        left.cmp(&right)
    }
    fn is_less_than() -> bool {
        true
    }
}

impl Comparator<u32> for CmpLessThan {
    fn cmp(left: u32, right: u32) -> bool {
        left < right
    }
    fn cmp_eq(left: u32, right: u32) -> bool {
        left <= right
    }
    fn ordering(left: u32, right: u32) -> Ordering {
        left.cmp(&right)
    }
    fn is_less_than() -> bool {
        true
    }
}

impl Comparator<i64> for CmpLessThan {
    fn cmp(left: i64, right: i64) -> bool {
        left < right
    }
    fn cmp_eq(left: i64, right: i64) -> bool {
        left <= right
    }
    fn ordering(left: i64, right: i64) -> Ordering {
        left.cmp(&right)
    }
    fn is_less_than() -> bool {
        true
    }
}

impl Comparator<u64> for CmpLessThan {
    fn cmp(left: u64, right: u64) -> bool {
        left < right
    }
    fn cmp_eq(left: u64, right: u64) -> bool {
        left <= right
    }
    fn ordering(left: u64, right: u64) -> Ordering {
        left.cmp(&right)
    }
    fn is_less_than() -> bool {
        true
    }
}

impl Comparator<OrderedFloat<f64>> for CmpLessThan {
    fn cmp(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> bool {
        left < right
    }
    fn cmp_eq(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> bool {
        left <= right
    }
    fn ordering(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> Ordering {
        left.cmp(&right)
    }
    fn is_less_than() -> bool {
        true
    }
}

impl Comparator<&'_ str> for CmpLessThan {
    fn cmp(left: &str, right: &str) -> bool {
        left < right
    }
    fn cmp_eq(left: &str, right: &str) -> bool {
        left <= right
    }
    fn ordering(left: &str, right: &str) -> Ordering {
        left.cmp(right)
    }
    fn is_less_than() -> bool {
        true
    }
}

impl<'a> Comparator<Option<&'a str>> for CmpLessThan {
    fn cmp(left: Option<&str>, right: Option<&str>) -> bool {
        match (left, right) {
            (Some(l), Some(r)) => l < r,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => false,
        }
    }
    fn cmp_eq(left: Option<&str>, right: Option<&str>) -> bool {
        match (left, right) {
            (Some(l), Some(r)) => l <= r,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => true,
        }
    }
    fn ordering(left: Option<&'a str>, right: Option<&'a str>) -> Ordering {
        match (left, right) {
            (Some(l), Some(r)) => l.cmp(r),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        }
    }
    fn is_less_than() -> bool {
        true
    }
}

// Bool < Integer < Str < Float < Null
impl<'a> Comparator<Val<'a>> for CmpLessThan {
    fn cmp(left: Val<'a>, right: Val<'a>) -> bool {
        match CmpLessThan::ordering(left, right) {
            Ordering::Less => true,
            Ordering::Equal => false,
            Ordering::Greater => false,
        }
    }

    fn cmp_eq(left: Val<'a>, right: Val<'a>) -> bool {
        match CmpLessThan::ordering(left, right) {
            Ordering::Less => true,
            Ordering::Equal => true,
            Ordering::Greater => false,
        }
    }

    fn ordering(left: Val<'a>, right: Val<'a>) -> Ordering {
        match (left, right) {
            (Val::Bool(l), Val::Bool(r)) => l.cmp(&r),
            (Val::Bool(_), _) => Ordering::Less,
            (_, Val::Bool(_)) => Ordering::Greater,
            (Val::Integer(l), Val::Integer(r)) => l.cmp(&r),
            (Val::Integer(_), _) => Ordering::Less,
            (_, Val::Integer(_)) => Ordering::Greater,
            (Val::Str(l), Val::Str(r)) => l.cmp(r),
            (Val::Str(_), _) => Ordering::Less,
            (_, Val::Str(_)) => Ordering::Greater,
            (Val::Float(l), Val::Float(r)) => l.cmp(&r),
            (Val::Float(_), _) => Ordering::Less,
            (_, Val::Float(_)) => Ordering::Greater,
            (Val::Null, Val::Null) => Ordering::Equal,
        }
    }

    fn is_less_than() -> bool {
        true
    }
}

#[derive(Debug)]
pub struct CmpGreaterThan;

impl Comparator<u8> for CmpGreaterThan {
    fn cmp(left: u8, right: u8) -> bool {
        left > right
    }
    fn cmp_eq(left: u8, right: u8) -> bool {
        left >= right
    }
    fn ordering(left: u8, right: u8) -> Ordering {
        right.cmp(&left)
    }
    fn is_less_than() -> bool {
        false
    }
}

impl Comparator<u16> for CmpGreaterThan {
    fn cmp(left: u16, right: u16) -> bool {
        left > right
    }
    fn cmp_eq(left: u16, right: u16) -> bool {
        left >= right
    }
    fn ordering(left: u16, right: u16) -> Ordering {
        right.cmp(&left)
    }
    fn is_less_than() -> bool {
        false
    }
}

impl Comparator<u32> for CmpGreaterThan {
    fn cmp(left: u32, right: u32) -> bool {
        left > right
    }
    fn cmp_eq(left: u32, right: u32) -> bool {
        left >= right
    }
    fn ordering(left: u32, right: u32) -> Ordering {
        right.cmp(&left)
    }
    fn is_less_than() -> bool {
        false
    }
}

impl Comparator<u64> for CmpGreaterThan {
    fn cmp(left: u64, right: u64) -> bool {
        left > right
    }
    fn cmp_eq(left: u64, right: u64) -> bool {
        left >= right
    }
    fn ordering(left: u64, right: u64) -> Ordering {
        right.cmp(&left)
    }
    fn is_less_than() -> bool {
        false
    }
}

impl Comparator<i64> for CmpGreaterThan {
    fn cmp(left: i64, right: i64) -> bool {
        left > right
    }
    fn cmp_eq(left: i64, right: i64) -> bool {
        left >= right
    }
    fn ordering(left: i64, right: i64) -> Ordering {
        right.cmp(&left)
    }
    fn is_less_than() -> bool {
        false
    }
}

impl Comparator<OrderedFloat<f64>> for CmpGreaterThan {
    fn cmp(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> bool {
        left > right
    }
    fn cmp_eq(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> bool {
        left >= right
    }
    fn ordering(left: OrderedFloat<f64>, right: OrderedFloat<f64>) -> Ordering {
        right.cmp(&left)
    }
    fn is_less_than() -> bool {
        false
    }
}

impl Comparator<Option<OrderedFloat<f64>>> for CmpGreaterThan {
    fn cmp(left: Option<OrderedFloat<f64>>, right: Option<OrderedFloat<f64>>) -> bool {
        left > right
    }
    fn cmp_eq(left: Option<OrderedFloat<f64>>, right: Option<OrderedFloat<f64>>) -> bool {
        left >= right
    }
    fn ordering(left: Option<OrderedFloat<f64>>, right: Option<OrderedFloat<f64>>) -> Ordering {
        right.cmp(&left)
    }
    fn is_less_than() -> bool {
        false
    }
}

impl Comparator<&str> for CmpGreaterThan {
    fn cmp(left: &str, right: &str) -> bool {
        left > right
    }
    fn cmp_eq(left: &str, right: &str) -> bool {
        left >= right
    }
    fn ordering(left: &str, right: &str) -> Ordering {
        right.cmp(left)
    }
    fn is_less_than() -> bool {
        false
    }
}

impl<'a> Comparator<Option<&'a str>> for CmpGreaterThan {
    fn cmp(left: Option<&str>, right: Option<&str>) -> bool {
        match (left, right) {
            (Some(l), Some(r)) => l > r,
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (None, None) => false,
        }
    }
    fn cmp_eq(left: Option<&str>, right: Option<&str>) -> bool {
        match (left, right) {
            (Some(l), Some(r)) => l >= r,
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (None, None) => true,
        }
    }
    fn ordering(left: Option<&'a str>, right: Option<&'a str>) -> Ordering {
        match (left, right) {
            (Some(l), Some(r)) => l.cmp(r),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }
    fn is_less_than() -> bool {
        false
    }
}

// Bool < Integer < Str < Float < Null
impl<'a> Comparator<Val<'a>> for CmpGreaterThan {
    fn cmp(left: Val<'a>, right: Val<'a>) -> bool {
        match CmpGreaterThan::ordering(left, right) {
            // This is intentional, greater than flips default less than ordering
            Ordering::Less => true,
            Ordering::Equal => false,
            Ordering::Greater => false,
        }
    }

    fn cmp_eq(left: Val<'a>, right: Val<'a>) -> bool {
        match CmpGreaterThan::ordering(left, right) {
            // This is intentional, greater than flips default less than ordering
            Ordering::Less => true,
            Ordering::Equal => true,
            Ordering::Greater => false,
        }
    }

    fn ordering(left: Val<'a>, right: Val<'a>) -> Ordering {
        match (left, right) {
            (Val::Bool(l), Val::Bool(r)) => r.cmp(&l),
            (Val::Bool(_), _) => Ordering::Greater,
            (_, Val::Bool(_)) => Ordering::Less,
            (Val::Integer(l), Val::Integer(r)) => r.cmp(&l),
            (Val::Integer(_), _) => Ordering::Greater,
            (_, Val::Integer(_)) => Ordering::Less,
            (Val::Str(l), Val::Str(r)) => r.cmp(l),
            (Val::Str(_), _) => Ordering::Greater,
            (_, Val::Str(_)) => Ordering::Less,
            (Val::Float(l), Val::Float(r)) => r.cmp(&l),
            (Val::Float(_), _) => Ordering::Greater,
            (_, Val::Float(_)) => Ordering::Less,
            (Val::Null, Val::Null) => Ordering::Equal,
        }
    }

    fn is_less_than() -> bool {
        false
    }
}
