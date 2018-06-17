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

impl<'a> Comparator<&'a str> for CmpLessThan {
    fn cmp(left: &str, right: &str) -> bool { left < right }
    fn cmp_eq(left: &str, right: &str) -> bool { left <= right }
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

impl Comparator<i64> for CmpGreaterThan {
    fn cmp(left: i64, right: i64) -> bool { left > right }
    fn cmp_eq(left: i64, right: i64) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

impl<'a> Comparator<&'a str> for CmpGreaterThan {
    fn cmp(left: &str, right: &str) -> bool { left > right }
    fn cmp_eq(left: &str, right: &str) -> bool { left >= right }
    fn is_less_than() -> bool { false }
}

