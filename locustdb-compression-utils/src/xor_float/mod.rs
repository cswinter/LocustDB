pub mod double;
pub mod single;

#[derive(Debug, PartialEq)]
pub enum Error {
    Eof,
}

// Special NaN value that we use to represent NULLs in the data.
// Can't use f64::from_bits because it is not a canonical NaN value.
#[allow(clippy::transmute_int_to_float)]
pub const NULL: f64 = unsafe { std::mem::transmute::<u64, f64>(0x7ffa_aaaa_aaaa_aaaau64) };