pub mod double;
pub mod single;

#[derive(Debug, PartialEq)]
pub enum Error {
    Eof,
}

// Special NaN value that we use to represent NULLs in the data.
pub const NULL: f64 = unsafe { std::mem::transmute::<u64, f64>(0x7ffa_aaaa_aaaa_aaaau64) };