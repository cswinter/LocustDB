
// BitReader and BitWriter are adapted from https://github.com/jeromefroe/tsz-rs/tree/b3e2dce64707c42c10019d9159f88a0f594458af
mod bit_reader;
mod bit_writer;

pub(crate) use bit_reader::BitReader;
pub(crate) use bit_writer::BitWriter;

pub mod xor_float;
pub mod column;
pub mod test_data;