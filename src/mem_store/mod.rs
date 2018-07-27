pub mod column;
pub mod codec;
pub mod partition;
pub mod value;
pub mod column_builder;
pub mod table;
pub mod raw_col;
pub mod integers;
pub mod strings;
#[cfg(feature = "enable_lz4")]
pub mod lz4;
mod mixed_column;

pub use self::column::{Column, DataSection};
pub use self::codec::{Codec, CodecOp};


#[cfg(not(feature = "enable_lz4"))]
pub mod lz4 {
    use std::fmt::Debug;

    pub unsafe fn encode<T: Debug>(_: &[T]) -> Vec<u8> {
        panic!("lz4 not supported in this build of LocustDB. Recompile with --features enable_lz4.")
    }
}
