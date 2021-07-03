pub mod codec;
pub mod column;
pub mod column_builder;
pub mod integers;
pub(crate) mod lru;
#[cfg(feature = "enable_lz4")]
pub mod lz4;
mod mixed_column;
pub mod partition;
pub mod raw_col;
pub mod strings;
pub mod table;
pub mod tree;
pub mod value;

pub use self::codec::{Codec, CodecOp};
pub use self::column::{Column, DataSection, DataSource};
pub use self::lru::Lru;
pub use self::table::TableStats;
pub use self::tree::*;
pub use self::value::Val;

#[cfg(not(feature = "enable_lz4"))]
pub mod lz4 {
    use std::fmt::Debug;

    pub fn encode<T: Debug>(_: &[T]) -> Vec<u8> {
        panic!("lz4 not supported in this build of LocustDB. Recompile with --features enable_lz4.")
    }
}
