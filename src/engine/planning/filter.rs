use crate::engine::{BufferRef, Nullable};

#[derive(Clone, Copy, Default)]
pub enum Filter {
    #[default]
    None,
    U8(BufferRef<u8>),
    NullableU8(BufferRef<Nullable<u8>>),
    Indices(BufferRef<usize>),
}
