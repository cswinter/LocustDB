use crate::engine::{BufferRef, Nullable};

#[derive(Clone, Copy)]
pub enum Filter {
    None,
    U8(BufferRef<u8>),
    NullableU8(BufferRef<Nullable<u8>>),
    Indices(BufferRef<usize>),
}

impl Default for Filter {
    fn default() -> Filter { Filter::None }
}