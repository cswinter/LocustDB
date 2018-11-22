use engine::BufferRef;

#[derive(Clone, Copy)]
pub enum Filter {
    None,
    U8(BufferRef<u8>),
    Indices(BufferRef<usize>),
}

impl Default for Filter {
    fn default() -> Filter { Filter::None }
}