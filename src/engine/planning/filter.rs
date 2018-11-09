use engine::BufferRef;

#[derive(Clone, Copy)]
pub enum Filter {
    None,
    BitVec(BufferRef<u8>),
    Indices(BufferRef<usize>),
}

impl Default for Filter {
    fn default() -> Filter { Filter::None }
}