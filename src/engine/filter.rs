use engine::vector_op::vector_operator::*;

#[derive(Clone, Copy)]
pub enum Filter {
    None,
    BitVec(BufferRef<u8>),
    Indices(BufferRef<usize>),
}

impl Default for Filter {
    fn default() -> Filter { Filter::None }
}