use engine::vector_op::vector_operator::BufferRef;

#[derive(Clone, Copy)]
pub enum Filter {
    None,
    BitVec(BufferRef),
    Indices(BufferRef),
}

impl Default for Filter {
    fn default() -> Filter { Filter::None }
}