use crate::engine::{BufferRef, Nullable, QueryPlanner, TypedBufferRef};

#[derive(Clone, Copy, Default)]
pub enum Filter {
    #[default]
    None,
    Null,
    U8(BufferRef<u8>),
    NullableU8(BufferRef<Nullable<u8>>),
    Indices(BufferRef<usize>),
}


impl Filter {
    pub fn apply_filter(self, planner: &mut QueryPlanner, plan: TypedBufferRef) -> TypedBufferRef {
        match self {
            Filter::U8(filter) => planner.filter(plan, filter),
            Filter::NullableU8(filter) => planner.nullable_filter(plan, filter),
            Filter::Indices(indices) => planner.select(plan, indices),
            Filter::Null => planner.empty(plan.tag),
            Filter::None => plan,
        }
    }
}