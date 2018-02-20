use engine::typed_vec::TypedVec;
use mem_store::ingest::RawVal;


pub trait PointCodec<T> {
    fn decode(&self, data: &[T]) -> TypedVec;
    fn to_raw(&self, elem: T) -> RawVal;
    fn is_order_preserving(&self) -> bool;
}
