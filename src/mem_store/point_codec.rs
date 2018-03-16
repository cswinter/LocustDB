use engine::typed_vec::TypedVec;
use ingest::raw_val::RawVal;


pub trait PointCodec<T> {
    fn decode(&self, data: &[T]) -> TypedVec;
    fn index_decode(&self, data: &[T], indices: &[usize]) -> TypedVec;
    fn to_raw(&self, elem: T) -> RawVal;
    fn is_order_preserving(&self) -> bool;
}
