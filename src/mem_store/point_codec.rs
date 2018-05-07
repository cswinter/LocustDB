use engine::typed_vec::BoxedVec;
use ingest::raw_val::RawVal;


pub trait PointCodec<T>: Sync + Send {
    fn decode(&self, data: &[T]) -> BoxedVec;
    fn index_decode(&self, data: &[T], indices: &[usize]) -> BoxedVec;
    fn to_raw(&self, elem: T) -> RawVal;
    fn max_cardinality(&self) -> usize;
}
