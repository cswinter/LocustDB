use std::fmt::Debug;
use std::hash::Hash;
use std::mem;

use engine::typed_vec::TypedVec;
use ingest::raw_val::RawVal;
use mem_store::point_codec::PointCodec;
use num::PrimInt;


pub trait VecType<T>: PartialEq + PartialOrd + Copy + Debug {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [T] where T: 'a;
    fn wrap<'a>(data: Vec<T>) -> TypedVec<'a> where T: 'a;
}

impl VecType<u8> for u8 {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [u8] where u8: 'a { vec.cast_ref_u8().0 }
    fn wrap<'a>(data: Vec<u8>) -> TypedVec<'a> { TypedVec::EncodedU8(data, &IdentityCodec) }
}

impl VecType<u16> for u16 {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [u16] where u16: 'a { vec.cast_ref_u16().0 }
    fn wrap<'a>(data: Vec<u16>) -> TypedVec<'a> { TypedVec::EncodedU16(data, &IdentityCodec) }
}

impl VecType<u32> for u32 {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [u32] where u32: 'a { vec.cast_ref_u32().0 }
    fn wrap<'a>(data: Vec<u32>) -> TypedVec<'a> { TypedVec::EncodedU32(data, &IdentityCodec) }
}

impl VecType<i64> for i64 {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [i64] where i64: 'a { vec.cast_ref_i64() }
    fn wrap<'a>(data: Vec<i64>) -> TypedVec<'a> { TypedVec::Integer(data) }
}

impl<'c> VecType<&'c str> for &'c str {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [&'c str] where &'c str: 'a {
        // TODO(clemens): No idea whether this is even correct, but after many hours I haven't found any other way of making all of this work :(
        // Might require associated type constructors to solve easily...
        unsafe {
            mem::transmute::<_, &'b [&'c str]>(vec.cast_ref_str())
        }
    }
    fn wrap<'a>(data: Vec<&'c str>) -> TypedVec<'a> {
        unsafe {
            mem::transmute::<_, TypedVec<'a>>(TypedVec::String(data))
        }
    }
}


pub trait IntVecType<T>: VecType<T> + Into<i64> + PrimInt + Copy + Hash + 'static {}

impl<T> IntVecType<T> for T where T: VecType<T> + Into<i64> + PrimInt + Copy + Hash + 'static {}

pub trait ConstType<T> {
    fn unwrap(vec: &TypedVec) -> T;
}

impl ConstType<i64> for i64 {
    fn unwrap(vec: &TypedVec) -> i64 { vec.cast_int_const() }
}

impl ConstType<String> for String {
    fn unwrap(vec: &TypedVec) -> String { vec.cast_str_const() }
}


pub trait IntoUsize {
    fn cast_usize(&self) -> usize;
}

impl IntoUsize for u8 {
    fn cast_usize(&self) -> usize { *self as usize }
}

impl IntoUsize for u16 {
    fn cast_usize(&self) -> usize { *self as usize }
}

impl IntoUsize for u32 {
    fn cast_usize(&self) -> usize { *self as usize }
}

impl IntoUsize for i64 {
    fn cast_usize(&self) -> usize { *self as usize }
}


pub struct IdentityCodec;

impl<T: Send + Sync> PointCodec<T> for IdentityCodec {
    fn decode(&self, _data: &[T]) -> TypedVec { unimplemented!() }
    fn index_decode(&self, _data: &[T], _indices: &[usize]) -> TypedVec { unimplemented!() }
    fn to_raw(&self, _elem: T) -> RawVal { unimplemented!() }
    fn max_cardinality(&self) -> usize { unimplemented!() }
}
