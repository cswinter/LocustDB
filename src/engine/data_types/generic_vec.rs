use std::cmp::min;
use std::fmt::{Debug, Display, Write};
use std::fmt;
use std::hash::Hash;
use std::i64;
use std::mem;
use std::io::Cursor;

use num::PrimInt;
use byteorder::{NativeEndian, ReadBytesExt};
use heapsize::HeapSizeOf;
use ingest::raw_val::RawVal;
use itertools::Itertools;

use mem_store::value::Val;
use engine::data_types::*;


pub trait GenericVec<T>: PartialEq + Ord + Copy + Debug + Display + Sync + Send + HeapSizeOf {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [T] where T: 'a;
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<T> where T: 'a;
    fn wrap_one(_value: T) -> RawVal { panic!("Can't wrap scalar of type {:?}", Self::t()) }
    fn t() -> EncodingType;
}

impl GenericVec<u8> for u8 {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [u8] where u8: 'a { vec.cast_ref_u8() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<u8> where u8: 'a { vec.cast_ref_mut_u8() }
    fn t() -> EncodingType { EncodingType::U8 }
}

impl GenericVec<u16> for u16 {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [u16] where u16: 'a { vec.cast_ref_u16() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<u16> where u16: 'a { vec.cast_ref_mut_u16() }
    fn t() -> EncodingType { EncodingType::U16 }
}

impl GenericVec<u32> for u32 {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [u32] where u32: 'a { vec.cast_ref_u32() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<u32> where u32: 'a { vec.cast_ref_mut_u32() }
    fn wrap_one(value: u32) -> RawVal { RawVal::Int(i64::from(value)) }
    fn t() -> EncodingType { EncodingType::U32 }
}

impl GenericVec<i64> for i64 {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [i64] where i64: 'a { vec.cast_ref_i64() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<i64> where i64: 'a { vec.cast_ref_mut_i64() }
    fn wrap_one(value: i64) -> RawVal { RawVal::Int(value) }
    fn t() -> EncodingType { EncodingType::I64 }
}

impl GenericVec<u64> for u64 {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [u64] where u64: 'a { vec.cast_ref_u64() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<u64> where u64: 'a { vec.cast_ref_mut_u64() }
    fn wrap_one(value: u64) -> RawVal { RawVal::Int(value as i64) }
    fn t() -> EncodingType { EncodingType::U64 }
}

impl GenericVec<usize> for usize {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [usize] where usize: 'a { vec.cast_ref_usize() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<usize> where usize: 'a { vec.cast_ref_mut_usize() }
    fn t() -> EncodingType { EncodingType::USize }
}

impl<'c> GenericVec<&'c str> for &'c str {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [&'c str] where &'c str: 'a {
        // TODO(clemens): Probably wrong, but after many hours I haven't found any other way of making all of this work :(
        // Might require associated type constructors to solve easily...
        unsafe {
            mem::transmute::<_, &'b [&'c str]>(vec.cast_ref_str())
        }
    }

    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<&'c str> where &'c str: 'a {
        unsafe {
            mem::transmute::<_, &'b mut Vec<&'c str>>(vec.cast_ref_mut_str())
        }
    }

    fn wrap_one(value: &'c str) -> RawVal { RawVal::Str(value.to_string()) }

    fn t() -> EncodingType { EncodingType::Str }
}

impl<'c> GenericVec<Val<'c>> for Val<'c> {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [Val<'c>] where Val<'c>: 'a {
        unsafe {
            mem::transmute::<_, &'b [Val<'c>]>(vec.cast_ref_mixed())
        }
    }

    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<Val<'c>> where RawVal: 'a {
        unsafe {
            mem::transmute::<_, &'b mut Vec<Val<'c>>>(vec.cast_ref_mut_mixed())
        }
    }

    fn wrap_one(value: Val<'c>) -> RawVal { (&value).into() }

    fn t() -> EncodingType { EncodingType::Val }
}
/*
impl<'c> GenericVec<&'c str> for ByteSlices<'c> {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [ByteSlices<'c>] where ByteSlices<'c>: 'a {
        unsafe {
            mem::transmute::<_, &'b [ByteSlices<'c>]>(vec.cast_ref_str())
        }
    }

    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<ByteSlices<'c>> where ByteSlices<'c>: 'a {
        unsafe {
            mem::transmute::<_, &'b mut Vec<ByteSlices<'c>>>(vec.cast_ref_mut_str())
        }
    }

    fn wrap_one(value: &'c str) -> RawVal { RawVal::Str(value.to_string()) }

    fn t() -> EncodingType { EncodingType::Str }
}
*/

pub trait GenericIntVec<T>: GenericVec<T> + CastUsize + FromBytes<T> + PrimInt + Hash + 'static {}

impl<T> GenericIntVec<T> for T where T: GenericVec<T> + CastUsize + FromBytes<T> + PrimInt + Copy + Hash + 'static {}

pub trait ConstType<T>: Clone {
    fn unwrap(vec: &AnyVec) -> T;
}

impl ConstType<i64> for i64 {
    fn unwrap(vec: &AnyVec) -> i64 { vec.cast_i64_const() }
}

impl ConstType<String> for String {
    fn unwrap(vec: &AnyVec) -> String { vec.cast_str_const() }
}


pub trait FromBytes<T> {
    fn from_bytes(bytes: &[u8]) -> T;
}

impl FromBytes<u8> for u8 {
    fn from_bytes(bytes: &[u8]) -> u8 {
        Cursor::new(bytes).read_u8().unwrap()
    }
}

impl FromBytes<u16> for u16 {
    fn from_bytes(bytes: &[u8]) -> u16 {
        Cursor::new(bytes).read_u16::<NativeEndian>().unwrap()
    }
}

impl FromBytes<u32> for u32 {
    fn from_bytes(bytes: &[u8]) -> u32 {
        Cursor::new(bytes).read_u32::<NativeEndian>().unwrap()
    }
}

impl FromBytes<u64> for u64 {
    fn from_bytes(bytes: &[u8]) -> u64 {
        Cursor::new(bytes).read_u64::<NativeEndian>().unwrap()
    }
}

impl FromBytes<i64> for i64 {
    fn from_bytes(bytes: &[u8]) -> i64 {
        Cursor::new(bytes).read_i64::<NativeEndian>().unwrap()
    }
}

pub trait CastUsize {
    fn cast_usize(&self) -> usize;
}

impl CastUsize for u8 {
    fn cast_usize(&self) -> usize { *self as usize }
}

impl CastUsize for u16 {
    fn cast_usize(&self) -> usize { *self as usize }
}

impl CastUsize for u32 {
    fn cast_usize(&self) -> usize { *self as usize }
}

impl CastUsize for i64 {
    fn cast_usize(&self) -> usize { *self as usize }
}

impl CastUsize for u64 {
    fn cast_usize(&self) -> usize { *self as usize }
}

#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone, HeapSizeOf)]
pub enum MergeOp {
    TakeLeft,
    TakeRight,
    MergeRight,
}

impl Display for MergeOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", &self)
    }
}

impl GenericVec<MergeOp> for MergeOp {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [MergeOp] where MergeOp: 'a { vec.cast_ref_merge_op() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<MergeOp> where MergeOp: 'a { vec.cast_ref_mut_merge_op() }
    fn t() -> EncodingType { EncodingType::MergeOp }
}


#[derive(Debug, PartialEq, PartialOrd, Ord, Eq, Copy, Clone, HeapSizeOf)]
pub struct Premerge {
    pub left: u32,
    pub right: u32,
}

impl Display for Premerge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}|{}", self.left, self.right)
    }
}

impl GenericVec<Premerge> for Premerge {
    fn unwrap<'a, 'b>(vec: &'b AnyVec<'a>) -> &'b [Premerge] where Premerge: 'a { vec.cast_ref_premerge() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut AnyVec<'a>) -> &'b mut Vec<Premerge> where Premerge: 'a { vec.cast_ref_mut_premerge() }
    fn t() -> EncodingType { EncodingType::Premerge }
}


pub fn display_slice<T: Display>(slice: &[T], max_chars: usize) -> String {
    let mut length = slice.len();
    loop {
        let result = _display_slice(slice, length);
        if result.len() < max_chars { break; }
        length = min(length - 1, max_chars * length / result.len());
        if length < 3 {
            return _display_slice(slice, 2);
        }
    }
    if length == slice.len() {
        return _display_slice(slice, slice.len());
    }
    for l in length..max_chars {
        if _display_slice(slice, l).len() > max_chars {
            return _display_slice(slice, l - 1);
        }
    }
    "display_slice error!".to_owned()
}

fn _display_slice<T: Display>(slice: &[T], max: usize) -> String {
    let mut result = String::new();
    write!(result, "[").unwrap();
    write!(result, "{}", slice[..max].iter().map(|x| format!("{}", x)).join(", ")).unwrap();
    if max < slice.len() {
        write!(result, ", ...] ({} more)", slice.len() - max).unwrap();
    } else {
        write!(result, "]").unwrap();
    }
    result
}
