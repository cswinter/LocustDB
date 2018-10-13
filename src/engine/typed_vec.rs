use std::cmp::min;
use std::fmt::{Debug, Display, Write};
use std::fmt;
use std::hash::Hash;
use std::i64;
use std::mem;
use std::string;

use num::PrimInt;

use engine::types::*;
use heapsize::HeapSizeOf;
use ingest::raw_val::RawVal;
use itertools::Itertools;
use mem_store::value::Val;


pub type BoxedVec<'a> = Box<AnyVec<'a> + 'a>;

pub trait AnyVec<'a>: Send + Sync {
    fn len(&self) -> usize;
    fn get_raw(&self, i: usize) -> RawVal;
    fn get_type(&self) -> EncodingType;
    fn sort_indices_desc(&self, indices: &mut Vec<usize>);
    fn sort_indices_asc(&self, indices: &mut Vec<usize>);
    fn type_error(&self, func_name: &str) -> String;
    fn append_all(&mut self, other: &AnyVec<'a>, count: usize) -> Option<BoxedVec<'a>>;
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedVec<'b> where 'a: 'b;

    fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] { panic!(self.type_error("cast_ref_str")) }
    fn cast_ref_usize(&self) -> &[usize] { panic!(self.type_error("cast_ref_usize")) }
    fn cast_ref_i64(&self) -> &[i64] { panic!(self.type_error("cast_ref_i64")) }
    fn cast_ref_u64(&self) -> &[u64] { panic!(self.type_error("cast_ref_u64")) }
    fn cast_ref_u32(&self) -> &[u32] { panic!(self.type_error("cast_ref_u32")) }
    fn cast_ref_u16(&self) -> &[u16] { panic!(self.type_error("cast_ref_u16")) }
    fn cast_ref_u8(&self) -> &[u8] { panic!(self.type_error("cast_ref_u8")) }
    fn cast_ref_mixed(&self) -> &[Val<'a>] { panic!(self.type_error("cast_ref_mixed")) }
    fn cast_ref_merge_op(&self) -> &[MergeOp] { panic!(self.type_error("cast_ref_merge_op")) }
    fn cast_ref_premerge(&self) -> &[Premerge] { panic!(self.type_error("cast_ref_merge_op")) }
    fn cast_str_const(&self) -> string::String { panic!(self.type_error("cast_str_const")) }
    fn cast_i64_const(&self) -> i64 { panic!(self.type_error("cast_i64_const")) }

    fn cast_ref_mut_str(&mut self) -> &mut Vec<&'a str> { panic!(self.type_error("cast_ref_mut_str")) }
    fn cast_ref_mut_usize(&mut self) -> &mut Vec<usize> { panic!(self.type_error("cast_ref_mut_usize")) }
    fn cast_ref_mut_i64(&mut self) -> &mut Vec<i64> { panic!(self.type_error("cast_ref_mut_i64")) }
    fn cast_ref_mut_u64(&mut self) -> &mut Vec<u64> { panic!(self.type_error("cast_ref_mut_u64")) }
    fn cast_ref_mut_u32(&mut self) -> &mut Vec<u32> { panic!(self.type_error("cast_ref_mut_u32")) }
    fn cast_ref_mut_u16(&mut self) -> &mut Vec<u16> { panic!(self.type_error("cast_ref_mut_u16")) }
    fn cast_ref_mut_u8(&mut self) -> &mut Vec<u8> { panic!(self.type_error("cast_ref_mut_u8")) }
    fn cast_ref_mut_mixed(&mut self) -> &mut Vec<Val<'a>> { panic!(self.type_error("cast_ref_mut_mixed")) }
    fn cast_ref_mut_merge_op(&mut self) -> &mut Vec<MergeOp> { panic!(self.type_error("cast_ref_merge_op")) }
    fn cast_ref_mut_premerge(&mut self) -> &mut Vec<Premerge> { panic!(self.type_error("cast_ref_merge_op")) }

    fn to_mixed(&self) -> Vec<Val<'a>> { panic!(self.type_error("to_mixed")) }

    fn display(&self) -> String;
}

impl<'a> AnyVec<'a> {
    pub fn owned<T: GenericVec<T> + 'a>(data: Vec<T>) -> BoxedVec<'a> { Box::new(data) }
    pub fn constant(value: RawVal) -> BoxedVec<'a> { Box::new(value) }
    pub fn empty(length: usize) -> BoxedVec<'a> { Box::new(length) }
}


impl<'a, T: GenericVec<T> + 'a> AnyVec<'a> for Vec<T> {
    fn len(&self) -> usize { Vec::len(self) }
    fn get_raw(&self, i: usize) -> RawVal { T::wrap_one(self[i]) }
    fn get_type(&self) -> EncodingType { T::t() }
    fn sort_indices_desc(&self, indices: &mut Vec<usize>) {
        indices.sort_unstable_by(|i, j| self[*i].cmp(&self[*j]).reverse());
    }
    fn sort_indices_asc(&self, indices: &mut Vec<usize>) {
        indices.sort_unstable_by_key(|i| self[*i]);
    }
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedVec<'b> where 'a: 'b {
        let to = min(to, self.len());
        Box::new(&self[from..to])
    }

    fn type_error(&self, func_name: &str) -> String { format!("Vec<{:?}>.{}", T::t(), func_name) }

    default fn append_all(&mut self, other: &AnyVec<'a>, count: usize) -> Option<BoxedVec<'a>> {
        if other.get_type() != self.get_type() {
            let mut mixed = self.to_mixed();
            if other.get_type() == EncodingType::Val {
                mixed.extend(other.cast_ref_mixed().iter().take(count));
            } else {
                mixed.append_all(&other.to_mixed(), count);
            }
            Some(Box::new(mixed))
        } else {
            let x = T::unwrap(other);
            self.extend_from_slice(&x[0..min(x.len(), count)]);
            None
        }
    }

    fn display(&self) -> String { format!("Vec<{:?}>{}", T::t(), display_slice(&self, 120)) }
}

impl<'a> AnyVec<'a> for Vec<&'a str> {
    fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] { self }
    fn cast_ref_mut_str<'b>(&'b mut self) -> &'b mut Vec<&'a str> { self }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter().map(|s| Val::Str(*s)).collect()
    }
}

impl<'a> AnyVec<'a> for Vec<Val<'a>> {
    fn cast_ref_mixed<'b>(&'b self) -> &'b [Val<'a>] { self }
    fn cast_ref_mut_mixed<'b>(&'b mut self) -> &'b mut Vec<Val<'a>> { self }

    fn append_all(&mut self, other: &AnyVec<'a>, count: usize) -> Option<BoxedVec<'a>> {
        if other.get_type() == EncodingType::Val {
            self.extend(other.cast_ref_mixed().iter().take(count));
        } else {
            self.extend(other.to_mixed().iter().take(count));
        }
        None
    }
}

impl<'a> AnyVec<'a> for Vec<usize> {
    fn cast_ref_usize(&self) -> &[usize] { self }
    fn cast_ref_mut_usize(&mut self) -> &mut Vec<usize> { self }
}

impl<'a> AnyVec<'a> for Vec<i64> {
    fn cast_ref_i64(&self) -> &[i64] { self }
    fn cast_ref_mut_i64(&mut self) -> &mut Vec<i64> { self }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter().map(|i| Val::Integer(*i)).collect()
    }
}

impl<'a> AnyVec<'a> for Vec<u64> {
    fn cast_ref_u64(&self) -> &[u64] { self }
    fn cast_ref_mut_u64(&mut self) -> &mut Vec<u64> { self }
}

impl<'a> AnyVec<'a> for Vec<u32> {
    fn cast_ref_u32(&self) -> &[u32] { self }
    fn cast_ref_mut_u32(&mut self) -> &mut Vec<u32> { self }
}

impl<'a> AnyVec<'a> for Vec<u16> {
    fn cast_ref_u16(&self) -> &[u16] { self }
    fn cast_ref_mut_u16(&mut self) -> &mut Vec<u16> { self }
}

impl<'a> AnyVec<'a> for Vec<u8> {
    fn cast_ref_u8(&self) -> &[u8] { self }
    fn cast_ref_mut_u8(&mut self) -> &mut Vec<u8> { self }
}

impl<'a> AnyVec<'a> for Vec<MergeOp> {
    fn cast_ref_merge_op(&self) -> &[MergeOp] { self }
    fn cast_ref_mut_merge_op(&mut self) -> &mut Vec<MergeOp> { self }
}

impl<'a> AnyVec<'a> for Vec<Premerge> {
    fn cast_ref_premerge(&self) -> &[Premerge] { self }
    fn cast_ref_mut_premerge(&mut self) -> &mut Vec<Premerge> { self }
}


impl<'a, T: GenericVec<T> + 'a> AnyVec<'a> for &'a [T] {
    fn len(&self) -> usize { <[T]>::len(self) }
    fn get_raw(&self, i: usize) -> RawVal { T::wrap_one(self[i]) }
    fn get_type(&self) -> EncodingType { T::t() }
    fn sort_indices_desc(&self, indices: &mut Vec<usize>) {
        indices.sort_unstable_by(|i, j| self[*i].cmp(&self[*j]).reverse());
    }
    fn sort_indices_asc(&self, indices: &mut Vec<usize>) {
        indices.sort_unstable_by_key(|i| self[*i]);
    }
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedVec<'b> where 'a: 'b {
        let to = min(to, self.len());
        Box::new(&self[from..to])
    }

    fn append_all(&mut self, _other: &AnyVec<'a>, _count: usize) -> Option<BoxedVec<'a>> {
        panic!("append_all on borrow")
    }

    fn type_error(&self, func_name: &str) -> String { format!("[{:?}].{}", T::t(), func_name) }

    fn display(&self) -> String { format!("&{:?}{}", T::t(), display_slice(&self, 120)) }
}

impl<'a> AnyVec<'a> for &'a [&'a str] {
    fn cast_ref_str(&self) -> &[&'a str] { self }
}

impl<'a> AnyVec<'a> for &'a [Val<'a>] {
    fn cast_ref_mixed(&self) -> &[Val<'a>] { self }
}

impl<'a> AnyVec<'a> for &'a [usize] {
    fn cast_ref_usize(&self) -> &[usize] { self }
}

impl<'a> AnyVec<'a> for &'a [i64] {
    fn cast_ref_i64(&self) -> &[i64] { self }
}

impl<'a> AnyVec<'a> for &'a [u64] {
    fn cast_ref_u64(&self) -> &[u64] { self }
}

impl<'a> AnyVec<'a> for &'a [u32] {
    fn cast_ref_u32(&self) -> &[u32] { self }
}

impl<'a> AnyVec<'a> for &'a [u16] {
    fn cast_ref_u16(&self) -> &[u16] { self }
}

impl<'a> AnyVec<'a> for &'a [u8] {
    fn cast_ref_u8(&self) -> &[u8] { self }
}

impl<'a> AnyVec<'a> for &'a [MergeOp] {
    fn cast_ref_merge_op(&self) -> &[MergeOp] { self }
}

impl<'a> AnyVec<'a> for &'a [Premerge] {
    fn cast_ref_premerge(&self) -> &[Premerge] { self }
}


impl<'a> AnyVec<'a> for usize {
    fn len(&self) -> usize { *self }
    fn get_raw(&self, i: usize) -> RawVal {
        assert!(i < *self);
        RawVal::Null
    }
    fn get_type(&self) -> EncodingType { EncodingType::Null }
    fn sort_indices_desc(&self, _indices: &mut Vec<usize>) { panic!("EmptyVector.sort_indices_desc") }
    fn sort_indices_asc(&self, _indices: &mut Vec<usize>) { panic!("EmptyVector.sort_indices_asc") }
    fn type_error(&self, func_name: &str) -> String { format!("EmptyVector.{}", func_name) }

    fn append_all(&mut self, other: &AnyVec<'a>, count: usize) -> Option<BoxedVec<'a>> {
        if other.get_type() == EncodingType::Null {
            *self += count;
            None
        } else {
            let mut upcast = Box::new(vec![Val::Null; *self]);
            upcast.append_all(other, count).or_else(|| Some(upcast))
        }
    }

    fn to_mixed(&self) -> Vec<Val<'a>> {
        vec![Val::Null; *self]
    }

    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedVec<'b> where 'a: 'b { Box::new(min(to, *self) - from) }

    fn display(&self) -> String { format!("null({})", self) }
}

impl<'a> AnyVec<'a> for RawVal {
    fn len(&self) -> usize { 0 }
    fn get_raw(&self, _i: usize) -> RawVal { self.clone() }
    fn get_type(&self) -> EncodingType { EncodingType::Constant }
    fn sort_indices_desc(&self, _indices: &mut Vec<usize>) {}
    fn sort_indices_asc(&self, _indices: &mut Vec<usize>) {}
    fn type_error(&self, func_name: &str) -> String { format!("Constant.{}", func_name) }
    fn append_all(&mut self, _other: &AnyVec<'a>, _count: usize) -> Option<BoxedVec<'a>> { panic!("Constant.extend") }
    fn slice_box<'b>(&'b self, _: usize, _: usize) -> BoxedVec<'b> where 'a: 'b { Box::new(self.clone()) }
    fn cast_str_const(&self) -> string::String {
        match self {
            RawVal::Str(s) => s.clone(),
            _ => panic!("{}.cast_str_const", &self),
        }
    }
    fn cast_i64_const(&self) -> i64 {
        match self {
            RawVal::Int(i) => *i,
            _ => panic!("{}.cast_i64_const", &self),
        }
    }

    fn display(&self) -> String { format!("Scalar({})", self) }
}

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


pub trait GenericIntVec<T>: GenericVec<T> + CastUsize + PrimInt + Hash + 'static {}

impl<T> GenericIntVec<T> for T where T: GenericVec<T> + CastUsize + PrimInt + Copy + Hash + 'static {}

pub trait ConstType<T> {
    fn unwrap(vec: &AnyVec) -> T;
}

impl ConstType<i64> for i64 {
    fn unwrap(vec: &AnyVec) -> i64 { vec.cast_i64_const() }
}

impl ConstType<String> for String {
    fn unwrap(vec: &AnyVec) -> String { vec.cast_str_const() }
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


fn display_slice<T: Display>(slice: &[T], max_chars: usize) -> String {
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