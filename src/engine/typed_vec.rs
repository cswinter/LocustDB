use std::cmp::min;
use std::fmt::Debug;
use std::hash::Hash;
use std::mem;
use std::string;

use bit_vec::BitVec;
use heapsize::HeapSizeOf;
use num::PrimInt;

use engine::types::*;
use ingest::raw_val::RawVal;


pub type BoxedVec<'a> = Box<TypedVec<'a> + 'a>;


pub trait TypedVec<'a>: Send + Sync {
    fn len(&self) -> usize;
    fn get_raw(&self, i: usize) -> RawVal;
    fn get_type(&self) -> EncodingType;
    fn sort_indices_desc(&self, indices: &mut Vec<usize>);
    fn sort_indices_asc(&self, indices: &mut Vec<usize>);
    fn type_error(&self, func_name: &str) -> String;
    fn extend(&mut self, other: BoxedVec<'a>, count: usize) -> Option<BoxedVec<'a>>;
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedVec<'b> where 'a: 'b;

    fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] { panic!(self.type_error("cast_ref_str")) }
    fn cast_ref_usize(&self) -> &[usize] { panic!(self.type_error("cast_ref_usize")) }
    fn cast_ref_i64(&self) -> &[i64] { panic!(self.type_error("cast_ref_i64")) }
    fn cast_ref_u32<'b>(&'b self) -> &[u32] { panic!(self.type_error("cast_ref_u32")) }
    fn cast_ref_u16<'b>(&'b self) -> &[u16] { panic!(self.type_error("cast_ref_u16")) }
    fn cast_ref_u8<'b>(&'b self) -> &[u8] { panic!(self.type_error("cast_ref_u8")) }
    fn cast_str_const(&self) -> string::String { panic!(self.type_error("cast_str_const")) }
    fn cast_i64_const(&self) -> i64 { panic!(self.type_error("cast_str_const")) }

    fn cast_ref_mut_str<'b>(&'b mut self) -> &'b mut Vec<&'a str> { panic!(self.type_error("cast_ref_mut_str")) }
    fn cast_ref_mut_usize(&mut self) -> &mut Vec<usize> { panic!(self.type_error("cast_ref_mut_usize")) }
    fn cast_ref_mut_i64(&mut self) -> &mut Vec<i64> { panic!(self.type_error("cast_ref_mut_i64")) }
    fn cast_ref_mut_u32(&mut self) -> &mut Vec<u32> { panic!(self.type_error("cast_ref_mut_u32")) }
    fn cast_ref_mut_u16(&mut self) -> &mut Vec<u16> { panic!(self.type_error("cast_ref_mut_u16")) }
    fn cast_ref_mut_u8(&mut self) -> &mut Vec<u8> { panic!(self.type_error("cast_ref_mut_u8")) }

    fn cast_ref_mut_bit_vec(&mut self) -> &mut BitVec { panic!(self.type_error("cast_ref_mut_bit_vec")) }
    fn cast_ref_bit_vec(&self) -> &BitVec { panic!(self.type_error("cast_ref_bit_vec")) }
}

impl<'a> TypedVec<'a> {
    pub fn owned<T: VecType<T> + 'a>(data: Vec<T>) -> BoxedVec<'a> { Box::new(data) }
    pub fn bit_vec(value: BitVec) -> BoxedVec<'a> { Box::new(value) }
    pub fn constant(value: RawVal) -> BoxedVec<'a> { Box::new(value) }
    pub fn empty(length: usize) -> BoxedVec<'a> { Box::new(length) }
}


impl<'a, T: VecType<T> + 'a> TypedVec<'a> for Vec<T> {
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

    fn extend(&mut self, other: BoxedVec<'a>, count: usize) -> Option<BoxedVec<'a>> {
        // TODO(clemens): handle empty, null, type conversions to Mixed
        let x = T::unwrap(other.as_ref());
        self.extend_from_slice(&x[0..min(x.len(), count)]);
        None
    }
}

impl<'a> TypedVec<'a> for Vec<&'a str> {
    fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] { self }
    fn cast_ref_mut_str<'b>(&'b mut self) -> &'b mut Vec<&'a str> { self }
}

impl<'a> TypedVec<'a> for Vec<usize> {
    fn cast_ref_usize(&self) -> &[usize] { self }
    fn cast_ref_mut_usize(&mut self) -> &mut Vec<usize> { self }
}

impl<'a> TypedVec<'a> for Vec<i64> {
    fn cast_ref_i64(&self) -> &[i64] { self }
    fn cast_ref_mut_i64(&mut self) -> &mut Vec<i64> { self }
}

impl<'a> TypedVec<'a> for Vec<u32> {
    fn cast_ref_u32(&self) -> &[u32] { self }
    fn cast_ref_mut_u32(&mut self) -> &mut Vec<u32> { self }
}

impl<'a> TypedVec<'a> for Vec<u16> {
    fn cast_ref_u16(&self) -> &[u16] { self }
    fn cast_ref_mut_u16(&mut self) -> &mut Vec<u16> { self }
}

impl<'a> TypedVec<'a> for Vec<u8> {
    fn cast_ref_u8(&self) -> &[u8] { self }
    fn cast_ref_mut_u8(&mut self) -> &mut Vec<u8> { self }
}

impl<'a, T: VecType<T> + 'a> TypedVec<'a> for &'a [T] {
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


    fn type_error(&self, func_name: &str) -> String { format!("[{:?}].{}", T::t(), func_name) }

    fn extend(&mut self, _other: BoxedVec<'a>, _count: usize) -> Option<BoxedVec<'a>> {
        // TODO(clemens): convert into owned
        unimplemented!()
    }
}

impl<'a> TypedVec<'a> for &'a [&'a str] {
    fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] { self }
}

impl<'a> TypedVec<'a> for &'a [usize] {
    fn cast_ref_usize<'b>(&'b self) -> &'b [usize] { self }
}

impl<'a> TypedVec<'a> for &'a [i64] {
    fn cast_ref_i64<'b>(&'b self) -> &'b [i64] { self }
}

impl<'a> TypedVec<'a> for &'a [u32] {
    fn cast_ref_u32<'b>(&'b self) -> &'b [u32] { self }
}

impl<'a> TypedVec<'a> for &'a [u16] {
    fn cast_ref_u16<'b>(&'b self) -> &'b [u16] { self }
}

impl<'a> TypedVec<'a> for &'a [u8] {
    fn cast_ref_u8<'b>(&'b self) -> &'b [u8] { self }
}

impl<'a> TypedVec<'a> for BitVec {
    fn len(&self) -> usize { BitVec::len(self) }
    fn get_raw(&self, _i: usize) -> RawVal { panic!("BitVec.get_raw") }
    fn get_type(&self) -> EncodingType { EncodingType::BitVec }
    fn sort_indices_desc(&self, _indices: &mut Vec<usize>) {}
    fn sort_indices_asc(&self, _indices: &mut Vec<usize>) {}
    fn type_error(&self, func_name: &str) -> String { format!("BitVec.{}", func_name) }
    fn extend(&mut self, _other: BoxedVec<'a>, _count: usize) -> Option<BoxedVec<'a>> { panic!("BitVec.extend") }
    fn cast_ref_mut_bit_vec(&mut self) -> &mut BitVec { self }
    fn cast_ref_bit_vec(&self) -> &BitVec { self }
    fn slice_box<'b>(&'b self, _: usize, _: usize) -> BoxedVec<'b> where 'a: 'b { panic!("BitVec.slice_box()") }
}

impl<'a> TypedVec<'a> for usize {
    fn len(&self) -> usize { *self }
    fn get_raw(&self, i: usize) -> RawVal {
        assert!(i < *self);
        RawVal::Null
    }
    fn get_type(&self) -> EncodingType { EncodingType::Null }
    fn sort_indices_desc(&self, _indices: &mut Vec<usize>) { panic!("EmptyVector.sort_indices_desc") }
    fn sort_indices_asc(&self, _indices: &mut Vec<usize>) { panic!("EmptyVector.sort_indices_asc") }
    fn type_error(&self, func_name: &str) -> String { format!("EmptyVector.{}", func_name) }
    fn extend(&mut self, _other: BoxedVec<'a>, _count: usize) -> Option<BoxedVec<'a>> { panic!("EmptyVector.extend") }
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedVec<'b> where 'a: 'b { Box::new(from - min(to, *self)) }
}

impl<'a> TypedVec<'a> for RawVal {
    fn len(&self) -> usize { panic!("Length is not defined for constants") }
    fn get_raw(&self, _i: usize) -> RawVal { self.clone() }
    fn get_type(&self) -> EncodingType { EncodingType::Constant }
    fn sort_indices_desc(&self, _indices: &mut Vec<usize>) {}
    fn sort_indices_asc(&self, _indices: &mut Vec<usize>) {}
    fn type_error(&self, func_name: &str) -> String { format!("Constant.{}", func_name) }
    fn extend(&mut self, _other: BoxedVec<'a>, _count: usize) -> Option<BoxedVec<'a>> { panic!("Constant.extend") }
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
}

pub trait VecType<T>: PartialEq + Ord + Copy + Debug + Sync + Send + HeapSizeOf {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [T] where T: 'a;
    fn unwrap_mut<'a, 'b>(vec: &'b mut TypedVec<'a>) -> &'b mut Vec<T> where T: 'a;
    fn wrap_one(_value: T) -> RawVal { panic!("Can't wrap scalar of type {:?}", Self::t()) }
    fn t() -> EncodingType;
}

impl VecType<u8> for u8 {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [u8] where u8: 'a { vec.cast_ref_u8() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut TypedVec<'a>) -> &'b mut Vec<u8> where u8: 'a { vec.cast_ref_mut_u8() }
    fn t() -> EncodingType { EncodingType::U8 }
}

impl VecType<u16> for u16 {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [u16] where u16: 'a { vec.cast_ref_u16() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut TypedVec<'a>) -> &'b mut Vec<u16> where u16: 'a { vec.cast_ref_mut_u16() }
    fn t() -> EncodingType { EncodingType::U16 }
}

impl VecType<u32> for u32 {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [u32] where u32: 'a { vec.cast_ref_u32() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut TypedVec<'a>) -> &'b mut Vec<u32> where u32: 'a { vec.cast_ref_mut_u32() }
    fn wrap_one(value: u32) -> RawVal { RawVal::Int(value as i64) }
    fn t() -> EncodingType { EncodingType::U32 }
}

impl VecType<i64> for i64 {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [i64] where i64: 'a { vec.cast_ref_i64() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut TypedVec<'a>) -> &'b mut Vec<i64> where i64: 'a { vec.cast_ref_mut_i64() }
    fn wrap_one(value: i64) -> RawVal { RawVal::Int(value) }
    fn t() -> EncodingType { EncodingType::I64 }
}

impl VecType<usize> for usize {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [usize] where usize: 'a { vec.cast_ref_usize() }
    fn unwrap_mut<'a, 'b>(vec: &'b mut TypedVec<'a>) -> &'b mut Vec<usize> where usize: 'a { vec.cast_ref_mut_usize() }
    fn t() -> EncodingType { EncodingType::USize }
}


impl<'c> VecType<&'c str> for &'c str {
    fn unwrap<'a, 'b>(vec: &'b TypedVec<'a>) -> &'b [&'c str] where &'c str: 'a {
        // TODO(clemens): Probably wrong, but after many hours I haven't found any other way of making all of this work :(
        // Might require associated type constructors to solve easily...
        unsafe {
            mem::transmute::<_, &'b [&'c str]>(vec.cast_ref_str())
        }
    }

    fn unwrap_mut<'a, 'b>(vec: &'b mut TypedVec<'a>) -> &'b mut Vec<&'c str> where &'c str: 'a {
        unsafe {
            mem::transmute::<_, &'b mut Vec<&'c str>>(vec.cast_ref_mut_str())
        }
    }

    fn wrap_one(value: &'c str) -> RawVal { RawVal::Str(value.to_string()) }

    fn t() -> EncodingType { EncodingType::Str }
}


pub trait IntVecType<T>: VecType<T> + Into<i64> + IntoUsize + PrimInt + Hash + 'static {}

impl<T> IntVecType<T> for T where T: VecType<T> + Into<i64> + IntoUsize + PrimInt + Copy + Hash + 'static {}

pub trait ConstType<T> {
    fn unwrap(vec: &TypedVec) -> T;
}

impl ConstType<i64> for i64 {
    fn unwrap(vec: &TypedVec) -> i64 { vec.cast_i64_const() }
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

