use std::fmt;
use std::mem;

use crate::ingest::raw_val::RawVal;

use super::*;

#[derive(Debug)]
pub struct ScalarVal<T> {
    pub val: T,
}

impl<'a> Data<'a> for ScalarVal<i64> {
    fn cast_scalar_i64(&self) -> i64 {
        self.val
    }
}

impl<'a> Data<'a> for ScalarVal<&'a str> {
    default fn len(&self) -> usize {
        1
    }
    fn get_raw(&self, _: usize) -> RawVal {
        RawVal::Str(self.val.to_string())
    }
    default fn get_type(&self) -> EncodingType {
        EncodingType::ScalarStr
    }
    default fn slice_box<'b>(&'b self, _: usize, _: usize) -> BoxedData<'b>
    where
        'a: 'b,
    {
        panic!("{}", self.type_error("slice_box"))
    }
    default fn type_error(&self, func_name: &str) -> String {
        format!("Vec<{:?}>.{}", self.get_type(), func_name)
    }

    fn append_all(&mut self, _: &dyn Data<'a>, _: usize) -> Option<BoxedData<'a>> {
        panic!("{}", self.type_error("slice_box"))
    }

    fn display(&self) -> String {
        format!("Scalar<{:?}>({:?})", self.get_type(), &self.val)
    }

    fn cast_scalar_str(&self) -> &'a str {
        self.val
    }
}

impl<'a, T: ScalarData<T>> Data<'a> for ScalarVal<T> {
    default fn len(&self) -> usize {
        1
    }
    default fn get_raw(&self, _: usize) -> RawVal {
        T::raw_val(&self.val)
    }
    default fn get_type(&self) -> EncodingType {
        T::t()
    }
    default fn slice_box<'b>(&'b self, _: usize, _: usize) -> BoxedData<'b>
    where
        'a: 'b,
    {
        panic!("{}", self.type_error("slice_box"))
    }
    default fn type_error(&self, func_name: &str) -> String {
        format!("Vec<{:?}>.{}", T::t(), func_name)
    }

    default fn append_all(&mut self, _: &dyn Data<'a>, _: usize) -> Option<BoxedData<'a>> {
        panic!("{}", self.type_error("slice_box"))
    }

    default fn display(&self) -> String {
        format!("Scalar<{:?}>{:?}", T::t(), &self)
    }

    // Copied from Data and marked default because specialization demands it
    default fn cast_scalar_str(&self) -> &'a str {
        panic!("{}", self.type_error("cast_scalar_str"))
    }
    default fn cast_ref_scalar_string(&self) -> &String {
        panic!("{}", self.type_error("cast_ref_scalar_string"))
    }
    default fn cast_scalar_i64(&self) -> i64 {
        panic!("{}", self.type_error("cast_scalar_i64"))
    }
}

impl<'a> Data<'a> for ScalarVal<String> {
    fn cast_ref_scalar_string(&self) -> &String {
        &self.val
    }
}

pub trait ScalarData<T>: Clone + Sync + Send + fmt::Debug {
    fn unwrap(vec: &dyn Data) -> T;
    fn raw_val(val: &T) -> RawVal;
    fn t() -> EncodingType;
}

impl ScalarData<i64> for i64 {
    fn unwrap(vec: &dyn Data) -> i64 {
        vec.cast_scalar_i64()
    }
    fn raw_val(val: &i64) -> RawVal {
        RawVal::Int(*val)
    }
    fn t() -> EncodingType {
        EncodingType::ScalarI64
    }
}

impl<'a> ScalarData<&'a str> for &'a str {
    fn unwrap(vec: &dyn Data) -> &'a str {
        // TODO(#96): fix. wait for associated type constructors?
        unsafe { mem::transmute::<&str, &'a str>(vec.cast_scalar_str()) }
    }

    fn raw_val(val: &&'a str) -> RawVal {
        RawVal::Str((*val).to_string())
    }
    fn t() -> EncodingType {
        EncodingType::ScalarStr
    }
}

impl ScalarData<String> for String {
    fn unwrap(vec: &dyn Data) -> String {
        vec.cast_ref_scalar_string().to_string()
    }
    fn raw_val(val: &String) -> RawVal {
        RawVal::Str(val.clone())
    }
    fn t() -> EncodingType {
        EncodingType::ScalarStr
    }
}
