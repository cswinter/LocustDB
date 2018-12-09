use std::cmp::min;
use std::fmt;
use std::fmt::Write;

use itertools::Itertools;

use mem_store::value::Val;
use bitvec::*;
use ingest::raw_val::RawVal;
use engine::data_types::*;

pub struct NullableVec<T> {
    pub data: Vec<T>,
    pub present: Vec<u8>,
}

impl<'a, T: VecData<T> + 'a> Data<'a> for NullableVec<T> {
    fn len(&self) -> usize { self.data.len() }
    fn get_raw(&self, i: usize) -> RawVal {
        if self.present.is_set(i) { T::wrap_one(self.data[i]) } else { RawVal::Null }
    }
    fn get_type(&self) -> EncodingType { T::t().nullable() }
    fn type_error(&self, func_name: &str) -> String { format!("NullableVec<{:?}>.{}", T::t(), func_name) }
    fn slice_box<'b>(&'b self, _: usize, _: usize) -> BoxedData<'b> where 'a: 'b {
        panic!("nullable slice box!")
    }

    default fn append_all(&mut self, other: &Data<'a>, count: usize) -> Option<BoxedData<'a>> {
        if other.get_type() != self.get_type() {
            let mut mixed = self.to_mixed();
            if other.get_type() == EncodingType::Val {
                mixed.extend(other.cast_ref_mixed().iter().take(count));
            } else {
                mixed.append_all(&other.to_mixed(), count);
            }
            Some(Box::new(mixed))
        } else {
            let data = T::unwrap(other);
            let present = other.cast_ref_null_map();
            let len = self.len();
            let max = min(data.len(), count);
            self.data.extend_from_slice(&data[0..max]);
            for i in 0..max {
                if present.is_set(i) {
                    self.present.set(len + i);
                }
            }
            None
        }
    }

    fn cast_ref_null_map(&self) -> &[u8] { &self.present }

    fn display(&self) -> String {
        format!("NullableVec<{:?}>{}", T::t(),
                display_nullable_slice(&self.data, &self.present, 120))
    }
}

impl<'a> Data<'a> for NullableVec<i64> {
    fn cast_ref_i64(&self) -> &[i64] { &self.data }
    // fn cast_ref_mut_i64(&mut self) -> &mut Vec<i64> { &mut self.data }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.data.iter().enumerate().map(|(i, x)| {
            if self.present.is_set(i) { Val::Integer(*x) } else { Val::Null }
        }).collect()
    }
}

impl<'a> Data<'a> for NullableVec<u32> {
    fn cast_ref_u32(&self) -> &[u32] { &self.data }
    // fn cast_ref_mut_u32(&mut self) -> &mut Vec<u32> { &mut self.data }
}

impl<'a> Data<'a> for NullableVec<u16> {
    fn cast_ref_u16(&self) -> &[u16] { &self.data }
    // fn cast_ref_mut_u16(&mut self) -> &mut Vec<u16> { &mut self.data }
}

impl<'a> Data<'a> for NullableVec<u8> {
    fn cast_ref_u8(&self) -> &[u8] { &self.data }
    // fn cast_ref_mut_u8(&mut self) -> &mut Vec<u8> { &mut self.data }
}

impl<'a> Data<'a> for NullableVec<&'a str> {
    fn cast_ref_str(&self) -> &[&'a str] { &self.data }
}

pub fn display_nullable_slice<T: fmt::Display>(slice: &[T], present: &[u8], max_chars: usize) -> String {
    let mut length = slice.len();
    loop {
        let result = _display_nullable_slice(slice, present, length);
        if result.len() < max_chars { break; }
        length = min(length - 1, max_chars * length / result.len());
        if length < 3 {
            return _display_nullable_slice(slice, present, 2);
        }
    }
    if length == slice.len() {
        return _display_nullable_slice(slice, present, slice.len());
    }
    for l in length..max_chars {
        if _display_nullable_slice(slice, present, l).len() > max_chars {
            return _display_nullable_slice(slice, present, l - 1);
        }
    }
    "display_slice error!".to_owned()
}

fn _display_nullable_slice<T: fmt::Display>(slice: &[T], present: &[u8], max: usize) -> String {
    let mut result = String::new();
    write!(result, "[").unwrap();
    write!(result, "{}", slice[..max].iter()
        .enumerate()
        .map(|(i, x)| if present.is_set(i) { format!("{}", x) } else { "null".to_string() })
        .join(", ")).unwrap();
    if max < slice.len() {
        write!(result, ", ...] ({} more)", slice.len() - max).unwrap();
    } else {
        write!(result, "]").unwrap();
    }
    result
}
