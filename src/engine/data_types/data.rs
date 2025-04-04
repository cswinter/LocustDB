use std::cmp::min;
use std::fmt;
use std::mem;
use std::string;

use crate::engine::data_types::*;
use crate::ingest::raw_val::RawVal;
use crate::mem_store::codec::Codec;
use crate::mem_store::column::DataSource;
use crate::mem_store::value::Val;

pub type BoxedData<'a> = Box<dyn Data<'a> + 'a>;

pub trait Data<'a>: Send + Sync {
    fn len(&self) -> usize;
    fn get_raw(&self, i: usize) -> RawVal;
    fn get_type(&self) -> EncodingType;
    fn type_error(&self, func_name: &str) -> String;
    fn append_all(&mut self, other: &dyn Data<'a>, count: usize) -> Option<BoxedData<'a>>;
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedData<'b>
    where
        'a: 'b;

    fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] {
        panic!("{}", self.type_error("cast_ref_str"))
    }
    fn cast_ref_opt_str<'b>(&'b self) -> &'b [Option<&'a str>] {
        panic!("{}", self.type_error("cast_ref_opt_str"))
    }
    fn cast_ref_i64(&self) -> &[i64] {
        panic!("{}", self.type_error("cast_ref_i64"))
    }
    fn cast_ref_f64(&self) -> &[OrderedFloat<f64>] {
        panic!("{}", self.type_error("cast_ref_f64"))
    }
    fn cast_ref_opt_f64(&self) -> &[Option<OrderedFloat<f64>>] {
        panic!("{}", self.type_error("cast_ref_opt_f64"))
    }
    fn cast_ref_u32(&self) -> &[u32] {
        panic!("{}", self.type_error("cast_ref_u32"))
    }
    fn cast_ref_u16(&self) -> &[u16] {
        panic!("{}", self.type_error("cast_ref_u16"))
    }
    fn cast_ref_u8(&self) -> &[u8] {
        panic!("{}", self.type_error("cast_ref_u8"))
    }

    fn cast_ref_null_map(&self) -> &[u8] {
        panic!("{}", self.type_error("cast_ref_null_map"))
    }

    fn cast_ref_u64(&self) -> &[u64] {
        panic!("{}", self.type_error("cast_ref_u64"))
    }
    fn cast_ref_usize(&self) -> &[usize] {
        panic!("{}", self.type_error("cast_ref_usize"))
    }

    fn cast_ref_mixed(&self) -> &[Val<'a>] {
        panic!("{}", self.type_error("cast_ref_mixed"))
    }
    fn cast_ref_merge_op(&self) -> &[MergeOp] {
        panic!("{}", self.type_error("cast_ref_merge_op"))
    }
    fn cast_ref_premerge(&self) -> &[Premerge] {
        panic!("{}", self.type_error("cast_ref_merge_op"))
    }
    fn cast_ref_scalar_string(&self) -> &String {
        panic!("{}", self.type_error("cast_ref_scalar_string"))
    }
    fn cast_scalar_i64(&self) -> i64 {
        panic!("{}", self.type_error("cast_scalar_i64"))
    }
    fn cast_scalar_f64(&self) -> of64 {
        panic!("{}", self.type_error("cast_scalar_f64"))
    }
    fn cast_scalar_str(&self) -> &'a str {
        panic!("{}", self.type_error("cast_scalar_str"))
    }
    fn cast_ref_byte_slices(&self) -> &ByteSlices<'a> {
        panic!("{}", self.type_error("cast_ref_byte_slices"))
    }
    fn cast_ref_val_rows(&self) -> &ValRows<'a> {
        panic!("{}", self.type_error("cast_ref_val_rows"))
    }

    fn cast_ref_mut_str(&mut self) -> &mut Vec<&'a str> {
        panic!("{}", self.type_error("cast_ref_mut_str"))
    }
    fn cast_ref_mut_opt_str(&mut self) -> &mut Vec<Option<&'a str>> {
        panic!("{}", self.type_error("cast_ref_mut_opt_str"))
    }
    fn cast_ref_mut_i64(&mut self) -> &mut Vec<i64> {
        panic!("{}", self.type_error("cast_ref_mut_i64"))
    }
    fn cast_ref_mut_u32(&mut self) -> &mut Vec<u32> {
        panic!("{}", self.type_error("cast_ref_mut_u32"))
    }
    fn cast_ref_mut_u16(&mut self) -> &mut Vec<u16> {
        panic!("{}", self.type_error("cast_ref_mut_u16"))
    }
    fn cast_ref_mut_u8(&mut self) -> &mut Vec<u8> {
        panic!("{}", self.type_error("cast_ref_mut_u8"))
    }

    fn cast_ref_mut_u64(&mut self) -> &mut Vec<u64> {
        panic!("{}", self.type_error("cast_ref_mut_u64"))
    }
    fn cast_ref_mut_f64(&mut self) -> &mut Vec<OrderedFloat<f64>> {
        panic!("{}", self.type_error("cast_ref_mut_f64"))
    }
    fn cast_ref_mut_opt_f64(&mut self) -> &mut Vec<Option<OrderedFloat<f64>>> {
        panic!("{}", self.type_error("cast_ref_mut_opt_f64"))
    }
    fn cast_ref_mut_usize(&mut self) -> &mut Vec<usize> {
        panic!("{}", self.type_error("cast_ref_mut_usize"))
    }
    fn cast_ref_mut_null(&mut self) -> &mut usize {
        panic!("{}", self.type_error("cast_ref_mut_null"))
    }

    fn cast_ref_mut_mixed(&mut self) -> &mut Vec<Val<'a>> {
        panic!("{}", self.type_error("cast_ref_mut_mixed"))
    }
    fn cast_ref_mut_merge_op(&mut self) -> &mut Vec<MergeOp> {
        panic!("{}", self.type_error("cast_ref_mut_merge_op"))
    }
    fn cast_ref_mut_premerge(&mut self) -> &mut Vec<Premerge> {
        panic!("{}", self.type_error("cast_ref_mut_premerge_op"))
    }
    fn cast_ref_mut_byte_slices(&mut self) -> &mut ByteSlices<'a> {
        panic!("{}", self.type_error("cast_ref_mut_byte_slices"))
    }
    fn cast_ref_mut_val_rows(&mut self) -> &mut ValRows<'a> {
        panic!("{}", self.type_error("cast_ref_mut_val_rows"))
    }

    fn to_mixed(&self) -> Vec<Val<'a>> {
        panic!("{}", self.type_error("to_mixed"))
    }

    fn make_nullable(&mut self, _present: &[u8]) -> BoxedData<'a> {
        panic!("{}", self.type_error("nullable"))
    }

    fn display(&self) -> String;
}

impl<'a> DataSource for BoxedData<'a> {
    fn encoding_type(&self) -> EncodingType {
        self.get_type()
    }
    fn range(&self) -> Option<(i64, i64)> {
        None
    }
    fn codec(&self) -> Codec {
        Codec::identity(self.get_type().cast_to_basic())
    }
    fn len(&self) -> usize {
        (**self).len()
    }
    fn data_sections(&self) -> Vec<&dyn Data> {
        vec![unsafe { mem::transmute::<&dyn Data, &dyn Data>(&**self) }]
    }
    fn full_type(&self) -> Type {
        Type::new(self.encoding_type().cast_to_basic(), self.codec())
    }
}

impl<'a> fmt::Debug for BoxedData<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display())
    }
}

pub fn owned_data<'a, T: VecData<T> + 'a>(data: Vec<T>) -> BoxedData<'a> {
    Box::new(data)
}
pub fn constant_data<'a>(value: RawVal) -> BoxedData<'a> {
    Box::new(value)
}
pub fn scalar_i64_data<'a>(value: i64) -> BoxedData<'a> {
    Box::new(RawVal::Int(value))
}
pub fn scalar_data<'a, T: ScalarData<T> + 'a>(val: T) -> BoxedData<'a> {
    Box::new(ScalarVal { val })
}
pub fn empty_data<'a>(length: usize) -> BoxedData<'a> {
    Box::new(length)
}

impl<'a, T: VecData<T> + 'a> Data<'a> for Vec<T> {
    fn len(&self) -> usize {
        Vec::len(self)
    }
    fn get_raw(&self, i: usize) -> RawVal {
        T::wrap_one(self[i])
    }
    fn get_type(&self) -> EncodingType {
        T::t()
    }
    fn type_error(&self, func_name: &str) -> String {
        format!("Vec<{:?}>.{}", T::t(), func_name)
    }
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedData<'b>
    where
        'a: 'b,
    {
        let to = min(to, self.len());
        Box::new(&self[from..to])
    }

    default fn append_all(&mut self, other: &dyn Data<'a>, count: usize) -> Option<BoxedData<'a>> {
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

    fn make_nullable(&mut self, present: &[u8]) -> BoxedData<'a> {
        let data = mem::take(self);
        Box::new(NullableVec {
            data,
            present: present.to_vec(),
        })
    }

    fn display(&self) -> String {
        format!("Vec<{:?}>{}", T::t(), display_slice(self, 120))
    }

    // Copied from Data and marked default because specialization demands it
    default fn cast_ref_u64(&self) -> &[u64] {
        panic!("{}", self.type_error("cast_ref_u64"))
    }
    default fn cast_ref_f64(&self) -> &[OrderedFloat<f64>] {
        panic!("{}", self.type_error("cast_ref_f64"))
    }
    default fn cast_ref_opt_f64(&self) -> &[Option<OrderedFloat<f64>>] {
        panic!("{}", self.type_error("cast_ref_opt_f64"))
    }
    default fn cast_ref_usize(&self) -> &[usize] {
        panic!("{}", self.type_error("cast_ref_usize"))
    }
    default fn cast_ref_i64(&self) -> &[i64] {
        panic!("{}", self.type_error("cast_ref_i64"))
    }
    default fn cast_ref_u32(&self) -> &[u32] {
        panic!("{}", self.type_error("cast_ref_u32"))
    }
    default fn cast_ref_u16(&self) -> &[u16] {
        panic!("{}", self.type_error("cast_ref_u16"))
    }
    default fn cast_ref_u8(&self) -> &[u8] {
        panic!("{}", self.type_error("cast_ref_u8"))
    }
    default fn cast_ref_mut_premerge(&mut self) -> &mut Vec<Premerge> {
        panic!("{}", self.type_error("cast_ref_mut_premerge_op"))
    }
    default fn cast_ref_premerge(&self) -> &[Premerge] {
        panic!("{}", self.type_error("cast_ref_merge_op"))
    }
    default fn cast_ref_mut_merge_op(&mut self) -> &mut Vec<MergeOp> {
        panic!("{}", self.type_error("cast_ref_mut_merge_op"))
    }
    default fn cast_ref_merge_op(&self) -> &[MergeOp] {
        panic!("{}", self.type_error("cast_ref_merge_op"))
    }
    default fn cast_ref_mut_str(&mut self) -> &mut Vec<&'a str> {
        panic!("{}", self.type_error("cast_ref_mut_str"))
    }
    default fn cast_ref_mut_opt_str(&mut self) -> &mut Vec<Option<&'a str>> {
        panic!("{}", self.type_error("cast_ref_mut_opt_str"))
    }
    default fn cast_ref_mut_u64(&mut self) -> &mut Vec<u64> {
        panic!("{}", self.type_error("cast_ref_mut_u64"))
    }
    default fn cast_ref_mut_f64(&mut self) -> &mut Vec<OrderedFloat<f64>> {
        panic!("{}", self.type_error("cast_ref_mut_f64"))
    }
    default fn cast_ref_mut_opt_f64(&mut self) -> &mut Vec<Option<OrderedFloat<f64>>> {
        panic!("{}", self.type_error("cast_ref_mut_opt_f64"))
    }
    default fn cast_ref_mut_usize(&mut self) -> &mut Vec<usize> {
        panic!("{}", self.type_error("cast_ref_mut_usize"))
    }
    default fn cast_ref_mut_i64(&mut self) -> &mut Vec<i64> {
        panic!("{}", self.type_error("cast_ref_mut_i64"))
    }
    default fn cast_ref_mut_u32(&mut self) -> &mut Vec<u32> {
        panic!("{}", self.type_error("cast_ref_mut_u32"))
    }
    default fn cast_ref_mut_u16(&mut self) -> &mut Vec<u16> {
        panic!("{}", self.type_error("cast_ref_mut_u16"))
    }
    default fn cast_ref_mut_u8(&mut self) -> &mut Vec<u8> {
        panic!("{}", self.type_error("cast_ref_mut_u8"))
    }
    default fn to_mixed(&self) -> Vec<Val<'a>> {
        panic!("{}", self.type_error("to_mixed"))
    }
    default fn cast_ref_mixed(&self) -> &[Val<'a>] {
        panic!("{}", self.type_error("cast_ref_mixed"))
    }
    default fn cast_ref_mut_mixed(&mut self) -> &mut Vec<Val<'a>> {
        panic!("{}", self.type_error("cast_ref_mut_mixed"))
    }
    default fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] {
        panic!("{}", self.type_error("cast_ref_str"))
    }
    default fn cast_ref_opt_str<'b>(&'b self) -> &'b [Option<&'a str>] {
        panic!("{}", self.type_error("cast_ref_opt_str"))
    }
}

impl<'a> Data<'a> for Vec<&'a str> {
    fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] {
        self
    }
    fn cast_ref_mut_str<'b>(&'b mut self) -> &'b mut Vec<&'a str> {
        self
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter().map(|s| Val::Str(s)).collect()
    }
}

impl<'a> Data<'a> for Vec<Option<&'a str>> {
    fn cast_ref_opt_str<'b>(&'b self) -> &'b [Option<&'a str>] {
        self
    }
    fn cast_ref_mut_opt_str<'b>(&'b mut self) -> &'b mut Vec<Option<&'a str>> {
        self
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter()
            .map(|s| match s {
                None => Val::Null,
                Some(s) => Val::Str(s),
            })
            .collect()
    }
}

impl<'a> Data<'a> for Vec<Val<'a>> {
    fn cast_ref_mixed<'b>(&'b self) -> &'b [Val<'a>] {
        self
    }
    fn cast_ref_mut_mixed<'b>(&'b mut self) -> &'b mut Vec<Val<'a>> {
        self
    }

    fn append_all(&mut self, other: &dyn Data<'a>, count: usize) -> Option<BoxedData<'a>> {
        if other.get_type() == EncodingType::Val {
            self.extend(other.cast_ref_mixed().iter().take(count));
        } else {
            self.extend(other.to_mixed().iter().take(count));
        }
        None
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.clone()
    }
}

impl<'a> Data<'a> for Vec<usize> {
    fn cast_ref_usize(&self) -> &[usize] {
        self
    }
    fn cast_ref_mut_usize(&mut self) -> &mut Vec<usize> {
        self
    }
}

impl<'a> Data<'a> for Vec<i64> {
    fn cast_ref_i64(&self) -> &[i64] {
        self
    }
    fn cast_ref_mut_i64(&mut self) -> &mut Vec<i64> {
        self
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter().map(|i| Val::Integer(*i)).collect()
    }
}

impl<'a> Data<'a> for Vec<u64> {
    fn cast_ref_u64(&self) -> &[u64] {
        self
    }
    fn cast_ref_mut_u64(&mut self) -> &mut Vec<u64> {
        self
    }
}

impl<'a> Data<'a> for Vec<u32> {
    fn cast_ref_u32(&self) -> &[u32] {
        self
    }
    fn cast_ref_mut_u32(&mut self) -> &mut Vec<u32> {
        self
    }
}

impl<'a> Data<'a> for Vec<u16> {
    fn cast_ref_u16(&self) -> &[u16] {
        self
    }
    fn cast_ref_mut_u16(&mut self) -> &mut Vec<u16> {
        self
    }
}

impl<'a> Data<'a> for Vec<u8> {
    fn cast_ref_u8(&self) -> &[u8] {
        self
    }
    fn cast_ref_mut_u8(&mut self) -> &mut Vec<u8> {
        self
    }
}

impl<'a> Data<'a> for Vec<OrderedFloat<f64>> {
    fn cast_ref_f64(&self) -> &[OrderedFloat<f64>] {
        self
    }
    fn cast_ref_mut_f64(&mut self) -> &mut Vec<OrderedFloat<f64>> {
        self
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter().map(|i| Val::Float(*i)).collect()
    }
}

impl<'a> Data<'a> for Vec<MergeOp> {
    fn cast_ref_merge_op(&self) -> &[MergeOp] {
        self
    }
    fn cast_ref_mut_merge_op(&mut self) -> &mut Vec<MergeOp> {
        self
    }
}

impl<'a> Data<'a> for Vec<Premerge> {
    fn cast_ref_premerge(&self) -> &[Premerge] {
        self
    }
    fn cast_ref_mut_premerge(&mut self) -> &mut Vec<Premerge> {
        self
    }
}

impl<'a, T: VecData<T> + 'a> Data<'a> for &'a [T] {
    fn len(&self) -> usize {
        <[T]>::len(self)
    }
    fn get_raw(&self, i: usize) -> RawVal {
        T::wrap_one(self[i])
    }
    fn get_type(&self) -> EncodingType {
        T::t()
    }
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedData<'b>
    where
        'a: 'b,
    {
        let to = min(to, self.len());
        Box::new(&self[from..to])
    }

    fn append_all(&mut self, other: &dyn Data<'a>, count: usize) -> Option<BoxedData<'a>> {
        let mut owned = Vec::from(*self);
        owned.append_all(other, count).or(Some(Box::new(owned)))
    }

    fn type_error(&self, func_name: &str) -> String {
        format!("[{:?}].{}", T::t(), func_name)
    }

    fn display(&self) -> String {
        format!("&{:?}{}", T::t(), display_slice(self, 120))
    }

    fn make_nullable(&mut self, present: &[u8]) -> BoxedData<'a> {
        Box::new(NullableVec {
            data: self.to_vec(),
            present: present.to_vec(),
        })
    }

    // Copied from Data and marked default because specialization demands it
    default fn cast_ref_merge_op(&self) -> &[MergeOp] {
        panic!("{}", self.type_error("cast_ref_merge_op"))
    }
    default fn cast_ref_premerge(&self) -> &[Premerge] {
        panic!("{}", self.type_error("cast_ref_merge_op"))
    }
    default fn cast_ref_str<'b>(&'b self) -> &'b [&'a str] {
        panic!("{}", self.type_error("cast_ref_str"))
    }
    default fn cast_ref_i64(&self) -> &[i64] {
        panic!("{}", self.type_error("cast_ref_i64"))
    }
    default fn cast_ref_u64(&self) -> &[u64] {
        panic!("{}", self.type_error("cast_ref_u64"))
    }
    default fn cast_ref_u32(&self) -> &[u32] {
        panic!("{}", self.type_error("cast_ref_u32"))
    }
    default fn cast_ref_u16(&self) -> &[u16] {
        panic!("{}", self.type_error("cast_ref_u16"))
    }
    default fn cast_ref_u8(&self) -> &[u8] {
        panic!("{}", self.type_error("cast_ref_u8"))
    }
    default fn cast_ref_f64(&self) -> &[OrderedFloat<f64>] {
        panic!("{}", self.type_error("cast_ref_f64"))
    }
    default fn cast_ref_usize(&self) -> &[usize] {
        panic!("{}", self.type_error("cast_ref_usize"))
    }
    default fn cast_ref_mixed(&self) -> &[Val<'a>] {
        panic!("{}", self.type_error("cast_ref_mixed"))
    }
    default fn cast_ref_opt_str<'b>(&'b self) -> &'b [Option<&'a str>] {
        panic!("{}", self.type_error("cast_ref_opt_str"))
    }
    default fn cast_ref_opt_f64(&self) -> &[Option<OrderedFloat<f64>>] {
        panic!("{}", self.type_error("cast_ref_opt_f64"))
    }
    default fn to_mixed(&self) -> Vec<Val<'a>> {
        panic!("{}", self.type_error("to_mixed"))
    }
}

impl<'a> Data<'a> for &'a [&'a str] {
    fn cast_ref_str(&self) -> &[&'a str] {
        self
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter().map(|s| Val::Str(s)).collect()
    }
}

impl<'a> Data<'a> for &'a [Option<&'a str>] {
    fn cast_ref_opt_str(&self) -> &[Option<&'a str>] {
        self
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter()
            .map(|s| match s {
                None => Val::Null,
                Some(s) => Val::Str(s),
            })
            .collect()
    }
}

impl<'a> Data<'a> for &'a [Val<'a>] {
    fn cast_ref_mixed(&self) -> &[Val<'a>] {
        self
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.to_vec()
    }
}

impl<'a> Data<'a> for &'a [usize] {
    fn cast_ref_usize(&self) -> &[usize] {
        self
    }
}

impl<'a> Data<'a> for &'a [i64] {
    fn cast_ref_i64(&self) -> &[i64] {
        self
    }
    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter().map(|i| Val::Integer(*i)).collect()
    }
}

impl<'a> Data<'a> for &'a [u64] {
    fn cast_ref_u64(&self) -> &[u64] {
        self
    }
}

impl<'a> Data<'a> for &'a [u32] {
    fn cast_ref_u32(&self) -> &[u32] {
        self
    }
}

impl<'a> Data<'a> for &'a [u16] {
    fn cast_ref_u16(&self) -> &[u16] {
        self
    }
}

impl<'a> Data<'a> for &'a [u8] {
    fn cast_ref_u8(&self) -> &[u8] {
        self
    }
}

impl<'a> Data<'a> for &'a [OrderedFloat<f64>] {
    fn cast_ref_f64(&self) -> &[OrderedFloat<f64>] {
        self
    }

    fn to_mixed(&self) -> Vec<Val<'a>> {
        self.iter().map(|i| Val::Float(*i)).collect()
    }
}

impl<'a> Data<'a> for &'a [MergeOp] {
    fn cast_ref_merge_op(&self) -> &[MergeOp] {
        self
    }
}

impl<'a> Data<'a> for &'a [Premerge] {
    fn cast_ref_premerge(&self) -> &[Premerge] {
        self
    }
}

impl<'a> Data<'a> for usize {
    fn len(&self) -> usize {
        *self
    }
    fn get_raw(&self, i: usize) -> RawVal {
        assert!(i < *self);
        RawVal::Null
    }
    fn get_type(&self) -> EncodingType {
        EncodingType::Null
    }
    fn type_error(&self, func_name: &str) -> String {
        format!("EmptyVector.{}", func_name)
    }

    fn append_all(&mut self, other: &dyn Data<'a>, count: usize) -> Option<BoxedData<'a>> {
        if other.get_type() == EncodingType::Null {
            *self += count;
            None
        } else {
            let mut upcast = Box::new(vec![Val::Null; *self]);
            upcast.append_all(other, count).or(Some(upcast))
        }
    }

    fn cast_ref_mut_null(&mut self) -> &mut usize {
        self
    }

    fn to_mixed(&self) -> Vec<Val<'a>> {
        vec![Val::Null; *self]
    }

    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedData<'b>
    where
        'a: 'b,
    {
        Box::new(min(to, *self) - from)
    }

    fn display(&self) -> String {
        format!("null({})", self)
    }
}

impl<'a> Data<'a> for RawVal {
    fn len(&self) -> usize {
        0
    }
    fn get_raw(&self, _i: usize) -> RawVal {
        self.clone()
    }
    fn get_type(&self) -> EncodingType {
        EncodingType::ConstVal
    }
    fn type_error(&self, func_name: &str) -> String {
        format!("Constant.{}", func_name)
    }
    fn append_all(&mut self, _other: &dyn Data<'a>, _count: usize) -> Option<BoxedData<'a>> {
        panic!("Constant.extend")
    }
    fn slice_box<'b>(&'b self, _: usize, _: usize) -> BoxedData<'b>
    where
        'a: 'b,
    {
        Box::new(self.clone())
    }
    fn cast_ref_scalar_string(&self) -> &string::String {
        match self {
            RawVal::Str(s) => s,
            _ => panic!("{}.cast_str_const", &self),
        }
    }
    fn cast_scalar_i64(&self) -> i64 {
        match self {
            RawVal::Int(i) => *i,
            _ => panic!("{}.cast_i64_const", &self),
        }
    }
    fn cast_scalar_f64(&self) -> of64 {
        match self {
            RawVal::Float(f) => *f,
            _ => panic!("{}.cast_f64_const", &self),
        }
    }

    fn display(&self) -> String {
        format!("Scalar({})", self)
    }
}
