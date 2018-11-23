use std::cmp::min;

use ingest::raw_val::RawVal;

use engine::data_types::*;

pub struct NullableVec<T> {
    data: Vec<T>,
    present: Vec<u8>,
}

pub struct RefNullableVec<'a, T> {
    data: &'a [T],
    present: &'a [u8],
}

impl<'a, T: VecData<T> + 'a> Data<'a> for NullableVec<T> {
    fn len(&self) -> usize { self.data.len() }
    fn get_raw(&self, i: usize) -> RawVal {
        if self.present.is_set(i) { T::wrap_one(self.data[i]) } else { RawVal::Null }
    }
    fn get_type(&self) -> EncodingType { T::t().nullable() }
    fn type_error(&self, func_name: &str) -> String { format!("NullableVec<{:?}>.{}", T::t(), func_name) }
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedData<'b> where 'a: 'b {
        if from % 8 != 0 { panic!("Misaligned nullable slice box!") }
        let to = min(to, self.len());
        Box::new(RefNullableVec {
            data: &self.data[from..to],
            present: &self.present[from >> 3..to >> 3],
        })
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
            // TODO(clemens): align bits
            let x = T::unwrap(other);
            self.data.extend_from_slice(&x[0..min(x.len(), count)]);
            None
        }
    }

    fn display(&self) -> String { format!("NullableVec") }//<{:?}>{}", T::t(), display_slice(&self, 120)) }
}

impl<'a, T: VecData<T> + 'a> Data<'a> for RefNullableVec<'a, T> {
    fn len(&self) -> usize { self.data.len() }
    fn get_raw(&self, i: usize) -> RawVal {
        if self.present.is_set(i) { T::wrap_one(self.data[i]) } else { RawVal::Null }
    }
    fn get_type(&self) -> EncodingType { T::t().nullable() }
    fn type_error(&self, func_name: &str) -> String { format!("RefNullableVec<{:?}>.{}", T::t(), func_name) }
    fn slice_box<'b>(&'b self, from: usize, to: usize) -> BoxedData<'b> where 'a: 'b {
        if from % 8 != 0 { panic!("Misaligned nullable slice box!") }
        let to = min(to, self.len());
        Box::new(RefNullableVec {
            data: &self.data[from..to],
            present: &self.present[from >> 3..to >> 3],
        })
    }

    fn append_all(&mut self, _: &Data<'a>, _: usize) -> Option<BoxedData<'a>> {
        panic!("append_all on borrow")
    }

    fn display(&self) -> String { format!("RepNullableVec") }//<{:?}>{}", T::t(), display_slice(&self, 120)) }
}

trait BitVec {
    fn is_set(&self, index: usize) -> bool;
}

impl BitVec for Vec<u8> {
    fn is_set(&self, index: usize) -> bool {
        self[index >> 3] & (1 << (index as u8 & 7)) > 0
    }
}

impl<'a> BitVec for &'a [u8] {
    fn is_set(&self, index: usize) -> bool {
        self[index >> 3] & (1 << (index as u8 & 7)) > 0
    }
}
