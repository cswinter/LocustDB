use std::convert::From;
use std::{u8, u16, u32};

use bit_vec::BitVec;
use engine::typed_vec::TypedVec;
use engine::types::*;
use heapsize::HeapSizeOf;
use ingest::raw_val::RawVal;
use mem_store::column::{ColumnData, ColumnCodec};
use mem_store::point_codec::PointCodec;
use num::traits::NumCast;


pub struct IntegerColumn {
    values: Vec<i64>,
}

impl IntegerColumn {
    // TODO(clemens): do not subtract offset if it does not change encoding size
    pub fn new_boxed(mut values: Vec<i64>, min: i64, max: i64) -> Box<ColumnData> {
        let maximum = (max - min) as usize;
        if max - min <= From::from(u8::MAX) {
            Box::new(IntegerOffsetColumn::<u8>::new(values, min, maximum))
        } else if max - min <= From::from(u16::MAX) {
            Box::new(IntegerOffsetColumn::<u16>::new(values, min, maximum))
        } else if max - min <= From::from(u32::MAX) {
            Box::new(IntegerOffsetColumn::<u32>::new(values, min, maximum))
        } else {
            values.shrink_to_fit();
            Box::new(IntegerColumn { values })
        }
    }
}

impl ColumnData for IntegerColumn {
    fn collect_decoded(&self) -> TypedVec {
        TypedVec::Integer(self.values.clone())
    }

    fn filter_decode<'a>(&'a self, filter: &BitVec) -> TypedVec {
        let mut results = Vec::with_capacity(self.values.len());
        for (i, select) in filter.iter().enumerate() {
            if select {
                results.push(self.values[i]);
            }
        }
        TypedVec::Integer(results)
    }

    fn index_decode<'a>(&'a self, filter: &[usize]) -> TypedVec {
        let mut results = Vec::with_capacity(filter.len());
        for &i in filter {
            results.push(self.values[i]);
        }
        TypedVec::Integer(results)
    }

    fn basic_type(&self) -> BasicType { BasicType::Integer }

    fn len(&self) -> usize { self.values.len() }
}


struct IntegerOffsetColumn<T: IntLike> {
    values: Vec<T>,
    offset: i64,
    maximum: usize,
}

impl<T: IntLike> IntegerOffsetColumn<T> {
    fn new(values: Vec<i64>, offset: i64, maximum: usize) -> IntegerOffsetColumn<T> {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push(T::from(v - offset).unwrap());
        }
        IntegerOffsetColumn {
            values: encoded_vals,
            offset,
            maximum,
        }
    }
}

impl<T: IntLike> ColumnData for IntegerOffsetColumn<T> {
    fn collect_decoded(&self) -> TypedVec {
        self.decode(&self.values)
    }

    fn filter_decode(&self, filter: &BitVec) -> TypedVec {
        let mut result = Vec::with_capacity(self.values.len());
        for (i, select) in filter.iter().enumerate() {
            if select {
                result.push(self.values[i].to_i64().unwrap() + self.offset);
            }
        }
        TypedVec::Integer(result)
    }

    fn index_decode<'a>(&'a self, filter: &[usize]) -> TypedVec {
        PointCodec::index_decode(self, &self.values, filter)
    }

    fn basic_type(&self) -> BasicType {
        BasicType::Integer
    }

    fn to_codec(&self) -> Option<&ColumnCodec> { Some(self as &ColumnCodec) }

    fn len(&self) -> usize { self.values.len() }
}

impl<T: IntLike> PointCodec<T> for IntegerOffsetColumn<T> {
    fn decode(&self, data: &[T]) -> TypedVec {
        let mut result = Vec::with_capacity(self.values.len());
        for value in data {
            result.push(value.to_i64().unwrap() + self.offset);
        }
        TypedVec::Integer(result)
    }

    fn index_decode<'a>(&'a self, data: &[T], filter: &[usize]) -> TypedVec {
        let mut result = Vec::with_capacity(filter.len());
        for &i in filter {
            result.push(data[i].to_i64().unwrap() + self.offset);
        }
        TypedVec::Integer(result)
    }

    fn to_raw(&self, elem: T) -> RawVal {
        RawVal::Int(elem.to_i64().unwrap() + self.offset)
    }

    fn max_cardinality(&self) -> usize { self.maximum }

    fn is_order_preserving(&self) -> bool { true }
}

impl<T: IntLike> ColumnCodec for IntegerOffsetColumn<T> {
    fn get_encoded(&self) -> TypedVec {
        T::borrowed_typed_vec(&self.values, self as &PointCodec<T>)
    }

    fn filter_encoded(&self, filter: &BitVec) -> TypedVec {
        let filtered_values = self.values.iter().zip(filter.iter())
            .filter(|&(_, select)| select)
            .map(|(i, _)| *i)
            .collect();
        T::typed_vec(filtered_values, self as &PointCodec<T>)
    }

    fn index_encoded<'a>(&'a self, filter: &[usize]) -> TypedVec {
        let mut result = Vec::with_capacity(filter.len());
        for &i in filter {
            result.push(self.values[i]);
        }
        T::typed_vec(result, self as &PointCodec<T>)
    }

    fn encode_int(&self, val: i64) -> RawVal {
        // TODO(clemens): Underflow. Check for this in query planner?
        RawVal::Int(val - self.offset)
    }

    fn is_summation_preserving(&self) -> bool { self.offset == 0 }

    fn encoding_type(&self) -> EncodingType { T::t() }
}

impl HeapSizeOf for IntegerColumn {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}

trait IntLike: NumCast + HeapSizeOf + Copy + Send + Sync {
    fn borrowed_typed_vec<'a>(values: &'a [Self], codec: &'a PointCodec<Self>) -> TypedVec<'a>;
    fn typed_vec(values: Vec<Self>, codec: &PointCodec<Self>) -> TypedVec;
    fn t() -> EncodingType;
}

impl IntLike for u8 {
    fn borrowed_typed_vec<'a>(values: &'a [Self], codec: &'a PointCodec<Self>) -> TypedVec<'a> {
        TypedVec::BorrowedEncodedU8(values, codec)
    }

    fn typed_vec(values: Vec<Self>, codec: &PointCodec<Self>) -> TypedVec {
        TypedVec::EncodedU8(values, codec)
    }

    fn t() -> EncodingType { EncodingType::U8 }
}

impl IntLike for u16 {
    fn borrowed_typed_vec<'a>(values: &'a [Self], codec: &'a PointCodec<Self>) -> TypedVec<'a> {
        TypedVec::BorrowedEncodedU16(values, codec)
    }

    fn typed_vec(values: Vec<Self>, codec: &PointCodec<Self>) -> TypedVec {
        TypedVec::EncodedU16(values, codec)
    }

    fn t() -> EncodingType { EncodingType::U16 }
}

impl IntLike for u32 {
    fn borrowed_typed_vec<'a>(values: &'a [Self], codec: &'a PointCodec<Self>) -> TypedVec<'a> {
        TypedVec::BorrowedEncodedU32(values, codec)
    }

    fn typed_vec(values: Vec<Self>, codec: &PointCodec<Self>) -> TypedVec {
        TypedVec::EncodedU32(values, codec)
    }

    fn t() -> EncodingType { EncodingType::U32 }
}

impl<T: IntLike> HeapSizeOf for IntegerOffsetColumn<T> {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}
