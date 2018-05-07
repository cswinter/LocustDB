use std::convert::From;
use std::{u8, u16, u32};

use bit_vec::BitVec;
use engine::typed_vec::{BoxedVec, TypedVec};
use engine::types::*;
use engine::*;
use heapsize::HeapSizeOf;
use ingest::raw_val::RawVal;
use mem_store::column::{ColumnData, ColumnCodec};
use mem_store::point_codec::PointCodec;


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
    fn collect_decoded(&self) -> BoxedVec {
        TypedVec::owned(self.values.clone())
    }

    fn filter_decode<'a>(&'a self, filter: &BitVec) -> BoxedVec {
        let mut results = Vec::with_capacity(self.values.len());
        for (i, select) in filter.iter().enumerate() {
            if select {
                results.push(self.values[i]);
            }
        }
        TypedVec::owned(results)
    }

    fn index_decode<'a>(&'a self, filter: &[usize]) -> BoxedVec {
        let mut results = Vec::with_capacity(filter.len());
        for &i in filter {
            results.push(self.values[i]);
        }
        TypedVec::owned(results)
    }

    fn basic_type(&self) -> BasicType { BasicType::Integer }

    fn len(&self) -> usize { self.values.len() }
}


struct IntegerOffsetColumn<T> {
    values: Vec<T>,
    offset: i64,
    maximum: usize,
}

impl<T: IntVecType<T>> IntegerOffsetColumn<T> {
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

impl<T: IntVecType<T>> ColumnData for IntegerOffsetColumn<T> {
    fn collect_decoded(&self) -> BoxedVec {
        self.decode(&self.values)
    }

    fn filter_decode(&self, filter: &BitVec) -> BoxedVec {
        let mut result = Vec::with_capacity(self.values.len());
        for (i, select) in filter.iter().enumerate() {
            if select {
                result.push(self.values[i].to_i64().unwrap() + self.offset);
            }
        }
        TypedVec::owned(result)
    }

    fn index_decode<'b>(&'b self, filter: &[usize]) -> BoxedVec {
        PointCodec::index_decode(self, &self.values, filter)
    }

    fn basic_type(&self) -> BasicType {
        BasicType::Integer
    }

    fn to_codec(&self) -> Option<&ColumnCodec> { Some(self as &ColumnCodec) }

    fn len(&self) -> usize { self.values.len() }
}

impl<T: IntVecType<T>> PointCodec<T> for IntegerOffsetColumn<T> {
    fn decode(&self, data: &[T]) -> BoxedVec {
        let mut result = Vec::with_capacity(self.values.len());
        for value in data {
            result.push(value.to_i64().unwrap() + self.offset);
        }
        TypedVec::owned(result)
    }

    fn index_decode<'b>(&'b self, data: &[T], filter: &[usize]) -> BoxedVec {
        let mut result = Vec::with_capacity(filter.len());
        for &i in filter {
            result.push(data[i].to_i64().unwrap() + self.offset);
        }
        TypedVec::owned(result)
    }

    fn to_raw(&self, elem: T) -> RawVal {
        RawVal::Int(elem.to_i64().unwrap() + self.offset)
    }

    fn max_cardinality(&self) -> usize { self.maximum }
}

impl<'a, T: IntVecType<T> + 'a> ColumnCodec for IntegerOffsetColumn<T> {
    fn get_encoded(&self) -> BoxedVec {
        TypedVec::borrowed(&self.values)
    }

    fn unwrap_decode<'b>(&'b self, data: &TypedVec<'b>) -> BoxedVec<'b> {
        self.decode(T::unwrap(data))
    }

    fn filter_encoded(&self, filter: &BitVec) -> BoxedVec {
        let filtered_values = self.values.iter().zip(filter.iter())
            .filter(|&(_, select)| select)
            .map(|(i, _)| *i)
            .collect();
        TypedVec::owned(filtered_values)
    }

    fn index_encoded<'b>(&'b self, filter: &[usize]) -> BoxedVec {
        let mut result = Vec::with_capacity(filter.len());
        for &i in filter {
            result.push(self.values[i]);
        }
        TypedVec::owned(result)
    }

    fn encode_int(&self, val: i64) -> RawVal {
        // TODO(clemens): Underflow. Check for this in query planner?
        RawVal::Int(val - self.offset)
    }

    fn is_summation_preserving(&self) -> bool { self.offset == 0 }
    fn is_order_preserving(&self) -> bool { true }
    fn is_positive_integer(&self) -> bool { true }
    fn encoding_type(&self) -> EncodingType { T::t() }
    fn encoding_range(&self) -> Option<(i64, i64)> { Some((0, self.maximum as i64)) }
}

impl HeapSizeOf for IntegerColumn {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}

impl<T:     HeapSizeOf> HeapSizeOf for IntegerOffsetColumn<T> {
    fn heap_size_of_children(&self) -> usize {
        self.values.heap_size_of_children()
    }
}

