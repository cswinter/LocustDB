use std::convert::From;
use std::fmt;
use std::marker::PhantomData;
use std::{u8, u16, u32};

use heapsize::HeapSizeOf;

use engine::*;
use engine::typed_vec::TypedVec;
use engine::types::*;
use ingest::raw_val::RawVal;
use mem_store::*;

pub struct IntegerColumn;

impl IntegerColumn {
    pub fn new_boxed(name: &str, mut values: Vec<i64>, min: i64, max: i64) -> Box<Column> {
        let range = Some((0, max - min));
        if min >= 0 && max <= From::from(u8::MAX) {
            Column::encoded(name, IntegerColumn::encode::<u8>(values, 0), IntegerCodec::<u8>::new(), Some((min, max)))
        } else if max - min <= From::from(u8::MAX) {
            Column::encoded(name, IntegerColumn::encode::<u8>(values, min), IntegerOffsetCodec::<u8>::new(min), range)
        } else if min >= 0 && max <= From::from(u16::MAX) {
            Column::encoded(name, IntegerColumn::encode::<u16>(values, 0), IntegerCodec::<u16>::new(), Some((min, max)))
        } else if max - min <= From::from(u16::MAX) {
            Column::encoded(name, IntegerColumn::encode::<u16>(values, min), IntegerOffsetCodec::<u16>::new(min), range)
        } else if min >= 0 && max <= From::from(u32::MAX) {
            Column::encoded(name, IntegerColumn::encode::<u32>(values, 0), IntegerCodec::<u32>::new(), Some((min, max)))
        } else if max - min <= From::from(u32::MAX) {
            Column::encoded(name, IntegerColumn::encode::<u32>(values, min), IntegerOffsetCodec::<u32>::new(min), range)
        } else {
            values.shrink_to_fit();
            Column::plain(name, values, Some((min, max)))
        }
    }


    pub fn encode<T: IntVecType<T>>(values: Vec<i64>, offset: i64) -> Vec<T> {
        let mut encoded_vals = Vec::with_capacity(values.len());
        for v in values {
            encoded_vals.push(T::from(v - offset).unwrap());
        }
        encoded_vals
    }
}

#[derive(Clone, Copy)]
pub struct IntegerOffsetCodec<T> {
    offset: i64,
    t: PhantomData<T>,
}

impl<T> IntegerOffsetCodec<T> {
    pub fn new(offset: i64) -> IntegerOffsetCodec<T> {
        IntegerOffsetCodec {
            offset,
            t: PhantomData,
        }
    }
}

impl<'a, T: IntVecType<T>> ColumnCodec<'a> for IntegerOffsetCodec<T> {
    fn unwrap_decode<'b>(&self, data: &TypedVec<'b>, buffer: &mut TypedVec<'b>) where 'a: 'b {
        let data = T::unwrap(data);
        let result = <i64>::unwrap_mut(buffer);
        for value in data {
            result.push(value.to_i64().unwrap() + self.offset);
        }
    }

    fn encode_int(&self, val: i64) -> RawVal {
        // TODO(clemens): Underflow. Check for this in query planner?
        RawVal::Int(val - self.offset)
    }

    fn is_summation_preserving(&self) -> bool { self.offset == 0 }
    fn is_order_preserving(&self) -> bool { true }
    fn is_positive_integer(&self) -> bool { true }
    fn decoded_type(&self) -> BasicType { BasicType::Integer }
    fn encoding_type(&self) -> EncodingType { T::t() }
    // TODO(clemens): under/overflow?
    fn decode_range(&self, (min, max): (i64, i64)) -> Option<(i64, i64)> { Some((min + self.offset, max + self.offset)) }
}

impl<T: IntVecType<T>> fmt::Debug for IntegerOffsetCodec<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if f.alternate() {
            write!(f, "Subtract({})", self.offset)
        } else {
            write!(f, "Subtract(Int)")
        }
    }
}

impl<T> HeapSizeOf for IntegerOffsetCodec<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}


#[derive(Clone, Copy)]
pub struct IntegerCodec<T> {
    t: PhantomData<T>,
}

impl<T> IntegerCodec<T> {
    pub fn new() -> IntegerCodec<T> {
        IntegerCodec {
            t: PhantomData,
        }
    }
}

impl<'a, T: IntVecType<T>> ColumnCodec<'a> for IntegerCodec<T> {
    fn unwrap_decode<'b>(&self, data: &TypedVec<'b>, buffer: &mut TypedVec<'b>) where 'a: 'b {
        let data = T::unwrap(data);
        let result = <i64>::unwrap_mut(buffer);
        for value in data {
            result.push(value.to_i64().unwrap());
        }
    }

    fn encode_int(&self, val: i64) -> RawVal {
        RawVal::Int(val)
    }

    fn is_summation_preserving(&self) -> bool { true }
    fn is_order_preserving(&self) -> bool { true }
    fn is_positive_integer(&self) -> bool { true }
    fn decoded_type(&self) -> BasicType { BasicType::Integer }
    fn encoding_type(&self) -> EncodingType { T::t() }
    fn decode_range(&self, range: (i64, i64)) -> Option<(i64, i64)> { Some(range) }
}

impl<T: IntVecType<T>> fmt::Debug for IntegerCodec<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "IntCast")
    }
}

impl<T> HeapSizeOf for IntegerCodec<T> {
    fn heap_size_of_children(&self) -> usize {
        0
    }
}
