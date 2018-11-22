use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::result::Result;

use QueryError;
use engine::data_types::*;
use ingest::raw_val::RawVal;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct BufferRef<T> {
    pub i: usize,
    pub name: &'static str,
    pub t: PhantomData<T>,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TypedBufferRef {
    pub buffer: BufferRef<Any>,
    pub tag: EncodingType,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Any {}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Scalar<T> { t: PhantomData<T> }

pub fn error_buffer_ref(name: &'static str) -> BufferRef<Any> {
    BufferRef {
        i: 0xdead_beef,
        name,
        t: PhantomData,
    }
}

impl BufferRef<Any> {
    pub fn merge_op(self) -> BufferRef<MergeOp> { self.transmute() }
    pub fn premerge(self) -> BufferRef<Premerge> { self.transmute() }
    pub fn raw_val(self) -> BufferRef<RawVal> { self.transmute() }
    pub fn i64(self) -> BufferRef<i64> { self.transmute() }
    pub fn u64(self) -> BufferRef<u64> { self.transmute() }
    pub fn u32(self) -> BufferRef<u32> { self.transmute() }
    pub fn u16(self) -> BufferRef<u16> { self.transmute() }
    pub fn u8(self) -> BufferRef<u8> { self.transmute() }

    pub fn scalar_i64(self) -> BufferRef<Scalar<i64>> { self.transmute() }
    pub fn scalar_str<'a>(self) -> BufferRef<Scalar<&'a str>> { self.transmute() }
    pub fn scalar_string<'a>(self) -> BufferRef<Scalar<String>> { self.transmute() }

    pub fn string(self) -> BufferRef<String> { self.transmute() }
    pub fn str<'a>(self) -> BufferRef<&'a str> { self.transmute() }
    pub fn usize(self) -> BufferRef<usize> { self.transmute() }
    fn transmute<T>(self) -> BufferRef<T> { unsafe { mem::transmute(self) } }
}

impl BufferRef<u32> {
    pub fn tagged(&self) -> TypedBufferRef {
        TypedBufferRef::new(self.any(), EncodingType::U32)
    }
}

impl BufferRef<u8> {
    pub fn tagged(&self) -> TypedBufferRef {
        TypedBufferRef::new(self.any(), EncodingType::U8)
    }
}

impl BufferRef<i64> {
    pub fn tagged(&self) -> TypedBufferRef {
        TypedBufferRef::new(self.any(), EncodingType::I64)
    }
}

impl BufferRef<Scalar<i64>> {
    pub fn tagged(&self) -> TypedBufferRef {
        TypedBufferRef::new(self.any(), EncodingType::ScalarI64)
    }
}

impl BufferRef<usize> {
    pub fn tagged(&self) -> TypedBufferRef {
        TypedBufferRef::new(self.any(), EncodingType::USize)
    }
}

impl<T: Clone> BufferRef<T> {
    pub fn any(&self) -> BufferRef<Any> { unsafe { mem::transmute(self.clone()) } }
}

impl<T> fmt::Display for BufferRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}_{}", self.name, self.i)
    }
}

impl TypedBufferRef {
    pub fn new(buffer: BufferRef<Any>, tag: EncodingType) -> TypedBufferRef {
        TypedBufferRef { buffer, tag }
    }

    pub fn any(&self) -> BufferRef<Any> { self.buffer.any() }

    pub fn str<'a>(&self) -> Result<BufferRef<&'a str>, QueryError> {
        ensure!(self.tag == EncodingType::Str, "{:?} != Str", self.tag);
        Ok(self.buffer.str())
    }

    pub fn i64(&self) -> Result<BufferRef<i64>, QueryError> {
        ensure!(self.tag == EncodingType::I64, "{:?} != I64", self.tag);
        Ok(self.buffer.i64())
    }

    pub fn u64(&self) -> Result<BufferRef<u64>, QueryError> {
        ensure!(self.tag == EncodingType::U64, "{:?} != U64", self.tag);
        Ok(self.buffer.u64())
    }

    pub fn u32(&self) -> Result<BufferRef<u32>, QueryError> {
        ensure!(self.tag == EncodingType::U32, "{:?} != U32", self.tag);
        Ok(self.buffer.u32())
    }

    pub fn u8(&self) -> Result<BufferRef<u8>, QueryError> {
        ensure!(self.tag == EncodingType::U8, "{:?} != U8", self.tag);
        Ok(self.buffer.u8())
    }

    pub fn usize(&self) -> Result<BufferRef<usize>, QueryError> {
        ensure!(self.tag == EncodingType::USize, "{:?} != USize", self.tag);
        Ok(self.buffer.usize())
    }

    pub fn merge_op(&self) -> Result<BufferRef<MergeOp>, QueryError> {
        ensure!(self.tag == EncodingType::MergeOp, "{:?} != MergeOp", self.tag);
        Ok(self.buffer.merge_op())
    }

    pub fn premerge(&self) -> Result<BufferRef<Premerge>, QueryError> {
        ensure!(self.tag == EncodingType::Premerge, "{:?} != Premerge", self.tag);
        Ok(self.buffer.premerge())
    }


    // TODO(clemens): better typing for Constants
    pub fn raw_val(&self) -> Result<BufferRef<RawVal>, QueryError> {
        // ensure!(self.tag == EncodingType::Str, "{:?} != Str", self.tag);
        Ok(self.buffer.raw_val())
    }

    pub fn string(&self) -> Result<BufferRef<String>, QueryError> {
        ensure!(self.tag == EncodingType::Val, "{:?} != Val", self.tag);
        Ok(self.buffer.string())
    }

    pub fn scalar_i64(&self) -> Result<BufferRef<Scalar<i64>>, QueryError> {
        ensure!(self.tag == EncodingType::ScalarI64, "{:?} != ScalarI64", self.tag);
        Ok(self.buffer.scalar_i64())
    }

    pub fn scalar_str<'a, 'b>(&'b self) -> Result<BufferRef<Scalar<&'a str>>, QueryError> {
        ensure!(self.tag == EncodingType::ScalarStr, "{:?} != ScalarStr", self.tag);
        Ok(self.buffer.scalar_str())
    }

    pub fn scalar_string(&self) -> Result<BufferRef<Scalar<String>>, QueryError> {
        ensure!(self.tag == EncodingType::ScalarString, "{:?} != ScalaString", self.tag);
        Ok(self.buffer.scalar_string())
    }
}
