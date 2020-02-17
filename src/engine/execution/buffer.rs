use engine::data_types::*;
use ingest::raw_val::RawVal;
use mem_store::value::Val;
use QueryError;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::result::Result;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct BufferRef<T> {
    pub i: usize,
    pub name: &'static str,
    pub t: PhantomData<T>,
}

impl<T: Clone> Copy for BufferRef<T> {}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TypedBufferRef {
    pub buffer: BufferRef<Any>,
    pub tag: EncodingType,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Any {}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Scalar<T> { t: PhantomData<T> }

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Nullable<T> { t: PhantomData<T> }

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

    pub fn cast_nullable_any(&self) -> BufferRef<Nullable<Any>> { self.transmute() }

    pub fn nullable_u8(self) -> BufferRef<Nullable<u8>> { self.transmute() }
    pub fn nullable_u16(self) -> BufferRef<Nullable<u16>> { self.transmute() }
    pub fn nullable_u32(self) -> BufferRef<Nullable<u32>> { self.transmute() }
    pub fn nullable_i64(self) -> BufferRef<Nullable<i64>> { self.transmute() }
    pub fn nullable_str<'a>(self) -> BufferRef<Nullable<&'a str>> { self.transmute() }

    pub fn scalar_i64(self) -> BufferRef<Scalar<i64>> { self.transmute() }
    pub fn scalar_str<'a>(self) -> BufferRef<Scalar<&'a str>> { self.transmute() }
    pub fn scalar_string(self) -> BufferRef<Scalar<String>> { self.transmute() }

    pub fn val_rows<'a>(self) -> BufferRef<ValRows<'a>> { self.transmute() }
    pub fn val<'a>(self) -> BufferRef<Val<'a>> { self.transmute() }

    pub fn string(self) -> BufferRef<String> { self.transmute() }
    pub fn str<'a>(self) -> BufferRef<&'a str> { self.transmute() }
    pub fn opt_str<'a>(self) -> BufferRef<Option<&'a str>> { self.transmute() }
    pub fn usize(self) -> BufferRef<usize> { self.transmute() }
    fn transmute<T>(self) -> BufferRef<T> { unsafe { mem::transmute(self) } }
}

impl From<TypedBufferRef> for BufferRef<u32> {
    fn from(buffer: TypedBufferRef) -> BufferRef<u32> { buffer.u32().unwrap() }
}

impl From<TypedBufferRef> for BufferRef<i64> {
    fn from(buffer: TypedBufferRef) -> BufferRef<i64> { buffer.i64().unwrap() }
}

// this is a temporary hack because there is no buffer type for ByteSlices and can be removed once there is
impl From<BufferRef<Any>> for TypedBufferRef {
    fn from(buffer: BufferRef<Any>) -> TypedBufferRef {
        TypedBufferRef::new(buffer, EncodingType::Null)
    }
}

impl From<BufferRef<u32>> for TypedBufferRef {
    fn from(buffer: BufferRef<u32>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::U32)
    }
}

impl From<BufferRef<u8>> for TypedBufferRef {
    fn from(buffer: BufferRef<u8>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::U8)
    }
}

impl From<BufferRef<Nullable<i64>>> for TypedBufferRef {
    fn from(buffer: BufferRef<Nullable<i64>>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::NullableI64)
    }
}

impl<'a> From<BufferRef<&'a str>> for TypedBufferRef {
    fn from(buffer: BufferRef<&'a str>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::Str)
    }
}

impl From<BufferRef<i64>> for TypedBufferRef {
    fn from(buffer: BufferRef<i64>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::I64)
    }
}

impl<'a> From<BufferRef<Scalar<&'a str>>> for TypedBufferRef {
    fn from(buffer: BufferRef<Scalar<&'a str>>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::ScalarStr)
    }
}

impl From<BufferRef<Scalar<i64>>> for TypedBufferRef {
    fn from(buffer: BufferRef<Scalar<i64>>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::ScalarI64)
    }
}

impl From<BufferRef<usize>> for TypedBufferRef {
    fn from(buffer: BufferRef<usize>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::USize)
    }
}

impl<'a> From<BufferRef<ValRows<'a>>> for TypedBufferRef {
    fn from(buffer: BufferRef<ValRows<'a>>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::ValRows)
    }
}

impl<'a> From<BufferRef<Val<'a>>> for TypedBufferRef {
    fn from(buffer: BufferRef<Val<'a>>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::Val)
    }
}

impl From<BufferRef<MergeOp>> for TypedBufferRef {
    fn from(buffer: BufferRef<MergeOp>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::MergeOp)
    }
}

impl From<BufferRef<Premerge>> for TypedBufferRef {
    fn from(buffer: BufferRef<Premerge>) -> TypedBufferRef {
        TypedBufferRef::new(buffer.any(), EncodingType::Premerge)
    }
}

impl<T> BufferRef<Nullable<T>> {
    pub fn cast_non_nullable(self) -> BufferRef<T> { unsafe { mem::transmute(self) } }
    pub fn nullable_any(self) -> BufferRef<Nullable<Any>> { unsafe { mem::transmute(self) } }
}

impl<T: Clone> BufferRef<T> {
    #[allow(clippy::clone_on_copy)]
    pub fn any(&self) -> BufferRef<Any> { unsafe { mem::transmute(self.clone()) } }
}

impl<T> fmt::Display for BufferRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", color_code(&format!("{}_{}", self.name, self.i), self.i))
    }
}

fn color_code(s: &str, i: usize) -> String {
    let colors = vec![
        "\x1b[31m",
        "\x1b[32m",
        "\x1b[33m",
        "\x1b[34m",
        "\x1b[35m",
        "\x1b[36m",
    ];
    if std::env::var("COLOR").is_ok() {
        format!("{}{}\x1b[0m", colors[i % colors.len()], s)
    } else {
        s.to_string()
    }
}

impl TypedBufferRef {
    pub fn new(buffer: BufferRef<Any>, tag: EncodingType) -> TypedBufferRef {
        TypedBufferRef { buffer, tag }
    }

    pub fn any(&self) -> BufferRef<Any> { self.buffer.any() }

    pub fn forget_nullability(&self) -> TypedBufferRef {
        TypedBufferRef { buffer: self.buffer, tag: self.tag.non_nullable() }
    }

    pub fn is_nullable(&self) -> bool { self.tag.is_nullable() }

    pub fn nullable_any(&self) -> Result<BufferRef<Nullable<Any>>, QueryError> {
        ensure!(self.tag.is_nullable(), "{:?} is not nullable", self.tag);
        Ok(self.buffer.cast_nullable_any())
    }

    pub fn str<'a>(&self) -> Result<BufferRef<&'a str>, QueryError> {
        ensure!(self.tag == EncodingType::Str, "{:?} != Str", self.tag);
        Ok(self.buffer.str())
    }

    pub fn opt_str<'a>(&self) -> Result<BufferRef<Option<&'a str>>, QueryError> {
        ensure!(self.tag == EncodingType::OptStr, "{:?} != OptStr", self.tag);
        Ok(self.buffer.opt_str())
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

    pub fn u16(&self) -> Result<BufferRef<u16>, QueryError> {
        ensure!(self.tag == EncodingType::U16, "{:?} != U16", self.tag);
        Ok(self.buffer.u16())
    }

    pub fn u8(&self) -> Result<BufferRef<u8>, QueryError> {
        ensure!(self.tag == EncodingType::U8, "{:?} != U8", self.tag);
        Ok(self.buffer.u8())
    }

    pub fn nullable_u8(&self) -> Result<BufferRef<Nullable<u8>>, QueryError> {
        ensure!(self.tag == EncodingType::NullableU8, "{:?} != NullableU8", self.tag);
        Ok(self.buffer.nullable_u8())
    }

    pub fn nullable_u16(&self) -> Result<BufferRef<Nullable<u16>>, QueryError> {
        ensure!(self.tag == EncodingType::NullableU16, "{:?} != NullableU16", self.tag);
        Ok(self.buffer.nullable_u16())
    }

    pub fn nullable_u32(&self) -> Result<BufferRef<Nullable<u32>>, QueryError> {
        ensure!(self.tag == EncodingType::NullableU32, "{:?} != NullableU32", self.tag);
        Ok(self.buffer.nullable_u32())
    }

    pub fn nullable_i64(&self) -> Result<BufferRef<Nullable<i64>>, QueryError> {
        ensure!(self.tag == EncodingType::NullableI64, "{:?} != NullableI64", self.tag);
        Ok(self.buffer.nullable_i64())
    }

    pub fn nullable_str<'a>(&self) -> Result<BufferRef<Nullable<&'a str>>, QueryError> {
        ensure!(self.tag == EncodingType::NullableStr, "{:?} != NullableStr", self.tag);
        Ok(self.buffer.nullable_str())
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

    pub fn raw_val(&self) -> Result<BufferRef<RawVal>, QueryError> {
        // ensure!(self.tag == EncodingType::Str, "{:?} != Str", self.tag);
        Ok(self.buffer.raw_val())
    }

    pub fn val_rows<'a>(&self) -> Result<BufferRef<ValRows<'a>>, QueryError> {
        ensure!(self.tag == EncodingType::ValRows, "{:?} != ValRows", self.tag);
        Ok(self.buffer.val_rows())
    }

    pub fn val<'a>(&self) -> Result<BufferRef<Val<'a>>, QueryError> {
        ensure!(self.tag == EncodingType::Val, "{:?} != Val", self.tag);
        Ok(self.buffer.val())
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
