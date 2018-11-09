use std::marker::PhantomData;
use std::mem;
use std::fmt;

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

    pub fn str<'a>(&self) -> BufferRef<&'a str> {
        assert_eq!(self.tag, EncodingType::Str);
        self.buffer.str()
    }

    pub fn i64(&self) -> BufferRef<i64> {
        assert_eq!(self.tag, EncodingType::I64);
        self.buffer.i64()
    }

    pub fn u64(&self) -> BufferRef<u64> {
        assert_eq!(self.tag, EncodingType::U64);
        self.buffer.u64()
    }

    pub fn u32(&self) -> BufferRef<u32> {
        assert_eq!(self.tag, EncodingType::U32);
        self.buffer.u32()
    }

    pub fn u8(&self) -> BufferRef<u8> {
        assert_eq!(self.tag, EncodingType::U8);
        self.buffer.u8()
    }

    pub fn usize(&self) -> BufferRef<usize> {
        assert_eq!(self.tag, EncodingType::USize);
        self.buffer.usize()
    }

    pub fn merge_op(&self) -> BufferRef<MergeOp> {
        assert_eq!(self.tag, EncodingType::MergeOp);
        self.buffer.merge_op()
    }

    pub fn premerge(&self) -> BufferRef<Premerge> {
        assert_eq!(self.tag, EncodingType::Premerge);
        self.buffer.premerge()
    }


    // TODO(clemens): better typing for Constants
    pub fn raw_val(&self) -> BufferRef<RawVal> {
        assert_eq!(self.tag, EncodingType::Val);
        self.buffer.raw_val()
    }

    pub fn string(&self) -> BufferRef<String> {
        assert_eq!(self.tag, EncodingType::Val);
        self.buffer.string()
    }

    pub fn const_i64(&self) -> BufferRef<i64> {
        // assert_eq!(self.tag, EncodingType::I64);
        self.buffer.i64()
    }
}
