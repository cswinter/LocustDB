use std::marker::PhantomData;
use std::io::Read;
use std::fmt;
use std::mem;

use engine::*;
use engine::vector_op::vector_operator::*;
use mem_store::lz4;


pub struct LZ4Decode<'a, T> {
    pub encoded: BufferRef,
    pub decoded: BufferRef,
    pub  decoded_len: usize,
    pub reader: Box<Read + 'a>,
    pub has_more: bool,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for LZ4Decode<'a, T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut decoded = scratchpad.get_mut::<T>(self.decoded);
        let len = unsafe { lz4::decode(&mut self.reader, &mut decoded) };
        if len < decoded.len() {
            decoded.truncate(len);
            self.has_more = false;
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.decoded, Box::new(vec![T::zero(); batch_size]));
        let encoded = scratchpad.get::<u8>(self.encoded);
        // TODO(clemens): eliminate unsafe? could store in scratchpad...
        self.reader = unsafe {
            let decoder: Box<Read> = Box::new(lz4::decoder(encoded.as_ref()));
            mem::transmute::<_, Box<Read + 'a>>(decoder)
        };
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.encoded] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.decoded] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn is_streaming_producer(&self) -> bool { true }
    fn has_more(&self) -> bool { self.has_more }
    fn custom_output_len(&self) -> Option<usize> { Some(self.decoded_len) }

    fn display_op(&self, _: bool) -> String {
        format!("lz4_decode({})", self.encoded)
    }
}

impl<'a, T> fmt::Debug for LZ4Decode<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LZ4Decode {{ encoded: {}, decoded: {} }}", self.encoded, self.decoded)
    }
}

