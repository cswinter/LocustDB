use std::io::Read;
use std::fmt;

use engine::*;
use engine::vector_op::vector_operator::*;
use mem_store::lz4;


pub struct LZ4Decode<'a, T> {
    pub encoded: BufferRef<u8>,
    pub decoded: BufferRef<T>,
    pub decoded_len: usize,
    pub reader: Box<Read + 'a>,
    pub has_more: bool,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for LZ4Decode<'a, T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut decoded = scratchpad.get_mut(self.decoded);
        let len = lz4::decode(&mut self.reader, &mut decoded);
        if len < decoded.len() {
            decoded.truncate(len);
            self.has_more = false;
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.decoded, vec![T::zero(); batch_size]);
        let encoded = scratchpad.get_pinned(self.encoded);
        self.reader = Box::new(lz4::decoder(encoded));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.encoded.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.decoded.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
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

