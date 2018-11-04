use std::mem;
use std::fmt;
use std::str;

use hex;

use engine::vector_op::vector_operator::*;
use stringpack::PackedBytesIterator;


pub struct UnhexpackStrings<'a> {
    pub packed: BufferRef<u8>,
    pub unpacked: BufferRef<&'a str>,
    pub stringstore: BufferRef<u8>,
    pub iterator: Option<PackedBytesIterator<'a>>,
    pub total_bytes: usize,
    // TODO(clemens): initializing this properly is required for safety
    pub uppercase: bool,
    pub has_more: bool,
}

impl<'a> VecOperator<'a> for UnhexpackStrings<'a> {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) {
        unsafe { scratchpad.unpin(self.stringstore.any()) };
        {
            let mut decoded = scratchpad.get_mut(self.unpacked);
            let mut stringstore = scratchpad.get_mut(self.stringstore);
            if streaming { decoded.clear() }
            for elem in self.iterator.as_mut().unwrap() {
                let string = if self.uppercase {
                    hex::encode_upper(elem)
                } else {
                    hex::encode(elem)
                };
                let bytes = string.as_bytes();
                // unsafe if this were false
                assert!(stringstore.len() + bytes.len() <= stringstore.capacity());
                stringstore.extend_from_slice(bytes);
                decoded.push(unsafe {
                    mem::transmute::<_, &'a str>(
                        str::from_utf8_unchecked(&stringstore[stringstore.len() - bytes.len()..])
                    )
                });
                if decoded.capacity() == decoded.len() { break; }
            }
        }
        scratchpad.pin(self.stringstore.any());
        self.has_more = self.iterator.as_ref().unwrap().has_more();
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.unpacked, Vec::with_capacity(batch_size));
        // Initializing with sufficient capacity is required for safety - this vector must never get reallocated
        scratchpad.set(self.stringstore, Vec::with_capacity(self.total_bytes));
        let encoded = scratchpad.get_pinned(self.packed);
        self.iterator = Some(PackedBytesIterator::from_slice(encoded.as_ref()));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.packed.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.unpacked.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }
    fn is_streaming_producer(&self) -> bool { true }
    fn has_more(&self) -> bool { self.has_more }

    fn display_op(&self, _: bool) -> String {
        format!("unhexpack_strings({}, {})", self.packed, if self.uppercase { "upper" } else { "lower" })
    }
}

impl<'a> fmt::Debug for UnhexpackStrings<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnpackStrings {{ packed: {}, unpacked: {} }}", self.packed, self.unpacked)
    }
}

