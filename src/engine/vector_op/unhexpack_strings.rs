use std::mem;
use std::fmt;
use std::str;

use hex;

use engine::vector_op::vector_operator::*;
use stringpack::PackedBytesIterator;


pub struct UnhexpackStrings<'a> {
    pub packed: BufferRef,
    pub unpacked: BufferRef,
    pub stringstore: BufferRef,
    pub iterator: Option<PackedBytesIterator<'a>>,
    pub total_bytes: usize,
    // TODO(clemens): initializing this properly is required for safety
    pub uppercase: bool,
    pub has_more: bool,
}

impl<'a> VecOperator<'a> for UnhexpackStrings<'a> {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut decoded = scratchpad.get_mut::<&'a str>(self.unpacked);
        // TODO(clemens): method that checks whether buffer is pinned, and returns with lifetime 'a? allows getting rid of transmute below...
        let mut stringstore = scratchpad.get_mut::<u8>(self.stringstore);
        if streaming { decoded.clear() }
        let iter = self.iterator.as_mut().unwrap();
        while let Some(elem) = iter.next() {
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
            if decoded.capacity() == decoded.len() { return; }
        }
        self.has_more = false;
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): escape analysis, only need to pin if it makes it into output column
        scratchpad.pin(self.stringstore);
        scratchpad.set(self.unpacked, Box::new(Vec::<&'a str>::with_capacity(batch_size)));
        // Initializing with sufficient capacity is required for safety - this vector must never get reallocated
        scratchpad.set(self.stringstore, Box::new(Vec::<u8>::with_capacity(self.total_bytes)));
        let encoded = scratchpad.get::<u8>(self.packed);
        // TODO(clemens): eliminate mem::transmute by storing in scratchpad?
        self.iterator = Some(unsafe {
            let iterator: PackedBytesIterator = PackedBytesIterator::from_slice(encoded.as_ref());
            mem::transmute::<_, PackedBytesIterator<'a>>(iterator)
        });
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.packed] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.unpacked] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
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

