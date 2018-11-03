use std::mem;
use std::fmt;

use engine::vector_op::vector_operator::*;
use stringpack::StringPackerIterator;


pub struct UnpackStrings<'a> {
    pub packed: BufferRef<u8>,
    pub unpacked: BufferRef<&'a str>,
    pub iterator: Option<StringPackerIterator<'a>>,
    pub has_more: bool,
}

impl<'a> VecOperator<'a> for UnpackStrings<'a> {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) {
        let mut decoded = scratchpad.get_mut::<&'a str>(self.unpacked);
        if streaming { panic!("Not supported") }
        for elem in self.iterator.as_mut().unwrap() {
            decoded.push(elem);
            if decoded.capacity() == decoded.len() { return; }
        }
        self.has_more = false;
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        // TODO(clemens): escape analysis, only need to pin if it makes it into output column
        scratchpad.pin(self.packed.any());
        scratchpad.set(self.unpacked, Vec::with_capacity(batch_size));
        let encoded = scratchpad.get::<u8>(self.packed);
        // TODO(clemens): eliminate mem::transmute by storing in scratchpad?
        self.iterator = Some(unsafe {
            let iterator: StringPackerIterator = StringPackerIterator::from_slice(encoded.as_ref());
            mem::transmute::<_, StringPackerIterator<'a>>(iterator)
        });
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.packed.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.unpacked.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }
    fn is_streaming_producer(&self) -> bool { true }
    fn has_more(&self) -> bool { self.has_more }

    fn display_op(&self, _: bool) -> String {
        format!("unpack_strings({})", self.packed)
    }
}

impl<'a> fmt::Debug for UnpackStrings<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UnpackStrings {{ packed: {}, unpacked: {} }}", self.packed, self.unpacked)
    }
}

