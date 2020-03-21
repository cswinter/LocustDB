use crate::engine::*;
use std::fmt;
use crate::stringpack::StringPackerIterator;

pub struct UnpackStrings<'a> {
    pub packed: BufferRef<u8>,
    pub unpacked: BufferRef<&'a str>,
    pub iterator: Option<StringPackerIterator<'a>>,
    pub has_more: bool,
}

impl<'a> VecOperator<'a> for UnpackStrings<'a> {
    fn execute(&mut self, streaming: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let mut decoded = scratchpad.get_mut(self.unpacked);
        if streaming { panic!("Not supported") }
        for elem in self.iterator.as_mut().unwrap() {
            decoded.push(elem);
            if decoded.capacity() == decoded.len() { return Ok(()); }
        }
        self.has_more = false;
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.unpacked, Vec::with_capacity(batch_size));
        let encoded = scratchpad.get_pinned(self.packed);
        self.iterator = Some(unsafe { StringPackerIterator::from_slice(encoded) });
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

