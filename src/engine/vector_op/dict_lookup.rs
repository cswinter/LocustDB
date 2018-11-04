use std::str;

use engine::*;
use engine::typed_vec::AnyVec;
use engine::vector_op::vector_operator::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct DictLookup<'a, T> {
    pub indices: BufferRef<T>,
    pub dict_indices: BufferRef<u64>,
    pub dict_data: BufferRef<u8>,
    pub output: BufferRef<&'a str>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for DictLookup<'a, T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let dict_data = scratchpad.get_pinned(self.dict_data);
        let indices = scratchpad.get::<T>(self.indices);
        let dict_indices = scratchpad.get::<u64>(self.dict_indices);
        let mut output = scratchpad.get_mut::<&str>(self.output);
        if stream { output.clear(); }
        for i in indices.iter() {
            let offset_len = dict_indices[i.cast_usize()];
            let offset = (offset_len >> 24) as usize;
            let len = (offset_len & 0x00ff_ffff) as usize;
            // TODO(clemens): eliminate transmute?
            let string = unsafe {
                str::from_utf8_unchecked(&dict_data[offset..(offset + len)])
            };
            output.push(string);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.indices.any(), self.dict_indices.any(), self.dict_data.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, buffer: usize) -> bool { buffer == self.indices.i }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}[{}]]", self.dict_data, self.dict_indices, self.indices)
    }
}

#[derive(Debug)]
pub struct InverseDictLookup {
    pub dict_indices: BufferRef<u64>,
    pub dict_data: BufferRef<u8>,
    pub constant: BufferRef<String>,
    pub output: BufferRef<RawVal>,
}

impl<'a> VecOperator<'a> for InverseDictLookup {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let mut result = -1;
            let constant = scratchpad.get_const::<String>(&self.constant);
            let constant = constant.as_bytes();
            let dict_indices = scratchpad.get::<u64>(self.dict_indices);
            let dict_data = scratchpad.get::<u8>(self.dict_data);
            for (i, offset_len) in dict_indices.iter().enumerate() {
                let offset = (offset_len >> 24) as usize;
                let len = (offset_len & 0x00ff_ffff) as usize;
                if &dict_data[offset..(offset + len)] == constant {
                    result = i as i64;
                    break;
                }
            }
            result
        };
        scratchpad.set_any(self.output.any(), AnyVec::constant(RawVal::Int(result)));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.constant.any(), self.dict_indices.any(), self.dict_data.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("inverse_dict_lookup({}, {}, {})", self.dict_indices, self.dict_data, self.constant)
    }
}
