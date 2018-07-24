use std::marker::PhantomData;
use std::mem;
use std::str;

use engine::*;
use engine::typed_vec::AnyVec;
use engine::vector_op::vector_operator::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct DictLookup<T> {
    pub indices: BufferRef,
    pub dict_indices: BufferRef,
    pub dict_data: BufferRef,
    pub output: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for DictLookup<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let indices = scratchpad.get::<T>(self.indices);
        let dict_indices = scratchpad.get::<u64>(self.dict_indices);
        let dict_data = scratchpad.get::<u8>(self.dict_data);
        let mut output = scratchpad.get_mut::<&str>(self.output);
        if stream { output.clear(); }
        for i in indices.iter() {
            let offset_len = dict_indices[i.cast_usize()];
            let offset = (offset_len >> 24) as usize;
            let len = (offset_len & 0xffffff) as usize;
            // TODO(clemens): eliminate transmute?
            let string = unsafe {
                mem::transmute(str::from_utf8_unchecked(&dict_data[offset..(offset + len)]))
            };
            output.push(string);
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Box::new(Vec::<&str>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.indices, self.dict_indices, self.dict_data] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, buffer: BufferRef) -> bool { buffer == self.indices }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}[{}]]", self.dict_data, self.dict_indices, self.indices)
    }
}

#[derive(Debug)]
pub struct InverseDictLookup {
    pub dict_indices: BufferRef,
    pub dict_data: BufferRef,
    pub constant: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for InverseDictLookup {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let mut result = -1;
            let constant = scratchpad.get_const::<String>(self.constant);
            let dict_indices = scratchpad.get::<u64>(self.dict_indices);
            let dict_data = scratchpad.get::<u8>(self.dict_data);
            for (i, offset_len) in dict_indices.iter().enumerate() {
                let offset = (offset_len >> 24) as usize;
                let len = (offset_len & 0xffffff) as usize;
                let entry = unsafe {
                    str::from_utf8_unchecked(&dict_data[offset..(offset + len)])
                };
                if entry == &constant {
                    result = i as i64;
                    break;
                }
            }
            result
        };
        scratchpad.set(self.output, AnyVec::constant(RawVal::Int(result)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.constant, self.dict_indices, self.dict_data] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("inverse_dict_lookup({}, {}, {})", self.dict_indices, self.dict_data, self.constant)
    }
}
