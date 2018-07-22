use std::marker::PhantomData;
use std::mem;

use engine::*;
use engine::typed_vec::AnyVec;
use engine::vector_op::vector_operator::*;
use ingest::raw_val::RawVal;


#[derive(Debug)]
pub struct DictLookup<T> {
    pub indices: BufferRef,
    pub dictionary: BufferRef,
    pub output: BufferRef,
    pub t: PhantomData<T>,
}

impl<'a, T: GenericIntVec<T>> VecOperator<'a> for DictLookup<T> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) {
        let indices = scratchpad.get::<T>(self.indices);
        // TODO(clemens): fix
        let dictionary: &'a [String] = unsafe { mem::transmute(scratchpad.get_any(self.dictionary).cast_ref_string()) };
        let mut output = scratchpad.get_mut::<&str>(self.output);
        if stream { output.clear(); }
        for i in indices.iter() {
            output.push(dictionary[i.cast_usize()].as_ref());
        }
    }

    fn init(&mut self, _: usize, batch_size: usize, _: bool, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Box::new(Vec::<&str>::with_capacity(batch_size)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.indices, self.dictionary] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, buffer: BufferRef) -> bool { buffer != self.dictionary }
    fn can_stream_output(&self, _: BufferRef) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}[{}]", self.dictionary, self.indices)
    }
}

#[derive(Debug)]
pub struct InverseDictLookup {
    pub dictionary: BufferRef,
    pub constant: BufferRef,
    pub output: BufferRef,
}

impl<'a> VecOperator<'a> for InverseDictLookup {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) {
        let result = {
            let mut result = -1;
            let constant = scratchpad.get_const::<String>(self.constant);
            let dictionary = scratchpad.get_any(self.dictionary);
            for (i, entry) in dictionary.cast_ref_string().iter().enumerate() {
                if entry == &constant {
                    result = i as i64;
                    break;
                }
            }
            result
        };
        scratchpad.set(self.output, AnyVec::constant(RawVal::Int(result)));
    }

    fn inputs(&self) -> Vec<BufferRef> { vec![self.constant, self.dictionary] }
    fn outputs(&self) -> Vec<BufferRef> { vec![self.output] }
    fn can_stream_input(&self, _: BufferRef) -> bool { false }
    fn can_stream_output(&self, _: BufferRef) -> bool { false }
    fn allocates(&self) -> bool { false }

    fn display_op(&self, _: bool) -> String {
        format!("inverse_dict_lookup({}, {})", self.dictionary, self.constant)
    }
}
