use std::collections::HashMap;
use std::marker::PhantomData;
use std::result::Result;

use engine::*;
use self::query_plan::prepare;
use ::QueryError;


#[derive(Default)]
pub struct QueryPlanner {
    pub operations: Vec<QueryPlan>,
    pub buffer_to_operation: Vec<Option<usize>>,
    pub cache: HashMap<[u8; 16], Vec<TypedBufferRef>>,
    checkpoint: usize,
    cache_checkpoint: HashMap<[u8; 16], Vec<TypedBufferRef>>,

    buffer_count: usize,
    shared_buffers: HashMap<&'static str, TypedBufferRef>,
}

impl QueryPlanner {
    pub fn prepare<'a>(&self) -> Result<QueryExecutor<'a>, QueryError> {
        let mut result = QueryExecutor::default();
        result.set_buffer_count(self.buffer_count);
        for operation in &self.operations {
            prepare(operation.clone(), &mut result)?;
        }
        Ok(result)
    }

    pub fn checkpoint(&mut self) {
        self.checkpoint = self.operations.len();
        self.cache_checkpoint = self.cache.clone();
    }

    pub fn reset(&mut self) {
        self.operations.truncate(self.checkpoint);
        std::mem::swap(&mut self.cache, &mut self.cache_checkpoint);
    }

    pub fn resolve(&self, buffer: &TypedBufferRef) -> &QueryPlan {
        let op_index = self.buffer_to_operation[buffer.buffer.i]
            .expect(&format!("Not entry found for {:?}", buffer));
        &self.operations[op_index]
    }

    pub fn enable_common_subexpression_elimination(&self) -> bool { true }

    pub fn named_buffer(&mut self, name: &'static str, tag: EncodingType) -> TypedBufferRef {
        let buffer = TypedBufferRef::new(BufferRef { i: self.buffer_count, name, t: PhantomData }, tag);
        self.buffer_count += 1;
        buffer
    }

    pub fn buffer_str<'a>(&mut self, name: &'static str) -> BufferRef<&'a str> {
        self.named_buffer(name, EncodingType::Str).str().unwrap()
    }

    pub fn buffer_usize(&mut self, name: &'static str) -> BufferRef<usize> {
        self.named_buffer(name, EncodingType::USize).usize().unwrap()
    }

    pub fn buffer_i64(&mut self, name: &'static str) -> BufferRef<i64> {
        self.named_buffer(name, EncodingType::I64).i64().unwrap()
    }

    pub fn buffer_u32(&mut self, name: &'static str) -> BufferRef<u32> {
        self.named_buffer(name, EncodingType::U32).u32().unwrap()
    }

    pub fn buffer_u8(&mut self, name: &'static str) -> BufferRef<u8> {
        self.named_buffer(name, EncodingType::U8).u8().unwrap()
    }

    pub fn buffer_scalar_i64(&mut self, name: &'static str) -> BufferRef<Scalar<i64>> {
        self.named_buffer(name, EncodingType::ScalarI64).scalar_i64().unwrap()
    }

    pub fn buffer_scalar_str<'a>(&mut self, name: &'static str) -> BufferRef<Scalar<&'a str>> {
        self.named_buffer(name, EncodingType::ScalarStr).scalar_str().unwrap()
    }

    pub fn buffer_scalar_string(&mut self, name: &'static str) -> BufferRef<Scalar<String>> {
        self.named_buffer(name, EncodingType::ScalarString).scalar_string().unwrap()
    }

    pub fn shared_buffer(&mut self, name: &'static str, tag: EncodingType) -> TypedBufferRef {
        if self.shared_buffers.get(name).is_none() {
            let buffer = self.named_buffer(name, tag);
            self.shared_buffers.insert(name, buffer);
        }
        self.shared_buffers[name]
    }
}

