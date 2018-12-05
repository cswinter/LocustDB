use std::collections::HashMap;
use std::marker::PhantomData;
use std::result::Result;

use engine::*;
use self::query_plan::prepare;
use ::QueryError;
use self::QueryPlan::*;


#[derive(Default)]
pub struct QueryPlanner {
    pub operations: Vec<QueryPlan>,
    pub buffer_to_operation: Vec<Option<usize>>,
    pub cache: HashMap<[u8; 16], Vec<TypedBufferRef>>,
    checkpoint: usize,
    cache_checkpoint: HashMap<[u8; 16], Vec<TypedBufferRef>>,
    pub buffer_provider: BufferProvider,
}

impl QueryPlanner {
    pub fn prepare<'a>(&mut self, mut constant_vecs: Vec<BoxedData<'a>>) -> Result<QueryExecutor<'a>, QueryError> {
        self.perform_rewrites();

        let mut result = QueryExecutor::default();
        result.set_buffer_count(self.buffer_provider.buffer_count());
        for operation in &self.operations {
            prepare(operation.clone(), &mut constant_vecs, &mut result)?;
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


    fn perform_rewrites(&mut self) {
        for i in 0..self.operations.len() {
            match propagate_nullability(&self.operations[i], &mut self.buffer_provider) {
                Rewrite::ReplaceWith(ops) => {
                    trace!("Replacing {:#?} with {:#?}", self.operations[i], ops);
                    self.operations[i] = ops[0].clone();
                    for op in ops.into_iter().skip(1) {
                        self.operations.push(op);
                    }
                }
                Rewrite::None => {}
            }
        }
    }
}

enum Rewrite {
    None,
    ReplaceWith(Vec<QueryPlan>),
}

fn propagate_nullability(operation: &QueryPlan, bp: &mut BufferProvider) -> Rewrite {
    match *operation {
        Cast { input, casted } if input.is_nullable() => {
            let casted_non_nullable = bp.named_buffer("casted_non_nullable", casted.tag.non_nullable());
            let cast = Cast {
                input: input.forget_nullability(),
                casted: casted_non_nullable,
            };
            let nullable = PropagateNullability {
                nullable: input,
                data: casted_non_nullable,
                nullable_data: casted,
            };
            Rewrite::ReplaceWith(vec![cast, nullable])
        }
        Add { lhs, rhs, sum } if sum.is_nullable() => {
            let sum_non_null = bp.named_buffer("sum_non_null", sum.tag.non_nullable());
            let add = Add {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                sum: sum_non_null,
            };
            let nullable = PropagateNullability {
                nullable: if lhs.is_nullable() { lhs } else { rhs },
                data: sum_non_null.into(),
                nullable_data: sum,
            };
            Rewrite::ReplaceWith(vec![add, nullable])
        }
        MergeKeep { take_left, lhs, rhs, merged } if lhs.is_nullable() != rhs.is_nullable() => {
            let mut ops = Vec::with_capacity(2);
            let lhs = if lhs.is_nullable() { lhs } else {
                let lhs_nullable = bp.named_buffer("lhs_nullable", lhs.tag.nullable());
                ops.push(MakeNullable { data: lhs, present: bp.buffer_u8("present"), nullable: lhs_nullable });
                lhs_nullable
            };
            let rhs = if rhs.is_nullable() { rhs } else {
                let rhs_nullable = bp.named_buffer("rhs_nullable", rhs.tag.nullable());
                ops.push(MakeNullable { data: rhs, present: bp.buffer_u8("present"), nullable: rhs_nullable });
                rhs_nullable
            };
            ops.push(MergeKeep { take_left, lhs, rhs, merged });
            Rewrite::ReplaceWith(ops)
        }
        _ => Rewrite::None,
    }
}

#[derive(Default)]
pub struct BufferProvider {
    buffer_count: usize,
    shared_buffers: HashMap<&'static str, TypedBufferRef>,
}

impl BufferProvider {
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

    pub fn buffer_merge_op(&mut self, name: &'static str) -> BufferRef<MergeOp> {
        self.named_buffer(name, EncodingType::MergeOp).merge_op().unwrap()
    }

    pub fn buffer_premerge(&mut self, name: &'static str) -> BufferRef<Premerge> {
        self.named_buffer(name, EncodingType::Premerge).premerge().unwrap()
    }

    pub fn shared_buffer(&mut self, name: &'static str, tag: EncodingType) -> TypedBufferRef {
        if self.shared_buffers.get(name).is_none() {
            let buffer = self.named_buffer(name, tag);
            self.shared_buffers.insert(name, buffer);
        }
        self.shared_buffers[name]
    }

    pub fn buffer_count(&self) -> usize { self.buffer_count }
}