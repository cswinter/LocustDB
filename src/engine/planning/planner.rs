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
            let mut ops = vec![Add {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                sum: sum_non_null,
            }];
            ops.extend(combine_nulls(bp, lhs, rhs, sum_non_null, sum));
            Rewrite::ReplaceWith(ops)
        }
        Subtract { lhs, rhs, difference } if difference.is_nullable() => {
            let difference_non_null = bp.named_buffer("difference_non_null", difference.tag.non_nullable());
            let mut ops = vec![Subtract {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                difference: difference_non_null,
            }];
            ops.extend(combine_nulls(bp, lhs, rhs, difference_non_null, difference));
            Rewrite::ReplaceWith(ops)
        }
        Multiply { lhs, rhs, product } if product.is_nullable() => {
            let product_non_null = bp.named_buffer("product_non_null", product.tag.non_nullable());
            let mut ops = vec![Add {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                sum: product_non_null,
            }];
            ops.extend(combine_nulls(bp, lhs, rhs, product_non_null, product));
            Rewrite::ReplaceWith(ops)
        }
        Divide { lhs, rhs, division } if division.is_nullable() => {
            let division_non_null = bp.named_buffer("division_non_null", division.tag.non_nullable());
            let mut ops = vec![Divide {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                division: division_non_null,
            }];
            ops.extend(combine_nulls(bp, lhs, rhs, division_non_null, division));
            Rewrite::ReplaceWith(ops)
        }
        Modulo { lhs, rhs, modulo } if modulo.is_nullable() => {
            let modulo_non_null = bp.named_buffer("modulo_non_null", modulo.tag.non_nullable());
            let mut ops = vec![Divide {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                division: modulo_non_null,
            }];
            ops.extend(combine_nulls(bp, lhs, rhs, modulo_non_null, modulo));
            Rewrite::ReplaceWith(ops)
        }
        And { lhs, rhs, and } if and.is_nullable() => {
            let and_non_null = bp.named_buffer("and_non_null", and.tag.non_nullable());
            let mut ops = vec![And {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                and: and_non_null,
            }];
            ops.extend(combine_nulls(bp, lhs, rhs, and_non_null, and));
            Rewrite::ReplaceWith(ops)
        }
        Or { lhs, rhs, or } if or.is_nullable() => {
            let or_non_null = bp.named_buffer("or_non_null", or.tag.non_nullable());
            let mut ops = vec![Or {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                or: or_non_null,
            }];
            ops.extend(combine_nulls(bp, lhs, rhs, or_non_null, or));
            Rewrite::ReplaceWith(ops)
        }
        LessThan { lhs, rhs, less_than } if less_than.is_nullable() => {
            let less_than_non_null = bp.named_buffer("less_than_non_null", less_than.tag.non_nullable());
            let less_than_op = LessThan {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                less_than: less_than_non_null,
            };
            let mut ops = combine_nulls(bp, lhs, rhs, less_than_non_null, less_than);
            ops.push(less_than_op);
            Rewrite::ReplaceWith(ops)
        }
        LessThanEquals { lhs, rhs, less_than_equals } if less_than_equals.is_nullable() => {
            let less_than_equals_non_null = bp.named_buffer("less_than_equals_non_null", less_than_equals.tag.non_nullable());
            let less_than_equals_op = LessThanEquals {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                less_than_equals: less_than_equals_non_null,
            };
            let mut ops = combine_nulls(bp, lhs, rhs, less_than_equals_non_null, less_than_equals);
            ops.push(less_than_equals_op);
            Rewrite::ReplaceWith(ops)
        }
        Equals { lhs, rhs, equals } if equals.is_nullable() => {
            let equals_non_null = bp.named_buffer("equals_non_null", equals.tag.non_nullable());
            let equals_op = Equals {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                equals: equals_non_null,
            };
            let mut ops = combine_nulls(bp, lhs, rhs, equals_non_null, equals);
            ops.push(equals_op);
            Rewrite::ReplaceWith(ops)
        }
        NotEquals { lhs, rhs, not_equals } if not_equals.is_nullable() => {
            let not_equals_non_null = bp.named_buffer("not_equals_non_null", not_equals.tag.non_nullable());
            let not_equals_op = NotEquals {
                lhs: lhs.forget_nullability(),
                rhs: rhs.forget_nullability(),
                not_equals: not_equals_non_null,
            };
            let mut ops = combine_nulls(bp, lhs, rhs, not_equals_non_null, not_equals);
            ops.push(not_equals_op);
            Rewrite::ReplaceWith(ops)
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

fn combine_nulls(bp: &mut BufferProvider,
                 lhs: TypedBufferRef,
                 rhs: TypedBufferRef,
                 data: TypedBufferRef,
                 nullable_data: TypedBufferRef) -> Vec<QueryPlan> {
    if lhs.is_nullable() && rhs.is_nullable() {
        let combined_null_map = bp.buffer_u8("combined_null_map");
        vec![
            CombineNullMaps {
                lhs,
                rhs,
                present: combined_null_map,
            },
            AssembleNullable {
                data,
                present: combined_null_map,
                nullable: nullable_data,
            }
        ]
    } else {
        vec![
            PropagateNullability {
                nullable: if lhs.is_nullable() { lhs } else { rhs },
                data,
                nullable_data,
            }]
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