use std::collections::HashMap;
use std::i64;
use std::result::Result;
use std::sync::Arc;

use chrono::{Datelike, NaiveDateTime};
use regex::Regex;

use ::QueryError;
use engine::*;
use ingest::raw_val::RawVal;
use mem_store::*;
use mem_store::column::DataSource;
use syntax::expression::*;
use self::syntax::*;
use locustdb_derive::ASTBuilder;


#[derive(Default)]
pub struct QueryPlanner {
    operations: Vec<QueryPlan>,
    buffer_to_operation: Vec<Option<usize>>,
    cache: HashMap<[u8; 16], Vec<TypedBufferRef>>,
    checkpoint: usize,
    cache_checkpoint: HashMap<[u8; 16], Vec<TypedBufferRef>>,
}

impl QueryPlanner {
    /*fn make_nullable(&mut self, data: TypedBufferRef, result: &mut QueryExecutor) -> TypedBufferRef {
        let present = result.buffer_u8("present");
        let nullable = result.named_buffer("nullable", data.tag.nullable());
        self.operations.push(QueryPlan::MakeNullable { data, present, nullable });
        nullable
    }*/

    pub fn prepare(&self, result: &mut QueryExecutor) -> Result<(), QueryError> {
        for operation in &self.operations {
            prepare(operation.clone(), result)?;
        }
        Ok(())
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

    fn enable_common_subexpression_elimination(&self) -> bool { true }
}


#[derive(Debug, Clone, ASTBuilder)]
pub enum QueryPlan {
    /// Retrieves the buffer for the specified section of the column with name `name`.
    #[newstyle]
    ColumnSection {
        name: String,
        section: usize,
        #[nohash]
        range: Option<(i64, i64)>,
        #[output(t_provided)]
        column_section: TypedBufferRef,
    },

    /// Retrieves a specific buffer.
    ReadBuffer { buffer: TypedBufferRef },

    #[newstyle]
    Connect {
        input: TypedBufferRef,
        output: TypedBufferRef,
    },

    /// Combines a vector with a null map.
    #[newstyle]
    AssembleNullable {
        data: TypedBufferRef,
        present: BufferRef<u8>,
        #[output(t = "data.nullable")]
        nullable: TypedBufferRef,
    },

    /// Combines a vector with the null map of another vector.
    PropagateNullability { nullable: Box<QueryPlan>, data: Box<QueryPlan> },

    /// Combines a vector with a null map where none of the elements are null.
    #[newstyle]
    MakeNullable {
        data: TypedBufferRef,
        #[internal]
        present: BufferRef<u8>,
        #[output(t = "data.nullable")]
        nullable: TypedBufferRef,
    },

    /// Resolves dictionary indices to their original string value.
    #[newstyle]
    DictLookup {
        indices: TypedBufferRef,
        offset_len: BufferRef<u64>,
        backing_store: BufferRef<u8>,
        #[output]
        decoded: BufferRef<&'static str>,
    },

    /// Determines what dictionary index a string constant corresponds to.
    #[newstyle]
    InverseDictLookup {
        offset_len: BufferRef<u64>,
        backing_store: BufferRef<u8>,
        constant: BufferRef<Scalar<&'static str>>,
        #[output]
        decoded: BufferRef<Scalar<i64>>,
    },

    /// Casts `input` to the specified type.
    #[newstyle]
    Cast {
        input: TypedBufferRef,
        #[output(t_provided)]
        casted: TypedBufferRef,
    },

    /// LZ4 decodes `bytes` into `decoded_len` elements of type `t`.
    #[newstyle]
    LZ4Decode {
        bytes: BufferRef<u8>,
        decoded_len: usize,
        #[output(t_provided)]
        decoded: TypedBufferRef,
    },

    /// Decodes a byte array of tightly packed strings.
    #[newstyle]
    UnpackStrings {
        bytes: BufferRef<u8>,
        #[output]
        unpacked_strings: BufferRef<&'static str>,
    },

    /// Decodes a byte array of tightly packed, hex encoded string.
    #[newstyle]
    UnhexpackStrings {
        bytes: BufferRef<u8>,
        uppercase: bool,
        total_bytes: usize,
        #[internal]
        string_store: BufferRef<u8>,
        #[output]
        unpacked_strings: BufferRef<&'static str>,
    },

    /// Decodes delta encoded integers.
    #[newstyle]
    DeltaDecode {
        plan: TypedBufferRef,
        #[output]
        delta_decoded: BufferRef<i64>,
    },

    /// Creates a byte vector of size `max_index` and sets all entries
    /// corresponding to `indices` to 1.
    #[newstyle]
    Exists {
        indices: TypedBufferRef,
        max_index: BufferRef<Scalar<i64>>,
        #[output]
        exists: BufferRef<u8>,
    },

    /// Deletes all zero entries from `plan`.
    #[newstyle]
    NonzeroCompact {
        plan: TypedBufferRef,
        #[output(t = "plan")]
        compacted: TypedBufferRef,
    },

    /// Determines the indices of all entries in `plan` that are non-zero.
    #[newstyle]
    NonzeroIndices {
        plan: TypedBufferRef,
        #[output(t_provided)]
        nonzero_indices: TypedBufferRef,
    },

    /// Deletes all entries in `plan` for which the corresponding entry in `select` is 0.
    #[newstyle]
    Compact {
        plan: TypedBufferRef,
        select: TypedBufferRef,
        #[output(t = "plan")]
        compacted: TypedBufferRef,
    },

    /// Sums `lhs` and `rhs << shift`.
    #[newstyle]
    BitPack {
        lhs: BufferRef<i64>,
        rhs: BufferRef<i64>,
        shift: i64,
        #[output]
        bit_packed: BufferRef<i64>,
    },

    /// Retrieves the integer bit packed into `plan` at `shift..shift+width`.
    #[newstyle]
    BitUnpack {
        plan: BufferRef<i64>,
        shift: u8,
        width: u8,
        #[output]
        unpacked: BufferRef<i64>,
    },

    #[newstyle]
    SlicePack {
        plan: TypedBufferRef,
        stride: usize,
        offset: usize,
        #[output(shared)]
        packed: BufferRef<Any>,
    },

    #[newstyle]
    SliceUnpack {
        plan: BufferRef<Any>,
        stride: usize,
        offset: usize,
        #[output(t_provided)]
        unpacked: TypedBufferRef,
    },

    #[newstyle]
    LessThan {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        less_than: BufferRef<u8>,
    },
    #[newstyle]
    LessThanEquals {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        less_than_equals: BufferRef<u8>,
    },
    #[newstyle]
    Equals {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        equals: BufferRef<u8>,
    },
    #[newstyle]
    NotEquals {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        not_equals: BufferRef<u8>,
    },

    #[newstyle]
    Add {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        sum: BufferRef<i64>,
    },
    #[newstyle]
    Subtract {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        difference: BufferRef<i64>,
    },
    #[newstyle]
    Multiply {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        product: BufferRef<i64>,
    },
    #[newstyle]
    Divide {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        division: BufferRef<i64>,
    },
    #[newstyle]
    Modulo {
        lhs: TypedBufferRef,
        rhs: TypedBufferRef,
        #[output]
        modulo: BufferRef<i64>,
    },

    #[newstyle]
    And {
        lhs: BufferRef<u8>,
        rhs: BufferRef<u8>,
        #[output]
        and: BufferRef<u8>,
    },
    #[newstyle]
    Or {
        lhs: BufferRef<u8>,
        rhs: BufferRef<u8>,
        #[output]
        or: BufferRef<u8>,
    },
    #[newstyle]
    Not {
        input: BufferRef<u8>,
        #[output]
        not: BufferRef<u8>,
    },

    #[newstyle]
    ToYear {
        timestamp: BufferRef<i64>,
        #[output]
        year: BufferRef<i64>,
    },

    #[newstyle]
    Regex {
        plan: BufferRef<&'static str>,
        regex: String,
        #[output]
        matches: BufferRef<u8>,
    },

    /// Outputs a vector of indices from `0..plan.len()`
    #[newstyle]
    Indices {
        plan: TypedBufferRef,
        #[output]
        indices: BufferRef<usize>,
    },

    /// Outputs a permutation of `indices` under which `ranking` is sorted.
    #[newstyle]
    SortBy {
        ranking: TypedBufferRef,
        indices: BufferRef<usize>,
        desc: bool,
        stable: bool,
        #[output]
        permutation: BufferRef<usize>,
    },

    /// Outputs the `n` largest/smallest elements of `ranking` and their corresponding indices.
    #[newstyle]
    TopN {
        ranking: TypedBufferRef,
        n: usize,
        desc: bool,
        #[internal(t = "ranking")]
        tmp_keys: TypedBufferRef,
        #[output]
        top_n: BufferRef<usize>,
    },

    /// Outputs all elements in `plan` where the index corresponds to an entry in `indices`.
    #[newstyle]
    Select {
        plan: TypedBufferRef,
        indices: BufferRef<usize>,
        #[output(t = "plan")]
        selection: TypedBufferRef,
    },

    /// Outputs all elements in `plan` for which the corresponding entry in `select` is nonzero.
    #[newstyle]
    Filter {
        plan: TypedBufferRef,
        select: BufferRef<u8>,
        #[output(t = "plan")]
        filtered: TypedBufferRef,
    },

    Constant { value: RawVal, hide_value: bool },
    #[newstyle]
    ScalarI64 {
        value: i64,
        hide_value: bool,
        #[output]
        scalar_i64: BufferRef<Scalar<i64>>,
    },
    #[newstyle]
    ScalarStr {
        value: String,
        #[internal]
        pinned_string: BufferRef<Scalar<String>>,
        #[output]
        scalar_str: BufferRef<Scalar<&'static str>>,
    },

    /// Outputs a vector with all values equal to `value`.
    #[newstyle]
    ConstantExpand {
        value: i64,
        len: usize,
        #[output(t_provided)]
        expanded: TypedBufferRef,
    },

    /// Merges `lhs` and `rhs` such that the resulting vector contains an element from lhs at position `i` iff `take_left[i] == 1`.
    MergeKeep { take_left: Box<QueryPlan>, lhs: Box<QueryPlan>, rhs: Box<QueryPlan> },
}

// TODO(clemens): make method private?
pub fn prepare(plan: QueryPlan, result: &mut QueryExecutor) -> Result<TypedBufferRef, QueryError> {
    trace!("{:?}", &plan);
    let operation: Box<VecOperator> = match plan {
        QueryPlan::Select { plan, indices, selection } => VecOperator::select(plan, indices, selection)?,
        QueryPlan::ColumnSection { name, section, column_section, .. } =>
            VecOperator::read_column_data(name, section, column_section.any()),
        QueryPlan::AssembleNullable { data, present, nullable } =>
            VecOperator::nullable(data, present, nullable)?,
        QueryPlan::MakeNullable { data, present, nullable } => {
            result.set_last_buffer(nullable);
            VecOperator::make_nullable(data, present, nullable)?
        }
        QueryPlan::PropagateNullability { nullable, data } => {
            let data = prepare(*data, result)?;
            VecOperator::propagate_nullability(prepare(*nullable, result)?.nullable_any()?,
                                               data,
                                               result.named_buffer("nullable", data.tag.nullable()))?
        }
        QueryPlan::Filter { plan, select, filtered } => VecOperator::filter(plan, select, filtered)?,
        QueryPlan::Constant { ref value, hide_value } => VecOperator::constant(value.clone(), hide_value, result.buffer_raw_val("constant")),
        QueryPlan::ScalarI64 { value, hide_value, scalar_i64 } => VecOperator::scalar_i64(value, hide_value, scalar_i64),
        QueryPlan::ScalarStr { value, pinned_string, scalar_str } => VecOperator::scalar_str(value.to_string(), pinned_string, scalar_str),
        QueryPlan::ConstantExpand { value, len, expanded } => VecOperator::constant_expand(value, len, expanded)?,
        QueryPlan::DictLookup { indices, offset_len, backing_store, decoded } => VecOperator::dict_lookup(indices, offset_len, backing_store, decoded)?,
        QueryPlan::InverseDictLookup { offset_len, backing_store, constant, decoded } => VecOperator::inverse_dict_lookup(offset_len, backing_store, constant, decoded),
        QueryPlan::Cast { input, casted } => VecOperator::type_conversion(input, casted)?,
        QueryPlan::DeltaDecode { plan, delta_decoded } => VecOperator::delta_decode(plan, delta_decoded)?,
        QueryPlan::LZ4Decode { bytes, decoded_len, decoded } => VecOperator::lz4_decode(bytes, decoded_len, decoded)?,
        QueryPlan::UnpackStrings { bytes, unpacked_strings } => VecOperator::unpack_strings(bytes, unpacked_strings),
        QueryPlan::UnhexpackStrings { bytes, uppercase, total_bytes, string_store, unpacked_strings } => VecOperator::unhexpack_strings(bytes, uppercase, total_bytes, string_store, unpacked_strings),
        QueryPlan::Exists { indices, max_index, exists } => VecOperator::exists(indices, max_index, exists)?,
        QueryPlan::Compact { plan, select, compacted } => VecOperator::compact(plan, select, compacted)?,
        QueryPlan::NonzeroIndices { plan, nonzero_indices } => VecOperator::nonzero_indices(plan, nonzero_indices)?,
        QueryPlan::NonzeroCompact { plan, compacted } => VecOperator::nonzero_compact(plan, compacted)?,
        QueryPlan::BitPack { lhs, rhs, shift, bit_packed } => VecOperator::bit_shift_left_add(lhs, rhs, bit_packed, shift),
        QueryPlan::BitUnpack { plan, shift, width, unpacked } => VecOperator::bit_unpack(plan, shift, width, unpacked),
        QueryPlan::SlicePack { plan, stride, offset, packed } => VecOperator::slice_pack(plan, stride, offset, packed)?,
        QueryPlan::SliceUnpack { plan, stride, offset, unpacked } => VecOperator::slice_unpack(plan, stride, offset, unpacked)?,
        QueryPlan::LessThan { lhs, rhs, less_than } => VecOperator::less_than(lhs, rhs, less_than)?,
        QueryPlan::LessThanEquals { lhs, rhs, less_than_equals } => VecOperator::less_than_equals(lhs, rhs, less_than_equals)?,
        QueryPlan::Equals { lhs, rhs, equals } => VecOperator::equals(lhs, rhs, equals)?,
        QueryPlan::NotEquals { lhs, rhs, not_equals } => VecOperator::not_equals(lhs, rhs, not_equals)?,
        QueryPlan::Add { lhs, rhs, sum } => VecOperator::addition(lhs, rhs, sum)?,
        QueryPlan::Subtract { lhs, rhs, difference } => VecOperator::subtraction(lhs, rhs, difference)?,
        QueryPlan::Multiply { lhs, rhs, product } => VecOperator::multiplication(lhs, rhs, product)?,
        QueryPlan::Divide { lhs, rhs, division } => VecOperator::division(lhs, rhs, division)?,
        QueryPlan::Modulo { lhs, rhs, modulo } => VecOperator::modulo(lhs, rhs, modulo)?,
        QueryPlan::Or { lhs, rhs, or } => VecOperator::or(lhs, rhs, or),
        QueryPlan::And { lhs, rhs, and } => VecOperator::and(lhs, rhs, and),
        QueryPlan::Not { input, not } => VecOperator::not(input, not),
        QueryPlan::ToYear { timestamp, year } => VecOperator::to_year(timestamp, year),
        QueryPlan::Regex { plan, regex, matches } => VecOperator::regex(plan, &regex, matches),
        QueryPlan::Indices { plan, indices } => VecOperator::indices(plan, indices),
        QueryPlan::SortBy { ranking, indices, desc, stable, permutation } => VecOperator::sort_by(ranking, indices, desc, stable, permutation)?,
        QueryPlan::TopN { ranking, n, desc, tmp_keys, top_n } => VecOperator::top_n(ranking, tmp_keys, n, desc, top_n)?,
        QueryPlan::ReadBuffer { buffer } => return Ok(buffer),
        QueryPlan::Connect { input, output } => VecOperator::identity(input, output),
        QueryPlan::MergeKeep { take_left, lhs, rhs } => {
            let mut lhs = prepare(*lhs, result)?;
            let mut rhs = prepare(*rhs, result)?;
            if lhs.tag.is_nullable() && !rhs.tag.is_nullable() {
                let mut qp = QueryPlanner::default();
                rhs = qp.make_nullable(rhs, result);
                qp.prepare(result)?;
            } else if !lhs.tag.is_nullable() && rhs.tag.is_nullable() {
                let mut qp = QueryPlanner::default();
                lhs = qp.make_nullable(lhs, result);
                qp.prepare(result)?;
            }
            VecOperator::merge_keep(
                prepare(*take_left, result)?.u8()?,
                lhs,
                rhs,
                result.named_buffer("merged", lhs.tag))?
        }
    };
    result.push(operation);
    Ok(result.last_buffer())
}

fn propagate_nulls1<'a, F>(plan: Box<QueryPlan>,
                           result: &mut QueryExecutor<'a>, op: F) -> Result<TypedBufferRef, QueryError>
    where F: Fn(TypedBufferRef, &mut QueryExecutor<'a>) -> Result<BoxedOperator<'a>, QueryError> {
    let plan = prepare(*plan, result)?;
    if plan.tag.is_nullable() {
        let operator = op(plan.forget_nullability(), result)?;
        result.push(operator);
        prepare(propagate_nullability(plan, result.last_buffer()), result)
    } else {
        let operator = op(plan, result)?;
        result.push(operator);
        Ok(result.last_buffer())
    }
}

fn propagate_nulls2<'a, F>(lhs: Box<QueryPlan>,
                           rhs: Box<QueryPlan>,
                           result: &mut QueryExecutor<'a>, op: F) -> Result<TypedBufferRef, QueryError>
    where F: Fn(TypedBufferRef, TypedBufferRef, &mut QueryExecutor<'a>) -> Result<BoxedOperator<'a>, QueryError> {
    let lhs = prepare(*lhs, result)?;
    let rhs = prepare(*rhs, result)?;
    if lhs.tag.is_nullable() {
        let operator = op(lhs.forget_nullability(), rhs, result)?;
        result.push(operator);
        prepare(propagate_nullability(lhs, result.last_buffer()), result)
    } else {
        let operator = op(lhs, rhs, result)?;
        result.push(operator);
        Ok(result.last_buffer())
    }
}

pub fn prepare_hashmap_grouping(raw_grouping_key: TypedBufferRef,
                                max_cardinality: usize,
                                result: &mut QueryExecutor)
                                -> Result<(Option<TypedBufferRef>,
                                           TypedBufferRef,
                                           Type,
                                           BufferRef<Scalar<i64>>), QueryError> {
    let unique_out = result.named_buffer("unique", raw_grouping_key.tag.clone());
    let grouping_key_out = result.buffer_u32("grouping_key");
    let cardinality_out = result.buffer_scalar_i64("cardinality");
    result.push(
        VecOperator::hash_map_grouping(raw_grouping_key,
                                       unique_out,
                                       grouping_key_out,
                                       cardinality_out,
                                       max_cardinality)?);
    Ok((Some(unique_out),
        grouping_key_out.into(),
        Type::encoded(Codec::opaque(EncodingType::U32, BasicType::Integer, false, false, true, true)),
        cardinality_out))
}

// TODO(clemens): add QueryPlan::Aggregation and merge with prepare function
pub fn prepare_aggregation<'a>(plan: TypedBufferRef,
                               plan_type: Type,
                               grouping_key: TypedBufferRef,
                               max_index: BufferRef<Scalar<i64>>,
                               aggregator: Aggregator,
                               planner: &mut QueryPlanner,
                               result: &mut QueryExecutor<'a>)
                               -> Result<(TypedBufferRef, Type), QueryError> {
    let output_location;
    let (operation, t): (BoxedOperator<'a>, _) = match (aggregator, plan) {
        (Aggregator::Count, _) => {
            output_location = result.named_buffer("count", EncodingType::U32);
            (VecOperator::count(grouping_key,
                                output_location.u32()?,
                                max_index)?,
             Type::encoded(Codec::integer_cast(EncodingType::U32)))
        }
        (Aggregator::Sum, mut plan) => {
            output_location = result.named_buffer("sum", EncodingType::I64);
            if !plan_type.is_summation_preserving() {
                plan = plan_type.codec.clone().unwrap().decode(plan, planner, result);
            }
            (VecOperator::summation(plan,
                                    grouping_key,
                                    output_location.i64()?,
                                    max_index)?, // TODO(clemens): determine dense groupings
             Type::unencoded(BasicType::Integer))
        }
    };
    result.push(operation);
    Ok((output_location, t))
}

pub fn order_preserving((plan, t): (TypedBufferRef, Type),
                        planner: &mut QueryPlanner,
                        result: &mut QueryExecutor) -> (TypedBufferRef, Type) {
    if t.is_order_preserving() {
        (plan, t)
    } else {
        let new_type = t.decoded();
        (t.codec.unwrap().decode(plan, planner, result), new_type)
    }
}

type Factory = Box<Fn(&mut QueryPlanner, TypedBufferRef, TypedBufferRef, &mut QueryExecutor) -> TypedBufferRef + Sync>;

struct Function2 {
    pub factory: Factory,
    pub type_rhs: BasicType,
    pub type_lhs: BasicType,
    pub type_out: Type,
    pub encoding_invariance: bool,
}

impl Function2 {
    pub fn integer_op(factory: Factory) -> Function2 {
        Function2 {
            factory,
            type_lhs: BasicType::Integer,
            type_rhs: BasicType::Integer,
            type_out: Type::unencoded(BasicType::Integer).mutable(),
            encoding_invariance: false,
        }
    }

    pub fn comparison_op(factory: Factory,
                         t: BasicType) -> Function2 {
        Function2 {
            factory,
            type_lhs: t,
            type_rhs: t,
            type_out: Type::unencoded(BasicType::Boolean).mutable(),
            encoding_invariance: true,
        }
    }
}

lazy_static! {
    static ref FUNCTION2_REGISTRY: HashMap<Func2Type, Vec<Function2>> = function2_registry();
}

fn function2_registry() -> HashMap<Func2Type, Vec<Function2>> {
    vec![
        (Func2Type::Add,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs, x| qp.add(lhs, rhs, x).into()))]),
        (Func2Type::Subtract,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs, x| qp.subtract(lhs, rhs, x).into()))]),
        (Func2Type::Multiply,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs, x| qp.multiply(lhs, rhs, x).into()))]),
        (Func2Type::Divide,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs, x| qp.divide(lhs, rhs, x).into()))]),
        (Func2Type::Modulo,
         vec![Function2::integer_op(Box::new(|qp, lhs, rhs, x| qp.modulo(lhs, rhs, x).into()))]),
        (Func2Type::LT,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.less_than(lhs, rhs, x).into()),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.less_than(lhs, rhs, x).into()),
                                       BasicType::String)]),
        (Func2Type::LTE,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.less_than_equals(lhs, rhs, x).into()),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.less_than_equals(lhs, rhs, x).into()),
                                       BasicType::String)]),
        (Func2Type::GT,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.less_than(rhs, lhs, x).into()),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.less_than(rhs, lhs, x).into()),
                                       BasicType::String)]),
        (Func2Type::GTE,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.less_than_equals(rhs, lhs, x).into()),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.less_than_equals(rhs, lhs, x).into()),
                                       BasicType::String)]),
        (Func2Type::Equals,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.equals(lhs, rhs, x).into()),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.equals(lhs, rhs, x).into()),
                                       BasicType::String)]),
        (Func2Type::NotEquals,
         vec![Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.not_equals(lhs, rhs, x).into()),
                                       BasicType::Integer),
              Function2::comparison_op(Box::new(|qp, lhs, rhs, x| qp.not_equals(lhs, rhs, x).into()),
                                       BasicType::String)]),
    ].into_iter().collect()
}

impl QueryPlan {
    pub fn compile_expr(
        expr: &Expr,
        filter: Filter,
        columns: &HashMap<String, Arc<DataSource>>,
        planner: &mut QueryPlanner,
        result: &mut QueryExecutor) -> Result<(TypedBufferRef, Type), QueryError> {
        use self::Expr::*;
        use self::Func2Type::*;
        Ok(match *expr {
            ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(c) => {
                    let mut plan = planner.column_section(name, 0, c.range(), c.encoding_type(), result);
                    let mut t = c.full_type();
                    if !c.codec().is_elementwise_decodable() {
                        let (codec, fixed_width) = c.codec().ensure_fixed_width(plan, planner, result);
                        t = Type::encoded(codec);
                        plan = fixed_width;
                    }
                    plan = match filter {
                        Filter::U8(filter) => planner.filter(plan, filter, result),
                        Filter::Indices(indices) => planner.select(plan, indices, result),
                        Filter::None => plan,
                    };
                    (plan, t)
                }
                None => bail!(QueryError::NotImplemented, "Referencing missing column {}", name)
            }
            Func2(Or, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::compile_expr(lhs, filter, columns, planner, result)?;
                let (plan_rhs, type_rhs) = QueryPlan::compile_expr(rhs, filter, columns, planner, result)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} OR {}, expected bool OR bool")
                }
                (planner.or(plan_lhs.u8()?, plan_rhs.u8()?, result).into(), Type::bit_vec())
            }
            Func2(And, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::compile_expr(lhs, filter, columns, planner, result)?;
                let (plan_rhs, type_rhs) = QueryPlan::compile_expr(rhs, filter, columns, planner, result)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} AND {}, expected bool AND bool")
                }
                (planner.and(plan_lhs.u8()?, plan_rhs.u8()?, result).into(), Type::bit_vec())
            }
            Func2(RegexMatch, ref expr, ref regex) => {
                match regex {
                    box Const(RawVal::Str(regex)) => {
                        Regex::new(&regex).map_err(|e| QueryError::TypeError(
                            format!("`{}` is not a valid regex: {}", regex, e)))?;
                        let (mut plan, t) = QueryPlan::compile_expr(expr, filter, columns, planner, result)?;
                        if t.decoded != BasicType::String {
                            bail!(QueryError::TypeError, "Expected expression of type `String` as first argument to regex. Actual: {:?}", t)
                        }
                        if let Some(codec) = t.codec.clone() {
                            plan = codec.decode(plan, planner, result);
                        }
                        (planner.regex(plan.str()?, regex, result).into(), t)
                    }
                    _ => bail!(QueryError::TypeError, "Expected string constant as second argument to `regex`, actual: {:?}", regex),
                }
            }
            Func2(function, ref lhs, ref rhs) => {
                let (mut plan_lhs, mut type_lhs) = QueryPlan::compile_expr(lhs, filter, columns, planner, result)?;
                let (mut plan_rhs, mut type_rhs) = QueryPlan::compile_expr(rhs, filter, columns, planner, result)?;

                let declarations = match FUNCTION2_REGISTRY.get(&function) {
                    Some(patterns) => patterns,
                    None => bail!(QueryError::NotImplemented, "function {:?}", function),
                };
                let declaration = match declarations.iter().find(
                    |p| p.type_lhs == type_lhs.decoded && p.type_rhs == type_rhs.decoded) {
                    Some(declaration) => declaration,
                    None => bail!(
                        QueryError::TypeError,
                        "Function {:?} is not implemented for types {:?}, {:?}",
                        function, type_lhs, type_rhs),
                };

                if declaration.encoding_invariance && type_lhs.is_scalar && type_rhs.is_encoded() {
                    plan_lhs = if type_rhs.decoded == BasicType::Integer {
                        if let QueryPlan::ScalarI64 { value, .. } = *planner.resolve(&plan_lhs) {
                            planner.scalar_i64(type_rhs.codec.unwrap().encode_int(value), false, result).into()
                        } else {
                            panic!("whoops");
                        }
                    } else if type_rhs.decoded == BasicType::String {
                        type_rhs.codec.clone().unwrap().encode_str(plan_lhs.scalar_str()?, planner, result).into()
                    } else {
                        panic!("whoops");
                    };
                } else if declaration.encoding_invariance && type_rhs.is_scalar && type_lhs.is_encoded() {
                    plan_rhs = if type_lhs.decoded == BasicType::Integer {
                        if let QueryPlan::ScalarI64 { value, .. } = *planner.resolve(&plan_rhs) {
                            planner.scalar_i64(type_lhs.codec.unwrap().encode_int(value), false, result).into()
                        } else {
                            panic!("whoops");
                        }
                    } else if type_lhs.decoded == BasicType::String {
                        type_lhs.codec.clone().unwrap().encode_str(plan_rhs.scalar_str()?, planner, result).into()
                    } else {
                        panic!("whoops");
                    };
                } else {
                    if let Some(codec) = type_lhs.codec {
                        plan_lhs = codec.decode(plan_lhs, planner, result);
                    }
                    if let Some(codec) = type_rhs.codec {
                        plan_rhs = codec.decode(plan_rhs, planner, result);
                    }
                }

                let plan = (declaration.factory)(planner, plan_lhs, plan_rhs, result);
                (plan, declaration.type_out.clone())
            }
            Func1(ftype, ref inner) => {
                let (plan, t) = QueryPlan::compile_expr(inner, filter, columns, planner, result)?;
                let decoded = match t.codec.clone() {
                    Some(codec) => codec.decode(plan, planner, result),
                    None => plan,
                };
                let plan = match ftype {
                    Func1Type::ToYear => {
                        if t.decoded != BasicType::Integer {
                            bail!(QueryError::TypeError, "Found to_year({:?}), expected to_year(integer)", &t)
                        }
                        planner.to_year(decoded.i64()?, result).into()
                    }
                    Func1Type::Not => {
                        if t.decoded != BasicType::Boolean {
                            bail!(QueryError::TypeError, "Found NOT({:?}), expected NOT(boolean)", &t)
                        }
                        planner.not(decoded.u8()?, result).into()
                    }
                    Func1Type::Negate => {
                        bail!(QueryError::TypeError, "Found negate({:?}), expected negate(integer)", &t)
                    }
                };
                (plan, t.decoded())
            }
            Const(RawVal::Int(i)) => (planner.scalar_i64(i, false, result).into(), Type::scalar(BasicType::Integer)),
            Const(RawVal::Str(ref s)) => (planner.scalar_str(s, result).into(), Type::scalar(BasicType::String)),
            ref x => bail!(QueryError::NotImplemented, "{:?}.compile_vec()", x),
        })
    }
}

fn encoding_range(plan: &TypedBufferRef, planner: &QueryPlanner) -> Option<(i64, i64)> {
    // TODO(clemens): need more principled approach - this currently doesn't work for all partially decodings
    // Example: [LZ4, Add, Delta] will have as bottom decoding range the range after indices, max_index Delta, but without the Add :/
    // This works in this case because we always have to decode the Delta, but is hard to reason about and has caused bugs
    use self::QueryPlan::*;
    match *planner.resolve(plan) {
        ColumnSection { range, .. } => range,
        ToYear { timestamp, .. } => encoding_range(&timestamp.into(), planner).map(|(min, max)|
            (i64::from(NaiveDateTime::from_timestamp(min, 0).year()),
             i64::from(NaiveDateTime::from_timestamp(max, 0).year()))
        ),
        Filter { ref plan, .. } => encoding_range(plan, planner),
        // TODO(clemens): this is just wrong
        Divide { ref lhs, ref rhs, .. } => if let ScalarI64 { value: c, .. } = planner.resolve(rhs) {
            encoding_range(lhs, planner).map(|(min, max)|
                if *c > 0 { (min / *c, max / *c) } else { (max / *c, min / *c) })
        } else {
            None
        },
        Add { ref lhs, ref rhs, .. } => if let ScalarI64 { value: c, .. } = planner.resolve(rhs) {
            encoding_range(lhs, planner).map(|(min, max)| (min + *c, max + *c))
        } else {
            None
        },
        Cast { ref input, .. } => encoding_range(input, planner),
        LZ4Decode { bytes, .. } => encoding_range(&bytes.into(), planner),
        DeltaDecode { ref plan, .. } => encoding_range(plan, planner),
        _ => None, // TODO(clemens): many more cases where we can determine range
    }
}

fn replace_common_subexpression(plan: QueryPlan, _executor: &mut QueryExecutor) -> (Box<QueryPlan>, [u8; 16]) {
    // use std::intrinsics::discriminant_value;
    // use self::QueryPlan::*;
    return (Box::new(plan), [0u8; 16]);
    /*
        unsafe /* dicriminant_value */ {
            let mut signature = [0u8; 16];
            let mut hasher = Md5::new();
            hasher.input(&discriminant_value(&plan).to_ne_bytes());
            let plan = match plan {
                ColumnSection { name, section, range, t } => {
                    hasher.input_str(&name);
                    hasher.input(&section.to_ne_bytes());
                    ColumnSection { name, section, range, t }
                }
                ReadBuffer { buffer } => {
                    hasher.input(&buffer.buffer.i.to_ne_bytes());
                    ReadBuffer { buffer }
                }
                AssembleNullable { data, present } => {
                    let (data, s1) = replace_common_subexpression(*data, executor);
                    let (present, s2) = replace_common_subexpression(*present, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    AssembleNullable { data, present }
                }
                MakeNullable { data, present, nullable } => {
                    hasher.input(&data.buffer.i.to_ne_bytes());
                    hasher.input(&present.i.to_ne_bytes());
                    hasher.input(&nullable.buffer.i.to_ne_bytes());
                    MakeNullable { data, present, nullable }
                }
                PropagateNullability { nullable, data } => {
                    let (data, s1) = replace_common_subexpression(*data, executor);
                    let (nullable, s2) = replace_common_subexpression(*nullable, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    PropagateNullability { nullable, data }
                }
                d @ DictLookup { .. } => d,
                InverseDictLookup { offset_len, backing_store, constant } => {
                    let (offset_len, s1) = replace_common_subexpression(*offset_len, executor);
                    let (backing_store, s2) = replace_common_subexpression(*backing_store, executor);
                    let (constant, s3) = replace_common_subexpression(*constant, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    hasher.input(&s3);
                    InverseDictLookup { offset_len, backing_store, constant }
                }
                Cast { input, target_type } => {
                    let (input, s1) = replace_common_subexpression(*input, executor);
                    hasher.input(&s1);
                    hasher.input(&discriminant_value(&target_type).to_ne_bytes());
                    Cast { input, target_type }
                }
                LZ4Decode { bytes, decoded_len, t } => {
                    let (bytes, s1) = replace_common_subexpression(*bytes, executor);
                    hasher.input(&s1);
                    hasher.input(&discriminant_value(&t).to_ne_bytes());
                    LZ4Decode { bytes, decoded_len, t }
                }
                UnpackStrings { bytes } => {
                    let (bytes, s1) = replace_common_subexpression(*bytes, executor);
                    hasher.input(&s1);
                    UnpackStrings { bytes }
                }
                UnhexpackStrings { bytes, uppercase, total_bytes } => {
                    let (bytes, s1) = replace_common_subexpression(*bytes, executor);
                    hasher.input(&s1);
                    hasher.input(&total_bytes.to_ne_bytes());
                    hasher.input(&[uppercase as u8]);
                    UnhexpackStrings { bytes, uppercase, total_bytes }
                }
                DeltaDecode { plan } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    hasher.input(&s1);
                    DeltaDecode { plan }
                }
                x @ Exists { .. } => x,
                NonzeroCompact { plan } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    hasher.input(&s1);
                    NonzeroCompact { plan }
                }
                NonzeroIndices { plan, indices_type } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    hasher.input(&s1);
                    hasher.input(&discriminant_value(&indices_type).to_ne_bytes());
                    NonzeroIndices { plan, indices_type }
                }
                Compact { plan, select } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    let (select, s2) = replace_common_subexpression(*select, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Compact { plan, select }
                }
                EncodeIntConstant { constant, codec } => {
                    // TODO(clemens): codec needs to be part of signature (easy once we encode using actual query plan rather than codec)
                    let (constant, s1) = replace_common_subexpression(*constant, executor);
                    hasher.input(&s1);
                    EncodeIntConstant { constant, codec }
                }
                BitPack { lhs, rhs, shift } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    hasher.input(&(shift as u64).to_ne_bytes());
                    BitPack { lhs, rhs, shift }
                }
                BitUnpack { plan, shift, width } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    hasher.input(&s1);
                    hasher.input(&shift.to_ne_bytes());
                    hasher.input(&width.to_ne_bytes());
                    BitUnpack { plan, shift, width }
                }
                SlicePack { plan, stride, offset } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    hasher.input(&s1);
                    hasher.input(&stride.to_ne_bytes());
                    hasher.input(&offset.to_ne_bytes());
                    SlicePack { plan, stride, offset }
                }
                SliceUnpack { plan, t, stride, offset } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    hasher.input(&s1);
                    hasher.input(&stride.to_ne_bytes());
                    hasher.input(&offset.to_ne_bytes());
                    hasher.input(&discriminant_value(&t).to_ne_bytes());
                    SliceUnpack { plan, t, stride, offset }
                }
                Convergence(plans) => {
                    let mut new_plans = Vec::new();
                    for p in plans {
                        let (p, s) = replace_common_subexpression(*p, executor);
                        new_plans.push(p);
                        hasher.input(&s);
                    }
                    Convergence(new_plans)
                }
                LessThan { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    LessThan { lhs, rhs }
                }
                LessThanEquals { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    LessThanEquals { lhs, rhs }
                }
                Equals { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Equals { lhs, rhs }
                }
                NotEquals { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    NotEquals { lhs, rhs }
                }
                Add { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Add { lhs, rhs }
                }
                Subtract { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Subtract { lhs, rhs }
                }
                Multiply { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Multiply { lhs, rhs }
                }
                Divide { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Divide { lhs, rhs }
                }
                Modulo { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Modulo { lhs, rhs }
                }
                And { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    And { lhs, rhs }
                }
                Or { lhs, rhs } => {
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Or { lhs, rhs }
                }
                Not { input } => {
                    let (input, s1) = replace_common_subexpression(*input, executor);
                    hasher.input(&s1);
                    Not { input }
                }
                Regex { plan, regex } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    hasher.input(&s1);
                    hasher.input_str(&regex);
                    Regex { plan, regex }
                }
                ToYear { timestamp } => {
                    let (timestamp, s1) = replace_common_subexpression(*timestamp, executor);
                    hasher.input(&s1);
                    ToYear { timestamp }
                }
                Indices { plan } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    hasher.input(&s1);
                    Indices { plan }
                }
                SortBy { ranking, indices, desc, stable } => {
                    let (ranking, s1) = replace_common_subexpression(*ranking, executor);
                    let (indices, s2) = replace_common_subexpression(*indices, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    hasher.input(&[desc as u8]);
                    SortBy { ranking, indices, desc, stable }
                }
                TopN { ranking, n, desc } => {
                    let (ranking, s1) = replace_common_subexpression(*ranking, executor);
                    hasher.input(&s1);
                    hasher.input(&n.to_ne_bytes());
                    hasher.input(&[desc as u8]);
                    TopN { ranking, n, desc }
                }
                Select { plan, indices } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    let (indices, s2) = replace_common_subexpression(*indices, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Select { plan, indices }
                }
                Filter { plan, select } => {
                    let (plan, s1) = replace_common_subexpression(*plan, executor);
                    let (select, s2) = replace_common_subexpression(*select, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    Filter { plan, select }
                }
                EncodedGroupByPlaceholder => EncodedGroupByPlaceholder,
                Constant { value, hide_value } => {
                    match value {
                        RawVal::Int(i) => hasher.input(&(i as u64).to_ne_bytes()),
                        RawVal::Str(ref s) => hasher.input_str(s),
                        RawVal::Null => {}
                    }
                    Constant { value, hide_value }
                }
                ScalarI64 { value, hide_value } => {
                    hasher.input(&(value as u64).to_ne_bytes());
                    ScalarI64 { value, hide_value }
                }
                ScalarStr { value } => {
                    hasher.input_str(&value);
                    ScalarStr { value }
                }
                ConstantExpand { value, t, len } => {
                    hasher.input(&value.to_ne_bytes());
                    hasher.input(&discriminant_value(&t).to_ne_bytes());
                    ConstantExpand { value, t, len }
                }
                QueryPlan::MergeKeep { take_left, lhs, rhs } => {
                    let (take_left, s3) = replace_common_subexpression(*take_left, executor);
                    let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                    let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                    hasher.input(&s1);
                    hasher.input(&s2);
                    hasher.input(&s3);
                    QueryPlan::MergeKeep { take_left, lhs, rhs }
                }
            };

            hasher.result(&mut signature);
            match executor.get(&signature) {
                Some(plan) => (Box::new(plan), signature),
                None => (Box::new(plan), signature),
            }
        }*/
}

pub fn compile_grouping_key(
    exprs: &[Expr],
    filter: Filter,
    columns: &HashMap<String, Arc<DataSource>>,
    partition_length: usize,
    planner: &mut QueryPlanner,
    executor: &mut QueryExecutor)
    -> Result<((TypedBufferRef, Type), i64, Vec<(TypedBufferRef, Type)>, TypedBufferRef), QueryError> {
    if exprs.is_empty() {
        let t = Type::new(BasicType::Integer, Some(Codec::opaque(EncodingType::U8, BasicType::Integer, true, true, true, true)));
        let mut plan = planner.constant_expand(0, partition_length, EncodingType::U8, executor);
        plan = match filter {
            Filter::U8(filter) => planner.filter(plan, filter, executor),
            Filter::Indices(indices) => planner.select(plan, indices, executor),
            Filter::None => plan,
        };
        Ok((
            (plan, t),
            1,
            vec![],
            executor.named_buffer("empty_group_by", EncodingType::Null)
        ))
    } else if exprs.len() == 1 {
        QueryPlan::compile_expr(&exprs[0], filter, columns, planner, executor)
            .map(|(gk_plan, gk_type)| {
                let encoding_range = encoding_range(&gk_plan, planner);
                debug!("Encoding range of {:?} for {:?}", &encoding_range, &gk_plan);
                let (max_cardinality, offset) = match encoding_range {
                    Some((min, max)) => if min < 0 { (max - min, -min) } else { (max, 0) }
                    None => (1 << 62, 0),
                };
                let gk_plan = if offset != 0 {
                    let offset = planner.scalar_i64(offset, true, executor);
                    planner.add(gk_plan, offset.into(), executor).into()
                } else { gk_plan };

                let encoded_group_by_placeholder = executor.named_buffer(
                    "encoded_group_by_placeholder", gk_type.encoding_type());
                let decoded_group_by = gk_type.codec.clone().map_or(
                    encoded_group_by_placeholder,
                    |codec| codec.decode(encoded_group_by_placeholder, planner, executor));
                let decoded_group_by = if offset == 0 { decoded_group_by } else {
                    let offset = planner.scalar_i64(-offset, true, executor);
                    let sum = planner.add(decoded_group_by, offset.into(), executor).into();
                    planner.cast(sum, gk_type.encoding_type(), executor)
                };

                ((gk_plan.clone(), gk_type.clone()),
                 max_cardinality,
                 vec![(decoded_group_by, gk_type.decoded())],
                 encoded_group_by_placeholder)
            })
    } else if let Some(result) = try_bitpacking(exprs, filter, columns, planner, executor)? {
        Ok(result)
    } else {
        info!("Failed to bitpack grouping key");
        let mut pack = Vec::new();
        let mut decode_plans = Vec::new();
        let encoded_group_by_placeholder =
            executor.named_buffer("encoded_group_by_placeholder", EncodingType::ByteSlices(exprs.len()));
        for (i, expr) in exprs.iter().enumerate() {
            let (query_plan, plan_type) = QueryPlan::compile_expr(expr, filter, columns, planner, executor)?;
            pack.push(planner.slice_pack(query_plan, exprs.len(), i, executor));

            // TODO(clemens): negative integers can throw off sort order - need to move into positive range
            let mut decode_plan = planner.slice_unpack(
                encoded_group_by_placeholder.any(),
                exprs.len(),
                i,
                plan_type.encoding_type(),
                executor);
            if let Some(codec) = plan_type.codec.clone() {
                decode_plan = codec.decode(decode_plan, planner, executor);
            }
            decode_plans.push(
                (decode_plan,
                 plan_type.decoded()));
        }
        let t = Type::encoded(Codec::opaque(
            EncodingType::ByteSlices(exprs.len()),
            BasicType::Val,
            false /* is_summation_preserving */,
            true  /* is_order_preserving */,
            false /* is_positive_integer */,
            false /* is_fixed_width */,
        ));
        Ok(((pack[0].into(), t),
            i64::MAX,
            decode_plans,
            encoded_group_by_placeholder))
    }
}

fn try_bitpacking(
    exprs: &[Expr],
    filter: Filter,
    columns: &HashMap<String, Arc<DataSource>>,
    planner: &mut QueryPlanner,
    executor: &mut QueryExecutor)
    -> Result<Option<((TypedBufferRef, Type), i64, Vec<(TypedBufferRef, Type)>, TypedBufferRef)>, QueryError> {
    planner.checkpoint();
    // TODO(clemens): use u64 as grouping key type
    let mut total_width = 0;
    let mut largest_key = 0;
    let mut plan: Option<BufferRef<i64>> = None;
    let mut decode_plans = Vec::with_capacity(exprs.len());
    let mut order_preserving = true;
    let encoded_group_by_placeholder =
        executor.buffer_i64("encoded_group_by_placeholder");
    for expr in exprs.iter().rev() {
        let (query_plan, plan_type) = QueryPlan::compile_expr(expr, filter, columns, planner, executor)?;
        let encoding_range = encoding_range(&query_plan, planner);
        debug!("Encoding range of {:?} for {:?}", &encoding_range, &query_plan);
        if let Some((min, max)) = encoding_range {
            fn bits(max: i64) -> i64 {
                ((max + 1) as f64).log2().ceil() as i64
            }

            // TODO(clemens): more intelligent criterion. threshold should probably be a function of total width.
            let subtract_offset = bits(max) - bits(max - min) > 1 || min < 0;
            let adjusted_max = if subtract_offset { max - min } else { max };
            order_preserving = order_preserving && plan_type.is_order_preserving();
            let query_plan = if subtract_offset {
                let offset = planner.scalar_i64(-min, true, executor);
                planner.add(query_plan, offset.into(), executor)
            } else {
                planner.cast(query_plan, EncodingType::I64, executor).i64()?
            };

            if total_width == 0 {
                plan = Some(query_plan);
            } else if adjusted_max > 0 {
                plan = plan.map(|plan| planner.bit_pack(plan, query_plan, total_width, executor));
            }

            let mut decode_plan = planner.bit_unpack(
                encoded_group_by_placeholder,
                total_width as u8,
                bits(adjusted_max) as u8,
                executor).into();
            if subtract_offset {
                let offset = planner.scalar_i64(min, true, executor);
                decode_plan = planner.add(decode_plan, offset.into(), executor).into();
            }
            decode_plan = planner.cast(decode_plan, plan_type.encoding_type(), executor);
            if let Some(codec) = plan_type.codec.clone() {
                decode_plan = codec.decode(decode_plan, planner, executor);
            }
            decode_plans.push((decode_plan, plan_type.decoded()));

            largest_key += adjusted_max << total_width;
            total_width += bits(adjusted_max);
        } else {
            planner.reset();
            return Ok(None);
        }
    }

    Ok(
        if total_width <= 64 {
            plan.map(|plan| {
                decode_plans.reverse();
                let t = Type::encoded(Codec::opaque(
                    EncodingType::I64, BasicType::Integer, false, order_preserving, true, true));
                ((plan.into(), t), largest_key, decode_plans, encoded_group_by_placeholder.into())
            })
        } else {
            planner.reset();
            None
        }
    )
}

impl From<TypedBufferRef> for QueryPlan {
    fn from(buffer: TypedBufferRef) -> QueryPlan { read_buffer(buffer) }
}

