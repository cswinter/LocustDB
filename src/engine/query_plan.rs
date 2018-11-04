use std::collections::HashMap;
use std::sync::Arc;
use std::i64;

use chrono::{Datelike, NaiveDateTime};
use crypto::digest::Digest;
use crypto::md5::Md5;
use itertools::Itertools;

use ::QueryError;
use engine::aggregator::Aggregator;
use engine::filter::Filter;
use engine::types::*;
use engine::vector_op::*;
use engine::vector_op::vector_operator::BufferRef;
use ingest::raw_val::RawVal;
use mem_store::*;
use mem_store::column::Column;
use syntax::expression::*;


type TypedPlan = (QueryPlan, Type);

#[derive(Debug, Clone)]
pub enum QueryPlan {
    ReadColumnSection(String, usize, Option<(i64, i64)>),
    ReadBuffer(BufferRef<Any>),

    DictLookup(Box<QueryPlan>, EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    InverseDictLookup(Box<QueryPlan>, Box<QueryPlan>, Box<QueryPlan>),
    Cast(Box<QueryPlan>, EncodingType, EncodingType),
    LZ4Decode(Box<QueryPlan>, usize, EncodingType),
    UnpackStrings(Box<QueryPlan>),
    UnhexpackStrings(Box<QueryPlan>, bool, usize),
    DeltaDecode(Box<QueryPlan>, EncodingType),

    Exists(Box<QueryPlan>, EncodingType, Box<QueryPlan>),
    NonzeroCompact(Box<QueryPlan>, EncodingType),
    NonzeroIndices(Box<QueryPlan>, EncodingType, EncodingType),
    Compact(Box<QueryPlan>, EncodingType, Box<QueryPlan>, EncodingType),

    EncodeIntConstant(Box<QueryPlan>, Codec),

    BitPack(Box<QueryPlan>, Box<QueryPlan>, i64),
    BitUnpack(Box<QueryPlan>, u8, u8),

    SlicePack(Box<QueryPlan>, EncodingType, usize, usize),
    SliceUnpack(Box<QueryPlan>, EncodingType, usize, usize),

    LessThanVS(EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    EqualsVS(EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    NotEqualsVS(EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    DivideVS(Box<QueryPlan>, Box<QueryPlan>),
    AddVS(EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    And(Box<QueryPlan>, Box<QueryPlan>),
    Or(Box<QueryPlan>, Box<QueryPlan>),
    ToYear(Box<QueryPlan>),

    SortIndices(Box<QueryPlan>, bool),
    TopN(Box<QueryPlan>, EncodingType, usize, bool),

    Select(Box<QueryPlan>, Box<QueryPlan>, EncodingType),
    Filter(Box<QueryPlan>, EncodingType, Box<QueryPlan>),

    EncodedGroupByPlaceholder,

    Convergence(Vec<Box<QueryPlan>>),

    Constant(RawVal, bool),
}

impl QueryPlan {
    fn is_constant(&self) -> bool {
        if let QueryPlan::Constant(_, _) = self { true } else { false }
    }
}

pub fn prepare(plan: QueryPlan, result: &mut QueryExecutor) -> BufferRef<Any> {
    _prepare(plan, false, result)
}

pub fn prepare_no_alias(plan: QueryPlan, result: &mut QueryExecutor) -> BufferRef<Any> {
    _prepare(plan, true, result)
}

fn _prepare(plan: QueryPlan, no_alias: bool, result: &mut QueryExecutor) -> BufferRef<Any> {
    trace!("{:?}", &plan);
    let (plan, signature) = if no_alias || plan.is_constant() {
        (plan, [0; 16])
    } else {
        // TODO(clemens): O(n^2) :(   use visitor pattern?
        let (box plan, signature) = replace_common_subexpression(plan, result);
        (plan, signature)
    };
    trace!("{:?} {}", &plan, to_hex_string(&signature));
    let operation: Box<VecOperator> = match plan {
        QueryPlan::Select(plan, indices, t) =>
            VecOperator::select((prepare(*plan, result), t),
                                prepare(*indices, result).usize(),
                                result.tagged_buffer("selection", t)),
        QueryPlan::ReadColumnSection(colname, section, _) =>
            VecOperator::read_column_data(colname, section, result.named_buffer("column")),
        QueryPlan::Filter(plan, t, filter) =>
            VecOperator::filter((prepare(*plan, result), t),
                                prepare(*filter, result).u8(),
                                result.tagged_buffer("filtered", t)),
        QueryPlan::Constant(ref c, hide_value) =>
            VecOperator::constant(c.clone(), hide_value, result.named_buffer("constant").raw_val()),
        QueryPlan::DictLookup(plan, t, dict_indices, dict_data) =>
            VecOperator::dict_lookup(
                (prepare(*plan, result), t),
                prepare(*dict_indices, result).u64(),
                prepare(*dict_data, result).u8(),
                result.named_buffer("decoded").str()),
        QueryPlan::InverseDictLookup(dict_indices, dict_data, constant) =>
            VecOperator::inverse_dict_lookup(
                prepare(*dict_indices, result).u64(),
                prepare(*dict_data, result).u8(),
                prepare(*constant, result).string(),
                result.named_buffer("encoded").raw_val()),
        QueryPlan::Cast(plan, initial_type, target_type) =>
            VecOperator::type_conversion(
                (prepare(*plan, result), initial_type),
                (result.named_buffer("casted"), target_type)),
        QueryPlan::DeltaDecode(plan, t) =>
            VecOperator::delta_decode(
                (prepare(*plan, result), t),
                result.named_buffer("decoded").i64()),
        QueryPlan::LZ4Decode(plan, decoded_len, t) =>
            VecOperator::lz4_decode(
                prepare(*plan, result).u8(),
                (result.named_buffer("decoded"), t),
                decoded_len),
        QueryPlan::UnpackStrings(plan) =>
            VecOperator::unpack_strings(
                prepare(*plan, result).u8(),
                result.named_buffer("unpacked").str()),
        QueryPlan::UnhexpackStrings(plan, uppercase, total_bytes) => {
            let stringstore = result.named_buffer("stringstore").u8();
            VecOperator::unhexpack_strings(
                prepare(*plan, result).u8(),
                result.named_buffer("unpacked").str(),
                stringstore, uppercase, total_bytes)
        }
        QueryPlan::Exists(indices, t, max_index) =>
            VecOperator::exists(
                (prepare(*indices, result), t),
                result.named_buffer("exists").u8(),
                prepare(*max_index, result).i64()),
        QueryPlan::Compact(data, data_t, select, select_t) => {
            let inplace = prepare(*data, result);
            let op = VecOperator::compact((inplace, data_t), (prepare(*select, result), select_t));
            result.push(op);
            return inplace;
        }
        QueryPlan::NonzeroIndices(indices, indices_t, output_t) =>
            VecOperator::nonzero_indices(
                (prepare(*indices, result), indices_t),
                (result.named_buffer("nonzero_indices"), output_t)),
        QueryPlan::NonzeroCompact(data, data_t) => {
            let inplace = prepare(*data, result);
            result.push(VecOperator::nonzero_compact((inplace, data_t)));
            return inplace;
        }
        QueryPlan::EncodeIntConstant(plan, codec) =>
            VecOperator::encode_int_const(
                prepare(*plan, result).i64(),
                result.named_buffer("encoded").i64(),
                codec),
        QueryPlan::BitPack(lhs, rhs, shift_amount) =>
            VecOperator::bit_shift_left_add(
                prepare(*lhs, result).i64(),
                prepare(*rhs, result).i64(),
                result.named_buffer("bitpacked").i64(),
                shift_amount),
        QueryPlan::BitUnpack(inner, shift, width) =>
            VecOperator::bit_unpack(
                prepare(*inner, result).i64(),
                result.named_buffer("unpacked").i64(),
                shift,
                width),

        QueryPlan::SlicePack(input, t, stride, offset) =>
            VecOperator::slice_pack(
                (prepare(*input, result), t),
                result.shared_buffer("slicepack"),
                stride,
                offset),
        QueryPlan::SliceUnpack(input, t, stride, offset) =>
            VecOperator::slice_unpack(
                prepare(*input, result),
                (result.named_buffer("unpacked"), t),
                stride,
                offset),

        QueryPlan::Convergence(plans) => {
            return plans.into_iter().map(|p| prepare(*p, result)).collect::<Vec<_>>()[0];
        }

        QueryPlan::LessThanVS(left_type, lhs, rhs) =>
            VecOperator::less_than_vs(
                (prepare(*lhs, result), left_type),
                prepare(*rhs, result).i64(),
                result.named_buffer("less_than").u8()),
        QueryPlan::EqualsVS(left_type, lhs, rhs) =>
            VecOperator::equals_vs(
                (prepare(*lhs, result), left_type),
                (prepare(*rhs, result), left_type),
                result.named_buffer("equals").u8()),
        QueryPlan::NotEqualsVS(left_type, lhs, rhs) =>
            VecOperator::not_equals_vs(
                (prepare(*lhs, result), left_type),
                (prepare(*rhs, result), left_type),
                result.named_buffer("equals").u8()),
        QueryPlan::DivideVS(lhs, rhs) =>
            VecOperator::divide_vs(
                prepare(*lhs, result).i64(),
                prepare(*rhs, result).i64(),
                result.named_buffer("division").i64()),
        QueryPlan::AddVS(left_type, lhs, rhs) =>
            VecOperator::addition_vs(
                (prepare(*lhs, result), left_type),
                prepare(*rhs, result).i64(),
                result.named_buffer("addition").i64()),
        QueryPlan::Or(lhs, rhs) => {
            let inplace = prepare(*lhs, result);
            // If we don't assign to `operation` and pass expression directly to push, we trigger an infinite loop in the compiler
            // Probably same issue as this: https://github.com/rust-lang/rust/issues/49936
            let operation = VecOperator::or(inplace.u8(), prepare(*rhs, result).u8());
            result.push(operation);
            return inplace;
        }
        QueryPlan::And(lhs, rhs) => {
            let inplace: BufferRef<Any> = prepare(*lhs, result);
            // If we don't assign to `operation` and pass expression directly to push, we trigger an infinite loop in the compiler
            // Probably same issue as this: https://github.com/rust-lang/rust/issues/49936
            let operation = VecOperator::and(inplace.u8(), prepare(*rhs, result).u8());
            result.push(operation);
            return inplace;
        }
        QueryPlan::ToYear(plan) =>
            VecOperator::to_year(prepare(*plan, result).i64(), result.named_buffer("year").i64()),
        QueryPlan::EncodedGroupByPlaceholder => return result.encoded_group_by().unwrap(),
        QueryPlan::SortIndices(plan, descending) =>
            VecOperator::sort_indices(
                prepare(*plan, result),
                result.named_buffer("permutation").usize(),
                descending),
        QueryPlan::TopN(plan, t, n, desc) =>
            VecOperator::top_n(
                (prepare(*plan, result), t),
                (result.named_buffer("tmp_keys"), t),
                result.named_buffer("top_n").usize(),
                n, desc),
        QueryPlan::ReadBuffer(buffer) => return buffer,
    };
    result.push(operation);
    if signature != [0; 16] {
        result.cache_last(signature);
    }
    result.last_buffer()
}

pub fn prepare_hashmap_grouping(raw_grouping_key: BufferRef<Any>,
                                grouping_key_type: EncodingType,
                                max_cardinality: usize,
                                result: &mut QueryExecutor)
                                -> (Option<BufferRef<Any>>, BufferRef<Any>, Type, BufferRef<Any>) {
    let unique_out = result.named_buffer("unique");
    let grouping_key_out = result.named_buffer("grouping_key");
    let cardinality_out = result.named_buffer("cardinality");
    result.push(
        VecOperator::hash_map_grouping((raw_grouping_key, grouping_key_type),
                                       (unique_out, grouping_key_type),
                                       grouping_key_out.u32(),
                                       cardinality_out.i64(),
                                       max_cardinality));
    (Some(unique_out),
     grouping_key_out,
     Type::encoded(Codec::opaque(EncodingType::U32, BasicType::Integer, false, false, true, true)),
     cardinality_out)
}

// TODO(clemens): add QueryPlan::Aggregation and merge with prepare function
pub fn prepare_aggregation<'a>(plan: QueryPlan,
                               mut plan_type: Type,
                               (grouping_key, grouping_type): TypedBufferRef,
                               max_index: BufferRef<Any>,
                               aggregator: Aggregator,
                               result: &mut QueryExecutor<'a>)
                               -> Result<(BufferRef<Any>, Type), QueryError> {
    let output_location;
    let (operation, t): (BoxedOperator<'a>, _) = match (aggregator, plan) {
        (Aggregator::Count, _) => {
            output_location = result.named_buffer("count");
            (VecOperator::count((grouping_key, grouping_type),
                                output_location.u32(),
                                max_index.i64()),
             Type::encoded(Codec::integer_cast(EncodingType::U32)))
        }
        (Aggregator::Sum, mut plan) => {
            output_location = result.named_buffer("sum");
            if !plan_type.is_summation_preserving() {
                plan = *plan_type.codec.clone().unwrap().decode(Box::new(plan));
                plan_type = plan_type.decoded();
            }
            (VecOperator::summation((prepare(plan, result), plan_type.encoding_type()),
                                    (grouping_key, grouping_type),
                                    output_location.i64(),
                                    max_index.i64()), // TODO(clemens): determine dense groupings
             Type::unencoded(BasicType::Integer))
        }
    };
    result.push(operation);
    Ok((output_location, t))
}

pub fn order_preserving((plan, t): (QueryPlan, Type)) -> (QueryPlan, Type) {
    if t.is_order_preserving() {
        (plan, t)
    } else {
        let new_type = t.decoded();
        (*t.codec.unwrap().decode(Box::new(plan)), new_type)
    }
}

impl QueryPlan {
    pub fn create_query_plan(
        expr: &Expr,
        filter: Filter,
        columns: &HashMap<String, Arc<Column>>) -> Result<(QueryPlan, Type), QueryError> {
        use self::Expr::*;
        use self::Func2Type::*;
        use self::Func1Type::*;
        Ok(match *expr {
            ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(c) => {
                    let mut plan = QueryPlan::ReadColumnSection(name.to_string(), 0, c.range());
                    let mut t = c.full_type();
                    if !c.codec().is_elementwise_decodable() {
                        let (codec, fixed_width) = c.codec().ensure_fixed_width(Box::new(plan));
                        t = Type::encoded(codec);
                        plan = *fixed_width;
                    }
                    plan = match filter {
                        Filter::BitVec(filter) => {
                            QueryPlan::Filter(
                                Box::new(plan),
                                t.encoding_type(),
                                Box::new(QueryPlan::ReadBuffer(filter.any())))
                        }
                        Filter::Indices(indices) => {
                            QueryPlan::Select(
                                Box::new(plan),
                                Box::new(QueryPlan::ReadBuffer(indices.any())),
                                t.encoding_type())
                        }
                        Filter::None => plan,
                    };
                    (plan, t)
                }
                None => bail!(QueryError::NotImplemented, "Referencing missing column {}", name)
            }
            Func2(LT, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = QueryPlan::EncodeIntConstant(Box::new(plan_rhs), type_lhs.codec.clone().unwrap());
                                QueryPlan::LessThanVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(encoded))
                            } else {
                                QueryPlan::LessThanVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(plan_rhs))
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "< operator only implemented for column < constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    _ => bail!(QueryError::TypeError, "{:?} < {:?}", type_lhs, type_rhs)
                }
            }
            Func2(Equals, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::String, BasicType::String) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = type_lhs.codec.clone().unwrap().encode_str(Box::new(plan_rhs));
                                QueryPlan::EqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), encoded)
                            } else {
                                QueryPlan::EqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(plan_rhs))
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "= operator only implemented for column = constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = QueryPlan::EncodeIntConstant(Box::new(plan_rhs), type_lhs.codec.clone().unwrap());
                                QueryPlan::EqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(encoded))
                            } else {
                                QueryPlan::EqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(plan_rhs))
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "= operator only implemented for column = constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    _ => bail!(QueryError::TypeError, "{:?} = {:?}", type_lhs, type_rhs)
                }
            }
            Func2(NotEquals, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::String, BasicType::String) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = type_lhs.codec.clone().unwrap().encode_str(Box::new(plan_rhs));
                                QueryPlan::NotEqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), encoded)
                            } else {
                                QueryPlan::NotEqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(plan_rhs))
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "<> operator only implemented for column <> constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = QueryPlan::EncodeIntConstant(Box::new(plan_rhs), type_lhs.codec.clone().unwrap());
                                QueryPlan::NotEqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(encoded))
                            } else {
                                QueryPlan::NotEqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(plan_rhs))
                            }
                        } else {
                            bail!(QueryError::NotImplemented, "<> operator only implemented for column <> constant")
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    _ => bail!(QueryError::TypeError, "{:?} <> {:?}", type_lhs, type_rhs)
                }
            }
            Func2(Or, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} OR {}, expected bool OR bool")
                }
                (QueryPlan::Or(Box::new(plan_lhs), Box::new(plan_rhs)), Type::bit_vec())
            }
            Func2(And, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} AND {}, expected bool AND bool")
                }
                (QueryPlan::And(Box::new(plan_lhs), Box::new(plan_rhs)), Type::bit_vec())
            }
            Func2(Divide, ref lhs, ref rhs) => {
                let (mut plan_lhs, mut type_lhs) = QueryPlan::create_query_plan(lhs, filter, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, filter, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if let Some(codec) = type_lhs.codec {
                                plan_lhs = *codec.decode(Box::new(plan_lhs));
                            }
                            QueryPlan::DivideVS(Box::new(plan_lhs), Box::new(plan_rhs))
                        } else {
                            bail!(QueryError::NotImplemented, "/ operator only implemented for column / constant")
                        };
                        (plan, Type::unencoded(BasicType::Integer).mutable())
                    }
                    _ => bail!(QueryError::TypeError, "{:?} / {:?}", type_lhs, type_rhs)
                }
            }
            Func1(ToYear, ref inner) => {
                let (plan, t) = QueryPlan::create_query_plan(inner, filter, columns)?;
                if t.decoded != BasicType::Integer {
                    bail!(QueryError::TypeError, "Found to_year({:?}), expected to_year(integer)", &t)
                }
                let decoded = match t.codec.clone() {
                    Some(codec) => *codec.decode(Box::new(plan)),
                    None => plan,
                };
                (QueryPlan::ToYear(Box::new(decoded)), t.decoded())
            }
            Const(ref v) => (QueryPlan::Constant(v.clone(), false), Type::scalar(v.get_type())),
            ref x => bail!(QueryError::NotImplemented, "{:?}.compile_vec()", x),
        })
    }

    fn encoding_range(&self) -> Option<(i64, i64)> {
        // TODO(clemens): need more principled approach - this currently doesn't work for all partially decodings
        // Example: [LZ4, Add, Delta] will have as bottom decoding range the range after Delta, but without the Add :/
        // This works in this case because we always have to decode the Delta, but is hard to reason about and has caused bugs
        use self::QueryPlan::*;
        match *self {
            ReadColumnSection(_, _, range) => range,
            ToYear(ref timestamps) => timestamps.encoding_range().map(|(min, max)|
                (i64::from(NaiveDateTime::from_timestamp(min, 0).year()),
                 i64::from(NaiveDateTime::from_timestamp(max, 0).year()))
            ),
            Filter(ref plan, _, _) => plan.encoding_range(),
            // TODO(clemens): this is just wrong
            DivideVS(ref left, box Constant(RawVal::Int(c), _)) =>
                left.encoding_range().map(|(min, max)|
                    if c > 0 { (min / c, max / c) } else { (max / c, min / c) }),
            AddVS(_, ref left, box Constant(RawVal::Int(c), _)) =>
                left.encoding_range().map(|(min, max)| (min + c, max + c)),
            Cast(ref left, _, _) => left.encoding_range(),
            LZ4Decode(ref plan, _, _) => plan.encoding_range(),
            DeltaDecode(ref plan, _) => plan.encoding_range(),
            _ => None, // TODO(clemens): many more cases where we can determine range
        }
    }
}

fn replace_common_subexpression(plan: QueryPlan, executor: &mut QueryExecutor) -> (Box<QueryPlan>, [u8; 16]) {
    use std::intrinsics::discriminant_value;
    use self::QueryPlan::*;

    unsafe /* dicriminant_value */ {
        let mut signature = [0u8; 16];
        let mut hasher = Md5::new();
        hasher.input(&discriminant_value(&plan).to_ne_bytes());
        let plan = match plan {
            ReadColumnSection(name, index, range) => {
                hasher.input_str(&name);
                hasher.input(&index.to_ne_bytes());
                ReadColumnSection(name, index, range)
            }
            ReadBuffer(buffer) => {
                hasher.input(&buffer.i.to_ne_bytes());
                ReadBuffer(buffer)
            }
            DictLookup(indices, t, offset_len, dict) => {
                let (indices, s1) = replace_common_subexpression(*indices, executor);
                let (offset_len, s2) = replace_common_subexpression(*offset_len, executor);
                let (dict, s3) = replace_common_subexpression(*dict, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&s3);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                DictLookup(indices, t, offset_len, dict)
            }
            InverseDictLookup(dict_indices, dict_data, constant) => {
                let (dict_indices, s1) = replace_common_subexpression(*dict_indices, executor);
                let (dict_data, s2) = replace_common_subexpression(*dict_data, executor);
                let (constant, s3) = replace_common_subexpression(*constant, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&s3);
                InverseDictLookup(dict_indices, dict_data, constant)
            }
            Cast(plan, initial_type, target_type) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&initial_type).to_ne_bytes());
                hasher.input(&discriminant_value(&target_type).to_ne_bytes());
                Cast(plan, initial_type, target_type)
            }
            LZ4Decode(plan, decoded_len, t) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                LZ4Decode(plan, decoded_len, t)
            }
            UnpackStrings(plan) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                UnpackStrings(plan)
            }
            UnhexpackStrings(plan, uppercase, total_bytes) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&total_bytes.to_ne_bytes());
                hasher.input(&[uppercase as u8]);
                UnhexpackStrings(plan, uppercase, total_bytes)
            }
            DeltaDecode(plan, t) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                DeltaDecode(plan, t)
            }
            Exists(indices, t, max_index) => {
                let (indices, s1) = replace_common_subexpression(*indices, executor);
                let (max_index, s2) = replace_common_subexpression(*max_index, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                Exists(indices, t, max_index)
            }
            NonzeroCompact(plan, t) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                NonzeroCompact(plan, t)
            }
            NonzeroIndices(plan, t1, t2) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&t1).to_ne_bytes());
                hasher.input(&discriminant_value(&t2).to_ne_bytes());
                NonzeroIndices(plan, t1, t2)
            }
            Compact(data, data_t, select, select_t) => {
                let (data, s1) = replace_common_subexpression(*data, executor);
                let (select, s2) = replace_common_subexpression(*select, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&discriminant_value(&data_t).to_ne_bytes());
                hasher.input(&discriminant_value(&select_t).to_ne_bytes());
                Compact(data, data_t, select, select_t)
            }
            EncodeIntConstant(plan, codec) => {
                // TODO(clemens): codec needs to be part of signature (easy once we encode using actual query plan rather than codec)
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                EncodeIntConstant(plan, codec)
            }
            BitPack(lhs, rhs, shift_amount) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&(shift_amount as u64).to_ne_bytes());
                BitPack(lhs, rhs, shift_amount)
            }
            BitUnpack(inner, shift, width) => {
                let (inner, s1) = replace_common_subexpression(*inner, executor);
                hasher.input(&s1);
                hasher.input(&shift.to_ne_bytes());
                hasher.input(&width.to_ne_bytes());
                BitUnpack(inner, shift, width)
            }
            SlicePack(inner, t, stride, offset) => {
                let (inner, s1) = replace_common_subexpression(*inner, executor);
                hasher.input(&s1);
                hasher.input(&stride.to_ne_bytes());
                hasher.input(&offset.to_ne_bytes());
                SlicePack(inner, t, stride, offset)
            }
            SliceUnpack(inner, t, stride, offset) => {
                let (inner, s1) = replace_common_subexpression(*inner, executor);
                hasher.input(&s1);
                hasher.input(&stride.to_ne_bytes());
                hasher.input(&offset.to_ne_bytes());
                SliceUnpack(inner, t, stride, offset)
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
            LessThanVS(left_type, lhs, rhs) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&discriminant_value(&left_type).to_ne_bytes());
                LessThanVS(left_type, lhs, rhs)
            }
            EqualsVS(left_type, lhs, rhs) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&discriminant_value(&left_type).to_ne_bytes());
                EqualsVS(left_type, lhs, rhs)
            }
            NotEqualsVS(left_type, lhs, rhs) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&discriminant_value(&left_type).to_ne_bytes());
                NotEqualsVS(left_type, lhs, rhs)
            }
            DivideVS(lhs, rhs) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                DivideVS(lhs, rhs)
            }
            AddVS(left_type, lhs, rhs) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&discriminant_value(&left_type).to_ne_bytes());
                AddVS(left_type, lhs, rhs)
            }
            And(lhs, rhs) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                And(lhs, rhs)
            }
            Or(lhs, rhs) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                Or(lhs, rhs)
            }
            ToYear(plan) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                ToYear(plan)
            }
            SortIndices(plan, descending) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&[descending as u8]);
                SortIndices(plan, descending)
            }
            TopN(plan, t, n, desc) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                hasher.input(&s1);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                hasher.input(&n.to_ne_bytes());
                hasher.input(&[desc as u8]);
                TopN(plan, t, n, desc)
            }
            Select(lhs, rhs, t) => {
                let (lhs, s1) = replace_common_subexpression(*lhs, executor);
                let (rhs, s2) = replace_common_subexpression(*rhs, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                Select(lhs, rhs, t)
            }
            Filter(plan, t, filter) => {
                let (plan, s1) = replace_common_subexpression(*plan, executor);
                let (filter, s2) = replace_common_subexpression(*filter, executor);
                hasher.input(&s1);
                hasher.input(&s2);
                hasher.input(&discriminant_value(&t).to_ne_bytes());
                Filter(plan, t, filter)
            }
            EncodedGroupByPlaceholder => EncodedGroupByPlaceholder,
            Constant(val, show) => {
                match val {
                    RawVal::Int(i) => hasher.input(&(i as u64).to_ne_bytes()),
                    RawVal::Str(ref s) => hasher.input_str(s),
                    RawVal::Null => {}
                }
                Constant(val, show)
            }
        };

        hasher.result(&mut signature);
        match executor.get(&signature) {
            Some(plan) => (plan, signature),
            None => (Box::new(plan), signature),
        }
    }
}

pub fn compile_grouping_key(
    exprs: &[Expr],
    filter: Filter,
    columns: &HashMap<String, Arc<Column>>)
    -> Result<(TypedPlan, i64, Vec<TypedPlan>), QueryError> {
    if exprs.len() == 1 {
        QueryPlan::create_query_plan(&exprs[0], filter, columns)
            .map(|(gk_plan, gk_type)| {
                let max_cardinality = QueryPlan::encoding_range(&gk_plan).map_or(1 << 62, |i| i.1);
                if QueryPlan::encoding_range(&gk_plan).is_none() {
                    warn!("Unknown range for {:?}", &gk_plan);
                }
                let decoded_group_by = gk_type.codec.clone().map_or(
                    QueryPlan::EncodedGroupByPlaceholder,
                    |codec| *codec.decode(Box::new(QueryPlan::EncodedGroupByPlaceholder)));
                ((gk_plan.clone(), gk_type.clone()),
                 max_cardinality,
                 vec![(decoded_group_by, gk_type.decoded())])
            })
    } else if let Some(result) = try_bitpacking(exprs, filter, columns)? {
        Ok(result)
    } else {
        info!("Failed to bitpack grouping key");
        let mut pack = Vec::new();
        let mut decode_plans = Vec::new();
        for (i, expr) in exprs.iter().enumerate() {
            let (query_plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
            pack.push(Box::new(
                QueryPlan::SlicePack(Box::new(query_plan),
                                     plan_type.encoding_type(),
                                     exprs.len(),
                                     i)));

            // TODO(clemens): negative integers can throw off sort oder - need to move into positive range
            let mut decode_plan = QueryPlan::SliceUnpack(
                Box::new(QueryPlan::EncodedGroupByPlaceholder),
                plan_type.encoding_type(),
                exprs.len(),
                i);
            if let Some(codec) = plan_type.codec.clone() {
                decode_plan = *codec.decode(Box::new(decode_plan));
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
        Ok(((QueryPlan::Convergence(pack), t),
            i64::MAX,
            decode_plans))
    }
}

fn try_bitpacking(
    exprs: &[Expr],
    filter: Filter,
    columns: &HashMap<String, Arc<Column>>)
    -> Result<Option<(TypedPlan, i64, Vec<TypedPlan>)>, QueryError> {
    // TODO(clemens): use u64 as grouping key type
    let mut total_width = 0;
    let mut largest_key = 0;
    let mut plan = None;
    let mut decode_plans = Vec::with_capacity(exprs.len());
    let mut order_preserving = true;
    for expr in exprs.iter().rev() {
        let (query_plan, plan_type) = QueryPlan::create_query_plan(expr, filter, columns)?;
        if let Some((min, max)) = QueryPlan::encoding_range(&query_plan) {
            fn bits(max: i64) -> i64 {
                ((max + 1) as f64).log2().ceil() as i64
            }

            // TODO(clemens): more intelligent criterion. threshold should probably be a function of total width.
            let subtract_offset = bits(max) - bits(max - min) > 1 || min < 0;
            let adjusted_max = if subtract_offset { max - min } else { max };
            order_preserving = order_preserving && plan_type.is_order_preserving();
            let query_plan = if subtract_offset {
                QueryPlan::AddVS(plan_type.encoding_type(),
                                 Box::new(query_plan),
                                 Box::new(QueryPlan::Constant(RawVal::Int(-min), true)))
            } else {
                syntax::cast(query_plan, plan_type.encoding_type(), EncodingType::I64)
            };

            if total_width == 0 {
                plan = Some(query_plan);
            } else if adjusted_max > 0 {
                plan = plan.map(|plan|
                    QueryPlan::BitPack(Box::new(plan), Box::new(query_plan), total_width));
            }

            let mut decode_plan = QueryPlan::BitUnpack(
                Box::new(QueryPlan::EncodedGroupByPlaceholder),
                total_width as u8,
                bits(adjusted_max) as u8);
            if subtract_offset {
                decode_plan = QueryPlan::AddVS(
                    EncodingType::I64,
                    Box::new(decode_plan),
                    Box::new(QueryPlan::Constant(RawVal::Int(min), true)));
            }
            decode_plan = syntax::cast(decode_plan, EncodingType::I64, plan_type.encoding_type());
            if let Some(codec) = plan_type.codec.clone() {
                decode_plan = *codec.decode(Box::new(decode_plan));
            }
            decode_plans.push((decode_plan, plan_type.decoded()));

            largest_key += adjusted_max << total_width;
            total_width += bits(adjusted_max);
        } else {
            return Ok(None);
        }
    }

    Ok(
        if total_width <= 64 {
            plan.map(|plan| {
                decode_plans.reverse();
                let t = Type::encoded(Codec::opaque(
                    EncodingType::I64, BasicType::Integer, false, order_preserving, true, true));
                ((plan, t), largest_key, decode_plans)
            })
        } else {
            None
        }
    )
}

fn to_hex_string(bytes: &[u8]) -> String {
    bytes.iter()
        .map(|b| format!("{:02X}", b))
        .join("")
}

mod syntax {
    use super::*;
    use super::QueryPlan::*;

    pub fn cast(plan: QueryPlan, input_type: EncodingType, output_type: EncodingType) -> QueryPlan {
        if input_type == output_type {
            plan
        } else {
            Cast(Box::new(plan), input_type, output_type)
        }
    }
}
