use std::collections::HashMap;
use std::sync::Arc;

use ::QueryError;
use chrono::{Datelike, NaiveDateTime};
use engine::aggregator::Aggregator;
use engine::filter::Filter;
use engine::types::*;
use engine::vector_op::*;
use engine::vector_op::vector_operator::BufferRef;
use ingest::raw_val::RawVal;
use mem_store::*;
use mem_store::column::Column;
use syntax::expression::*;


#[derive(Debug, Clone)]
pub enum QueryPlan {
    ReadColumnSection(String, usize, Option<(i64, i64)>),
    ReadBuffer(BufferRef),

    DictLookup(Box<QueryPlan>, EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    InverseDictLookup(Box<QueryPlan>, Box<QueryPlan>, Box<QueryPlan>),
    Widen(Box<QueryPlan>, EncodingType, EncodingType),
    LZ4Decode(Box<QueryPlan>, EncodingType),

    Exists(Box<QueryPlan>, EncodingType, Box<QueryPlan>),
    NonzeroCompact(Box<QueryPlan>, EncodingType),
    NonzeroIndices(Box<QueryPlan>, EncodingType, EncodingType),
    Compact(Box<QueryPlan>, EncodingType, Box<QueryPlan>, EncodingType),

    EncodeIntConstant(Box<QueryPlan>, Codec),

    BitPack(Box<QueryPlan>, Box<QueryPlan>, i64),
    BitUnpack(Box<QueryPlan>, u8, u8),

    LessThanVS(EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    EqualsVS(EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    NotEqualsVS(EncodingType, Box<QueryPlan>, Box<QueryPlan>),
    DivideVS(Box<QueryPlan>, Box<QueryPlan>),
    AddVS(Box<QueryPlan>, EncodingType, Box<QueryPlan>),
    And(Box<QueryPlan>, Box<QueryPlan>),
    Or(Box<QueryPlan>, Box<QueryPlan>),
    ToYear(Box<QueryPlan>),

    SortIndices(Box<QueryPlan>, bool),
    TopN(Box<QueryPlan>, EncodingType, usize, bool),

    Select(Box<QueryPlan>, Box<QueryPlan>, EncodingType),
    Filter(Box<QueryPlan>, EncodingType, Box<QueryPlan>),

    EncodedGroupByPlaceholder,

    Constant(RawVal, bool),
}

pub fn prepare<'a>(plan: QueryPlan, result: &mut QueryExecutor<'a>) -> BufferRef {
    let operation: Box<VecOperator> = match plan {
        QueryPlan::Select(plan, indices, t) =>
            VecOperator::select(t, prepare(*plan, result), prepare(*indices, result), result.named_buffer("selection")),
        QueryPlan::ReadColumnSection(colname, section, _) =>
            VecOperator::read_column_data(colname, section, result.named_buffer("column")),
        QueryPlan::Filter(plan, t, filter) =>
            VecOperator::filter(t, prepare(*plan, result), prepare(*filter, result), result.named_buffer("filtered")),
        QueryPlan::Constant(ref c, hide_value) =>
            VecOperator::constant(c.clone(), hide_value, result.named_buffer("constant")),
        QueryPlan::DictLookup(plan, t, dict_indices, dict_data) =>
            VecOperator::dict_lookup(
                prepare(*plan, result),
                prepare(*dict_indices, result),
                prepare(*dict_data, result),
                result.named_buffer("decoded"), t),
        QueryPlan::InverseDictLookup(dict_indices, dict_data, constant) =>
            VecOperator::inverse_dict_lookup(
                prepare(*dict_indices, result),
                prepare(*dict_data, result),
                prepare(*constant, result),
                result.named_buffer("encoded")),
        QueryPlan::Widen(plan, initial_type, target_type) => if initial_type == target_type {
            return prepare(*plan, result);
        } else {
            VecOperator::type_conversion(prepare(*plan, result), result.named_buffer("casted"), initial_type, target_type)
        },
        QueryPlan::LZ4Decode(plan, t) =>
            VecOperator::lz4_decode(prepare(*plan, result), result.named_buffer("decoded"), t),
        QueryPlan::Exists(indices, t, max_index) =>
            VecOperator::exists(prepare(*indices, result), result.named_buffer("exists"), t, prepare(*max_index, result)),
        QueryPlan::Compact(data, data_t, select, select_t) => {
            let inplace = prepare(*data, result);
            let op = VecOperator::compact(inplace, prepare(*select, result), data_t, select_t);
            result.push(op);
            return inplace;
        }
        QueryPlan::NonzeroIndices(indices, indices_t, output_t) =>
            VecOperator::nonzero_indices(prepare(*indices, result), result.named_buffer("nonzero_indices"), indices_t, output_t),
        QueryPlan::NonzeroCompact(data, data_t) => {
            let inplace = prepare(*data, result);
            result.push(VecOperator::nonzero_compact(inplace, data_t));
            return inplace;
        }
        QueryPlan::EncodeIntConstant(plan, codec) =>
            VecOperator::encode_int_const(prepare(*plan, result), result.named_buffer("encoded"), codec),
        QueryPlan::BitPack(lhs, rhs, shift_amount) =>
            VecOperator::bit_shift_left_add(prepare(*lhs, result), prepare(*rhs, result), result.named_buffer("bitpacked"), shift_amount),
        QueryPlan::BitUnpack(inner, shift, width) =>
            VecOperator::bit_unpack(prepare(*inner, result), result.named_buffer("unpacked"), shift, width),
        QueryPlan::LessThanVS(left_type, lhs, rhs) =>
            VecOperator::less_than_vs(left_type, prepare(*lhs, result), prepare(*rhs, result), result.named_buffer("less_than")),
        QueryPlan::EqualsVS(left_type, lhs, rhs) =>
            VecOperator::equals_vs(left_type, prepare(*lhs, result), prepare(*rhs, result), result.named_buffer("equals")),
        QueryPlan::NotEqualsVS(left_type, lhs, rhs) =>
            VecOperator::not_equals_vs(left_type, prepare(*lhs, result), prepare(*rhs, result), result.named_buffer("equals")),
        QueryPlan::DivideVS(lhs, rhs) =>
            VecOperator::divide_vs(prepare(*lhs, result), prepare(*rhs, result), result.named_buffer("division")),
        QueryPlan::AddVS(lhs, t, c) =>
            VecOperator::addition_vs(prepare(*lhs, result), prepare(*c, result), result.named_buffer("addition"), t),
        QueryPlan::Or(lhs, rhs) => {
            let inplace = prepare(*lhs, result);
            // If we don't assign to `operation` and pass expression directly to push, we trigger an infinite loop in the compiler
            // Probably same issue as this: https://github.com/rust-lang/rust/issues/49936
            let operation = VecOperator::or(inplace, prepare(*rhs, result));
            result.push(operation);
            return inplace;
        }
        QueryPlan::And(lhs, rhs) => {
            let inplace: BufferRef = prepare(*lhs, result);
            // If we don't assign to `operation` and pass expression directly to push, we trigger an infinite loop in the compiler
            // Probably same issue as this: https://github.com/rust-lang/rust/issues/49936
            let operation = VecOperator::and(inplace, prepare(*rhs, result));
            result.push(operation);
            return inplace;
        }
        QueryPlan::ToYear(plan) =>
            VecOperator::to_year(prepare(*plan, result), result.named_buffer("year")),
        QueryPlan::EncodedGroupByPlaceholder => return result.encoded_group_by().unwrap(),
        QueryPlan::SortIndices(plan, descending) =>
            VecOperator::sort_indices(prepare(*plan, result), result.named_buffer("permutation"), descending),
        QueryPlan::TopN(plan, t, n, desc) =>
            VecOperator::top_n(prepare(*plan, result), result.named_buffer("tmp_keys"), result.named_buffer("top_n"), t, n, desc),
        QueryPlan::ReadBuffer(buffer) => return buffer,
    };
    result.push(operation);
    result.last_buffer()
}

pub fn prepare_hashmap_grouping<'a>(raw_grouping_key: BufferRef,
                                    grouping_key_type: EncodingType,
                                    max_cardinality: usize,
                                    result: &mut QueryExecutor) -> (Option<BufferRef>, BufferRef, Type, BufferRef) {
    let unique_out = result.named_buffer("unique");
    let grouping_key_out = result.named_buffer("grouping_key");
    let cardinality_out = result.named_buffer("cardinality");
    result.push(VecOperator::hash_map_grouping(
        raw_grouping_key, unique_out, grouping_key_out, cardinality_out, grouping_key_type, max_cardinality));
    (Some(unique_out),
     grouping_key_out,
     Type::encoded(Codec::opaque(grouping_key_type, BasicType::Integer, false, false, true, true)),
     cardinality_out)
}

// TODO(clemens): add QueryPlan::Aggregation and merge with prepare function
pub fn prepare_aggregation<'a, 'b>(plan: QueryPlan,
                                   mut plan_type: Type,
                                   grouping_key: BufferRef,
                                   grouping_type: EncodingType,
                                   max_index: BufferRef,
                                   aggregator: Aggregator,
                                   result: &mut QueryExecutor<'a>) -> Result<(BufferRef, Type), QueryError> {
    let output_location;
    let (operation, t): (BoxedOperator<'a>, _) = match (aggregator, plan) {
        (Aggregator::Count, _) => {
            output_location = result.named_buffer("count");
            (VecOperator::count(grouping_key,
                                output_location,
                                grouping_type,
                                max_index),
             Type::encoded(Codec::integer_cast(EncodingType::U32)))
        }
        (Aggregator::Sum, mut plan) => {
            output_location = result.named_buffer("sum");
            if !plan_type.is_summation_preserving() {
                plan = *plan_type.codec.clone().unwrap().decode(Box::new(plan));
                plan_type = plan_type.decoded();
            }
            (VecOperator::summation(prepare(plan, result),
                                    grouping_key,
                                    output_location,
                                    plan_type.encoding_type(),
                                    grouping_type,
                                    max_index), // TODO(clemens): determine dense groupings
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
    pub fn create_query_plan<'a>(
        expr: &Expr,
        filter: Filter,
        columns: &'a HashMap<String, Arc<Column>>) -> Result<(QueryPlan, Type), QueryError> {
        use self::Expr::*;
        use self::Func2Type::*;
        use self::Func1Type::*;
        Ok(match *expr {
            ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(c) => {
                    let mut plan = QueryPlan::ReadColumnSection(name.to_string(), 0, c.range());
                    let mut t = c.full_type();
                    if !c.codec().is_fixed_width() {
                        let (codec, fixed_width) = c.codec().ensure_fixed_width(Box::new(plan));
                        t = Type::encoded(codec);
                        plan = *fixed_width;
                    }
                    plan = match filter {
                        Filter::BitVec(filter) => {
                            QueryPlan::Filter(
                                Box::new(plan),
                                t.encoding_type(),
                                Box::new(QueryPlan::ReadBuffer(filter)))
                        }
                        Filter::Indices(indices) => {
                            QueryPlan::Select(
                                Box::new(plan),
                                Box::new(QueryPlan::ReadBuffer(indices)),
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
                            bail!(QueryError::NotImplemented, "<> operator only implemented for column <>= constant")
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
                    bail!(QueryError::TypeError, "Found {} AND {}, expected bool AND bool")
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

    pub fn compile_grouping_key<'b>(
        exprs: &[Expr],
        filter: Filter,
        columns: &'b HashMap<String, Arc<Column>>)
        -> Result<(QueryPlan, Type, i64, Vec<(QueryPlan, Type)>), QueryError> {
        if exprs.len() == 1 {
            QueryPlan::create_query_plan(&exprs[0], filter, columns)
                .map(|(gk_plan, gk_type)| {
                    let max_cardinality = QueryPlan::encoding_range(&gk_plan).map_or(1 << 62, |i| i.1);
                    if QueryPlan::encoding_range(&gk_plan).is_none() {
                        println!("Uknown range for {:?}", &gk_plan);
                    }
                    let decoded_group_by = gk_type.codec.clone().map_or(
                        QueryPlan::EncodedGroupByPlaceholder,
                        |codec| *codec.decode(Box::new(QueryPlan::EncodedGroupByPlaceholder)));
                    (gk_plan.clone(), gk_type.clone(), max_cardinality, vec![(decoded_group_by, gk_type.decoded())])
                })
        } else {
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
                        QueryPlan::AddVS(Box::new(query_plan),
                                         plan_type.encoding_type(),
                                         Box::new(QueryPlan::Constant(RawVal::Int(-min), true)))
                    } else {
                        QueryPlan::Widen(Box::new(query_plan),
                                         plan_type.encoding_type(),
                                         EncodingType::I64)
                    };

                    #[cfg(feature = "nerf")]
                    let nerf = true;
                    #[cfg(not(feature = "nerf"))]
                    let nerf = false;

                    if total_width == 0 && (!nerf || plan.is_none()) {
                        plan = Some(query_plan);
                    } else if adjusted_max > 0 || nerf {
                        plan = plan.map(|plan|
                            QueryPlan::BitPack(Box::new(plan), Box::new(query_plan), total_width));
                    }

                    let mut decode_plan = QueryPlan::BitUnpack(
                        Box::new(QueryPlan::EncodedGroupByPlaceholder),
                        total_width as u8,
                        bits(adjusted_max) as u8);
                    if subtract_offset {
                        decode_plan = QueryPlan::AddVS(
                            Box::new(decode_plan),
                            EncodingType::I64,
                            Box::new(QueryPlan::Constant(RawVal::Int(min), true)));
                    }
                    decode_plan = QueryPlan::Widen(
                        Box::new(decode_plan),
                        EncodingType::I64,
                        plan_type.encoding_type());
                    if let Some(codec) = plan_type.codec.clone() {
                        decode_plan = *codec.decode(Box::new(decode_plan));
                    }
                    decode_plans.push((decode_plan, plan_type.decoded()));

                    largest_key += adjusted_max << total_width;
                    total_width += bits(adjusted_max);
                } else {
                    plan = None;
                    break;
                }
            }

            if let Some(plan) = plan {
                if total_width <= 64 {
                    decode_plans.reverse();
                    let t = Type::encoded(Codec::opaque(
                        EncodingType::I64, BasicType::Integer, false, order_preserving, true, true));
                    return Ok((plan, t, largest_key, decode_plans));
                }
            }
            // TODO(clemens): add more grouping key widths (u32. u16?)
            // TODO(clemens): implement general case using bites slice as grouping key
            bail!(QueryError::NotImplemented, "Failed to pack group by columns into 64 bit value")
        }
    }

    fn encoding_range(&self) -> Option<(i64, i64)> {
        use self::QueryPlan::*;
        match *self {
            ReadColumnSection(_, _, range) => range,
            ToYear(ref timestamps) => timestamps.encoding_range().map(|(min, max)|
                (NaiveDateTime::from_timestamp(min, 0).year() as i64,
                 NaiveDateTime::from_timestamp(max, 0).year() as i64)
            ),
            Filter(ref plan, _, _) => plan.encoding_range(),
            // TODO(clemens): this is just wrong
            DivideVS(ref left, box Constant(RawVal::Int(c), _)) =>
                left.encoding_range().map(|(min, max)|
                    if c > 0 { (min / c, max / c) } else { (max / c, min / c) }),
            AddVS(ref left, _, box Constant(RawVal::Int(c), _)) =>
                left.encoding_range().map(|(min, max)| (min + c, max + c)),
            Widen(ref left, _, _) => left.encoding_range(),
            LZ4Decode(ref plan, _) => plan.encoding_range(),
            _ => None, // TODO(clemens): many more cases where we can determine range
        }
    }
}

