use std::collections::HashMap;
use std::rc::Rc;
use syntax::expression::*;

use ::QueryError;
use bit_vec::BitVec;
use engine::aggregation_operator::*;
use engine::aggregator::Aggregator;
use engine::filter::Filter;
use engine::typed_vec::TypedVec;
use engine::types::*;
use engine::vector_op::*;
use ingest::raw_val::RawVal;
use mem_store::column::Column;
use mem_store::column::{ColumnData, ColumnCodec};


#[derive(Debug, Clone)]
pub enum QueryPlan<'a> {
    GetDecode(&'a ColumnData),
    FilterDecode(&'a ColumnData, Rc<BitVec>),
    IndexDecode(&'a ColumnData, Rc<Vec<usize>>),
    GetEncoded(&'a ColumnCodec),
    FilterEncoded(&'a ColumnCodec, Rc<BitVec>),
    IndexEncoded(&'a ColumnCodec, Rc<Vec<usize>>),

    Decode(Box<QueryPlan<'a>>),
    DecodeWith(Box<QueryPlan<'a>>, &'a ColumnCodec),
    TypeConversion(Box<QueryPlan<'a>>, EncodingType, EncodingType),

    EncodeStrConstant(Box<QueryPlan<'a>>, &'a ColumnCodec),
    EncodeIntConstant(Box<QueryPlan<'a>>, &'a ColumnCodec),

    BitPack(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>, i64),
    BitUnpack(Box<QueryPlan<'a>>, u8, u8),

    LessThanVS(EncodingType, Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    EqualsVS(EncodingType, Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    And(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    Or(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),

    GroupingKeyPlaceholder,

    Constant(RawVal),
}

pub fn prepare(plan: QueryPlan) -> BoxedOperator {
    match plan {
        QueryPlan::GetDecode(col) => Box::new(GetDecode::new(col)),
        QueryPlan::FilterDecode(col, filter) => Box::new(FilterDecode::new(col, filter)),
        QueryPlan::IndexDecode(col, filter) => Box::new(IndexDecode::new(col, filter)),
        QueryPlan::GetEncoded(col) => Box::new(GetEncoded::new(col)),
        QueryPlan::FilterEncoded(col, filter) => Box::new(FilterEncoded::new(col, filter)),
        QueryPlan::IndexEncoded(col, filter) => Box::new(IndexEncoded::new(col, filter)),
        QueryPlan::Constant(ref c) => Box::new(Constant::new(c.clone())),
        QueryPlan::Decode(plan) => Box::new(Decode::new(prepare(*plan))),
        QueryPlan::DecodeWith(plan, codec) => Box::new(DecodeWith::new(prepare(*plan), codec)),
        QueryPlan::TypeConversion(plan, initial_type, target_type) => VecOperator::type_conversion(prepare(*plan), initial_type, target_type),
        QueryPlan::EncodeStrConstant(plan, codec) => Box::new(EncodeStrConstant::new(prepare(*plan), codec)),
        QueryPlan::EncodeIntConstant(plan, codec) => Box::new(EncodeIntConstant::new(prepare(*plan), codec)),
        QueryPlan::BitPack(lhs, rhs, shift_amount) => VecOperator::bit_shift_left_add(prepare(*lhs), prepare(*rhs), shift_amount),
        QueryPlan::BitUnpack(inner, shift, width) => VecOperator::bit_unpack(prepare(*inner), shift, width),
        QueryPlan::LessThanVS(left_type, lhs, rhs) => VecOperator::less_than_vs(left_type, prepare(*lhs), prepare(*rhs)),
        QueryPlan::EqualsVS(left_type, lhs, rhs) => VecOperator::equals_vs(left_type, prepare(*lhs), prepare(*rhs)),
        QueryPlan::Or(lhs, rhs) => Boolean::or(prepare(*lhs), prepare(*rhs)),
        QueryPlan::And(lhs, rhs) => Boolean::and(prepare(*lhs), prepare(*rhs)),
        QueryPlan::GroupingKeyPlaceholder => unimplemented!(),
    }
}

// TODO(clemens): Whole method is temporary hack to make this possible until refactor
pub fn prepare_asdf<'a>(plan: QueryPlan<'a>, grouping_key: &TypedVec<'a>) -> BoxedOperator<'a> {
    match plan {
        QueryPlan::GetDecode(col) => Box::new(GetDecode::new(col)),
        QueryPlan::FilterDecode(col, filter) => Box::new(FilterDecode::new(col, filter)),
        QueryPlan::IndexDecode(col, filter) => Box::new(IndexDecode::new(col, filter)),
        QueryPlan::GetEncoded(col) => Box::new(GetEncoded::new(col)),
        QueryPlan::FilterEncoded(col, filter) => Box::new(FilterEncoded::new(col, filter)),
        QueryPlan::IndexEncoded(col, filter) => Box::new(IndexEncoded::new(col, filter)),
        QueryPlan::Constant(ref c) => Box::new(Constant::new(c.clone())),
        QueryPlan::Decode(plan) => Box::new(Decode::new(prepare_asdf(*plan, grouping_key))),
        QueryPlan::DecodeWith(plan, codec) => Box::new(DecodeWith::new(prepare_asdf(*plan, grouping_key), codec)),
        QueryPlan::TypeConversion(plan, initial_type, target_type) => VecOperator::type_conversion(prepare_asdf(*plan, grouping_key), initial_type, target_type),
        QueryPlan::EncodeStrConstant(plan, codec) => Box::new(EncodeStrConstant::new(prepare_asdf(*plan, grouping_key), codec)),
        QueryPlan::EncodeIntConstant(plan, codec) => Box::new(EncodeIntConstant::new(prepare_asdf(*plan, grouping_key), codec)),
        QueryPlan::BitPack(lhs, rhs, shift_amount) => VecOperator::bit_shift_left_add(prepare_asdf(*lhs, grouping_key), prepare_asdf(*rhs, grouping_key), shift_amount),
        QueryPlan::BitUnpack(inner, shift, width) => VecOperator::bit_unpack(prepare_asdf(*inner, grouping_key), shift, width),
        QueryPlan::LessThanVS(left_type, lhs, rhs) => VecOperator::less_than_vs(left_type, prepare_asdf(*lhs, grouping_key), prepare_asdf(*rhs, grouping_key)),
        QueryPlan::EqualsVS(left_type, lhs, rhs) => VecOperator::equals_vs(left_type, prepare_asdf(*lhs, grouping_key), prepare_asdf(*rhs, grouping_key)),
        QueryPlan::Or(lhs, rhs) => Boolean::or(prepare_asdf(*lhs, grouping_key), prepare_asdf(*rhs, grouping_key)),
        QueryPlan::And(lhs, rhs) => Boolean::and(prepare_asdf(*lhs, grouping_key), prepare_asdf(*rhs, grouping_key)),
        QueryPlan::GroupingKeyPlaceholder => Box::new(VectorConstant { val: grouping_key.clone() }),
    }
}

// TODO(clemens): add QueryPlan::Aggregation and merge with prepare function
pub fn prepare_aggregation<'a, 'b>(plan: QueryPlan<'a>,
                                   mut plan_type: Type,
                                   grouping: &'b TypedVec<'a>,
                                   max_index: usize,
                                   aggregator: Aggregator) -> Result<Box<VecOperator<'a> + 'b>, QueryError> {
    Ok(match (aggregator, plan) {
        (Aggregator::Count, QueryPlan::Constant(RawVal::Int(_))) => match grouping.get_type() {
            EncodingType::U8 => Box::new(VecCount::new(grouping.cast_ref_u8().0, max_index, false)),
            EncodingType::U16 => Box::new(VecCount::new(grouping.cast_ref_u16().0, max_index, false)),
            EncodingType::I64 => Box::new(VecCount::new(grouping.cast_ref_i64(), max_index, false)),
            t => bail!(QueryError::NotImplemented, "unsupported type {:?} for grouping key", t),
        }
        (Aggregator::Sum, mut plan) => {
            if !plan_type.is_summation_preserving() {
                plan = QueryPlan::Decode(Box::new(plan));
                plan_type = plan_type.decoded();
            }
            match (plan_type.encoding_type(), grouping.get_type()) {
                (EncodingType::U8, EncodingType::U8) => VecSum::<u8, u8>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::U8, EncodingType::U16) => VecSum::<u8, u16>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::U8, EncodingType::U32) => VecSum::<u8, u32>::boxed(prepare(plan), grouping, max_index, false),
                // (EncodingType::U8, EncodingType::I64) => VecSum::<u8, u64>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::U16, EncodingType::U8) => VecSum::<u16, u8>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::U16, EncodingType::U16) => VecSum::<u16, u16>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::U16, EncodingType::U32) => VecSum::<u16, u32>::boxed(prepare(plan), grouping, max_index, false),
                // (EncodingType::U16, EncodingType::I64) => VecSum::<u16, u64>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::U32, EncodingType::U8) => VecSum::<u32, u8>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::U32, EncodingType::U16) => VecSum::<u32, u16>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::U32, EncodingType::U32) => VecSum::<u32, u32>::boxed(prepare(plan), grouping, max_index, false),
                // (EncodingType::U32, EncodingType::I64) => VecSum::<u32, u64>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::I64, EncodingType::U8) => VecSum::<i64, u8>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::I64, EncodingType::U16) => VecSum::<i64, u16>::boxed(prepare(plan), grouping, max_index, false),
                (EncodingType::I64, EncodingType::U32) => VecSum::<i64, u32>::boxed(prepare(plan), grouping, max_index, false),
                // (EncodingType::I64, EncodingType::I64) => VecSum::<i64, u64>::boxed(prepare(plan), grouping, max_index, false),
                (pt, gt) => bail!(QueryError::FatalError, "invalid aggregation types {:?}, {:?}", pt, gt),
            }
        }
        (a, p) => bail!(QueryError::NotImplemented, "prepare_aggregation not implemented for {:?}, {:?}", &a, &p)
    })
}

impl<'a> QueryPlan<'a> {
    pub fn create_query_plan<'b>(expr: &Expr,
                                 columns: &HashMap<&'b str, &'b Column>,
                                 filter: Filter) -> Result<(QueryPlan<'b>, Type<'b>), QueryError> {
        use self::Expr::*;
        use self::FuncType::*;
        Ok(match *expr {
            ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(c) => {
                    let t = c.data().full_type();
                    match (c.data().to_codec(), filter) {
                        (None, Filter::None) => (QueryPlan::GetDecode(c.data()), t.decoded()),
                        (None, Filter::BitVec(f)) => (QueryPlan::FilterDecode(c.data(), f), t.decoded()),
                        (None, Filter::Indices(f)) => (QueryPlan::IndexDecode(c.data(), f), t.decoded()),
                        (Some(c), Filter::None) => (QueryPlan::GetEncoded(c), t),
                        (Some(c), Filter::BitVec(f)) => (QueryPlan::FilterEncoded(c, f), t.mutable()),
                        (Some(c), Filter::Indices(f)) => (QueryPlan::IndexEncoded(c, f), t.mutable()),
                    }
                }
                None => bail!(QueryError::NotImplemented, "Referencing missing column {}", name)
            }
            Func(LT, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, columns, filter.clone())?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, columns, filter)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = QueryPlan::EncodeIntConstant(Box::new(plan_rhs), type_lhs.codec.unwrap());
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
            Func(Equals, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, columns, filter.clone())?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, columns, filter)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::String, BasicType::String) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = QueryPlan::EncodeStrConstant(Box::new(plan_rhs), type_lhs.codec.unwrap());
                                QueryPlan::EqualsVS(type_lhs.encoding_type(), Box::new(plan_lhs), Box::new(encoded))
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
                                let encoded = QueryPlan::EncodeIntConstant(Box::new(plan_rhs), type_lhs.codec.unwrap());
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
            Func(Or, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, columns, filter.clone())?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, columns, filter)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} AND {}, expected bool AND bool")
                }
                (QueryPlan::Or(Box::new(plan_lhs), Box::new(plan_rhs)), Type::bit_vec())
            }
            Func(And, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, columns, filter.clone())?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, columns, filter)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} AND {}, expected bool AND bool")
                }
                (QueryPlan::And(Box::new(plan_lhs), Box::new(plan_rhs)), Type::bit_vec())
            }
            Const(ref v) => (QueryPlan::Constant(v.clone()), Type::scalar(v.get_type())),
            ref x => bail!(QueryError::NotImplemented, "{:?}.compile_vec()", x),
        })
    }

    pub fn compile_grouping_key<'b>(exprs: &[Expr],
                                    columns: &HashMap<&'b str, &'b Column>,
                                    filter: Filter) -> Result<(QueryPlan<'b>, Type<'b>, i64, Vec<QueryPlan<'b>>), QueryError> {
        if exprs.len() == 1 {
            QueryPlan::create_query_plan(&exprs[0], columns, filter)
                .map(|r| {
                    let max_cardinality = QueryPlan::encoding_range(&r.0).map_or(1 << 63, |i| i.1);
                    (r.0.clone(), r.1, max_cardinality, vec![QueryPlan::GroupingKeyPlaceholder])
                })
        } else {
            let mut total_width = 0;
            let mut largest_key = 0;
            let mut plan = None;
            let mut decode_plans = Vec::with_capacity(exprs.len());
            for expr in exprs.iter().rev() {
                // TODO(clemens): Grouping key should never be iXX. (use u64 instead of i64)
                let (query_plan, plan_type) = QueryPlan::create_query_plan(expr, columns, filter.clone())?;
                // TODO(clemens): Potentially subtract min if min is negative or this makes grouping key fit into 64 bits
                if let Some((min, max)) = QueryPlan::encoding_range(&query_plan) {
                    if min < 0 {
                        plan = None;
                        break;
                    }
                    let query_plan = QueryPlan::TypeConversion(Box::new(query_plan),
                                                               plan_type.encoding_type(),
                                                               EncodingType::I64);
                    let bits = (max as f64).log2().floor() as i64 + 1;
                    if total_width == 0 {
                        plan = Some(query_plan);
                    } else {
                        plan = plan.map(|plan|
                            QueryPlan::BitPack(Box::new(plan), Box::new(query_plan), total_width));
                    }

                    let mut decode_plan = QueryPlan::BitUnpack(
                        Box::new(QueryPlan::GroupingKeyPlaceholder),
                        total_width as u8,
                        bits as u8);
                    decode_plan = QueryPlan::TypeConversion(
                        Box::new(decode_plan),
                        EncodingType::I64,
                        plan_type.encoding_type());
                    if let Some(codec) = plan_type.codec {
                        decode_plan = QueryPlan::DecodeWith(
                            Box::new(decode_plan),
                            codec)
                    }
                    decode_plans.push(decode_plan);

                    largest_key += max << total_width;
                    total_width += bits;
                } else {
                    plan = None;
                    break;
                }
            }

            if let Some(plan) = plan {
                if total_width <= 64 {
                    decode_plans.reverse();
                    return Ok((plan, Type::new(BasicType::Integer, None), largest_key, decode_plans));
                }
            }
            // TODO(clemens): add u8, u16, u32, u128 grouping keys
            // TODO(clemens): implement general case using bites slice as grouping key
            bail!(QueryError::NotImplemented, "Can only group by a single column. Actual: {}", exprs.len())
        }
    }

    fn encoding_range(&self) -> Option<(i64, i64)> {
        use self::QueryPlan::*;
        match *self {
            GetEncoded(codec) | FilterEncoded(codec, _) | IndexEncoded(codec, _) => codec.encoding_range(),
            _ => None, // TODO(clemens): many more cases where we can determine range
        }
    }
}

