use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use ::QueryError;
use engine::aggregator::Aggregator;
use engine::filter::Filter;
use engine::types::*;
use engine::vector_op::*;
use engine::vector_op::vector_operator::BufferRef;
use ingest::raw_val::RawVal;
use mem_store::column::Column;
use mem_store::*;
use syntax::expression::*;
use mem_store::integers::IntegerOffsetCodec;


#[derive(Debug, Clone)]
pub enum QueryPlan<'a> {
    ReadColumn(&'a Column),
    DecodeColumn(&'a Column),
    ReadBuffer(BufferRef),
    // TODO(clemens): make it possible to replace this with Decode(ReadColumn)

    DecodeWith(Box<QueryPlan<'a>>, Codec<'a>),
    TypeConversion(Box<QueryPlan<'a>>, EncodingType, EncodingType),

    EncodeStrConstant(Box<QueryPlan<'a>>, Codec<'a>),
    EncodeIntConstant(Box<QueryPlan<'a>>, Codec<'a>),

    BitPack(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>, i64),
    BitUnpack(Box<QueryPlan<'a>>, u8, u8),

    LessThanVS(EncodingType, Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    EqualsVS(EncodingType, Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    And(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    Or(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),

    SortIndices(Box<QueryPlan<'a>>, bool),

    Select(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>, EncodingType),

    EncodedGroupByPlaceholder,

    Constant(RawVal),
}

pub struct QueryExecutor<'a> {
    stages: Vec<ExecutorStage<'a>>,
    count: usize,
}

#[derive(Default)]
struct ExecutorStage<'a> {
    ops: Vec<Box<VecOperator<'a> + 'a>>,
    encoded_group_by: Option<BufferRef>,
    filter: Filter,
}

impl<'a> QueryExecutor<'a> {
    pub fn new_stage(&mut self) {
        self.stages.push(ExecutorStage::default());
    }

    pub fn new_buffer(&mut self) -> BufferRef {
        self.count += 1;
        BufferRef(self.count - 1)
    }

    fn last_buffer(&self) -> BufferRef { BufferRef(self.count - 1) }

    fn push(&mut self, op: Box<VecOperator<'a> + 'a>) {
        self.stages.last_mut().unwrap().push(op);
    }

    pub fn set_encoded_group_by(&mut self, gb: BufferRef) {
        self.stages.last_mut().unwrap().encoded_group_by = Some(gb)
    }

    fn encoded_group_by(&self) -> Option<BufferRef> { self.stages.last().unwrap().encoded_group_by }

    fn filter(&self) -> Filter { self.stages.last().unwrap().filter }

    pub fn set_filter(&mut self, filter: Filter) {
        self.stages.last_mut().unwrap().filter = filter;
    }

    pub fn run(&mut self) -> Scratchpad<'a> {
        let mut scratchpad = Scratchpad::new(self.count);
        for stage in &mut self.stages {
            stage.run(&mut scratchpad);
        }
        scratchpad
    }
}

impl<'a> Default for QueryExecutor<'a> {
    fn default() -> QueryExecutor<'a> {
        QueryExecutor {
            stages: vec![ExecutorStage::default()],
            count: 0
        }
    }
}

impl<'a> fmt::Display for QueryExecutor<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, stage) in self.stages.iter().enumerate() {
            write!(f, "-- Stage {} --", i)?;
            for op in &stage.ops {
                write!(f, "\n{:?}", op)?;
            }
        }
        Ok(())
    }
}

impl<'a> ExecutorStage<'a> {
    fn push(&mut self, op: Box<VecOperator<'a> + 'a>) {
        self.ops.push(op);
    }

    fn run(&mut self, scratchpad: &mut Scratchpad<'a>) {
        for op in &mut self.ops {
            op.execute(scratchpad);
        }
    }
}

pub fn prepare<'a>(plan: QueryPlan<'a>, result: &mut QueryExecutor<'a>) -> BufferRef {
    let operation: Box<VecOperator> = match plan {
        QueryPlan::Select(plan, indices, t) =>
            VecOperator::select(t, prepare(*plan, result), prepare(*indices, result), result.new_buffer()),
        QueryPlan::DecodeColumn(col) => {
            let column_buffer = result.new_buffer();
            let column_op = VecOperator::get_decode(col, column_buffer);
            match result.filter() {
                Filter::BitVec(filter) => {
                    result.push(column_op);
                    VecOperator::filter(col.basic_type().to_encoded(), column_buffer, filter, result.new_buffer())
                }
                Filter::Indices(filter) => {
                    result.push(column_op);
                    VecOperator::select(col.basic_type().to_encoded(), column_buffer, filter, result.new_buffer())
                }
                Filter::None => column_op,
            }
        }
        QueryPlan::ReadColumn(col) => {
            let column_buffer = result.new_buffer();
            let column_op = VecOperator::get_encoded(col, column_buffer);
            match result.filter() {
                Filter::BitVec(filter) => {
                    result.push(column_op);
                    VecOperator::filter(col.encoding_type(), column_buffer, filter, result.new_buffer())
                }
                Filter::Indices(filter) => {
                    result.push(column_op);
                    VecOperator::select(col.encoding_type(), column_buffer, filter, result.new_buffer())
                }
                Filter::None => column_op,
            }
        }
        QueryPlan::Constant(ref c) =>
            VecOperator::constant(c.clone(), result.new_buffer()),
        QueryPlan::DecodeWith(plan, codec) =>
            VecOperator::decode(prepare(*plan, result), result.new_buffer(), codec),
        QueryPlan::TypeConversion(plan, initial_type, target_type) =>
            VecOperator::type_conversion(prepare(*plan, result), result.new_buffer(), initial_type, target_type),
        QueryPlan::EncodeStrConstant(plan, codec) =>
            VecOperator::encode_str_const(prepare(*plan, result), result.new_buffer(), codec),
        QueryPlan::EncodeIntConstant(plan, codec) =>
            VecOperator::encode_int_const(prepare(*plan, result), result.new_buffer(), codec),
        QueryPlan::BitPack(lhs, rhs, shift_amount) =>
            VecOperator::bit_shift_left_add(prepare(*lhs, result), prepare(*rhs, result), result.new_buffer(), shift_amount),
        QueryPlan::BitUnpack(inner, shift, width) =>
            VecOperator::bit_unpack(prepare(*inner, result), result.new_buffer(), shift, width),
        QueryPlan::LessThanVS(left_type, lhs, rhs) =>
            VecOperator::less_than_vs(left_type, prepare(*lhs, result), prepare(*rhs, result), result.new_buffer()),
        QueryPlan::EqualsVS(left_type, lhs, rhs) =>
            VecOperator::equals_vs(left_type, prepare(*lhs, result), prepare(*rhs, result), result.new_buffer()),
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
        QueryPlan::EncodedGroupByPlaceholder => return result.encoded_group_by().unwrap(),
        QueryPlan::SortIndices(plan, descending) =>
            VecOperator::sort_indices(prepare(*plan, result), result.new_buffer(), descending),
        QueryPlan::ReadBuffer(buffer) => return buffer,
    };
    result.push(operation);
    result.last_buffer()
}

pub fn prepare_unique(raw_grouping_key: BufferRef,
                      raw_grouping_key_type: EncodingType,
                      max_cardinality: usize,
                      result: &mut QueryExecutor) -> BufferRef {
    let output = result.new_buffer();
    result.push(VecOperator::unique(raw_grouping_key, output, raw_grouping_key_type, max_cardinality));
    output
}

pub fn prepare_hashmap_grouping<'a>(raw_grouping_key: BufferRef,
                                    grouping_key_type: EncodingType,
                                    max_cardinality: usize,
                                    result: &mut QueryExecutor) -> (BufferRef, BufferRef, Type<'a>, BufferRef) {
    let unique_out = result.new_buffer();
    let grouping_key_out = result.new_buffer();
    let cardinality_out = result.new_buffer();
    result.push(VecOperator::hash_map_grouping(
        raw_grouping_key, unique_out, grouping_key_out, cardinality_out, grouping_key_type, max_cardinality));
    (unique_out,
     grouping_key_out,
     Type::encoded(Arc::new(OpaqueCodec::new(BasicType::Integer, grouping_key_type).set_positive_integer(true))),
     cardinality_out)
}

// TODO(clemens): add QueryPlan::Aggregation and merge with prepare function
pub fn prepare_aggregation<'a, 'b>(plan: QueryPlan<'a>,
                                   mut plan_type: Type<'a>,
                                   grouping_key: BufferRef,
                                   grouping_type: EncodingType,
                                   max_index: usize,
                                   aggregator: Aggregator,
                                   result: &mut QueryExecutor<'a>) -> Result<(BufferRef, Type<'a>), QueryError> {
    let output_location = result.new_buffer();
    let (operation, t): (BoxedOperator<'a>, _) = match (aggregator, plan) {
        (Aggregator::Count, _) => (
            VecOperator::count(grouping_key,
                               output_location,
                               grouping_type,
                               max_index,
                               false),
            // TODO(clemens): plain integer encoding
            Type::encoded(Arc::new(IntegerOffsetCodec::<u32>::new(0, max_index)))
        ),

        (Aggregator::Sum, mut plan) => {
            if !plan_type.is_summation_preserving() {
                plan = QueryPlan::DecodeWith(Box::new(plan), plan_type.codec.clone().unwrap());
                plan_type = plan_type.decoded();
            }
            (VecOperator::summation(prepare(plan, result),
                                    grouping_key,
                                    output_location,
                                    plan_type.encoding_type(),
                                    grouping_type,
                                    max_index,
                                    false), // TODO(clemens): determine dense groupings
             Type::unencoded(BasicType::Integer))
        }
    };
    result.push(operation);
    Ok((output_location, t))
}

pub fn order_preserving<'a>((plan, t): (QueryPlan<'a>, Type<'a>)) -> (QueryPlan<'a>, Type<'a>) {
    if t.is_order_preserving() {
        (plan, t)
    } else {
        let new_type = t.decoded();
        (QueryPlan::DecodeWith(Box::new(plan), t.codec.unwrap()), new_type)
    }
}

impl<'a> QueryPlan<'a> {
    pub fn create_query_plan<'b>(expr: &Expr,
                                 columns: &HashMap<&'b str, &'b Column>) -> Result<(QueryPlan<'b>, Type<'b>), QueryError> {
        use self::Expr::*;
        use self::FuncType::*;
        Ok(match *expr {
            ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(c) => {
                    let t = c.full_type();
                    match c.codec() {
                        None => (QueryPlan::DecodeColumn(*c), t.decoded()),
                        Some(_) => (QueryPlan::ReadColumn(*c), t),
                    }
                }
                None => bail!(QueryError::NotImplemented, "Referencing missing column {}", name)
            }
            Func(LT, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, columns)?;
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
            Func(Equals, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, columns)?;
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::String, BasicType::String) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                let encoded = QueryPlan::EncodeStrConstant(Box::new(plan_rhs), type_lhs.codec.clone().unwrap());
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
            Func(Or, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, columns)?;
                if type_lhs.decoded != BasicType::Boolean || type_rhs.decoded != BasicType::Boolean {
                    bail!(QueryError::TypeError, "Found {} AND {}, expected bool AND bool")
                }
                (QueryPlan::Or(Box::new(plan_lhs), Box::new(plan_rhs)), Type::bit_vec())
            }
            Func(And, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = QueryPlan::create_query_plan(lhs, columns)?;
                let (plan_rhs, type_rhs) = QueryPlan::create_query_plan(rhs, columns)?;
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
                                    columns: &HashMap<&'b str, &'b Column>) -> Result<(QueryPlan<'b>, Type<'b>, i64, Vec<(QueryPlan<'b>, Type<'b>)>), QueryError> {
        if exprs.len() == 1 {
            QueryPlan::create_query_plan(&exprs[0], columns)
                .map(|(gk_plan, gk_type)| {
                    let max_cardinality = QueryPlan::encoding_range(&gk_plan).map_or(1 << 63, |i| i.1);
                    let decoded_group_by = gk_type.codec.clone().map_or(
                        QueryPlan::EncodedGroupByPlaceholder,
                        |codec| QueryPlan::DecodeWith(
                            Box::new(QueryPlan::EncodedGroupByPlaceholder),
                            codec));
                    (gk_plan.clone(), gk_type.clone(), max_cardinality, vec![(decoded_group_by, gk_type.decoded())])
                })
        } else if exprs.len() == 2 {
            let mut total_width = 0;
            let mut largest_key = 0;
            let mut plan = None;
            let mut decode_plans = Vec::with_capacity(exprs.len());
            let mut order_preserving = true;
            for expr in exprs.iter().rev() {
                let (query_plan, plan_type) = QueryPlan::create_query_plan(expr, columns)?;
                // TODO(clemens): Potentially subtract min if min is negative or this makes grouping key fit into 64 bits
                if let Some((min, max)) = QueryPlan::encoding_range(&query_plan) {
                    if min < 0 {
                        plan = None;
                        break;
                    }
                    order_preserving = order_preserving && plan_type.is_order_preserving();
                    let query_plan = QueryPlan::TypeConversion(Box::new(query_plan),
                                                               plan_type.encoding_type(),
                                                               EncodingType::I64);
                    let bits =
                        if max == 0 { 0 } else { (max as f64).log2().floor() as i64 + 1 };
                    if total_width == 0 {
                        plan = Some(query_plan);
                    } else {
                        plan = plan.map(|plan|
                            QueryPlan::BitPack(Box::new(plan), Box::new(query_plan), total_width));
                    }

                    let mut decode_plan = QueryPlan::BitUnpack(
                        Box::new(QueryPlan::EncodedGroupByPlaceholder),
                        total_width as u8,
                        bits as u8);
                    decode_plan = QueryPlan::TypeConversion(
                        Box::new(decode_plan),
                        EncodingType::I64,
                        plan_type.encoding_type());
                    if let Some(codec) = plan_type.codec.clone() {
                        decode_plan = QueryPlan::DecodeWith(
                            Box::new(decode_plan),
                            codec)
                    }
                    decode_plans.push((decode_plan, plan_type.decoded()));

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
                    let t = Type::encoded(Arc::new(OpaqueCodec::new(BasicType::Integer, EncodingType::I64)
                        .set_order_preserving(order_preserving)
                        .set_positive_integer(true)));
                    return Ok((plan, t, largest_key, decode_plans));
                }
            }
            // TODO(clemens): add u8, u16, u32, u128 grouping keys
            // TODO(clemens): implement general case using bites slice as grouping key
            bail!(QueryError::NotImplemented, "Failed to pack group by columns into 64 bit value")
        } else {
            bail!(QueryError::NotImplemented, "Can only group by one or two columns. Actual: {}", exprs.len())
        }
    }

    fn encoding_range(&self) -> Option<(i64, i64)> {
        use self::QueryPlan::*;
        match *self {
            ReadColumn(col) => col.codec().unwrap().encoding_range(),
            _ => None, // TODO(clemens): many more cases where we can determine range
        }
    }
}

