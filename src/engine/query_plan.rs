use bit_vec::BitVec;
use mem_store::column::{ColumnData, ColumnCodec};
use mem_store::ingest::RawVal;
use engine::vector_operator::*;
use engine::aggregation_operator::*;
use std::rc::Rc;
use engine::types::*;
use engine::typed_vec::TypedVec;
use aggregator::Aggregator;


#[derive(Debug)]
pub enum QueryPlan<'a> {
    GetDecode(&'a ColumnData),
    FilterDecode(&'a ColumnData, Rc<BitVec>),
    IndexDecode(&'a ColumnData, Rc<Vec<usize>>),
    GetEncoded(&'a ColumnCodec),
    FilterEncoded(&'a ColumnCodec, Rc<BitVec>),
    IndexEncoded(&'a ColumnCodec, Rc<Vec<usize>>),

    Decode(Box<QueryPlan<'a>>),

    EncodeStrConstant(Box<QueryPlan<'a>>, &'a ColumnCodec),

    LessThanVSi64(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    LessThanVSu8(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    EqualsVSString(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    EqualsVSU16(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),

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
        QueryPlan::LessThanVSi64(lhs, rhs) => {
            if let RawVal::Int(i) = rhs.get_const() {
                Box::new(LessThanVSi64::new(prepare(*lhs), i))
            } else {
                panic!("Wrong type")
            }
        }
        QueryPlan::LessThanVSu8(lhs, rhs) => {
            if let RawVal::Int(i) = rhs.get_const() {
                // TODO(clemens): use codec to encode constant value
                Box::new(LessThanVSu8::new(prepare(*lhs), i as u8))
            } else {
                panic!("Wrong type")
            }
        }
        QueryPlan::Decode(plan) => Box::new(Decode::new(prepare(*plan))),
        QueryPlan::EncodeStrConstant(plan, codec) =>
            Box::new(EncodeStrConstant::new(prepare(*plan), codec)),
        QueryPlan::EqualsVSString(lhs, rhs) =>
            Box::new(EqualsVSString::new(prepare(*lhs), prepare(*rhs))),
        QueryPlan::EqualsVSU16(lhs, rhs) =>
            Box::new(EqualsVSU16::new(prepare(*lhs), prepare(*rhs))),
    }
}

pub fn prepare_aggregation<'a, 'b>(plan: QueryPlan<'a>,
                                   grouping_key: &'b TypedVec<'a>,
                                   grouping_key_type: &Type,
                                   aggregator: Aggregator) -> Box<AggregationOperator<'a> + 'b> {
    match (grouping_key_type.encoding_type(), aggregator, plan) {
        (EncodingType::U8, Aggregator::Count, QueryPlan::Constant(RawVal::Int(i))) => {
            Box::new(HTSummationCi64::new(grouping_key.cast_ref_u8(), i))
        }
        (EncodingType::U16, Aggregator::Count, QueryPlan::Constant(RawVal::Int(i))) => {
            Box::new(HTSummationCi64::new(grouping_key.cast_ref_u16(), i))
        }
        (EncodingType::U32, Aggregator::Count, QueryPlan::Constant(RawVal::Int(i))) => {
            Box::new(HTSummationCi64::new(grouping_key.cast_ref_u32(), i))
        }
        (g, a, p) => panic!("prepare_aggregation not implemented for {:?}, {:?}, {:?}", &g, &a, &p)
    }
}


impl<'a> QueryPlan<'a> {
    fn get_const(self) -> RawVal {
        match self {
            QueryPlan::Constant(ref c) => c.clone(),
            x => panic!("{:?} not implemented get_const", x),
        }
    }
}

