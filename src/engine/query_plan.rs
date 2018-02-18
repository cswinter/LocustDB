use bit_vec::BitVec;
use mem_store::column::{ColumnData, ColumnCodec};
use mem_store::ingest::RawVal;
use engine::vector_operator::*;
use engine::aggregation_operator::*;
use std::rc::Rc;
use std::cell::RefCell;
use engine::types::Type;
use engine::typed_vec::TypedVec;
use aggregator::Aggregator;


#[derive(Debug)]
pub enum QueryPlan<'a> {
    Decode(&'a ColumnData),
    FilterDecode(&'a ColumnData, Rc<BitVec>),
    GetEncoded(&'a ColumnCodec),
    FilterEncoded(&'a ColumnCodec, Rc<BitVec>),
    LessThanVSi64(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    LessThanVSu8(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    Constant(RawVal),
    // Placeholder(Rc<RefCell<TypedVec<'a>>>),
}

pub fn prepare(plan: QueryPlan) -> BoxedOperator {
    match plan {
        QueryPlan::Decode(col) => Box::new(Decode::new(col)),
        QueryPlan::FilterDecode(col, filter) => Box::new(FilterDecode::new(col, filter)),
        QueryPlan::GetEncoded(col) => Box::new(GetEncoded::new(col)),
        QueryPlan::FilterEncoded(col, filter) => Box::new(FilterEncoded::new(col, filter)),
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
    }
}

pub fn prepare_aggregation<'a, 'b>(plan: QueryPlan<'a>,
                                   grouping_key: &'b TypedVec<'a>,
                                   grouping_key_type: Type,
                                   aggregator: Aggregator) -> Box<AggregationOperator<'a> + 'b> {
    match (grouping_key_type, aggregator, plan) {
        (Type::U16, Aggregator::Count, QueryPlan::Constant(RawVal::Int(i))) |
        (Type::RefU16, Aggregator::Count, QueryPlan::Constant(RawVal::Int(i))) => {
            Box::new(HTu16SummationCi64::new(grouping_key.cast_ref_u16(), i))
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

