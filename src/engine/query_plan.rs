use bit_vec::BitVec;
use mem_store::column::{ColumnData, ColumnCodec};
use mem_store::ingest::RawVal;
use engine::vector_operator::*;
use std::rc::Rc;


#[derive(Debug)]
pub enum QueryPlan<'a> {
    Decode(&'a ColumnData),
    FilterDecode(&'a ColumnData, Rc<BitVec>),
    GetEncoded(&'a ColumnCodec),
    FilterEncoded(&'a ColumnCodec, Rc<BitVec>),
    LessThanVSi64(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    LessThanVSu8(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    Constant(RawVal),
}

pub fn prepare(plan: QueryPlan) -> BoxedOperator {
    match plan {
        QueryPlan::Decode(col) => Box::new(Decode::new(col)),
        QueryPlan::FilterDecode(col, filter) => Box::new(FilterDecode::new(col, filter)),
        QueryPlan::GetEncoded(col) => Box::new(GetEncoded::new(col)),
        QueryPlan::FilterEncoded(col, filter) => Box::new(FilterEncoded::new(col, filter)),
        QueryPlan::Constant(ref c) => Box::new(Constant::new(c.clone())),
        QueryPlan::LessThanVSi64(lhs, rhs) => {
            if let RawVal::Int(i) = get_const(*rhs) {
                Box::new(LessThanVSi64::new(prepare(*lhs), i))
            } else {
                panic!("Wrong type")
            }
        }
        QueryPlan::LessThanVSu8(lhs, rhs) => {
            if let RawVal::Int(i) = get_const(*rhs) {
                // TODO(clemens): use codec to encode constant value
                Box::new(LessThanVSu8::new(prepare(*lhs), i as u8))
            } else {
                panic!("Wrong type")
            }
        }
    }
}

pub fn get_const<'a>(plan: QueryPlan<'a>) -> RawVal {
    match plan {
        QueryPlan::Constant(ref c) => c.clone(),
        x => panic!("{:?} not implemented get_const", x),
    }
}

