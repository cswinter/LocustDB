use mem_store::column::Column;
use mem_store::ingest::RawVal;
use engine::types::Type;
use engine::vector_operator::*;


#[derive(Debug)]
pub enum QueryPlan<'a> {
    CollectDecoded(&'a Column),
    LessThanVSi64(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>),
    Constant(RawVal),
}

pub fn prepare(plan: QueryPlan) -> BoxedOperator {
    match plan {
        QueryPlan::CollectDecoded(col) => Box::new(CollectDecoded::new(col)),
        QueryPlan::Constant(ref c) => Box::new(Constant::new(c.clone())),
        QueryPlan::LessThanVSi64(lhs, rhs) => {
            if let RawVal::Int(i) = get_const(*rhs) {
                Box::new(LessThanVSi64::new(prepare(*lhs), i))
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

