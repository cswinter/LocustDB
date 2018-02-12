use bit_vec::BitVec;
use mem_store::column::Column;
use mem_store::ingest::RawVal;
use engine::types::Type;
use engine::typed_vec::TypedVec;
use engine::vector_operator::*;


#[derive(Debug)]
pub enum QueryPlan<'a> {
    CollectTyped(&'a Column),
    LessThan(Box<QueryPlan<'a>>, Box<QueryPlan<'a>>, Type, Type),
    Constant(RawVal),
}

pub fn prepare(plan: QueryPlan, t: Type) -> BoxedOperator<TypedVec> {
    match t {
        Type::String => Box::new(TagStr::new(prepare_str(plan))),
        Type::I64 => Box::new(TagInt::new(prepare_int(plan))),
        Type::Boolean => Box::new(TagBool::new(prepare_bool(plan))),
        Type::Scalar => Box::new(TagConst::new(get_const(plan))),
        x => panic!("prepare for {:?} not implemented", x),
    }
}

pub fn prepare_str<'a>(plan: QueryPlan<'a>) -> Box<VecOperator<Output=Vec<&'a str>> + 'a> {
    match plan {
        QueryPlan::CollectTyped(col) => Box::new(CollectStr::new(col)),
        x => panic!("{:?} not implemented for type string", x),
    }
}

pub fn prepare_int<'a>(plan: QueryPlan<'a>) -> Box<VecOperator<Output=Vec<i64>> + 'a> {
    match plan {
        QueryPlan::CollectTyped(col) => Box::new(CollectInt::new(col)),
        x => panic!("{:?} not implemented for type integer", x),
    }
}

pub fn get_const<'a>(plan: QueryPlan<'a>) -> RawVal {
    match plan {
        QueryPlan::Constant(ref c) => c.clone(),
        x => panic!("{:?} not implemented get_const", x),
    }
}

pub fn prepare_bool<'a>(plan: QueryPlan<'a>) -> BoxedOperator<BitVec> {
    match plan {
        QueryPlan::LessThan(lhs, rhs, lt, rt) => {
            match (lt, rt) {
                (Type::I64, Type::Scalar) => {
                    if let RawVal::Int(i) = get_const(*rhs) {
                        Box::new(LessThanVi64S::new(prepare_int(*lhs), i))
                    } else {
                        panic!("Wrong type")
                    }
                }
                _ => panic!(" not implemented"),
            }
        }
        x => panic!("{:?} not implemented for type bool", x),
    }
}
