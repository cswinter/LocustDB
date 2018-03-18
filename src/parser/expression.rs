use std::collections::HashMap;
use std::collections::HashSet;

use engine::filter::Filter;
use engine::query_plan::*;
use engine::types::*;
use ingest::raw_val::RawVal;
use mem_store::column::Column;


#[derive(Debug, Clone)]
pub enum Expr {
    ColName(String),
    Func(FuncType, Box<Expr>, Box<Expr>),
    Const(RawVal),
}

#[derive(Debug, Copy, Clone)]
pub enum FuncType {
    Equals,
    LT,
    GT,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    RegexMatch,
    Negate,
}

use self::Expr::*;
use self::FuncType::*;

impl Expr {
    pub fn create_query_plan<'a>(&self,
                                 columns: &HashMap<&'a str, &'a Column>,
                                 filter: Filter) -> (QueryPlan<'a>, Type<'a>) {
        use self::Expr::*;
        match self {
            &ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(ref c) => {
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
                None => panic!("Not implemented")//VecOperator::Constant(VecValue::Constant(RawVal::Null)),
            }
            &Func(LT, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = lhs.create_query_plan(columns, filter.clone());
                let (plan_rhs, type_rhs) = rhs.create_query_plan(columns, filter);
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::Integer, BasicType::Integer) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                match type_lhs.encoding_type() {
                                    EncodingType::U8 => QueryPlan::LessThanVSu8(Box::new(plan_lhs), Box::new(plan_rhs)),
                                    _ => unimplemented!(),
                                }
                            } else {
                                QueryPlan::LessThanVSi64(Box::new(plan_lhs), Box::new(plan_rhs))
                            }
                        } else {
                            unimplemented!()
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    _ => panic!("type error: {:?} < {:?}", type_lhs, type_rhs)
                }
            }
            &Func(Equals, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = lhs.create_query_plan(columns, filter.clone());
                let (plan_rhs, type_rhs) = rhs.create_query_plan(columns, filter);
                match (type_lhs.decoded, type_rhs.decoded) {
                    (BasicType::String, BasicType::String) => {
                        let plan = if type_rhs.is_scalar {
                            if type_lhs.is_encoded() {
                                match type_lhs.encoding_type() {
                                    EncodingType::U16 => {
                                        let encoded = QueryPlan::EncodeStrConstant(
                                            Box::new(plan_rhs), type_lhs.codec.unwrap());
                                        QueryPlan::EqualsVSU16(Box::new(plan_lhs), Box::new(encoded))
                                    }
                                    _ => {
                                        let decoded = QueryPlan::Decode(Box::new(plan_lhs));
                                        QueryPlan::EqualsVSString(Box::new(decoded), Box::new(plan_rhs))
                                    }
                                }
                            } else {
                                unimplemented!()
                            }
                        } else {
                            unimplemented!()
                        };
                        (plan, Type::new(BasicType::Boolean, None).mutable())
                    }
                    _ => panic!("type error: {:?} = {:?}", type_lhs, type_rhs)
                }
            }
            &Func(Or, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = lhs.create_query_plan(columns, filter.clone());
                let (plan_rhs, type_rhs) = rhs.create_query_plan(columns, filter);
                assert!(type_lhs.decoded == BasicType::Boolean && type_rhs.decoded == BasicType::Boolean);
                (QueryPlan::Or(Box::new(plan_lhs), Box::new(plan_rhs)), Type::bit_vec())
            }
            &Func(And, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = lhs.create_query_plan(columns, filter.clone());
                let (plan_rhs, type_rhs) = rhs.create_query_plan(columns, filter);
                assert!(type_lhs.decoded == BasicType::Boolean && type_rhs.decoded == BasicType::Boolean);
                (QueryPlan::And(Box::new(plan_lhs), Box::new(plan_rhs)), Type::bit_vec())
            }
            &Const(ref v) => (QueryPlan::Constant(v.clone()), Type::scalar(v.get_type())),
            x => panic!("{:?}.compile_vec() not implemented", x),
        }
    }

    pub fn compile_grouping_key<'a>(exprs: &Vec<Expr>,
                                    columns: &HashMap<&'a str, &'a Column>,
                                    filter: Filter) -> (QueryPlan<'a>, Type<'a>) {
        assert!(exprs.len() == 1);
        exprs[0].create_query_plan(columns, filter)
    }

    pub fn add_colnames<'a>(&'a self, result: &mut HashSet<String>) {
        match self {
            &ColName(ref name) => {
                result.insert(name.to_string());
            }
            &Func(_, ref expr1, ref expr2) => {
                expr1.add_colnames(result);
                expr2.add_colnames(result);
            }
            _ => (),
        }
    }

    pub fn func(ftype: FuncType, expr1: Expr, expr2: Expr) -> Expr {
        Func(ftype, Box::new(expr1), Box::new(expr2))
    }
}

