use std::rc::Rc;
use std::collections::HashMap;
use std::collections::HashSet;

use bit_vec::BitVec;
use value::Val;
use mem_store::ingest::RawVal;
use regex::Regex;
use engine::query_plan::*;
use engine::types::*;
use mem_store::column::Column;


#[derive(Debug, Clone)]
pub enum Expr {
    ColName(Rc<String>),
    ColIndex(usize),
    Func(FuncType, Box<Expr>, Box<Expr>),
    Const(RawVal),
    CompiledRegex(Regex, Box<Expr>),
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
use self::Val::*;
use self::FuncType::*;

impl Expr {
    pub fn eval<'a>(&'a self, record: &Vec<Val<'a>>) -> Val<'a> {
        match self {
            &Func(ref functype, ref exp1, ref exp2) => {
                match (functype, exp1.eval(record), exp2.eval(record)) {
                    (&Equals, v1, v2) => Bool(v1 == v2),
                    (&Negate, Integer(i), _) => Integer(-i),
                    (_, Null, _) => Null,
                    (_, _, Null) => Null,
                    (&And, Bool(b1), Bool(b2)) => Bool(b1 && b2),
                    (&Or, Bool(b1), Bool(b2)) => Bool(b1 || b2),
                    (&LT, Integer(i1), Integer(i2)) => Bool(i1 < i2),
                    (&GT, Integer(i1), Integer(i2)) => Bool(i1 > i2),
                    (&Add, Integer(i1), Integer(i2)) => Integer(i1 + i2),
                    (&Subtract, Integer(i1), Integer(i2)) => Integer(i1 - i2),
                    (&Multiply, Integer(i1), Integer(i2)) => Integer(i1 * i2),
                    (&Divide, Integer(i1), Integer(i2)) => if i2 == 0 { Null } else { Integer(i1 / i2) },
                    (functype, v1, v2) => panic!("Type error: function {:?} not defined for values {:?} and {:?}", functype, v1, v2),
                }
            }
            &CompiledRegex(ref regex, ref expr) => {
                match expr.eval(record) {
                    Str(string) => Bool(regex.is_match(string)),
                    val => panic!("Type error: Regex cannot be evaluated for {:?}", val),
                }
            }
            &ColIndex(col) => record[col].clone(),
            &Const(ref value) => raw_to_val(value),
            &ColName(_) => {
                panic!("Trying to evaluate ColumnName expression. Compile this expression before evaluating.")
            }
        }
    }

    pub fn compile(&self, column_names: &HashMap<String, usize>) -> Expr {
        use self::Expr::*;
        match self {
            &ColName(ref name) => {
                column_names.get(name.as_ref()).map(|&index| ColIndex(index)).unwrap_or(Const(RawVal::Null))
            }
            &Const(ref v) => Const(v.clone()),
            &Func(RegexMatch, ref regex, ref expr) => {
                Expr::CompiledRegex(regex.compile_regex(), Box::new(expr.compile(column_names)))
            }
            &Func(ref ftype, ref expr1, ref expr2) => {
                Expr::func(*ftype,
                           expr1.compile(column_names),
                           expr2.compile(column_names))
            }
            &ColIndex(_) => panic!("Uncompiled Expr should not contain ColumnIndex."),
            &CompiledRegex(..) => panic!("Uncompiled Expr should not contain CompiledRegex."),
        }
    }

    pub fn create_query_plan<'a>(&self, columns: &HashMap<&'a str, &'a Column>, filter: Option<Rc<BitVec>>) -> (QueryPlan<'a>, Type) {
        use self::Expr::*;
        match self {
            &ColName(ref name) => match columns.get::<str>(name.as_ref()) {
                Some(ref c) => match (c.data().to_codec(), filter) {
                    (Some(c), Some(f)) => (QueryPlan::FilterEncoded(c, f), c.encoded_type()),
                    (Some(c), None) => (QueryPlan::GetEncoded(c), c.ref_encoded_type()),
                    (None, Some(f)) => (QueryPlan::FilterDecode(c.data(), f), c.data().decoded_type()),
                    (None, None) => (QueryPlan::Decode(c.data()), c.data().decoded_type()),
                }
                None => panic!("Not implemented")//VecOperator::Constant(VecValue::Constant(RawVal::Null)),
            }
            &Func(LT, ref lhs, ref rhs) => {
                let (plan_lhs, type_lhs) = lhs.create_query_plan(columns, filter.clone());
                let (plan_rhs, type_rhs) = rhs.create_query_plan(columns, filter);
                match (type_lhs, type_rhs) {
                    (Type::I64, Type::Scalar) => {
                        (QueryPlan::LessThanVSi64(Box::new(plan_lhs), Box::new(plan_rhs)), Type::Boolean)
                    }
                    (Type::RefU8, Type::Scalar) => {
                        (QueryPlan::LessThanVSu8(Box::new(plan_lhs), Box::new(plan_rhs)), Type::Boolean)
                    }
                    _ => panic!("type error: {:?} < {:?}", type_lhs, type_rhs)
                }
            }
            &Const(ref v) => (QueryPlan::Constant(v.clone()), Type::Scalar),
            x => panic!("{:?}.compile_vec() not implemented", x),
        }
    }

    pub fn compile_grouping_key<'a>(exprs: &Vec<Expr>,
                                    columns: &HashMap<&'a str, &'a Column>,
                                    filter: Option<Rc<BitVec>>) -> (QueryPlan<'a>, Type) {
        assert!(exprs.len() == 1);
        exprs[0].create_query_plan(columns, filter)
    }

    fn compile_regex(&self) -> Regex {
        match self {
            &Const(RawVal::Str(ref string)) => {
                Regex::new(string).expect(&format!("Error compiling regex /{}/", string))
            }
            _ => panic!("First argument to regex function must be a string constant!"),
        }
    }

    pub fn add_colnames<'a>(&'a self, result: &mut HashSet<&'a str>) {
        match self {
            &ColName(ref name) => {
                result.insert(name);
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

fn raw_to_val(raw: &RawVal) -> Val {
    match raw {
        &RawVal::Int(i) => Val::Integer(i),
        &RawVal::Str(ref s) => Val::Str(&s),
        &RawVal::Null => Val::Null,
    }
}

