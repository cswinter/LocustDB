use std::rc::Rc;
use std::collections::HashMap;
use std::collections::HashSet;

use value::ValueType;
use regex::Regex;


#[derive(Debug)]
pub enum Expr<'a> {
    ColName(Rc<String>),
    ColIndex(usize),
    Func(FuncType, Box<Expr<'a>>, Box<Expr<'a>>),
    Const(ValueType<'a>),
    CompiledRegex(Regex, Box<Expr<'a>>),
}

#[derive(Debug, Copy, Clone)]
pub enum FuncType {
    Equals,
    LT,
    GT,
    And,
    Or,
    RegexMatch,
}

use self::Expr::*;
use self::ValueType::*;
use self::FuncType::*;

impl<'a> Expr<'a> {
    pub fn eval(&self, record: &Vec<ValueType<'a>>) -> ValueType<'a> {
        match self {
            &Func(ref functype, ref exp1, ref exp2) =>
                match (functype, exp1.eval(record), exp2.eval(record)) {
                    (&Equals, v1,            v2)            => Bool(v1 == v2),
                    (_,       Null,          _)             => Null,
                    (_,       _,             Null)          => Null,
                    (&And,    Bool(b1),      Bool(b2))      => Bool(b1 && b2),
                    (&Or,     Bool(b1),      Bool(b2))      => Bool(b1 || b2),
                    (&LT,     Integer(i1),   Integer(i2))   => Bool(i1 < i2),
                    (&LT,     Timestamp(t1), Timestamp(t2)) => Bool(t1 < t2),
                    (&GT,     Integer(i1),   Integer(i2))   => Bool(i1 > i2),
                    (&GT,     Timestamp(t1), Timestamp(t2)) => Bool(t1 > t2),
                    (&GT,     Integer(i),    Timestamp(t))  if i >= 0 => Bool(i as u64 > t),
                    (functype, v1, v2) => panic!("Type error: function {:?} not defined for values {:?} and {:?}", functype, v1, v2),
                },
            &CompiledRegex(ref regex, ref expr) => {
                match expr.eval(record) {
                    Str(string) => Bool(regex.is_match(string)),
                    val => panic!("Type error: Regex cannot be evaluated for {:?}", val),
                }
            },
            &ColIndex(col) => record[col].clone(),
            &Const(ref value) => value.clone(),
            &ColName(_) => panic!("Trying to evaluate ColumnName expression. Compile this expression before evaluating.")
        }
    }

    pub fn compile(&self, column_names: &HashMap<String, usize>) -> Expr {
        use self::Expr::*;
        match self {
            &ColName(ref name) =>
                column_names.get(name.as_ref()).map(|&index| ColIndex(index)).unwrap_or(Const(Null)),
            &Const(ref v) => Const(v.clone()),
            &Func(RegexMatch, ref regex, ref expr) =>
                Expr::CompiledRegex(regex.compile_regex(), Box::new(expr.compile(column_names))),
            &Func(ref ftype, ref expr1, ref expr2) =>
                Expr::func(*ftype, expr1.compile(column_names), expr2.compile(column_names)),
            &ColIndex(_) => panic!("Uncompiled Expr should not contain ColumnIndex."),
            &CompiledRegex(..) => panic!("Uncompiled Expr should not contain CompiledRegex."),
        }
    }

    fn compile_regex(&self) -> Regex {
        match self {
            &Const(Str(string)) => Regex::new(string).expect(&format!("Error compiling regex /{}/", string)),
            _ => panic!("First argument to regex function must be a string constant!"),
        }
    }

    pub fn add_colnames(&self, result: &mut HashSet<Rc<String>>) {
        match self {
            &ColName(ref name) => {
                result.insert(name.clone());
            },
            &Func(_, ref expr1, ref expr2) => {
                expr1.add_colnames(result);
                expr2.add_colnames(result);
            },
            _ => (),
        }
    }

    pub fn func(ftype: FuncType, expr1: Expr<'a>, expr2: Expr<'a>) -> Expr<'a> {
        Func(ftype, Box::new(expr1), Box::new(expr2))
    }
}
