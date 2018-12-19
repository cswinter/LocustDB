use ingest::raw_val::RawVal;
use self::Expr::*;
use std::collections::HashSet;
use engine::*;

#[derive(Debug, Clone)]
pub enum Expr {
    ColName(String),
    Const(RawVal),
    Func1(Func1Type, Box<Expr>),
    Func2(Func2Type, Box<Expr>, Box<Expr>),
    Aggregate(Aggregator, Box<Expr>),
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Func2Type {
    Equals,
    NotEquals,
    LT,
    LTE,
    GT,
    GTE,
    And,
    Or,
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    RegexMatch,
    Like,
}

#[derive(Debug, Copy, Clone)]
pub enum Func1Type {
    Negate,
    ToYear,
    Not,
    IsNull,
    IsNotNull,
    Length,
}

impl Expr {
    pub fn add_colnames(&self, result: &mut HashSet<String>) {
        match *self {
            ColName(ref name) => {
                result.insert(name.to_string());
            }
            Func2(_, ref expr1, ref expr2) => {
                expr1.add_colnames(result);
                expr2.add_colnames(result);
            }
            Func1(_, ref expr) => expr.add_colnames(result),
            Aggregate(_, ref expr) => expr.add_colnames(result),
            Const(_) => {}
        }
    }

    pub fn func(ftype: Func2Type, expr1: Expr, expr2: Expr) -> Expr {
        Func2(ftype, Box::new(expr1), Box::new(expr2))
    }

    pub fn func1(ftype: Func1Type, expr: Expr) -> Expr {
        Func1(ftype, Box::new(expr))
    }
}

