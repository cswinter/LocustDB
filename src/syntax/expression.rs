use crate::ingest::raw_val::RawVal;
use self::Expr::*;
use std::collections::HashSet;
use crate::engine::*;

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
    NotLike,
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

    pub fn get_string(self) -> String {
        match self {
            ColName(ref name) => name.clone(),
            Const(ref val) => val.to_string(),
            Func1(ftype, expr) => {
                match ftype {
                    Func1Type::Negate => format!("-{}", expr.get_string()),
                    Func1Type::ToYear => format!("ToYear({})", expr.get_string()),
                    Func1Type::Not => format!("!{}", expr.get_string()),
                    Func1Type::IsNull => format!("IsNull({})", expr.get_string()),
                    Func1Type::IsNotNull => format!("IsNotNull({})", expr.get_string()),
                    Func1Type::Length => format!("Length({})", expr.get_string()),
                }
            },
            Func2(ftype, expr1, expr2) => {
                match ftype {
                    Func2Type::Equals => format!("{} = {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::NotEquals => format!("{} != {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::LT => format!("{} < {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::LTE => format!("{} <= {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::GT => format!("{} > {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::GTE => format!("{} >= {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::And => format!("{} And {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::Or => format!("{} Or {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::Add => format!("{} + {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::Subtract => format!("{} - {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::Multiply => format!("{} * {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::Divide => format!("{} / {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::Modulo => format!("{} % {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::RegexMatch => format!("RegexMatch({}, {})", expr1.get_string(), expr2.get_string()),
                    Func2Type::Like => format!("{} Like {}", expr1.get_string(), expr2.get_string()),
                    Func2Type::NotLike => format!("{} NotLike {}", expr1.get_string(), expr2.get_string()),
                }
            },
            Aggregate(agg, expr) => {
                agg.get_string(expr.get_string())
            }
        }
    }
}

