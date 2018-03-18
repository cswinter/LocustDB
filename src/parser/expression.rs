use std::collections::HashSet;

use ingest::raw_val::RawVal;


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

impl Expr {
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

