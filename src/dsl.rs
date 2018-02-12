use expression::*;
use ingest::RawVal;
use std::ops::Add;


const null: Expr = Expr::Const(RawVal::Null);


fn ci(i: i32) -> Expr {
    Expr::Const(RawVal::Int(i))
}

fn cs(s: &str) -> Expr {
    Expr::Const(RawVal::Str(s.to_string()))
}

fn i(s: &str) -> Expr {
    Expr::ColName(Rc::new(s.to_string()))
}

fn count(expr: Expr)-> Expr {

}

impl Add for Expr {
    type Output = Expr;

    fn add(self, other: Expr) -> Expr {
        Expr::Func(FuncType::Add, Box::new(self), Box::new(other))
    }
}

