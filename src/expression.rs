use std::rc::Rc;
use std::collections::HashMap;

use value::ValueType;


#[derive(Debug)]
pub enum Expr {
    ColName(Rc<String>),
    ColIndex(usize),
    Func(FuncType, Box<Expr>, Box<Expr>),
    Const(ValueType),
}

#[derive(Debug, Copy, Clone)]
pub enum FuncType {
    Equals,
    LT,
    GT,
    And,
    Or
}

use self::Expr::*;
use self::ValueType::*;
use self::FuncType::*;

impl Expr {
    pub fn eval(&self, record: &Vec<ValueType>) -> ValueType {
        match self {
            &Func(ref functype, ref exp1, ref exp2) =>
                match (functype, exp1.eval(record), exp2.eval(record)) {
                    (&Equals, v1,            v2)            => Bool(v1 == v2),
                    (&And,    Bool(b1),      Bool(b2))      => Bool(b1 && b2),
                    (&Or,     Bool(b1),      Bool(b2))      => Bool(b1 || b2),
                    (&LT,     Integer(i1),   Integer(i2))   => Bool(i1 < i2),
                    (&LT,     Timestamp(t1), Timestamp(t2)) => Bool(t1 < t2),
                    (&GT,     Integer(i1),   Integer(i2))   => Bool(i1 > i2),
                    (&GT,     Timestamp(t1), Timestamp(t2)) => Bool(t1 > t2),
                    (&GT,     Integer(i),    Timestamp(t))  if i >= 0 => Bool(i as u64 > t),
                    (functype, v1, v2) => panic!("Type error: function {:?} not defined for values {:?} and {:?}", functype, v1, v2),
                },
            &ColIndex(col) => record[col].clone(),
            &Const(ref value) => value.clone(),
            &ColName(_) => panic!("Trying to evaluate ColumnName expression. Compile this expression before evaluating.")
        }
    }

    pub fn compile(&self, column_names: &HashMap<String, usize>) -> Expr {
        use self::Expr::*;
        match self {
            &ColName(ref name) => ColIndex(*column_names.get(name.as_ref()).unwrap()),
            &Const(ref v) => Const(v.clone()),
            &Func(ref ftype, ref expr1, ref expr2) => Expr::func(*ftype, expr1.compile(column_names), expr2.compile(column_names)),
            &ColIndex(_) => panic!("Uncompiled Expr should not contain ColumnIndex."),
        }
    }

    pub fn func(ftype: FuncType, expr1: Expr, expr2: Expr) -> Expr {
        Func(ftype, Box::new(expr1), Box::new(expr2))
    }

    pub fn col(name: &str) -> Expr {
        ColName(Rc::new(name.to_string()))
    }
}
