use value::ValueType;


#[derive(Debug)]
pub enum Expr {
    Column(usize),
    Func(FuncType, Box<Expr>, Box<Expr>),
    Const(ValueType),
}

#[derive(Debug)]
pub enum FuncType {
    Equals,
    LT,
    GT,
    And,
    Or
}

pub fn eval(record: &Vec<ValueType>, condition: &Expr) -> ValueType {
    use self::Expr::*;
    use self::ValueType::*;
    match condition {
        &Func(ref functype, ref exp1, ref exp2) =>
            match (functype, eval(record, &exp1), eval(record, &exp2)) {
                (&FuncType::Equals, v1,            v2)            => Bool(v1 == v2),
                (&FuncType::And,    Bool(b1),      Bool(b2))      => Bool(b1 && b2),
                (&FuncType::Or,     Bool(b1),      Bool(b2))      => Bool(b1 || b2),
                (&FuncType::LT,     Integer(i1),   Integer(i2))   => Bool(i1 < i2),
                (&FuncType::LT,     Timestamp(t1), Timestamp(t2)) => Bool(t1 < t2),
                (&FuncType::GT,     Integer(i1),   Integer(i2))   => Bool(i1 > i2),
                (&FuncType::GT,     Timestamp(t1), Timestamp(t2)) => Bool(t1 > t2),
                (functype, v1, v2) => panic!("Type error: function {:?} not defined for values {:?} and {:?}", functype, v1, v2),
            },
        &Column(col) => record[col].clone(),
        &Const(ref value) => value.clone(),
    }
}
