use value::ValueType;
use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Query {
    pub select: Vec<usize>,
    pub filter: Expr,
    pub aggregate: Vec<(Aggregator, Expr)>,
}


#[derive(Debug, Clone, Copy)]
pub enum Aggregator {
    Sum,
    Count,
}

impl Aggregator {
    fn zero(self) -> ValueType {
        match self {
            Aggregator::Sum | Aggregator::Count => ValueType::Integer(0),
        }
    }

    fn reduce(self, accumulator: &ValueType, elem: &ValueType) -> ValueType {
        match (self, accumulator, elem) {
            (Aggregator::Sum, &ValueType::Integer(i1), &ValueType::Integer(i2)) => ValueType::Integer(i1 + i2),
            (Aggregator::Count, accumulator, &ValueType::Null) => accumulator.clone(),
            (Aggregator::Count, &ValueType::Integer(i1), _) => ValueType::Integer(i1 + 1),
            (aggregator, accumulator, elem) => panic!("Type error: aggregator {:?} not defined for values {:?} and {:?}", aggregator, *accumulator, *elem),
        }
    }
}

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

impl Query {
    fn run(&self, source: &Vec<Vec<ValueType>>) -> Vec<Vec<ValueType>> {
        if self.aggregate.len() == 0 {
            run_select_query(&self.select, &self.filter, source)
        } else {
            run_aggregate_query(&self.select, &self.filter, &self.aggregate, source)
        }
    }
}

fn run_select_query(select: &Vec<usize>, filter: &Expr, source: &Vec<Vec<ValueType>>) -> Vec<Vec<ValueType>> {
    source.iter()
        .filter(|record| eval(record, filter) == ValueType::Bool(true))
        .map(|record| select.iter().map(|&col| record[col].clone()).collect())
        .collect()
}

fn run_aggregate_query(select: &Vec<usize>, filter: &Expr, aggregation: &Vec<(Aggregator, Expr)>, source: &Vec<Vec<ValueType>>) -> Vec<Vec<ValueType>> {
    let mut groups: HashMap<Vec<ValueType>, Vec<ValueType>> = HashMap::new();

    for record in source.iter() {
        if eval(record, filter) == ValueType::Bool(true) {
            let group: Vec<ValueType> = select.iter().map(|&col| record[col].clone()).collect();
            let accumulator = groups.entry(group).or_insert(aggregation.iter().map(|x| x.0.zero()).collect());
            for (i, &(ref agg_func, ref expr)) in aggregation.iter().enumerate() {
                accumulator[i] = agg_func.reduce(&accumulator[i], &eval(&record, expr));
            }
        }
    }

    let mut result: Vec<Vec<ValueType>> = Vec::new();
    for (mut group, aggregate) in groups {
        group.extend(aggregate);
        result.push(group);
    }
    result
}

fn eval(record: &Vec<ValueType>, condition: &Expr) -> ValueType {
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

pub fn test() {
    let dataset = vec![
        record(1200, "/", 400),
        record(1231, "/", 300),
        record(1132, "/admin", 1200),
        record(994, "/admin/crashdash", 3400),
        record(931, "/", 800),
    ];

    use self::Expr::*;
    use self::FuncType::*;
    use ValueType::*;
    let query1 = Query {
        select: vec![1usize],
        filter: Func(And,
                     Box::new(Func(LT, Box::new(Column(2usize)), Box::new(Const(Integer(1000))))),
                     Box::new(Func(GT, Box::new(Column(0usize)), Box::new(Const(Timestamp(1000)))))),
        aggregate: vec![],
    };
    let query2 = Query {
        select: vec![0usize, 2usize],
        filter: Func(Equals, Box::new(Column(1usize)), Box::new(Const(String(Rc::new("/".to_string()))))),
        aggregate: vec![],
    };
    let count_query = Query {
        select: vec![1usize],
        filter: Const(Bool(true)),
        aggregate: vec![(Aggregator::Count, Const(Integer(0)))],
    };
    let sum_query = Query {
        select: vec![1usize],
        filter: Const(Bool(true)),
        aggregate: vec![(Aggregator::Sum, Column(2))],
    };

    let result1 = query1.run(&dataset);
    let result2 = query2.run(&dataset);
    let count_result = count_query.run(&dataset);
    let sum_result = sum_query.run(&dataset);

    println!("Result 1: {:?}", result1);
    println!("Result 2: {:?}", result2);
    println!("Count Result: {:?}", count_result);
    println!("Sum Result: {:?}", sum_result);
}

fn record(timestamp: u64, url: &str, loadtime: i64) -> Vec<ValueType> {
    vec![ValueType::Timestamp(timestamp), ValueType::String(Rc::new(url.to_string())), ValueType::Integer(loadtime)]
}
