use value::ValueType;
use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Query {
    pub select: Vec<usize>,
    pub filter: Condition,
    pub groupBy: Vec<usize>,
    pub aggregate: Vec<(Aggregator, usize)>,
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
pub enum Condition {
    True,
    False,
    Column(usize),
    Func(FuncType, Box<Condition>, Box<Condition>),
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

fn run(query: &Query, source: &Vec<Vec<ValueType>>) -> Vec<Vec<ValueType>> {
    let mut result = Vec::new();
    for record in source.iter() {
        if eval(record, &query.filter) == ValueType::Bool(true) {
            result.push(query.select.iter().map(|&col| record[col].clone()).collect());
        }
    }
    result
}

fn run_aggregate(select: &Vec<usize>, filter: &Condition, aggregation: &Vec<(Aggregator, usize)>, source: &Vec<Vec<ValueType>>) -> Vec<Vec<ValueType>> {
    let mut groups: HashMap<Vec<ValueType>, Vec<ValueType>> = HashMap::new();

    for record in source.iter() {
        if eval(record, filter) == ValueType::Bool(true) {
            let group: Vec<ValueType> = select.iter().map(|&col| record[col].clone()).collect();
            let accumulator = groups.entry(group).or_insert(aggregation.iter().map(|x| x.0.zero()).collect());
            for (i, &(ref agg_func, col)) in aggregation.iter().enumerate() {
                accumulator[i] = agg_func.reduce(&accumulator[i], &record[col]);
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

fn eval(record: &Vec<ValueType>, condition: &Condition) -> ValueType {
    use self::Condition::*;
    use self::ValueType::*;
    match condition {
        &True => Bool(true),
        &False => Bool(false),
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

    use self::Condition::*;
    use self::FuncType::*;
    use ValueType::*;
    let query1 = Query {
        select: vec![1usize],
        filter: Func(And,
                     Box::new(Func(LT, Box::new(Column(2usize)), Box::new(Const(Integer(1000))))),
                     Box::new(Func(GT, Box::new(Column(0usize)), Box::new(Const(Timestamp(1000)))))),
        groupBy: vec![],
        aggregate: vec![],
    };
    let query2 = Query {
        select: vec![0usize, 2usize],
        filter: Func(Equals, Box::new(Column(1usize)), Box::new(Const(String(Rc::new("/".to_string()))))),
        groupBy: vec![],
        aggregate: vec![],
    };
    let count_query = Query {
        select: vec![1usize],
        filter: True,
        groupBy: vec![1usize],
        aggregate: vec![(Aggregator::Count, 0)],
    };
    let sum_query = Query {
        select: vec![1usize],
        filter: True,
        groupBy: vec![1usize],
        aggregate: vec![(Aggregator::Sum, 2)],
    };

    let result1 = run(&query1, &dataset);
    let result2 = run(&query2, &dataset);
    let count_result = run_aggregate(&count_query.groupBy, &count_query.filter, &count_query.aggregate, &dataset);
    let sum_result = run_aggregate(&sum_query.groupBy, &sum_query.filter, &sum_query.aggregate, &dataset);

    println!("Result 1: {:?}", result1);
    println!("Result 2: {:?}", result2);
    println!("Count Result: {:?}", count_result);
    println!("Sum Result: {:?}", sum_result);
}

fn record(timestamp: u64, url: &str, loadtime: i64) -> Vec<ValueType> {
    vec![ValueType::Timestamp(timestamp), ValueType::String(Rc::new(url.to_string())), ValueType::Integer(loadtime)]
}
