use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;

use value::ValueType;
use expression::*;
use aggregator::*;


#[derive(Debug)]
pub struct Query {
    pub select: Vec<usize>,
    pub filter: Expr,
    pub aggregate: Vec<(Aggregator, Expr)>,
}


impl Query {
    pub fn run(&self, source: &Vec<Vec<ValueType>>) -> Vec<Vec<ValueType>> {
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
