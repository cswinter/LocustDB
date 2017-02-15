use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;

use value::ValueType;
use expression::*;
use aggregator::*;


#[derive(Debug)]
pub struct Query {
    pub select: Vec<Expr>,
    pub filter: Expr,
    pub aggregate: Vec<(Aggregator, Expr)>,
}


impl Query {
    pub fn run(&self, source: &Vec<Vec<ValueType>>, columns: &HashMap<String, usize>) -> (Vec<Rc<String>>, Vec<Vec<ValueType>>) {
        let query = self.compile(columns);
        let result = if self.aggregate.len() == 0 {
            run_select_query(&query.select, &query.filter, source)
        } else {
            run_aggregation_query(&query.select, &query.filter, &query.aggregate, source)
        };

        (self.result_column_names(), result)
    }

    fn result_column_names(&self) -> Vec<Rc<String>> {
        let mut anon_columns = -1;
        let select_cols = self.select
            .iter()
            .map(|expr| match expr {
                &Expr::ColName(ref name) => name.clone(),
                _ => {
                    anon_columns += 1;
                    Rc::new(format!("col_{}", anon_columns))
                },
            });
        let mut anon_aggregates = -1;
        let aggregate_cols = self.aggregate
            .iter()
            .map(|&(agg, _)| {
                anon_aggregates += 1;
                match agg {
                    Aggregator::Count => Rc::new(format!("count_{}", anon_aggregates)),
                    Aggregator::Sum => Rc::new(format!("sum_{}", anon_aggregates)),
                }
            });

        select_cols.chain(aggregate_cols).collect()
    }

    fn compile(&self, column_names: &HashMap<String, usize>) -> Query {
        Query {
            select: self.select.iter().map(|expr| expr.compile(column_names)).collect(),
            filter: self.filter.compile(column_names),
            aggregate: self.aggregate.iter().map(|&(agg, ref expr)| (agg, expr.compile(column_names))).collect(),
        }
    }
}

fn run_select_query(select: &Vec<Expr>, filter: &Expr, source: &Vec<Vec<ValueType>>) -> Vec<Vec<ValueType>> {
    source.iter()
        .filter(|record| filter.eval(record) == ValueType::Bool(true))
        .map(|record| select.iter().map(|expr| expr.eval(record)).collect())
        .collect()
}

fn run_aggregation_query(select: &Vec<Expr>, filter: &Expr, aggregation: &Vec<(Aggregator, Expr)>, source: &Vec<Vec<ValueType>>) -> Vec<Vec<ValueType>> {
    let mut groups: HashMap<Vec<ValueType>, Vec<ValueType>> = HashMap::new();

    for record in source.iter() {
        if filter.eval(record) == ValueType::Bool(true) {
            let group: Vec<ValueType> = select.iter().map(|expr| expr.eval(record)).collect();
            let accumulator = groups.entry(group).or_insert(aggregation.iter().map(|x| x.0.zero()).collect());
            for (i, &(ref agg_func, ref expr)) in aggregation.iter().enumerate() {
                accumulator[i] = agg_func.reduce(&accumulator[i], &expr.eval(record));
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

    let mut columns: HashMap<String, usize> = HashMap::new();
    columns.insert("timestamp".to_string(), 0);
    columns.insert("url".to_string(), 1);
    columns.insert("loadtime".to_string(), 2);

    let query1 = Query {
        select: vec![Expr::col("url")],
        filter: Expr::func(And,
                           Expr::func(LT, Expr::col("loadtime"), Const(Integer(1000))),
                           Expr::func(GT, Expr::col("timestamp"), Const(Timestamp(1000)))),
        aggregate: vec![],
    };
    let query2 = Query {
        select: vec![Expr::col("timestamp"), Expr::col("loadtime")],
        filter: Expr::func(Equals, Expr::col("url"), Const(Str(Rc::new("/".to_string())))),
        aggregate: vec![],
    };
    let count_query = Query {
        select: vec![Expr::col("url")],
        filter: Const(Bool(true)),
        aggregate: vec![(Aggregator::Count, Const(Integer(0)))],
    };
    let sum_query = Query {
        select: vec![Expr::col("url")],
        filter: Const(Bool(true)),
        aggregate: vec![(Aggregator::Sum, Expr::col("loadtime"))],
    };

    let result1 = query1.run(&dataset, &columns);
    let result2 = query2.run(&dataset, &columns);
    let count_result = count_query.run(&dataset, &columns);
    let sum_result = sum_query.run(&dataset, &columns);

    println!("Result 1: {:?}", result1);
    println!("Result 2: {:?}", result2);
    println!("Count Result: {:?}", count_result);
    println!("Sum Result: {:?}", sum_result);
}

fn record(timestamp: u64, url: &str, loadtime: i64) -> Vec<ValueType> {
    vec![ValueType::Timestamp(timestamp), ValueType::Str(Rc::new(url.to_string())), ValueType::Integer(loadtime)]
}
