use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;
use std::collections::HashSet;

use value::ValueType;
use expression::*;
use aggregator::*;
use util::fmt_table;
use columns::Column;
use columns::ColIter;


#[derive(Debug)]
pub struct Query {
    pub select: Vec<Expr>,
    pub filter: Expr,
    pub aggregate: Vec<(Aggregator, Expr)>,
}


impl Query {
    pub fn run(&self, source: &Vec<Box<Column>>) -> (Vec<Rc<String>>, Vec<Vec<ValueType>>) {
        let referenced_cols = self.find_referenced_cols();
        let efficient_source: Vec<&Box<Column>> = source.iter().filter(|col| referenced_cols.contains(&col.get_name().to_string())).collect();
        let mut coliter = efficient_source.iter().map(|col| col.iter()).collect();

        let column_indices = create_colname_map(&efficient_source);
        let compiled_selects = self.select.iter().map(|expr| expr.compile(&column_indices)).collect();
        let compiled_filter = self.filter.compile(&column_indices);
        let compiled_aggregate = self.aggregate.iter().map(|&(agg, ref expr)| (agg, expr.compile(&column_indices))).collect();

        let result = if self.aggregate.len() == 0 {
            run_select_query(&compiled_selects, &compiled_filter, &mut coliter)
        } else {
            run_aggregation_query(&compiled_selects, &compiled_filter, &compiled_aggregate, &mut coliter)
        };

        (self.result_column_names(), result)
    }

    fn find_referenced_cols(&self) -> HashSet<Rc<String>> {
        let mut colnames = HashSet::new();
        for expr in self.select.iter() {
            expr.add_colnames(&mut colnames);
        }
        self.filter.add_colnames(&mut colnames);
        for &(_, ref expr) in self.aggregate.iter() {
            expr.add_colnames(&mut colnames);
        }
        colnames
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
}

fn create_colname_map(source: &Vec<&Box<Column>>) -> HashMap<String, usize> {
    let mut columns = HashMap::new();
    for (i, col) in source.iter().enumerate() {
        columns.insert(col.get_name().to_string(), i as usize);
    }
    columns
}

fn run_select_query(select: &Vec<Expr>, filter: &Expr, source: &mut Vec<ColIter>) -> Vec<Vec<ValueType>> {
    let mut result = Vec::new();
    let mut record = Vec::with_capacity(source.len());
    loop {
        record.clear();
        for i in 0..source.len() {
            match source[i].next() {
                Some(item) => record.push(item),
                None => return result,
            }
        }
        if filter.eval(&record) == ValueType::Bool(true) {
            result.push(select.iter().map(|expr| expr.eval(&record)).collect());
        }
    }
}

fn run_aggregation_query(select: &Vec<Expr>, filter: &Expr, aggregation: &Vec<(Aggregator, Expr)>, source: &mut Vec<ColIter>) -> Vec<Vec<ValueType>> {
    let mut groups: HashMap<Vec<ValueType>, Vec<ValueType>> = HashMap::new();
    let mut record = Vec::with_capacity(source.len());
    'outer: loop {
        record.clear();
        for i in 0..source.len() {
            match source[i].next() {
                Some(item) => record.push(item),
                None => break 'outer,
            }
        }
        if filter.eval(&record) == ValueType::Bool(true) {
            let group: Vec<ValueType> = select.iter().map(|expr| expr.eval(&record)).collect();
            let accumulator = groups.entry(group).or_insert(aggregation.iter().map(|x| x.0.zero()).collect());
            for (i, &(ref agg_func, ref expr)) in aggregation.iter().enumerate() {
                accumulator[i] = agg_func.reduce(&accumulator[i], &expr.eval(&record));
            }
        }
        if source.len() == 0 { break }
    }

    let mut result: Vec<Vec<ValueType>> = Vec::new();
    for (mut group, aggregate) in groups {
        group.extend(aggregate);
        result.push(group);
    }
    result
}

fn format_results(r: &(Vec<Rc<String>>, Vec<Vec<ValueType>>)) -> String {
    let &(ref colnames, ref results) = r;
    let strcolnames: Vec<&str> = colnames.iter().map(|ref s| s.clone() as &str).collect();
    let formattedrows: Vec<Vec<String>> = results.iter().map(
        |row| row.iter().map(
            |val| format!("{}", val)).collect()).collect();
    let strrows = formattedrows.iter().map(|row| row.iter().map(|val| val as &str).collect()).collect();

    fmt_table(&strcolnames, &strrows)
}

pub fn test(source: &Vec<Box<Column>>) {
    use self::Expr::*;
    use self::FuncType::*;
    use ValueType::*;

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
    let missing_col_query = Query {
        select: vec![],
        filter: Const(Bool(true)),
        aggregate: vec![(Aggregator::Sum, Expr::col("doesntexist"))],
    } ;

    let result1 = query1.run(source);
    let result2 = query2.run(source);
    let count_result = count_query.run(source);
    let sum_result = sum_query.run(source);
    let missing_col_result = missing_col_query.run(source);

    println!("{}\n", format_results(&result1));
    println!("{}\n", format_results(&result2));
    println!("{}\n", format_results(&count_result));
    println!("{}\n", format_results(&sum_result));
    println!("{}\n", format_results(&missing_col_query.run(source)));
}
