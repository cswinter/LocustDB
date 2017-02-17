use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;
use std::collections::HashSet;
use time::precise_time_ns;
use std::ops::Add;

use value::ValueType;
use expression::*;
use aggregator::*;
use limit::*;
use util::fmt_table;
use columns::Column;
use columns::ColIter;
use columns::Batch;


#[derive(Debug)]
pub struct Query<'a> {
    pub select: Vec<Expr<'a>>,
    pub filter: Expr<'a>,
    pub limit: Option<LimitClause>,
    pub aggregate: Vec<(Aggregator, Expr<'a>)>,
}

pub struct CompiledQuery<'a> {
    select: Vec<Expr<'a>>,
    filter: Expr<'a>,
    aggregate: Vec<(Aggregator, Expr<'a>)>,
    coliter: Vec<ColIter<'a>>,
}

pub struct QueryResult<'a> {
    pub colnames: Vec<Rc<String>>,
    pub rows: Vec<Vec<ValueType<'a>>>,
    pub stats: QueryStats,
}

pub struct QueryStats {
    pub runtime_ns: u64,
    pub rows_scanned: u64,
}

impl Add for QueryStats {
    type Output = QueryStats;

    fn add(self, other: QueryStats) -> QueryStats {
        QueryStats{
            runtime_ns: self.runtime_ns + other.runtime_ns,
            rows_scanned: self.rows_scanned + other.rows_scanned,
        }
    }
}


impl<'a> CompiledQuery<'a> {
    pub fn run(&mut self) -> QueryResult {
        let colnames = self.result_column_names(); 
        let start_time_ns = precise_time_ns();
        let (result_rows, rows_touched) = if self.aggregate.len() == 0 {
            run_select_query(&self.select, &self.filter, &mut self.coliter)
        } else {
            run_aggregation_query(&self.select, &self.filter, &self.aggregate, &mut self.coliter)
        };

        QueryResult {
            colnames: colnames,
            rows: result_rows,
            stats: QueryStats {
                runtime_ns: precise_time_ns() - start_time_ns,
                rows_scanned: rows_touched,
            },
        }
    }

/*
    pub fn run_batches(&'a self, batches: &'a Vec<Batch>) -> QueryResult<'a> {
        self.run(&batches[0])
        /*let mut combined_rows = Vec::new();
        let mut combined_stats = QueryStats { runtime_ns: 0, rows_scanned: 0 };
        for batch in batches {
            let QueryResult { rows, stats, .. } = self.run(batch);
            combined_rows.extend(rows); // TODO: This isn't the right way to combine results!!!
            combined_stats = combined_stats + stats;
        }
        QueryResult {
            colnames: self.result_column_names(),
            rows: combined_rows,
            stats: combined_stats,
        }*/
    }*/

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

impl<'a> Query<'a> {
    pub fn compile(&'a self, source: &'a Batch) -> CompiledQuery<'a> {
        let referenced_cols = self.find_referenced_cols();
        let efficient_source: Vec<&Box<Column>> = source.cols.iter().filter(|col| referenced_cols.contains(&col.get_name().to_string())).collect();
        let mut coliter = efficient_source.iter().map(|col| col.iter()).collect();
        let column_indices = create_colname_map(&efficient_source);
        let compiled_selects = self.select.iter().map(|expr| expr.compile(&column_indices)).collect();
        let compiled_filter = self.filter.compile(&column_indices);
        let compiled_aggregate = self.aggregate.iter().map(|&(agg, ref expr)| (agg, expr.compile(&column_indices))).collect();
        CompiledQuery {
            select: compiled_selects,
            filter: compiled_filter,
            aggregate: compiled_aggregate,
            coliter: coliter,
        }
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
}

fn create_colname_map(source: &Vec<&Box<Column>>) -> HashMap<String, usize> {
    let mut columns = HashMap::new();
    for (i, col) in source.iter().enumerate() {
        columns.insert(col.get_name().to_string(), i as usize);
    }
    columns
}

fn run_select_query<'a>(select: &Vec<Expr<'a>>, filter: &'a Expr, source: &'a mut Vec<ColIter>) -> (Vec<Vec<ValueType<'a>>>, u64) {
    let mut result = Vec::new();
    let mut record = Vec::with_capacity(source.len());
    let mut rows_touched = 0;
    let mut result_count = 0;
    if source.len() == 0 { return (result, rows_touched) }
    loop {
        record.clear();
        for i in 0..source.len() {
            match source[i].next() {
                Some(item) => record.push(item),
                None => return (result, rows_touched),
            }
        }
        if filter.eval(&record) == ValueType::Bool(true) {
            result.push(select.iter().map(|expr| expr.eval(&record)).collect());
            result_count += 1;
        }
        rows_touched += 1
        //TODO(limit)
        //if self.limit != None {
        //    if result_count > self.limit.limit {
        //        break;
        //    }
        //}
    }
}


fn run_aggregation_query<'a>(select: &Vec<Expr<'a>>, filter: &'a Expr, aggregation: &Vec<(Aggregator, Expr<'a>)>, source: &'a mut Vec<ColIter>) -> (Vec<Vec<ValueType<'a>>>, u64) {
    let mut groups: HashMap<Vec<ValueType>, Vec<ValueType>> = HashMap::new();
    let mut record = Vec::with_capacity(source.len());
    let mut rows_touched = 0;
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
        rows_touched += 1;
    }

    let mut result: Vec<Vec<ValueType>> = Vec::new();
    for (mut group, aggregate) in groups {
        group.extend(aggregate);
        result.push(group);
    }
    (result, rows_touched)
}

pub fn print_query_result(results: &QueryResult) {
    let rt = results.stats.runtime_ns;
    let fmt_time = if rt < 10_000 {
        format!("{}ns", rt)
    } else if rt < 10_000_000 {
        format!("{}μs", rt / 1000)
    } else if rt < 10_000_000_000 {
        format!("{}ms", rt / 1_000_000)
    } else {
        format!("{}s", rt / 1_000_000_000)
    };

    println!("Scanned {} rows in {}!\n", results.stats.rows_scanned, fmt_time);
    println!("{}", format_results(&results.colnames, &results.rows));
}

fn format_results(colnames: &Vec<Rc<String>>, rows: &Vec<Vec<ValueType>>) -> String {
    let strcolnames: Vec<&str> = colnames.iter().map(|ref s| s.clone() as &str).collect();
    let formattedrows: Vec<Vec<String>> = rows.iter().map(
        |row| row.iter().map(
            |val| format!("{}", val)).collect()).collect();
    let strrows = formattedrows.iter().map(|row| row.iter().map(|val| val as &str).collect()).collect();

    fmt_table(&strcolnames, &strrows)
}

    // former test() function - just to show how LIMIT would work
    // DELETE ME once we fixe / figure out LIMIT

    //TODO(limit)
    //let limited_query = Query {
    //    select: vec![Expr::col("url")],
    //    filter: Expr::func(And,
    //                       Expr::func(LT, Expr::col("loadtime"), Const(Integer(1000))),
    //                       Expr::func(GT, Expr::col("timestamp"), Const(Timestamp(1000)))),
    //    aggregate: vec![],
    //    limit: LimitClause{ limit:3, offset:0 },
    //} ;

    //let limited_result = limited_query.run(source);

    //print_query_result(&limited_result);
