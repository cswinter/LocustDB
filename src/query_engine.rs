use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;
use std::collections::HashSet;
use time::precise_time_ns;
use std::cmp;

use engine::query_plan;
use engine::batch_merging::*;
use engine::aggregation_operator::*;
use engine::typed_vec::TypedVec;
use expression::*;
use aggregator::*;
use limit::*;
use util::fmt_table;
use mem_store::column::Column;
use mem_store::batch::Batch;
use mem_store::ingest::RawVal;
use engine::filter::Filter;


#[derive(Debug, Clone)]
pub struct Query {
    pub select: Vec<Expr>,
    pub table: String,
    pub filter: Expr,
    pub aggregate: Vec<(Aggregator, Expr)>,
    pub order_by: Option<String>,
    pub order_desc: bool,
    pub limit: LimitClause,
    pub order_by_index: Option<usize>,
}

pub struct CompiledQuery<'a> {
    query: Query,
    batches: Vec<HashMap<&'a str, &'a Column>>,
    output_colnames: Vec<Rc<String>>,
    aggregate: Vec<Aggregator>,
    stats: QueryStats,
}

pub struct QueryResult {
    pub colnames: Vec<Rc<String>>,
    pub rows: Vec<Vec<RawVal>>,
    pub stats: QueryStats,
}

const ENABLE_DETAILED_STATS: bool = false;

#[derive(Debug, Clone)]
pub struct QueryStats {
    pub runtime_ns: u64,
    pub ops: usize,
    start_time: u64,
    breakdown: HashMap<&'static str, u64>,
}

impl QueryStats {
    pub fn new() -> QueryStats {
        QueryStats {
            runtime_ns: 0,
            ops: 0,
            start_time: 0,
            breakdown: HashMap::new(),
        }
    }

    pub fn start(&mut self) {
        if ENABLE_DETAILED_STATS {
            self.start_time = precise_time_ns();
        }
    }

    pub fn record(&mut self, label: &'static str) {
        if ENABLE_DETAILED_STATS {
            let elapsed = precise_time_ns() - self.start_time;
            *self.breakdown.entry(label).or_insert(0) += elapsed;
        }
    }

    pub fn print(&self) {
        println!("Total runtime: {}ns", self.runtime_ns);
        let mut total = 0_u64;
        let mut sorted_breakdown = self.breakdown.iter().collect::<Vec<_>>();
        sorted_breakdown.sort_by_key(|&(l, _)| l);
        for (label, duration) in sorted_breakdown {
            println!("  {}: {}ns ({}%)", label, duration, duration * 100 / self.runtime_ns);
            total += *duration;
        }
        println!("  Unaccounted: {} ({}%)", self.runtime_ns - total, (self.runtime_ns - total) * 100 / self.runtime_ns)
    }
}


impl<'a> CompiledQuery<'a> {
    pub fn run(&mut self) -> QueryResult {
        let start_time = precise_time_ns();
        let colnames = self.output_colnames.clone();
        let limit = self.query.limit.limit as usize;
        let offset = self.query.limit.offset as usize;

        let mut batch_results = Vec::<BatchResult>::new();
        for batch in &self.batches {
            let mut batch_result = if self.aggregate.len() == 0 {
                self.query.run(batch, &mut self.stats)
            } else {
                self.query.run_aggregate(batch, &mut self.stats)
            };
            // Merge only with previous batch results of same level to get O(n log n) complexity
            loop {
                if !batch_results.is_empty() && batch_results.last().unwrap().level == batch_result.level {
                    // TODO(clemens): apply limit during combine when possible
                    batch_result = combine(batch_results.pop().unwrap(), batch_result);
                } else { break; }
            }
            batch_results.push(batch_result)
            /*if self.compiled_order_by.is_none() && (max_limit as usize) < combined_results.cols.len() {
                break;
            }*/
        }
        let mut full_result = None;
        for batch_result in batch_results.into_iter() {
            if let Some(partial) = full_result {
                full_result = Some(combine(partial, batch_result));
            } else {
                full_result = Some(batch_result);
            }
        }
        // TODO(clemens): empty table
        let full_result = full_result.unwrap();

        /*if let Some(ref order_by_expr) = self.compiled_order_by {
            result_rows.sort_by_key(|record| order_by_expr.eval(record));
        }*/

        self.stats.start();
        let mut result_rows = Vec::new();
        let count = cmp::min(limit, full_result.len() - offset);
        for i in offset..(count + offset) {
            let mut record = Vec::with_capacity(colnames.len());
            if let Some(ref g) = full_result.group_by {
                record.push(g.get_raw(i));
            }
            for col in full_result.select.iter() {
                record.push(col.get_raw(i));
            }
            result_rows.push(record);
        }
        self.stats.record(&"limit_collect");
        self.stats.runtime_ns += precise_time_ns() - start_time;

        QueryResult {
            colnames: colnames,
            rows: result_rows,
            stats: self.stats.clone(),
        }
    }
}


impl Query {
    pub fn compile<'a>(&self, source: &'a Vec<Batch>) -> CompiledQuery<'a> {
        let start_time = precise_time_ns();
        let mut stats = QueryStats::new();
        let mut query = self.clone();

        if query.is_select_star() {
            query.select = find_all_cols(source).into_iter().map(Expr::ColName).collect();
        }

        stats.start();
        let output_colnames = query.result_column_names();
        let mut order_by_index = None;
        if let Some(ref col) = query.order_by {
            for (i, name) in output_colnames.iter().enumerate() {
                if name.as_ref() == col {
                    order_by_index = Some(i);
                }
            }
        }
        query.order_by_index = order_by_index;
        stats.record(&"determine_output_colnames");

        stats.start();
        let batches = source.iter()
            .map(|batch| Query::prepare_batch(&query.find_referenced_cols(), batch))
            .collect();
        stats.record(&"prepare_batches");

        let aggregate = query.aggregate.iter().map(|&(aggregate, _)| aggregate).collect();

        stats.runtime_ns = precise_time_ns() - start_time;

        CompiledQuery {
            query: query,
            batches: batches,
            output_colnames: output_colnames,
            aggregate: aggregate,
            stats: stats,
        }
    }

    fn prepare_batch<'a>(referenced_cols: &HashSet<&str>, source: &'a Batch) -> HashMap<&'a str, &'a Column> {
        source.cols.iter()
            .filter(|col| referenced_cols.contains(&col.name()))
            .map(|col| (col.name(), col))
            .collect()
    }

    #[inline(never)] // produces more useful profiles
    fn run<'a>(&self, columns: &HashMap<&'a str, &'a Column>, stats: &mut QueryStats) -> BatchResult<'a> {
        stats.start();
        let (filter_plan, _) = self.filter.create_query_plan(columns, Filter::None);
        //println!("filter: {:?}", filter_plan);
        // TODO(clemens): type check
        let mut compiled_filter = query_plan::prepare(filter_plan);
        stats.record(&"compile_filter");

        let mut filter = match compiled_filter.execute(stats) {
            TypedVec::Boolean(b) => Filter::BitVec(Rc::new(b)),
            _ => Filter::None,
        };

        let mut result = Vec::new();
        if let Some(index) = self.order_by_index {
            // TODO(clemens): Reuse sort_column for result
            // TODO(clemens): Optimization: sort directly if only single column selected
            let (plan, _) = self.select[index].create_query_plan(columns, filter.clone());
            let mut compiled = query_plan::prepare(plan);
            let sort_column = compiled.execute(stats).order_preserving();
            let mut sort_indices = match filter {
                Filter::BitVec(vec) => vec.iter()
                    .enumerate()
                    .filter(|x| x.1)
                    .map(|x| x.0)
                    .collect(),
                Filter::None => (0..sort_column.len()).collect(),
                _ => panic!("surely this will never happen :)"),
            };
            if self.order_desc {
                sort_column.sort_indices_desc(&mut sort_indices);
            } else {
                sort_column.sort_indices_asc(&mut sort_indices);
            }
            sort_indices.truncate((self.limit.limit + self.limit.offset) as usize);
            filter = Filter::Indices(Rc::new(sort_indices));
        }
        for expr in &self.select {
            stats.start();
            let (plan, _) = expr.create_query_plan(columns, filter.clone());
            //println!("select: {:?}", plan);
            let mut compiled = query_plan::prepare(plan);
            stats.record(&"compile_select");
            result.push(compiled.execute(stats).decode());
        }

        BatchResult {
            group_by: None,
            sort_by: self.order_by_index,
            select: result,
            aggregators: Vec::with_capacity(0),
            level: 0,
        }
    }

    #[inline(never)] // produces more useful profiles
    fn run_aggregate<'a>(&self, columns: &HashMap<&'a str, &'a Column>, stats: &mut QueryStats) -> BatchResult<'a> {
        stats.start();
        let (filter_plan, _) = self.filter.create_query_plan(columns, Filter::None);
        // TODO(clemens): type check
        let mut compiled_filter = query_plan::prepare(filter_plan);
        stats.record(&"compile_filter");

        let filter = match compiled_filter.execute(stats) {
            TypedVec::Boolean(b) => Filter::BitVec(Rc::new(b)),
            _ => Filter::None,
        };

        stats.start();
        let (grouping_key_plan, _) = Expr::compile_grouping_key(&self.select, columns, filter.clone());
        let mut compiled_gk = query_plan::prepare(grouping_key_plan);
        stats.record(&"compile_grouping_key");
        let grouping_key = compiled_gk.execute(stats);
        let (grouping, max_index, groups) = grouping(grouping_key);
        let groups = groups.order_preserving();
        let mut grouping_sort_indices = (0..groups.len()).collect();
        groups.sort_indices_asc(&mut grouping_sort_indices);

        let mut result = Vec::new();
        for &(aggregator, ref expr) in &self.aggregate {
            stats.start();
            let (plan, _) = expr.create_query_plan(columns, filter.clone());
            let mut compiled = query_plan::prepare_aggregation(plan, &grouping, max_index, aggregator);
            stats.record(&"compile_aggregate");
            result.push(compiled.execute(stats).index_decode(&grouping_sort_indices));
        }

        BatchResult {
            group_by: Some(groups.index_decode(&grouping_sort_indices)),
            sort_by: None,
            select: result,
            aggregators: self.aggregate.iter().map(|x| x.0).collect(),
            level: 0,
        }
    }

    fn is_select_star(&self) -> bool {
        if self.select.len() == 1 {
            match self.select[0] {
                Expr::ColName(ref colname) if **colname == "*".to_string() => true,
                _ => false,
            }
        } else {
            false
        }
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
                }
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


    fn find_referenced_cols(&self) -> HashSet<&str> {
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

fn find_all_cols(source: &Vec<Batch>) -> Vec<Rc<String>> {
    let mut cols = HashSet::new();
    for batch in source {
        for column in &batch.cols {
            cols.insert(column.name().to_string());
        }
    }

    cols.into_iter().map(Rc::new).collect()
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

    results.stats.print();
    println!("Performed {} ops in {} ({}ns per op)!\n",
             results.stats.ops,
             fmt_time,
             rt.checked_div(results.stats.ops as u64).unwrap_or(0));
    println!();
    println!("{}\n", format_results(&results.colnames, &results.rows));
}

fn format_results(colnames: &Vec<Rc<String>>, rows: &Vec<Vec<RawVal>>) -> String {
    let strcolnames: Vec<&str> = colnames.iter().map(|ref s| s.clone() as &str).collect();
    let formattedrows: Vec<Vec<String>> = rows.iter()
        .map(|row| {
            row.iter()
                .map(|val| format!("{}", val))
                .collect()
        })
        .collect();
    let strrows =
        formattedrows.iter().map(|row| row.iter().map(|val| val as &str).collect()).collect();

    fmt_table(&strcolnames, &strrows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mem_store::csv_loader::ingest_file;
    use parser::parse_query;

    fn test_query(query: &str, expected_rows: Vec<Vec<RawVal>>) {
        let batches = ingest_file("test_data/tiny.csv", 20);
        let query = parse_query(query.as_bytes()).to_result().unwrap();
        let mut compiled_query = query.compile(&batches);
        let result = compiled_query.run();
        assert_eq!(result.rows, expected_rows);
    }

    #[test]
    fn test_select_string() {
        test_query(
            &"select first_name from default limit 2;",
            vec![vec!["Victor".into()],
                 vec!["Catherine".into()]
            ],
        )
    }

    #[test]
    fn test_select_string_integer() {
        test_query(
            &"select first_name, num from default limit 2;",
            vec![vec!["Victor".into(), 1.into()],
                 vec!["Catherine".into(), 1.into()]
            ],
        )
    }

    #[test]
    fn test_sort_string() {
        test_query(
            &"select first_name from default order by first_name limit 2;",
            vec![vec!["Adam".into()],
                 vec!["Adam".into()],
            ],
        )
    }

    /*
    #[test]
    fn test_sort_string_desc() {
        test_query(
            &"select first_name from default order by first_name desc limit 2;",
            vec![vec!["Willie".into()],
                 vec!["William".into()],
            ],
        )
    }*/

    #[test]
    fn group_by_integer_filter_integer_lt() {
        test_query(
            &"select num, count(1) from default where num < 8;",
            vec![vec![0.into(), 8.into()],
                 vec![1.into(), 49.into()],
                 vec![2.into(), 24.into()],
                 vec![3.into(), 11.into()],
                 vec![4.into(), 5.into()],
                 vec![5.into(), 2.into()]],
        )
    }

    #[test]
    fn group_by_string_filter_string_eq() {
        test_query(
            &"select first_name, count(1) from default where first_name = \"Adam\";",
            vec![vec!["Adam".into(), 2.into()]],
        )
    }

    #[test]
    fn test_and_or() {
        test_query(
            &"select first_name, last_name from default where ((first_name = \"Adam\") OR (first_name = \"Catherine\")) AND (last_name = \"Cox\");",
            vec![vec!["Adam".into(), "Cox".into()]],
        )
    }
}
