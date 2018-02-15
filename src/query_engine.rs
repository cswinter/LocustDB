use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;
use std::collections::HashSet;
use seahash::SeaHasher;
use std::hash::BuildHasherDefault;
use time::precise_time_ns;
use std::ops::Add;
use std::cmp;

use engine::vector_operator::BoxedOperator;
use engine::query_plan;
use engine::typed_vec::TypedVec;
use value::Val;
use expression::*;
use aggregator::*;
use limit::*;
use util::fmt_table;
use mem_store::column::Column;
use mem_store::batch::Batch;
use mem_store::ingest::RawVal;


#[derive(Debug, Clone)]
pub struct Query {
    pub select: Vec<Expr>,
    pub table: String,
    pub filter: Expr,
    pub aggregate: Vec<(Aggregator, Expr)>,
    pub order_by: Option<Expr>,
    pub limit: LimitClause,
}

pub struct CompiledQuery<'a> {
    query: &'a Query,
    batches: Vec<HashMap<String, &'a Column>>,
    output_colnames: Vec<Rc<String>>,
    aggregate: Vec<Aggregator>,
    stats: QueryStats,
}

pub struct QueryResult {
    pub colnames: Vec<Rc<String>>,
    pub rows: Vec<Vec<RawVal>>,
    pub stats: QueryStats,
}

struct SelectSubqueryResult<'a> {
    cols: Vec<Vec<Val<'a>>>,
    stats: QueryStats,
}


const ENABLE_DETAILED_STATS: bool = false;

#[derive(Debug, Clone)]
pub struct QueryStats {
    pub runtime_ns: u64,
    pub rows_scanned: u64,
    start_time: u64,
    breakdown: HashMap<&'static str, u64>,
}

impl QueryStats {
    pub fn new() -> QueryStats {
        QueryStats {
            runtime_ns: 0,
            rows_scanned: 0,
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

type BuildSeaHasher = BuildHasherDefault<SeaHasher>;


impl<'a> CompiledQuery<'a> {
    pub fn run(&mut self) -> QueryResult {
        let start_time = precise_time_ns();
        let colnames = self.output_colnames.clone();
        let limit = self.query.limit.limit;
        let offset = self.query.limit.offset;
        let max_limit = offset + limit;
        let result_cols = if self.aggregate.len() == 0 {
            let mut columns = Vec::new();
            for batch in &self.batches {
                columns.push(self.query.run(batch, &mut self.stats));
                /*if self.compiled_order_by.is_none() && (max_limit as usize) < combined_results.cols.len() {
                    break;
                }*/
            }
            columns
        } else {
            panic!("aggregates not implemented");
            /*let mut combined_results = AggregateSubqueryResult {
                groups: HashMap::default(),
                stats: QueryStats::new(0, 0),
            };
            for single_batch_query in &mut self.subqueries {
                let batch_result = single_batch_query.run_aggregation_query();
                combined_results.stats = combined_results.stats.combine(&batch_result.stats);
                for (group, accumulator1) in batch_result.groups.into_iter() {
                    if let Some(mut accumulator2) = combined_results.groups.get_mut(&group) {
                        for (i, agg_func) in self.aggregate.iter().enumerate() {
                            accumulator2[i] = agg_func.combine(&accumulator1[i], &accumulator2[i]);
                        }
                    }
                    if !combined_results.groups.contains_key(&group) {
                        combined_results.groups.insert(group, accumulator1);
                    }
                }
            }

            let mut result: Vec<Vec<Val>> = Vec::new();
            for (mut group, aggregate) in combined_results.groups {
                group.extend(aggregate);
                result.push(group);
            }
            (result, combined_results.stats)*/
        };

        /*if let Some(ref order_by_expr) = self.compiled_order_by {
            result_rows.sort_by_key(|record| order_by_expr.eval(record));
        }*/

        self.stats.start();
        let mut result_rows = Vec::new();
        let mut o = offset as usize;
        for batch in result_cols.iter() {
            let n = batch[0].len();
            if n <= o {
                o = o - n;
                continue;
            } else {
                let count = cmp::min(n - o, limit as usize - result_rows.len());
                for i in o..(count + o) {
                    let mut record = Vec::with_capacity(colnames.len());
                    for col in batch.iter() {
                        record.push(col.get_raw(i));
                    }
                    result_rows.push(record);
                }
            }
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
    pub fn compile<'a>(&'a self, source: &'a Vec<Batch>) -> CompiledQuery<'a> {
        let mut stats = QueryStats::new();
        stats.start();
        let start_time = precise_time_ns();

        // TODO(clemens): Reenable
        /*if self.is_select_star() {
            self.select = find_all_cols(source).into_iter().map(Expr::ColName).collect();
        }*/

        let referenced_cols = self.find_referenced_cols();
        let batches = source.iter().map(|batch| self.prepare_batch(&referenced_cols, batch)).collect();
        let limit = self.limit.clone();

        // Compile the order_by
        let output_colnames = self.result_column_names();
        let mut output_colmap = HashMap::new();
        for (i, output_colname) in output_colnames.iter().enumerate() {
            output_colmap.insert(output_colname.to_string(), i);
        }

        // Insert a placeholder sorter if ordering isn't specified
        let mut compiled_order_by = self.order_by.as_ref()
            .map(|order_by| order_by.compile(&output_colmap));
        stats.record(&"prepare");
        stats.runtime_ns = precise_time_ns() - start_time;

        CompiledQuery {
            query: &self,
            batches: batches,
            output_colnames: output_colnames,
            aggregate: self.aggregate.iter().map(|&(aggregate, _)| aggregate).collect(),
            stats: stats,
        }
    }

    fn prepare_batch<'a>(&self, referenced_cols: &HashSet<Rc<String>>, source: &'a Batch) -> HashMap<String, &'a Column> {
        source.cols.iter()
            .filter(|col| referenced_cols.contains(&col.name().to_string()))
            .map(|col| (col.name().to_string(), col))
            .collect()
    }

    fn run<'a>(&self, columns: &HashMap<String, &'a Column>, stats: &mut QueryStats) -> Vec<TypedVec<'a>> {
        stats.start();
        let (filter_plan, _) = self.filter.create_query_plan(columns, None);
        //println!("filter: {:?}", filter_plan);
        // TODO(clemens): type check
        let mut compiled_filter = query_plan::prepare(filter_plan);
        stats.record(&"compile_filter");

        let filter = match compiled_filter.execute(stats) {
            TypedVec::Boolean(b) => Some(Rc::new(b)), //(b.iter().filter(|x| *x).count(), Some(b)),
            _ => None,
        };

        // TODO(clemens): stats
        // let start_time_ns = precise_time_ns();

        let mut result = Vec::new();
        for expr in &self.select {
            stats.start();
            let (plan, _) = expr.create_query_plan(columns, filter.clone());
            //println!("select: {:?}", plan);
            let mut compiled = query_plan::prepare(plan);
            stats.record(&"compile_select");
            result.push(compiled.execute(stats));
        }
        result
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

    println!("Scanned {} rows in {} ({}ns per row)!\n",
             results.stats.rows_scanned,
             fmt_time,
             rt.checked_div(results.stats.rows_scanned).unwrap_or(0));
    results.stats.print();
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
