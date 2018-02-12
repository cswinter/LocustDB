use std::iter::Iterator;
use std::rc::Rc;
use std::collections::HashMap;
use std::collections::HashSet;
use seahash::SeaHasher;
use std::hash::BuildHasherDefault;
// use time::precise_time_ns;
use std::ops::Add;
use std::cmp;

use engine::vector_operator::VecOperator;
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
    subqueries: Vec<CompiledSingleBatchQuery<'a>>,
    output_colnames: Vec<Rc<String>>,
    aggregate: Vec<Aggregator>,
    compiled_order_by: Option<Expr>,
    limit: LimitClause,
}

type QueryOperator<'a> = Box<VecOperator<Output=TypedVec<'a>> + 'a>;

struct CompiledSingleBatchQuery<'a> {
    select: Vec<QueryOperator<'a>>,
    filter: QueryOperator<'a>,
    // .aggregate: Vec<(Aggregator, Expr)>,
    // coliter: Vec<ColIter<'a>>,
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

#[derive(Debug)]
struct AggregateSubqueryResult<'a> {
    groups: HashMap<Vec<Val<'a>>, Vec<Val<'a>>, BuildSeaHasher>,
    stats: QueryStats,
}

#[derive(Debug)]
pub struct QueryStats {
    pub runtime_ns: u64,
    pub rows_scanned: u64,
}

type BuildSeaHasher = BuildHasherDefault<SeaHasher>;

impl QueryStats {
    fn new(runtime_ns: u64, rows_scanned: u64) -> QueryStats {
        QueryStats {
            runtime_ns: runtime_ns,
            rows_scanned: rows_scanned,
        }
    }

    fn combine(&self, other: &QueryStats) -> QueryStats {
        QueryStats::new(self.runtime_ns + other.runtime_ns,
                        self.rows_scanned + other.rows_scanned)
    }
}

impl Add for QueryStats {
    type Output = QueryStats;

    fn add(self, other: QueryStats) -> QueryStats {
        QueryStats {
            runtime_ns: self.runtime_ns + other.runtime_ns,
            rows_scanned: self.rows_scanned + other.rows_scanned,
        }
    }
}


impl<'a> CompiledQuery<'a> {
    pub fn run(&mut self) -> QueryResult {
        let colnames = self.output_colnames.clone();
        let max_limit = self.limit.offset + self.limit.limit;
        let limit = self.limit.limit;
        let offset = self.limit.offset;
        let (result_cols, stats) = if self.aggregate.len() == 0 {
            let combined_results = SelectSubqueryResult {
                cols: Vec::new(),
                stats: QueryStats::new(0, 0),
            };
            let mut columns = Vec::new();
            for single_batch_query in &mut self.subqueries {
                columns.push(single_batch_query.run_select_query());
                if self.compiled_order_by.is_none() && (max_limit as usize) < combined_results.cols.len() {
                    break;
                }
            }
            (columns, combined_results.stats)
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

        QueryResult {
            colnames: colnames,
            rows: result_rows,
            stats: stats,
        }
    }
}

impl<'a> CompiledSingleBatchQuery<'a> {
    fn run_select_query(&mut self) -> Vec<TypedVec> {
        let mut f = TypedVec::Empty;
        self.filter.execute(4000, &None, &mut f);
        let (count, filter) = match f {
            TypedVec::Boolean(b) => (4000, Some(b)),//(b.iter().filter(|x| *x).count(), Some(b)),
            _ => (4000, None),
        };
        // TODO(clemens): chunk size, stats
        // let start_time_ns = precise_time_ns();
        self.select.iter_mut().map(|op| {
            let mut result = TypedVec::Empty;
            op.execute(4000, &filter, &mut result);
            result
        }).collect()
    }

    fn run_aggregation_query(&mut self) -> AggregateSubqueryResult {
        /*let mut groups = HashMap::<Vec<Val>, Vec<Val>, BuildSeaHasher>::default();
        let mut record = Vec::with_capacity(self.coliter.len());
        let start_time_ns = precise_time_ns();
        let mut rows_touched = 0;
        'outer: loop {
            record.clear();
            for i in 0..self.coliter.len() {
                match self.coliter[i].next() {
                    Some(item) => record.push(item),
                    None => break 'outer,
                }
            }
            if self.filter.eval(&record) == Val::Bool(true) {
                let group: Vec<Val> =
                    self.select.iter().map(|expr| expr.eval(&record)).collect();
                let accumulator = groups.entry(group)
                    .or_insert(self.aggregate.iter().map(|x| x.0.zero()).collect());
                for (i, &(ref agg_func, ref expr)) in self.aggregate.iter().enumerate() {
                    accumulator[i] = agg_func.reduce(&accumulator[i], &expr.eval(&record));
                }
            }
            if self.coliter.len() == 0 {
                break;
            }
            rows_touched += 1;
        }
        AggregateSubqueryResult {
            groups: groups,
            stats: QueryStats::new(precise_time_ns() - start_time_ns, rows_touched),
        }*/
        panic!(" not implemented")
    }
}

impl Query {
    pub fn compile<'a>(&mut self, source: &'a Vec<Batch>) -> CompiledQuery<'a> {
        if self.is_select_star() {
            self.select = find_all_cols(source).into_iter().map(Expr::ColName).collect();
        }

        let subqueries = source.iter().map(|batch| self.compile_for_batch(batch)).collect();
        let limit = self.limit.clone();

        // Compile the order_by
        let output_colnames = self.result_column_names();
        let mut output_colmap = HashMap::new();
        for (i, output_colname) in output_colnames.iter().enumerate() {
            output_colmap.insert(output_colname.to_string(), i);
        }

        // Insert a placeholder sorter if ordering isn't specified
        let compiled_order_by = match self.order_by {
            Some(ref order_by) => Some(order_by.compile(&output_colmap)),
            None => None,
        };

        CompiledQuery {
            subqueries: subqueries,
            output_colnames: output_colnames,
            aggregate: self.aggregate.iter().map(|&(aggregate, _)| aggregate).collect(),
            compiled_order_by: compiled_order_by,
            limit: limit,
        }
    }

    fn compile_for_batch<'a>(&self, source: &'a Batch) -> CompiledSingleBatchQuery<'a> {
        let referenced_cols = self.find_referenced_cols();
        let efficient_source: Vec<&Column> = source.cols
            .iter()
            .filter(|col| referenced_cols.contains(&col.get_name().to_string()))
            .collect();
        // let coliter = efficient_source.iter().map(|col| col.iter()).collect();
        let column_iter = create_coliter_map(&efficient_source);
        let compiled_selects = self.select.iter().map(|expr| {
            let (plan, t) = expr.create_query_plan(&column_iter);
            query_plan::prepare(plan, t)
        }).collect();

        let (filter_plan, filter_t) = self.filter.create_query_plan(&column_iter);
        // TODO(clemens): type check
        let compiled_filter = query_plan::prepare(filter_plan, filter_t);

        /*let compiled_aggregate = self.aggregate
            .iter()
            .map(|&(agg, ref expr)| (agg, expr.compile(&column_indices)))
            .collect();*/

        CompiledSingleBatchQuery {
            select: compiled_selects,
            filter: compiled_filter,
            // aggregate: compiled_aggregate,
            // coliter: coliter,
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
            cols.insert(column.get_name().to_string());
        }
    }

    cols.into_iter().map(Rc::new).collect()
}

fn create_coliter_map<'a>(source: &Vec<&'a Column>) -> HashMap<String, &'a Column> {
    let mut mem_store = HashMap::new();
    for col in source.iter() {
        mem_store.insert(col.get_name().to_string(), *col);
    }
    mem_store
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
