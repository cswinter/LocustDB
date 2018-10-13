use fmt_table::fmt_table;
use locustdb::*;
use locustdb::unit_fmt::*;

pub fn print_query_result(results: &QueryOutput) {
    let rt = results.stats.runtime_ns;

    println!();
    for (query_plan, count) in &results.query_plans {
        println!("Query plan in {} batches{}", count, query_plan)
    }
    println!("Scanned {} rows in {} ({:.2} rows/s)!",
             short_scale(results.stats.rows_scanned as f64),
             ns(rt as usize),
             billion(results.stats.rows_scanned as f64 / rt as f64));
    println!("\n{}", format_results(&results.colnames, &results.rows));
    println!();
}

fn format_results(colnames: &[String], rows: &[Vec<Value>]) -> String {
    let strcolnames: Vec<&str> = colnames.iter().map(|s| s as &str).collect();
    let formattedrows: Vec<Vec<String>> = rows.iter()
        .map(|row| {
            row.iter()
                .map(|val| format!("{}", val))
                .collect()
        })
        .collect();
    let strrows =
        formattedrows.iter().map(|row| row.iter().map(|val| val as &str).collect()).collect::<Vec<_>>();

    fmt_table(&strcolnames, &strrows)
}

