use fmt_table::fmt_table;
use locustdb::*;

pub fn print_query_result(results: &QueryOutput) {
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

    println!("Scanned {} rows in {} ({:.2}B rows/s)!\n",
             results.stats.rows_scanned,
             fmt_time,
             results.stats.rows_scanned as f64 / rt as f64);
    println!();
    println!("{}\n", format_results(&results.colnames, &results.rows));
    if results.query_plans.len() > 0 {
        for (query_plan, count) in &results.query_plans {
            println!("Query plan in {} batches{}\n", count, query_plan)
        }
    }
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

