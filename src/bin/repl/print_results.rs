use fmt_table::fmt_table;
use ruba::*;

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

    println!("Scanned {} rows in {} ({}ns per rows)!\n",
             results.stats.rows_scanned,
             fmt_time,
             rt.checked_div(results.stats.rows_scanned as u64).unwrap_or(0));
    println!();
    println!("{}\n", format_results(&results.colnames, &results.rows));
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

