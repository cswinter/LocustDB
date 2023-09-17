use std::path::PathBuf;

use failure::Fail;
use futures::executor::block_on;
use structopt::StructOpt;
use time::OffsetDateTime;

use locustdb::unit_fmt::*;
use locustdb::LocustDB;

mod fmt_table;
mod print_results;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "LocustDB",
    about = "Massively parallel, high performance analytics database that will rapidly devour all of your data.",
    author = "Clemens Winter <clemenswinter1@gmail.com>"
)]
struct Opt {
    /// Path to data directory
    #[structopt(long, name = "PATH", parse(from_os_str))]
    db_path: Option<PathBuf>,

    /// Load .csv or .csv.gz files into the database
    #[structopt(long, name = "FILES", parse(from_os_str))]
    load: Vec<PathBuf>,

    /// Name for the table populated with --load
    #[structopt(long, name = "NAME", default_value = "default")]
    table: String,

    /// Limit for in-memory size of tables in GiB")
    #[structopt(long, name = "GB", default_value = "8")]
    mem_limit_tables: usize,

    /// Comma separated list specifying the types and (optionally) names of all columns in files specified by `--load` option.
    /// Valid types: `s`, `string`, `i`, `integer`, `ns` (nullable string), `ni` (nullable integer)
    /// Example schema without column names: `int,string,string,string,int`
    /// Example schema with column names: `name:s,age:i,country:s`
    #[structopt(long, name="SCHEMA", conflicts_with_all(&["trips", "reduced-trips"]))]
    schema: Option<String>,

    /// Keep data cached in memory lz4 encoded. Decreases memory usage and query speeds
    #[structopt(long)]
    mem_lz4: bool,

    /// Number of rows per partition when loading new data
    #[structopt(long, name = "ROWS", default_value = "65536")]
    partition_size: usize,

    /// How much data to load at a time in MiB when reading from disk
    #[structopt(long, name = "MB", default_value = "256")]
    readahead: usize,

    /// Improves performance on HDD, can hurt performance on SSD.
    #[structopt(long)]
    seq_disk_read: bool,

    /// Number of worker threads. [default: number of cores]
    #[structopt(long, name = "INTEGER")]
    threads: Option<usize>,

    /// Set ingestion schema for select set of columns from nyc taxi ride dataset.
    #[structopt(long, conflicts_with_all(&["trips", "schema"]))]
    reduced_trips: bool,

    /// Set ingestion schema for nyc taxi ride dataset.
    #[structopt(long, conflicts_with_all(&["reduced-trips", "schema"]))]
    trips: bool,

    // TODO: make this a subcommand
    #[structopt(long)]
    server: bool,
}

fn main() {
    env_logger::init();

    let Opt {
        db_path,
        load,
        table,
        mem_limit_tables,
        schema,
        mem_lz4,
        partition_size,
        readahead,
        seq_disk_read,
        threads,
        reduced_trips,
        trips,
        server,
    } = Opt::from_args();

    let options = locustdb::Options {
        threads: threads.unwrap_or_else(num_cpus::get),
        read_threads: if seq_disk_read { 1 } else { num_cpus::get() },
        db_path: db_path.clone(),
        mem_size_limit_tables: mem_limit_tables * 1024 * 1024 * 1024,
        mem_lz4,
        readahead: readahead * 1024 * 1024,
        seq_disk_read,
    };

    if db_path.is_some() && !cfg!(feature = "enable_rocksdb") {
        println!("WARNING: --db-path option passed, but RocksDB storage backend is not enabled in this build of LocustDB.");
    }
    if options.readahead > options.mem_size_limit_tables {
        println!("WARNING: `mem-limit-tables` should be at least as large as `readahead`");
    }

    let locustdb = locustdb::LocustDB::new(&options);

    let start_time = OffsetDateTime::unix_epoch().unix_timestamp_nanos();
    let mut loads = Vec::new();
    let file_count = load.len();
    for file in load {
        let base_opts = if reduced_trips {
            locustdb::nyc_taxi_data::ingest_reduced_file(&file, &table)
        } else if trips {
            locustdb::nyc_taxi_data::ingest_file(&file, &table)
        } else if let Some(schema) = &schema {
            locustdb::LoadOptions::new(&file, &table).with_schema(schema)
        } else {
            locustdb::LoadOptions::new(&file, &table)
        };
        let opts = base_opts.with_partition_size(partition_size);
        let load = locustdb.load_csv(opts);
        loads.push(load);
        if file_count < 4 {
            println!("Loading {} into table {}.", file.to_string_lossy(), table);
        }
    }
    if file_count >= 4 {
        println!("Loading {} files into table {}.", file_count, table);
    }
    for l in loads {
        block_on(l).expect("Ingestion crashed!");
    }
    if file_count > 0 {
        println!(
            "Loaded data in {:.3}.",
            ns((OffsetDateTime::unix_epoch().unix_timestamp_nanos() - start_time) as usize)
        );
    }

    table_stats(&locustdb);

    if server {
        actix_web::rt::System::new("locustdb")
            .block_on(locustdb::server::run(locustdb))
            .unwrap();
    } else {
        repl(&locustdb);
    }
}

fn table_stats(locustdb: &LocustDB) {
    let stats = block_on(locustdb.table_stats()).expect("!?!");
    for table in stats {
        let size = table.batches_bytes + table.buffer_bytes;
        println!(
            "\n# Table `{}` ({} rows, {}) #",
            &table.name,
            table.rows,
            bite(size)
        );
        for &(ref columname, heapsize) in &table.size_per_column {
            println!("{}: {:.2}", columname, bite(heapsize));
        }
    }
}

#[allow(clippy::cognitive_complexity)]
fn repl(locustdb: &LocustDB) {
    let mut rl = rustyline::Editor::<()>::new();
    rl.load_history(".locustdb_history").ok();
    while let Ok(mut s) = rl.readline("locustdb> ") {
        if let Some('\n') = s.chars().next_back() {
            s.pop();
        }
        if let Some('\r') = s.chars().next_back() {
            s.pop();
        }
        if s.trim() == "" {
            continue;
        }
        if s == "exit" {
            break;
        }
        if s == "help" || s == "?" {
            println!("Special commands:
                      help - Display this message
                      :load <TABLE> <SCHEMA> <FILES>... - Load FILES into TABLE according to SCHEMA.
                      :memtree(<N>) - Display breakdown of memory usage up to a depth of N (at most 4).
                      :explain <QUERY> - Run and display the query plan for QUERY.
                      :show(<N>) <QUERY> - Run QUERY and show all intermediary results in partition N.:w

                      :ast <QUERY> - Show the abstract syntax tree for QUERY.
                      ");
            continue;
        }
        rl.add_history_entry(&s);

        let mut explain = false;
        let mut show = vec![];
        let mut s: &str = &s;
        if s.starts_with(":memtree") {
            let depth = if s.starts_with(":memtree(") {
                let end = s.find(')').unwrap();
                s[9..end]
                    .parse::<usize>()
                    .expect("must pass integer to :memtree(x) command")
            } else {
                2
            };
            match block_on(locustdb.mem_tree(depth)) {
                Ok(trees) => {
                    for tree in trees {
                        println!("{}\n", &tree)
                    }
                }
                _ => println!("Error: Query execution was canceled!"),
            }
            continue;
        }
        if s.starts_with(":restore") {
            let start = OffsetDateTime::unix_epoch().unix_timestamp_nanos();
            match block_on(locustdb.bulk_load()) {
                Ok(trees) => {
                    println!(
                        "Restored DB from disk in {}",
                        ns((OffsetDateTime::unix_epoch().unix_timestamp_nanos() - start) as usize)
                    );
                    for tree in trees {
                        println!("{}\n", &tree)
                    }
                }
                _ => println!("Error: Query execution was canceled!"),
            }
            continue;
        }
        if s.starts_with(":load") {
            let args = &s[6..].split(' ').collect::<Vec<_>>();
            if args.len() < 3 {
                println!("Expected at least 3 arguments to :load");
                continue;
            }
            let tablename = args[0];
            let schema = args[1];
            for file in &args[2..] {
                let opts = locustdb::LoadOptions::new(file, tablename).with_schema(schema);
                let load = locustdb.load_csv(opts);
                println!("Loading {} into table {}.", file, tablename);
                block_on(load).expect("Ingestion crashed!");
            }
            continue;
        }

        if s.starts_with(":explain") {
            explain = true;
            s = &s[9..];
        }
        if s.starts_with(":show") {
            show = if s.starts_with(":show(") {
                let end = s.find(')').unwrap();
                let partition = s[6..end]
                    .parse::<usize>()
                    .expect("must pass integer to :show(x) command");
                s = &s[(end + 2)..];
                vec![partition]
            } else {
                s = &s[6..];
                vec![0]
            }
        }
        if s.starts_with(":recover") {
            locustdb.recover();
            continue;
        }
        if s.starts_with(":ast") {
            println!("{}", locustdb.ast(&s[5..]));
            continue;
        }

        let query = locustdb.run_query(s, explain, show);
        match block_on(query) {
            Ok(result) => match result {
                Ok(output) => print_results::print_query_result(&output),
                Err(fail) => print_error(&fail),
            },
            _ => println!("Error: Query execution was canceled!"),
        }
    }
    rl.save_history(".locustdb_history").ok();
}

fn print_error(fail: &locustdb::QueryError) {
    println!("{}", fail);
    while let Some(cause) = fail.cause() {
        println!("{}", cause);
        if let Some(bt) = cause.backtrace() {
            println!("{}", bt);
        }
    }
}
