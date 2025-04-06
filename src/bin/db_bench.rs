use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use locustdb::logging_client::BufferFullPolicy;
use locustdb::LocustDB;
use locustdb_serialization::api::any_val_syntax::vf64;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use structopt::StructOpt;
use tempfile::tempdir;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "LocustDB Storage Benchmark",
    about = "Benchmark LocustDB storage performance.",
    author = "Clemens Winter <clemenswinter1@gmail.com>"
)]
struct Opts {
    /// Amount of data generated is exponential in this number
    #[structopt(long, name = "N", default_value = "10")]
    load_factor: u64,
    /// Only generate large tables
    #[structopt(long)]
    large_only: bool,
    /// Database path
    #[structopt(long, name = "PATH")]
    db_path: Option<PathBuf>,
    /// Don't generate data
    #[structopt(long)]
    no_ingest: bool,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let opts = Opts::from_args();
    let start_time = std::time::Instant::now();

    let load_factor = opts.load_factor;
    let mut rng = rand::rngs::SmallRng::seed_from_u64(0);

    let db_path: PathBuf = opts
        .db_path
        .clone()
        .unwrap_or(tempdir().unwrap().path().into());
    log::info!("Creating LocustDB at {:?}", db_path);
    let db = create_locustdb(db_path.clone());

    let small_tables = small_table_names(load_factor);
    let total_events = if opts.no_ingest {
        0
    } else {
        ingest(&opts, &db, &small_tables)
    };

    let perf_counter = db.perf_counter();

    log::info!("Ingestion done");

    // count number of files in db_path and all subdirectories
    let file_count = walkdir::WalkDir::new(&db_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();
    // calculate total size of all files in db_path and all subdirectories
    let size_on_disk = walkdir::WalkDir::new(db_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.metadata().unwrap().len())
        .sum::<u64>();

    println!();
    if !opts.large_only {
        query(
            &db,
            "Querying 100 related columns in small table",
            &format!(
                "SELECT {} FROM {}",
                (0..100)
                    .map(|c| format!("col_{c}"))
                    .collect::<Vec<String>>()
                    .join(", "),
                small_tables[0]
            ),
        )
        .await;
        query(
            &db,
            "Querying full small table",
            &format!("SELECT * FROM {}", small_tables[1]),
        )
        .await;
        query(
            &db,
            "Querying 100 random columns in small table",
            &format!(
                "SELECT {} FROM {}",
                (0..100)
                    .map(|_| format!("col_{}", rng.gen_range(0u64, 1 << load_factor)))
                    .collect::<Vec<String>>()
                    .join(", "),
                small_tables[2]
            ),
        )
        .await;
    }
    query(&db, "Querying 10 related columns in large table", "SELECT col_000000, col_000001, col_000002, col_000003, col_000004, col_000005, col_000006, col_000007, col_000008, col_000009 FROM event_log")
        .await;
    let n = 2f64.powf(1.5 * load_factor as f64 - 1.0).round() as u64;
    query(
        &db,
        "Querying 10 unrelated columns in large table",
        &format!(
            "SELECT {} FROM event_log",
            (0..10)
                .map(|i| format!("col_{:06}", n * i / 10))
                .collect::<Vec<String>>()
                .join(", ")
        ),
    )
    .await;

    println!();
    println!("elapsed: {:?}", start_time.elapsed());
    println!(
        "total uncompressed data: {}",
        locustdb::unit_fmt::bite(2 * 8 * (1 << (3 * load_factor)))
    );
    println!(
        "total size on disk: {} (SmallRng output is compressible)",
        locustdb::unit_fmt::bite(size_on_disk as usize)
    );
    println!("total files: {}", file_count);
    println!("total events: {}", total_events);
    println!("disk writes");
    println!(
        "  total:      {}",
        locustdb::unit_fmt::bite(perf_counter.disk_write_bytes() as usize)
    );
    println!(
        "  wal:        {}",
        locustdb::unit_fmt::bite(perf_counter.disk_write_wal_bytes() as usize)
    );
    println!(
        "  partition:  {}",
        locustdb::unit_fmt::bite(perf_counter.disk_write_new_partition_bytes() as usize)
    );
    println!(
        "  compaction: {}",
        locustdb::unit_fmt::bite(perf_counter.disk_write_compaction_bytes() as usize)
    );
    println!(
        "  meta store: {}",
        locustdb::unit_fmt::bite(perf_counter.disk_write_meta_store_bytes() as usize)
    );
    println!("files created");
    println!("  total:     {}", perf_counter.files_created());
    println!("  wal:       {}", perf_counter.files_created_wal());
    println!(
        "  partition: {}",
        perf_counter.files_created_new_partition()
    );
    println!(
        "  compaction: {}",
        perf_counter.files_created_compaction()
    );
    println!("  meta:      {}", perf_counter.files_created_meta_store());
    println!("network");
    println!(
        "  ingestion requests: {}",
        perf_counter.ingestion_requests()
    );
    println!(
        "  ingestion bytes:    {}",
        locustdb::unit_fmt::bite(perf_counter.network_read_ingestion_bytes() as usize)
    );
    println!("query");
    println!("  files opened: {}", perf_counter.files_opened_partition(),);
    println!(
        "  disk read:    {}",
        locustdb::unit_fmt::bite(perf_counter.disk_read_partition_bytes() as usize)
    );
}

fn ingest(opts: &Opts, db: &LocustDB, small_tables: &[String]) -> u64 {
    let load_factor = opts.load_factor;
    let addr = "http://localhost:8888";
    let mut log = locustdb::logging_client::LoggingClient::new(
        Duration::from_secs(1),
        addr,
        64 * (1 << 20),
        BufferFullPolicy::Block,
        None,
    );
    let mut rng = rand::rngs::SmallRng::seed_from_u64(0);
    if !opts.large_only {
        log::info!("Starting small table logging");
        for _row in 0..1 << load_factor {
            for table in small_tables {
                log.log(
                    table,
                    (0..1 << load_factor).map(|c| (format!("col_{c}"), vf64(rng.gen::<f64>()))),
                );
            }
        }
    }

    let large_tables = [
        "event_log",
        "customer_feedback_items_raw_data_unstructured_json",
        "advertiser_campaigns",
        "order_items",
    ];
    // large tables have n = 2^(1.5N - 1) rows and 2^(1.5N - 1) columns each
    log::info!("Starting large table logging");
    let n = 2f64.powf(1.5 * load_factor as f64 - 1.0).round() as u64;
    for _row in 0..n {
        for table in &large_tables {
            log.log(
                table,
                (0..n).map(|c| (format!("col_{c:06}"), vf64(rng.gen::<f64>()))),
            );
        }
    }
    let total_events = log.total_events;
    drop(log);
    db.force_flush();
    total_events
}

async fn query(db: &LocustDB, description: &str, query: &str) {
    let evicted_bytes = db.evict_cache();
    log::info!("Evicted {}", locustdb::unit_fmt::bite(evicted_bytes));
    println!("{}", description);
    let response = db
        .run_query(query, false, true, vec![])
        .await
        .unwrap()
        .unwrap();
    println!(
        "Returned {} columns with {} rows in {:?} ({} files opened, {})",
        response.columns.len(),
        response.columns.first().map(|c| c.1.len()).unwrap_or(0),
        Duration::from_nanos(response.stats.runtime_ns),
        response.stats.files_opened,
        locustdb::unit_fmt::bite(response.stats.disk_read_bytes as usize),
    );
}

fn create_locustdb(db_path: PathBuf) -> Arc<locustdb::LocustDB> {
    let options = locustdb::Options {
        db_path: Some(db_path),
        ..locustdb::Options::default()
    };
    let db = Arc::new(locustdb::LocustDB::new(&options));
    let _locustdb = db.clone();
    locustdb::server::run(_locustdb, false, vec![], "localhost:8888".to_string()).unwrap();
    db
}

fn small_table_names(load_factor: u64) -> Vec<String> {
    let mut rng = SmallRng::seed_from_u64(0);
    let words = random_word::all(random_word::Lang::En);
    // 2^N small tables with 2^N rows and 2^N columns each
    (0..1 << load_factor)
        .map(|i| format!("{}{i}", words[rng.gen_range(0, words.len())],))
        .collect()
}

// Stats at fbb7f27:
// $ RUST_BACKTRACE=1 RUST_LOG=info cargo run --release --bin db_bench -- --load-factor=9
// elapsed: 81.138535253s
// total uncompressed data: 1.00GiB
// total size on disk: 1.11GiB
// total files: 1540
// disk writes
//   total:      4.76GiB
//   wal:        3.36GiB
//   partition:  1.05GiB
//   compaction: 0.000B
//   meta store: 364MiB
// files created
//   total:     3112
//   wal:       1536
//   partition: 1525
//   meta:      51

// Stats at 0453932:
// $ RUST_BACKTRACE=1 RUST_LOG=info cargo run --release --bin db_bench -- --load-factor=8
// elapsed: 12.212316902s
// total uncompressed data: 256MiB
// total size on disk: 280MiB
// total files: 537
// disk writes
//   total:      1.10GiB
//   wal:        867MiB
//   partition:  247MiB
//   compaction: 0.000B
//   meta store: 9.32MiB
// files created
//   total:     965
//   wal:       516
//   partition: 440
//   meta:      9
// network
//   ingestion requests: 516
//   ingestion bytes:    739MiB

// Stats at e33766b
// $ RUST_BACKTRACE=1 RUST_LOG=info cargo run --bin db_bench -- --load-factor=8
// elapsed: 48.648535024s
// total uncompressed data: 256MiB
// total size on disk: 249MiB (SmallRng output is compressible)
// total files: 983
// total events: 73728
// disk writes
//   total:      1.14GiB
//   wal:        867MiB
//   partition:  277MiB
//   compaction: 0.000B
//   meta store: 28.0MiB
// files created
//   total:     2025
//   wal:       1028
//   partition: 986
//   meta:      11
// network
//   ingestion requests: 4
//   ingestion bytes:    266MiB

// Stats at 8f5a93b
// $ RUST_BACKTRACE=1 RUST_LOG=info cargo run --bin db_bench -- --load-factor=8
// elapsed: 24.591489803s
// total uncompressed data: 256MiB
// total size on disk: 278MiB (SmallRng output is compressible)
// total files: 795
// total events: 73728
// disk writes
//   total:      566MiB
//   wal:        280MiB
//   partition:  275MiB
//   compaction: 0.000B
//   meta store: 11.9MiB
// files created
//   total:     813
//   wal:       14
//   partition: 794
//   meta:      5
// network
//   ingestion requests: 14
//   ingestion bytes:    280MiB

// $ RUST_BACKTRACE=1 cargo run --bin db_bench -- --load-factor=8
// Querying 100 related columns in small table
// Returned 100 columns with 256 rows in 252.602ms (100 files opened, 24.0MiB)
// Querying full small table
// Returned 257 columns with 256 rows in 684.762302ms (257 files opened, 61.8MiB)
// Querying 100 random columns in small table
// Returned 100 columns with 256 rows in 195.1599ms (77 files opened, 18.5MiB)
// Querying 10 related columns in large table
// Returned 10 columns with 2048 rows in 610.756901ms (10 files opened, 62.1MiB)
// Querying 10 random columns in large table
// Returned 10 columns with 2048 rows in 605.867601ms (10 files opened, 68.4MiB)
//
// elapsed: 27.505325227s
// total uncompressed data: 256MiB
// total size on disk: 278MiB (SmallRng output is compressible)
// total files: 795
// total events: 73728
// disk writes
//   total:      566MiB
//   wal:        280MiB
//   partition:  275MiB
//   compaction: 0.000B
//   meta store: 11.9MiB
// files created
//   total:     813
//   wal:       14
//   partition: 794
//   meta:      5
// network
//   ingestion requests: 14
//   ingestion bytes:    280MiB
// query
//   files opened: 454
//   disk read:    235MiB
// RELEASE
// Querying 100 related columns in small table
// Returned 100 columns with 256 rows in 23.5521ms (100 files opened, 40.1MiB)
// Querying full small table
// Returned 257 columns with 256 rows in 61.5311ms (257 files opened, 103MiB)
// Querying 100 random columns in small table
// Returned 100 columns with 256 rows in 21.6757ms (85 files opened, 34.1MiB)
// Querying 10 related columns in large table
// Returned 10 columns with 2048 rows in 39.8148ms (10 files opened, 69.0MiB)
// Querying 10 random columns in large table
// Returned 10 columns with 2048 rows in 35.0397ms (10 files opened, 72.9MiB)

// $ RUST_BACKTRACE=1 cargo run --bin db_bench -- --load-factor=8
// Querying 100 related columns in small table
// Returned 100 columns with 256 rows in 8.0145ms (1 files opened, 250KiB)
// Querying full small table
// Returned 257 columns with 256 rows in 12.959ms (1 files opened, 250KiB)
// Querying 100 random columns in small table
// Returned 100 columns with 256 rows in 6.8562ms (1 files opened, 250KiB)
// Querying 10 related columns in large table
// Returned 10 columns with 2048 rows in 154.896201ms (3 files opened, 17.2MiB)
// Querying 10 random columns in large table
// Returned 10 columns with 2048 rows in 165.483502ms (2 files opened, 16.1MiB)

// elapsed: 32.362289132s
// total uncompressed data: 256MiB
// total size on disk: 277MiB (SmallRng output is compressible)
// total files: 791
// total events: 73728
// disk writes
//   total:      565MiB
//   wal:        282MiB
//   partition:  274MiB
//   compaction: 0.000B
//   meta store: 8.76MiB
// files created
//   total:     809
//   wal:       15
//   partition: 790
//   meta:      4
// network
//   ingestion requests: 15
//   ingestion bytes:    282MiB
// query
//   files opened: 8
//   disk read:    34.1MiB

// $ RUST_BACKTRACE=1 RUST_LOG=info cargo run --bin db_bench -- --load-factor=9 --large-only
//
// Querying 10 related columns in large table
// Returned 10 columns with 5793 rows in 771.009211ms (13 files opened, 107MiB)
// Querying 10 unrelated columns in large table
// Returned 10 columns with 5793 rows in 1.146029917s (33 files opened, 260MiB)
//
// elapsed: 104.06446965s
// total uncompressed data: 2.00GiB
// total size on disk: 1.03GiB (SmallRng output is compressible)
// total files: 170
// total events: 23172
// disk writes
//   total:      2.08GiB
//   wal:        1.02GiB
//   partition:  1.03GiB
//   compaction: 0.000B
//   meta store: 36.3MiB
// files created
//   total:     201
//   wal:       19
//   partition: 169
//   meta:      13
// network
//   ingestion requests: 19
//   ingestion bytes:    1.02GiB
// query
//   files opened: 46
//   disk read:    367MiB

// $ RUST_BACKTRACE=1 RUST_LOG=info cargo run --bin db_bench -- --load-factor=9 --large-only
//
// Querying 10 related columns in large table
// Returned 10 columns with 5793 rows in 96.807795ms (3 files opened, 24.4MiB)
// Querying 10 unrelated columns in large table
// Returned 10 columns with 5793 rows in 887.621246ms (22 files opened, 172MiB)
//
// elapsed: 139.104648554s
// total uncompressed data: 2.00GiB
// total size on disk: 1.01GiB (SmallRng output is compressible)
// total files: 138
// total events: 23172
// disk writes
//   total:      3.04GiB
//   wal:        1.02GiB
//   partition:  1.99GiB
//   compaction: 0.000B
//   meta store: 31.2MiB
// files created
//   total:     339
//   wal:       25
//   partition: 293
//   meta:      21
// network
//   ingestion requests: 25
//   ingestion bytes:    1.02GiB
// query
//   files opened: 25
//   disk read:    196MiB

// $ RUST_BACKTRACE=1 RUST_LOG=info cargo run --bin db_bench --release -- --load-factor=9 --large-only
//
// Querying 10 related columns in large table
// Returned 10 columns with 5793 rows in 9.5808ms (3 files opened, 24.3MiB)
// Querying 10 unrelated columns in large table
// Returned 10 columns with 5793 rows in 63.5807ms (22 files opened, 177MiB)
//
// elapsed: 38.21972169s
// total uncompressed data: 2.00GiB
// total size on disk: 1.01GiB (SmallRng output is compressible)
// total files: 138
// total events: 23172
// disk writes
//   total:      3.01GiB
//   wal:        1.02GiB
//   partition:  1.96GiB
//   compaction: 0.000B
//   meta store: 32.3MiB
// files created
//   total:     336
//   wal:       21
//   partition: 293
//   meta:      22
// network
//   ingestion requests: 21
//   ingestion bytes:    1.02GiB
// query
//   files opened: 25



// Testing azure (requires azure credentials):
//
// az login
// RUST_BACKTRACE=1 RUST_LOG=info cargo run --bin db_bench --release -- --load-factor=6 --large-only --db-path=az://locustdbstoragetesting/dev/240505-lf6