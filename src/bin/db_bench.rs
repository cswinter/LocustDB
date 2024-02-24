use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use rand::{FromEntropy, Rng};
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
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let opts = Opts::from_args();
    let load_factor = opts.load_factor;

    let db_path = tempdir().unwrap();
    log::info!("Creating LocustDB at {:?}", db_path.path());
    let db = create_locustdb(db_path.path().into());

    let addr = "http://localhost:8080";
    let mut log = locustdb::logging_client::LoggingClient::new(Duration::from_secs(1), addr);

    let start_time = std::time::Instant::now();

    let mut rng = rand::rngs::SmallRng::from_entropy();

    log::info!("Starting small table logging");
    let small_tables = small_table_names(load_factor);
    for _row in 0..1 << load_factor {
        for table in &small_tables {
            log.log(
                table,
                (0..1 << load_factor).map(|c| (format!("col_{c}"), rng.gen::<f64>())),
            );
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
                (0..n).map(|c| (format!("col_{c}"), rng.gen::<f64>())),
            );
        }
    }

    let total_events = log.total_events;
    drop(log);
    db.force_flush();

    let perf_counter = db.perf_counter();

    log::info!("Done");

    // count number of files in db_path and all subdirectories
    let file_count = walkdir::WalkDir::new(db_path.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .count();
    // calculate total size of all files in db_path and all subdirectories
    let size_on_disk = walkdir::WalkDir::new(db_path.path())
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .map(|e| e.metadata().unwrap().len())
        .sum::<u64>();

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

    // 1 large tables with 2^(2N) rows and 2^(2N) columns each
    // let large_table = format!(
    //     "{}_{i}",
    //     random_word::gen(random_word::Lang::En).to_string();
    // );
}

fn create_locustdb(db_path: PathBuf) -> Arc<locustdb::LocustDB> {
    let options = locustdb::Options {
        db_v2_path: Some(db_path),
        ..locustdb::Options::default()
    };
    let db = Arc::new(locustdb::LocustDB::new(&options));
    let _locustdb = db.clone();
    thread::spawn(move || {
        actix_web::rt::System::new()
            .block_on(locustdb::server::run(_locustdb))
            .unwrap();
    });
    db
}

fn small_table_names(load_factor: u64) -> Vec<String> {
    // 2^N small tables with 2^N rows and 2^N columns each
    (0..1 << load_factor)
        .map(|i| {
            format!(
                "{}_{i}",
                random_word::gen(random_word::Lang::En).to_string()
            )
        })
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
