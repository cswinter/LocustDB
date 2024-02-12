use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

    log::info!("Starting small table logging");
    let small_tables = small_table_names(load_factor);
    for _row in 0..1 << load_factor {
        for table in &small_tables{
            log.log(
                table,
                (0..1 << load_factor)
                    .map(|c| (format!("col_{c}"), rand::random::<f64>()))
                    .collect(),
            );
        }
    }

    drop(log);

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
    println!("total uncompressed data: {}", locustdb::unit_fmt::bite(8 * (1 << (3 * load_factor))));
    println!("total size on disk: {}", locustdb::unit_fmt::bite(size_on_disk as usize));
    println!("total files: {}", file_count);
    println!("disk writes");
    println!("  total:      {}", locustdb::unit_fmt::bite(perf_counter.disk_write_bytes() as usize));
    println!("  wal:        {}", locustdb::unit_fmt::bite(perf_counter.disk_write_wal_bytes() as usize));
    println!("  partition:  {}", locustdb::unit_fmt::bite(perf_counter.disk_write_new_partition_bytes() as usize));
    println!("  compaction: {}", locustdb::unit_fmt::bite(perf_counter.disk_write_compaction_bytes() as usize));
    println!("  meta store: {}", locustdb::unit_fmt::bite(perf_counter.disk_write_meta_store_bytes() as usize));
    println!("files created");
    println!("  total:     {}", perf_counter.files_created());
    println!("  wal:       {}", perf_counter.files_created_wal());
    println!("  partition: {}", perf_counter.files_created_new_partition());
    println!("  meta:      {}", perf_counter.files_created_meta_store());



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
