use std::path::PathBuf;
use std::sync::Arc;

use locustdb::disk_store::v2::{Storage, StorageV2};
use locustdb::perf_counter::PerfCounter;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "LocustDB DB Inspector",
    about = "Inspect database file structure.",
    author = "Clemens Winter <clemenswinter1@gmail.com>"
)]
struct Opt {
    /// Path to data directory based on v2 storage format
    #[structopt(long, name = "PATH_V2", parse(from_os_str))]
    db_v2_path: PathBuf,

    /// Wal detail. 0 = no detail, 1 = number of segments, 2 = tables per segment + total rows, 3 = rows per table, 4 = full table dump
    #[structopt(long, default_value = "1")]
    wal: u64,

    /// Meta detail. 0 = wal id and number of partitions, 1 = partition stats, 2 = partition column names
    #[structopt(long, default_value = "0")]
    meta: u64,

    /// Filter for table name
    #[structopt(long, name = "TABLE")]
    table: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let opts = Opt::from_args();
    let (storage, wal) = StorageV2::new(&opts.db_v2_path, Arc::new(PerfCounter::default()), true);

    {
        let meta = storage.meta_store().lock().unwrap();
        println!("### META STORE ###");
        println!("Next WAL ID: {}", meta.next_wal_id);
        println!("Number of partitions: {:?}", meta.partitions.len());
        if opts.meta > 0 {
            for partition in &meta.partitions {
                if opts
                    .table
                    .as_ref()
                    .map(|t| *t != partition.tablename)
                    .unwrap_or(false)
                {
                    continue;
                }
                println!(
                    "Partition {} for table {} has {} subpartitions and {} rows ({} bytes)",
                    partition.id,
                    partition.tablename,
                    partition.subpartitions.len(),
                    partition.len,
                    partition
                        .subpartitions
                        .iter()
                        .map(|sp| sp.size_bytes)
                        .sum::<u64>()
                );
                if opts.meta > 1 {
                    for (i, subpartition) in partition.subpartitions.iter().enumerate() {
                        println!(
                            "  Subpartition {} has {} columns ({} bytes)",
                            i,
                            subpartition.column_names.len(),
                            subpartition.size_bytes,
                        );
                        if opts.meta > 2 {
                            println!("    {:?}", subpartition.column_names);
                        }
                    }
                }
            }
        }

        if opts.wal > 0 {
            println!("");
            println!("### WAL ###");
            println!("Number of WAL segments: {:?}", wal.len());
            if opts.wal > 1 {
                for segment in &wal {
                    println!("Segment {} has {} tables", segment.id, segment.data.tables.len());
                    if opts.wal > 2 {
                        for (name, table) in segment.data.as_ref().tables {
                            println!("  Table {} has {} columns", name, table.columns.len());
                            if opts.wal > 3 {
                                for col in table.columns {
                                    println!("    {:?}", col);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
