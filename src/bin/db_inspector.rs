use std::path::PathBuf;

use locustdb::disk_store::v2::{StorageV2, Storage};
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
    #[structopt(long, default_value="1")]
    wal: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let opts = Opt::from_args();
    let (storage, wal) = StorageV2::new(&opts.db_v2_path, true);

    {
        let meta = storage.meta_store().lock().unwrap();
        println!("### META STORE ###");
        println!("Next WAL ID: {}", meta.next_wal_id);
        println!("Number of partitions: {:?}", meta.partitions.len());

        if opts.wal > 0 {
            println!("");
            println!("### WAL ###");
            println!("Number of WAL segments: {:?}", wal.len());
            if opts.wal > 1 {
                for segment in &wal {
                    println!("Segment {} has {} tables", segment.id, segment.data.len());
                    if opts.wal > 2 {
                        for (table, rows) in &segment.data {
                            println!("  Table {} has {} rows", table, rows.len());
                            if opts.wal > 3 { 
                                for row in rows {
                                    println!("    {:?}", row);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
