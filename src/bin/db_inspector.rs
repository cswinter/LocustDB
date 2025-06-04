#![feature(let_chains)]
use std::path::PathBuf;
use std::sync::Arc;

use locustdb::disk_store::meta_store::PartitionMetadata;
use locustdb::disk_store::storage::Storage;
use locustdb::observability::PerfCounter;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "LocustDB DB Inspector",
    about = "Inspect database file structure.",
    author = "Clemens Winter <clemenswinter1@gmail.com>"
)]
struct Opt {
    /// Database path
    #[structopt(long, name = "PATH", parse(from_os_str))]
    db_path: PathBuf,

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

fn main() {
    env_logger::init();
    let opts = Opt::from_args();
    let (storage, wal, _) = Storage::new(&opts.db_path, Arc::new(PerfCounter::default()), true, 1);

    {
        let meta = storage.meta_store().read().unwrap();
        println!("### META STORE ###");
        println!("Next WAL ID: {}", meta.next_wal_id());
        println!("Number of partitions: {:?}", meta.partitions().count());
        if opts.meta > 0 {
            let partitions = match &opts.table {
                Some(table) => Box::new(meta.partitions_for_table(table))
                    as Box<dyn Iterator<Item = &PartitionMetadata>>,
                None => Box::new(meta.partitions()),
            };
            for partition in partitions {
                println!(
                    "Partition {} for table {} has {} subpartitions and {} rows ({}..{}, {} bytes)",
                    partition.id,
                    partition.tablename,
                    partition.subpartitions.len(),
                    partition.len,
                    partition.offset,
                    partition.offset + partition.len,
                    partition
                        .subpartitions
                        .iter()
                        .map(|sp| sp.size_bytes)
                        .sum::<u64>()
                );
                if opts.meta > 1 {
                    for (i, subpartition) in partition.subpartitions.iter().enumerate() {
                        println!(
                            "  Subpartition {} has last column {} ({} bytes)",
                            i, subpartition.last_column, subpartition.size_bytes,
                        );
                        if opts.meta > 2 {
                            println!(
                                "    {:?}",
                                partition
                                    .subpartitions_by_last_column
                                    .iter()
                                    .filter(|(_, &idx)| idx == i)
                                    .map(|(name, _)| name)
                                    .collect::<Vec<_>>()
                            );
                        }
                    }
                }
            }
        }

        if opts.wal > 0 {
            println!();
            println!("### WAL ###");
            println!("Number of WAL segments: {:?}", wal.len());
            if opts.wal > 1 {
                for segment in &wal {
                    println!(
                        "Segment {} has {} tables",
                        segment.id,
                        segment.data.tables.len()
                    );
                    if opts.wal > 2 {
                        for (name, table) in &segment.data.as_ref().tables {
                            println!("  Table {} has {} columns", name, table.columns().count());
                            if opts.wal > 3 {
                                for (col, _) in table.columns() {
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
