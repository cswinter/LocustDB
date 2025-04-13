#![feature(let_chains)]
use std::path::PathBuf;
use std::sync::Arc;

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
    let (storage, wal, _) = Storage::new(&opts.db_path, Arc::new(PerfCounter::default()), true);

    {
        let meta = storage.meta_store().read().unwrap();
        println!("### META STORE ###");
        println!("Next WAL ID: {}", meta.next_wal_id);
        println!("Number of partitions: {:?}", meta.partitions.len());
        let column_names: usize = meta
            .partitions
            .values()
            .flat_map(|table| {
                table
                    .values()
                    .map(|part| part.column_name_to_subpartition_index.len())
            })
            .sum();
        let column_name_byes: usize = meta
            .partitions
            .values()
            .flat_map(|table| {
                table.values().flat_map(|part| {
                    part.column_name_to_subpartition_index
                        .keys()
                        .map(|s| s.len())
                })
            })
            .sum();
        println!("Number of column names (duplicated across partitions): {}", column_names);
        println!("Column name bytes: {}", column_name_byes);
        if opts.meta > 0 {
            for (table, partitions) in &meta.partitions {
                if let Some(filter) = &opts.table
                    && filter != table
                {
                    continue;
                }
                for partition in partitions.values() {
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
                                "  Subpartition {} has {} columns ({} bytes)",
                                i,
                                partition
                                    .column_name_to_subpartition_index
                                    .values()
                                    .filter(|&&idx| idx == i)
                                    .count(),
                                subpartition.size_bytes,
                            );
                            if opts.meta > 2 {
                                println!(
                                    "    {:?}",
                                    partition
                                        .column_name_to_subpartition_index
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
                            println!("  Table {} has {} columns", name, table.columns.len());
                            if opts.wal > 3 {
                                for col in &table.columns {
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
