extern crate rocksdb;

use std::sync::Arc;

use bincode::{serialize, deserialize};
use byteorder::{ByteOrder, BigEndian};
use self::rocksdb::*;

use disk_store::interface::*;
use mem_store::column::Column;

pub struct RocksDB {
    db: DB,
}

impl RocksDB {
    pub fn new(path: &str) -> RocksDB {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        let mut partitions_options = Options::default();
        #[cfg(feature = "enable_lz4")]
            partitions_options.set_compression_type(DBCompressionType::None);

        let db = DB::open_cf_descriptors(&options, path, vec![
            ColumnFamilyDescriptor::new("metadata", Options::default()),
            ColumnFamilyDescriptor::new("partitions", partitions_options),
        ]).unwrap();
        RocksDB { db }
    }

    fn metadata(&self) -> ColumnFamily {
        self.db.cf_handle("metadata").unwrap()
    }

    fn partitions(&self) -> ColumnFamily {
        self.db.cf_handle("partitions").unwrap()
    }
}

impl DiskStore for RocksDB {
    fn load_metadata(&self) -> Vec<PartitionMetadata> {
        let mut metadata = Vec::new();
        let iter = self.db.iterator_cf(self.metadata(), IteratorMode::Start).unwrap();
        for (key, value) in iter {
            let partition_id = BigEndian::read_u64(&key) as PartitionID;
            let MetaData { tablename, len, columns } = deserialize(&value).unwrap();
            metadata.push(PartitionMetadata {
                id: partition_id,
                len,
                tablename,
                columns,
            })
        }
        metadata
    }

    fn load_column(&self, partition: PartitionID, column_name: &str) -> Column {
        let data = self.db.get_cf(self.partitions(), &column_key(partition, column_name)).unwrap().unwrap();
        let mut col: Column = deserialize(&data).unwrap();
        // TODO(clemens): use serialisation library that makes this unnecessary
        col.shrink_to_fit_ish();
        col
    }

    fn bulk_load(&self, start: PartitionID, end: PartitionID) -> Vec<(PartitionID, Column)> {
        let mut key = [0; 8];
        BigEndian::write_u64(&mut key, start as u64);
        let mut result = Vec::new();
        let iterator = self.db
            .iterator_cf(self.partitions(), IteratorMode::From(&key, Direction::Forward))
            .unwrap();
        for (key, value) in iterator {
            let id = BigEndian::read_u64(&key);
            if id >= end as u64 { break; }
            result.push((id, deserialize(&value).unwrap()));
        }
        result
    }

    fn store_partition(&self, partition: PartitionID, tablename: &str, columns: &Vec<Arc<Column>>) {
        let mut tx = WriteBatch::default();

        let mut key = [0; 8];
        BigEndian::write_u64(&mut key, partition as u64);
        let md = MetaData {
            tablename: tablename.to_string(),
            len: columns[0].len(),
            columns: columns.iter().map(|c| c.name().to_string()).collect(),
        };
        tx.put_cf(self.metadata(), &key, &serialize(&md).unwrap()).unwrap();
        for column in columns {
            let key = column_key(partition, column.name());
            let data = serialize(column.as_ref()).unwrap();
            tx.put_cf(self.partitions(), &key, &data).unwrap();
        }

        self.db.write(tx).unwrap();
    }
}

fn column_key(id: PartitionID, column_name: &str) -> Vec<u8> {
    let mut key = vec![0; 8];
    BigEndian::write_u64(&mut key, id as u64);
    key.push('.' as u8);
    key.extend(column_name.as_bytes());
    key
}

#[derive(Serialize, Deserialize, Debug)]
struct MetaData {
    tablename: String,
    len: usize,
    columns: Vec<String>,
}
