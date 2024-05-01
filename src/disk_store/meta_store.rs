use capnp::serialize_packed;
use locustdb_serialization::dbmeta_capnp;
use std::collections::HashMap;

type TableName = String;
type PartitionID = u64;

#[derive(Clone)]
pub struct MetaStore {
    pub next_wal_id: u64,
    pub partitions: HashMap<TableName, HashMap<PartitionID, PartitionMetadata>>,
}

#[derive(Clone, Debug)]
pub struct PartitionMetadata {
    pub id: PartitionID,
    pub tablename: String,
    pub offset: usize,
    pub len: usize,
    pub subpartitions: Vec<SubpartitionMetadata>,
    pub column_name_to_subpartition_index: HashMap<String, usize>,
}

#[derive(Clone, Debug)]
pub struct SubpartitionMetadata {
    pub size_bytes: u64,
    pub subpartition_key: String,
}

impl PartitionMetadata {
    pub fn subpartition_key(&self, column_name: &str) -> String {
        let subpartition_index = self.column_name_to_subpartition_index[column_name];
        self.subpartitions[subpartition_index]
            .subpartition_key
            .clone()
    }
}

impl MetaStore {
    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = ::capnp::message::Builder::new_default();
        let mut dbmeta = builder.init_root::<dbmeta_capnp::d_b_meta::Builder>();
        dbmeta.set_next_wal_id(self.next_wal_id);

        let total_partitions = self.partitions.values().map(|x| x.len()).sum::<usize>();
        assert!(total_partitions < std::u32::MAX as usize);
        let mut partitions_builder = dbmeta.init_partitions(total_partitions as u32);
        let mut i = 0;
        for table in self.partitions.values() {
            for partition in table.values() {
                let mut subpartition_index_to_column_names = vec![Vec::new(); partition.subpartitions.len()];
                for (column_name, subpartition_index) in
                    &partition.column_name_to_subpartition_index
                {
                    subpartition_index_to_column_names[*subpartition_index]
                        .push(column_name.clone());
                }
                let mut partition_builder = partitions_builder.reborrow().get(i);
                partition_builder.set_id(partition.id);
                partition_builder.set_tablename(&partition.tablename);
                partition_builder.set_offset(partition.offset as u64);
                partition_builder.set_len(partition.len as u64);
                assert!(partition.subpartitions.len() < std::u32::MAX as usize);
                let mut subpartitions_builder =
                    partition_builder.init_subpartitions(partition.subpartitions.len() as u32);
                for (i, subpartition) in partition.subpartitions.iter().enumerate() {
                    let mut subpartition_builder = subpartitions_builder.reborrow().get(i as u32);
                    subpartition_builder.set_size_bytes(subpartition.size_bytes);
                    subpartition_builder.set_subpartition_key(&subpartition.subpartition_key);
                    let mut columns_builder = subpartition_builder
                        .init_columns(subpartition_index_to_column_names[i].len() as u32);
                    for (i, column_name) in
                        std::mem::take(&mut subpartition_index_to_column_names[i])
                            .into_iter()
                            .enumerate()
                    {
                        columns_builder.set(i as u32, column_name);
                    }
                }
                i += 1;
            }
        }

        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }

    pub fn deserialize(data: &[u8]) -> capnp::Result<MetaStore> {
        let message_reader =
            serialize_packed::read_message(&mut &data[..], ::capnp::message::ReaderOptions::new())?;
        let dbmeta = message_reader.get_root::<dbmeta_capnp::d_b_meta::Reader>()?;
        let next_wal_id = dbmeta.get_next_wal_id();
        let mut partitions = HashMap::<TableName, HashMap<PartitionID, PartitionMetadata>>::new();
        for partition in dbmeta.get_partitions()? {
            let id = partition.get_id();
            let tablename = partition.get_tablename()?.to_string().unwrap();
            let offset = partition.get_offset() as usize;
            let len = partition.get_len() as usize;
            let mut subpartitions = Vec::new();
            let mut column_name_to_subpartition_index = HashMap::new();
            for (i, subpartition) in partition.get_subpartitions()?.iter().enumerate() {
                let size_bytes = subpartition.get_size_bytes();
                let subpartition_key = subpartition.get_subpartition_key()?.to_string().unwrap();
                subpartitions.push(SubpartitionMetadata {
                    size_bytes,
                    subpartition_key,
                });
                for column in subpartition.get_columns()? {
                    let column = column?.to_string().unwrap();
                    column_name_to_subpartition_index.insert(column, i);
                }
            }
            let partition = PartitionMetadata {
                id,
                tablename: tablename.clone(),
                offset,
                len,
                subpartitions,
                column_name_to_subpartition_index,
            };
            partitions
                .entry(tablename)
                .or_default()
                .insert(id, partition);
        }

        Ok(MetaStore {
            next_wal_id,
            partitions,
        })
    }
}
