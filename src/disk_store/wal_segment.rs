use std::borrow::Cow;
use capnp::serialize_packed;
use locustdb_serialization::event_buffer::EventBuffer;
use locustdb_serialization::wal_segment_capnp;

pub struct WalSegment<'a> {
    pub id: u64,
    pub data: Cow<'a, EventBuffer>,
}

impl<'a> WalSegment<'a> {
    pub fn serialize(&self) -> Vec<u8> {
        let mut builder = capnp::message::Builder::new_default();
        let mut wal_segment = builder.init_root::<wal_segment_capnp::wal_segment::Builder>();
        wal_segment.set_id(self.id);
        let mut table_segment_list = wal_segment.get_data().unwrap();
        self.data.serialize_builder(&mut table_segment_list);
        let mut buf = Vec::new();
        serialize_packed::write_message(&mut buf, &builder).unwrap();
        buf
    }

    pub fn deserialize(data: &[u8]) -> capnp::Result<WalSegment<'static>> {
        let message_reader =
            serialize_packed::read_message(data, capnp::message::ReaderOptions::new()).unwrap();
        let wal_segment = message_reader.get_root::<wal_segment_capnp::wal_segment::Reader>()?;
        let id = wal_segment.get_id();
        let data = EventBuffer::deserialize_reader(wal_segment.get_data()?)?;
        Ok(WalSegment {
            id,
            data: Cow::Owned(data),
        })
    }
}
