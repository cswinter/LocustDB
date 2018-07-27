extern crate lz4;

use std::io::{Read, Write};
use std::mem;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::fmt::Debug;


pub fn decoder(data: &[u8]) -> lz4::Decoder<&[u8]> {
    lz4::Decoder::new(data).unwrap()
}

pub unsafe fn encode<T: Debug>(data: &[T]) -> Vec<u8> {
    let ptr_t = data.as_ptr();
    // Endianness? Never heard of it...
    let ptr_u8 = mem::transmute::<_, *const u8>(ptr_t);
    let data_u8: &[u8] = from_raw_parts(ptr_u8, data.len() * mem::size_of::<T>());

    let mut result = Vec::new();
    {
        let mut encoder = lz4::EncoderBuilder::new().build(&mut result).unwrap();
        encoder.write_all(data_u8).unwrap();
        encoder.finish().1.unwrap();
    }
    result
}

pub unsafe fn decode<T>(src: &mut Read, dst: &mut [T]) -> usize {
    let ptr_t = dst.as_ptr();
    let ptr_u8 = mem::transmute::<_, *mut u8>(ptr_t);
    let dst_u8: &mut [u8] = from_raw_parts_mut(ptr_u8, dst.len() * mem::size_of::<T>());

    let mut read = 0;
    // LZ4 decodes in blocks of at most 65536 elements, so might have to call multiple times to fill buffer
    while read < dst_u8.len() && 0 != {
        let len = src.read(&mut dst_u8[read..]).unwrap();
        read += len;
        len
    } {}
    if read % mem::size_of::<T>() != 0 {
        println!("{} {} {} {}", dst.len(), dst_u8.len(), read, mem::size_of::<T>());
    }
    assert_eq!(read % mem::size_of::<T>(), 0);
    read / mem::size_of::<T>()
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let data = vec![10i64, 12095, -51235, 3, 0, 0, 12353, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10];
        let encoded = unsafe { encode(&data) };
        let mut decoded = vec![0i64; data.len()];
        let count = unsafe { decode(&mut decoder(&encoded), &mut decoded) };
        assert_eq!(count, data.len());
        assert_eq!(decoded, data);
    }
}
