use crate::engine::*;

use std::fmt;

use ordered_float::OrderedFloat;
use pco::standalone::simple_decompress;

pub struct PcoDecode<T> {
    pub encoded: BufferRef<u8>,
    pub decoded: BufferRef<T>,
    pub decoded_len: usize,
    pub is_fp32: bool,
}

impl<'a, T: VecData<T> + PcoDecodable<T> + 'static> VecOperator<'a> for PcoDecode<T> {
    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let decoded = {
            let encoded = scratchpad.get(self.encoded);
            T::pco_decompress(&encoded, self.is_fp32)
        };
        scratchpad.set(self.decoded, decoded);
        Ok(())
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.encoded.any()] }
    fn inputs_mut(&mut self) -> Vec<&mut usize> { vec![&mut self.encoded.i] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.decoded.any()] }
    fn can_stream_input(&self, _: usize) -> bool { false }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }
    fn is_streaming_producer(&self) -> bool { false }
    fn custom_output_len(&self) -> Option<usize> { Some(self.decoded_len) }

    fn display_op(&self, _: bool) -> String {
        format!("pco_decode({})", self.encoded)
    }
}

impl<T> fmt::Debug for PcoDecode<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PcoDecode {{ encoded: {}, decoded: {} }}",
            self.encoded, self.decoded
        )
    }
}

trait PcoDecodable<T> {
    fn pco_decompress(data: &[u8], is_fp32: bool) -> Vec<T>;
}

impl PcoDecodable<u8> for u8 {
    fn pco_decompress(data: &[u8], _: bool) -> Vec<u8> {
        simple_decompress::<u32>(data)
            .unwrap()
            .into_iter()
            .map(|x| x as u8)
            .collect()
    }
}

impl PcoDecodable<u16> for u16 {
    fn pco_decompress(data: &[u8], _: bool) -> Vec<u16> {
        simple_decompress::<u32>(data)
            .unwrap()
            .into_iter()
            .map(|x| x as u16)
            .collect()
    }
}

impl PcoDecodable<u32> for u32 {
    fn pco_decompress(data: &[u8], _: bool) -> Vec<u32> {
        simple_decompress::<u32>(data).unwrap()
    }
}

impl PcoDecodable<u64> for u64 {
    fn pco_decompress(data: &[u8], _: bool) -> Vec<u64> {
        simple_decompress::<u64>(data).unwrap()
    }
}

impl PcoDecodable<i64> for i64 {
    fn pco_decompress(data: &[u8], _: bool) -> Vec<i64> {
        simple_decompress::<i64>(data).unwrap()
    }
}

impl PcoDecodable<of64> for of64 {
    fn pco_decompress(data: &[u8], is_fp32: bool) -> Vec<of64> {
        if is_fp32 {
            simple_decompress::<f32>(data)
                .unwrap()
                .into_iter()
                .map(|x| OrderedFloat(x as f64))
                .collect()
        } else {
            unsafe {
                std::mem::transmute::<Vec<f64>, Vec<of64>>(simple_decompress::<f64>(data).unwrap())
            }
        }
    }
}
