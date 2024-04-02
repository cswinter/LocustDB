use bitbuffer::{BitReadBuffer, BitReadStream, BitWriteStream, LittleEndian};

use super::Error;

pub fn encode(floats: &[f64], max_regret: u32, mantissa: Option<u32>) -> Vec<u8> {
    let mut write_bytes = vec![];
    let mut writer = BitWriteStream::new(&mut write_bytes, LittleEndian);
    writer.write_int(floats.len() as u64, 64).unwrap();
    match floats.first() {
        Some(first) => writer.write_int(first.to_bits(), 64).unwrap(),
        None => return write_bytes,
    };
    let mut last_value = floats[0];
    let mut last_leading_zeros = 65;
    let mut last_trailing_zeros = 65;
    let mut last_significant_bits = 0;
    let mut regret = 0;
    let mask = match mantissa {
        Some(mantissa) => {
            assert!(mantissa <= 52, "f64 has at most 52 bits of mantissa");
            u64::MAX - ((1 << (52 - mantissa)) - 1)
        }
        None => u64::MAX,
    };
    for &f in floats.iter().skip(1) {
        let xor = (f.to_bits() ^ last_value.to_bits()) & mask;
        let leading_zeros = xor.leading_zeros().min(31);
        let trailing_zeros = xor.trailing_zeros();

        if trailing_zeros == 64 {
            writer.write_int(0, 1).unwrap();
        } else {
            let significant_bits = 64 - leading_zeros - trailing_zeros;
            if leading_zeros >= last_leading_zeros
                && trailing_zeros >= last_trailing_zeros
                && (regret < max_regret || significant_bits == last_significant_bits)
            {
                // We want to write first a 1 bit and then a 0 bit, but because we are in LittleEndian mode 
                // the bits get written in reverse order. So we write 0b01 to get 0b10 in the output.
                writer.write_int(0b01, 2).unwrap();
                let xor = xor >> last_trailing_zeros;
                writer
                    .write_int(xor, last_significant_bits as usize)
                    .unwrap();
                regret += last_significant_bits - significant_bits;
            } else {
                last_leading_zeros = leading_zeros;
                last_trailing_zeros = trailing_zeros;
                last_significant_bits = significant_bits;
                regret = 0;
                writer.write_int(0b11, 2).unwrap();
                writer.write_int(leading_zeros, 5).unwrap();
                writer.write_int(significant_bits - 1, 6).unwrap();
                let xor = xor >> last_trailing_zeros;
                writer.write_int(xor, significant_bits as usize).unwrap();
            }
        }
        last_value = f;
    }
    write_bytes
}

pub fn decode(data: &[u8]) -> Result<Vec<f64>, Error> {
    let buffer = BitReadBuffer::new(data, LittleEndian);
    let mut reader = BitReadStream::new(buffer);
    let length = reader.read_int(64).map_err(|_| Error::Eof)?;
    let mut decoded = vec![f64::from_bits(0u64); length];

    if length == 0 {
        return Ok(decoded);
    }

    let first = reader.read_int(64).map_err(|_| Error::Eof)?;
    decoded[0] = f64::from_bits(first);

    let mut last = first;
    let mut last_leading_zeros: u32;
    let mut last_trailing_zeros = 65u32;
    let mut last_significant_bits = 0;
    for decoded in &mut decoded[1..length] {
        //match reader.read_bit().ok_or(Error::Eof)? {
        match reader.read_int::<u8>(1).map_err(|_| Error::Eof)? {
            0 => {
                *decoded = f64::from_bits(last);
            }
            1 => {
                //if reader.read_bit().ok_or(Error::Eof)? {
                if reader.read_int::<u8>(1).map_err(|_| Error::Eof)? == 1u8 {
                    last_leading_zeros = reader.read_int(5).map_err(|_| Error::Eof)?;
                    last_significant_bits = reader.read_int::<u32>(6).map_err(|_| Error::Eof)? + 1;
                    last_trailing_zeros = 64 - last_leading_zeros - last_significant_bits;
                }
                let xor: u64 = reader
                    .read_int(last_significant_bits as usize)
                    .map_err(|_| Error::Eof)?;
                last ^= xor << last_trailing_zeros;
                *decoded = f64::from_bits(last);
            }
            _ => {
                return Err(Error::Eof);
            }
        }
    }

    Ok(decoded)
}

pub fn verbose_encode(
    name: &str,
    floats: &[f64],
    max_regret: u32,
    mantissa: Option<u32>,
    verbose: bool,
) -> Vec<u8> {
    let mut write_bytes = vec![];
    let mut writer = BitWriteStream::new(&mut write_bytes, LittleEndian);

    let mask = match mantissa {
        Some(mantissa) => {
            assert!(mantissa <= 52, "f64 has at most 52 bits of mantissa");
            let mask = u64::MAX - ((1 << (52 - mantissa)) - 1);
            if verbose {
                println!("MASK: {:064b}", mask);
            }
            mask
        }
        None => u64::MAX,
    };

    // ANSI bold escape code
    if verbose {
        println!(
            "\x1b[1m{:23}\x1b[0m  \x1b[1m{:64}\x1b[0m  \x1b[1m{:64}\x1b[0m  \x1b[1m{:10}\x1b[0m",
            "DECIMAL", "BITS IN", "XOR", "BITS OUT"
        );
        println!(
            "{:<23}  {:64}  {:64}  {}",
            floats[0],
            format_f64_bits(floats[0].to_bits()),
            "",
            format_f64_bits(floats[0].to_bits())
        );
    }

    writer.write_int(floats.len(), 64).unwrap();
    writer.write_int(floats[0].to_bits(), 64).unwrap();
    let mut last_value = floats[0];
    let mut last_leading_zeros = 65;
    let mut last_trailing_zeros = 65;
    let mut last_significant_bits = 0;
    let mut regret = 0;

    for &f in floats.iter().skip(1) {
        let xor = (f.to_bits() ^ last_value.to_bits()) & mask;

        let leading_zeros = xor.leading_zeros().min(31);
        let trailing_zeros = xor.trailing_zeros();

        let mut bits_string = String::new();
        if trailing_zeros == 64 {
            writer.write_int(0, 1).unwrap();
            bits_string.push_str("\x1b[1;31m0\x1b[0m");
        } else {
            let significant_bits = 64 - leading_zeros - trailing_zeros;
            if leading_zeros >= last_leading_zeros
                && trailing_zeros >= last_trailing_zeros
                && (regret < max_regret || significant_bits == last_significant_bits)
            {
                writer.write_int(0b10, 2).unwrap();
                bits_string.push_str("\x1b[1;31m10\x1b[0m");
                let xor = xor >> last_trailing_zeros;
                writer
                    .write_int(xor, last_significant_bits as usize)
                    .unwrap();
                if verbose {
                    bits_string.push_str(&format!(
                        "\x1b[1;33m{:0width$b}\x1b[0m",
                        xor,
                        width = last_significant_bits as usize
                    ));
                }
                regret += last_significant_bits - significant_bits;
            } else {
                last_leading_zeros = leading_zeros;
                last_trailing_zeros = trailing_zeros;
                last_significant_bits = significant_bits;
                regret = 0;

                writer.write_int(0b11, 2).unwrap();
                bits_string.push_str("\x1b[1;31m11\x1b[0m");
                writer.write_int(leading_zeros, 5).unwrap();
                bits_string.push_str(&format!("\x1b[1;32m{:05b}\x1b[0m", leading_zeros));
                writer.write_int(significant_bits - 1, 6).unwrap();
                bits_string.push_str(&format!("\x1b[1;34m{:06b}\x1b[0m", significant_bits - 1));
                let xor = xor >> last_trailing_zeros;
                writer.write_int(xor, significant_bits as usize).unwrap();
                if verbose {
                    bits_string.push_str(&format!(
                        "\x1b[1;33m{:0width$b}\x1b[0m",
                        xor,
                        width = significant_bits as usize
                    ));
                }
            }
        }
        if verbose {
            println!(
                "{:<23}  {:64}  {:64}  {:32}",
                f,
                format_f64_bits(f.to_bits()),
                format_f64_bits_highlight_remainder(xor),
                bits_string
            );
        }
        last_value = f;
    }

    // 8 bytes per value and 8 addtional bytes for the length
    let uncompressed_size = std::mem::size_of_val(floats) + 8;
    println!(
        "Compression ratio of {:.2} for {name} (max_regret={max_regret})",
        uncompressed_size as f64 / write_bytes.len() as f64,
    );
    write_bytes
}

fn format_f64_bits(bits: u64) -> String {
    let bits_str = format!("{:064b}", bits);
    // ANSI escape codes for color on sign, exponent, and mantissa
    format!(
        "\x1b[1;31m{}\x1b[0m\x1b[1;32m{}\x1b[0m\x1b[1;34m{}\x1b[0m",
        &bits_str[0..1],
        &bits_str[1..12],
        &bits_str[12..]
    )
}

fn format_f64_bits_highlight_remainder(bits: u64) -> String {
    let mut bits_str = format!("{:064b}", bits);
    // Highlight the bits that are different from the previous value
    if let (Some(start_bold), Some(end_bold)) = (bits_str.find('1'), bits_str.rfind('1')) {
        bits_str = format!(
            "{}\x1b[1;33m{}\x1b[0m{}",
            &bits_str[..start_bold],
            &bits_str[start_bold..end_bold + 1],
            &bits_str[end_bold + 1..]
        );
    }
    bits_str
}

#[cfg(test)]
mod test {
    use super::{decode, encode};
    use crate::test_data::FLOATS;

    #[test]
    fn test_xor_float_encode_decode() {
        for &(floats, _) in FLOATS {
            for max_regret in [0, 30, 100, 1000] {
                let encoded = encode(floats, max_regret, None);
                let decoded = decode(&encoded).unwrap();
                assert_eq!(floats, decoded);
            }
        }
    }
}
