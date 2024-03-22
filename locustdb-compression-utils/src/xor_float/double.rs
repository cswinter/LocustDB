use crate::bit_reader::Error;
use crate::bit_writer::Bit;
use crate::{BitReader, BitWriter};

pub fn encode(floats: &[f64], max_regret: u32, mantissa: Option<u32>) -> Box<[u8]> {
    let mut writer = BitWriter::new();
    writer.write_bits(floats.len() as u64, 64);
    writer.write_bits(floats[0].to_bits(), 64);
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
            writer.write_zero();
        } else {
            let significant_bits = 64 - leading_zeros - trailing_zeros;
            if leading_zeros >= last_leading_zeros
                && trailing_zeros >= last_trailing_zeros
                && (regret < max_regret || significant_bits == last_significant_bits)
            {
                writer.write_one();
                writer.write_zero();
                let xor = xor >> last_trailing_zeros;
                writer.write_bits(xor, last_significant_bits);
                regret += last_significant_bits - significant_bits;
            } else {
                last_leading_zeros = leading_zeros;
                last_trailing_zeros = trailing_zeros;
                last_significant_bits = significant_bits;
                regret = 0;
                writer.write_one();
                writer.write_one();
                writer.write_bits(leading_zeros as u64, 5);
                writer.write_bits(significant_bits as u64, 6);
                let xor = xor >> last_trailing_zeros;
                writer.write_bits(xor, significant_bits);
            }
        }
        last_value = f;
    }
    writer.close()
}

pub fn decode(data: &[u8]) -> Result<Vec<f64>, Error> {
    let mut reader = BitReader::new(data);
    let length = reader.read_bits(64)? as usize;
    let mut decoded = Vec::with_capacity(length);

    let first = reader.read_bits(64).unwrap();
    decoded.push(f64::from_bits(first));

    let mut last = first;
    let mut last_leading_zeros;
    let mut last_trailing_zeros = 65;
    let mut last_significant_bits = 0;
    for _ in 1..length {
        match reader.read_bit()? {
            Bit::Zero => {
                decoded.push(f64::from_bits(last));
            }
            Bit::One => {
                if let Bit::One = reader.read_bit()? {
                    last_leading_zeros = reader.read_bits(5)? as u32;
                    last_significant_bits = reader.read_bits(6)? as u32;
                    last_trailing_zeros = 64 - last_leading_zeros - last_significant_bits;
                }
                let xor = reader.read_bits(last_significant_bits)?;
                last ^= xor << last_trailing_zeros;
                decoded.push(f64::from_bits(last));
            }
        }
    }

    Ok(decoded)
}


pub fn verbose_encode(name: &str, floats: &[f64], max_regret: u32, mantissa: Option<u32>, verbose: bool) -> Box<[u8]> {
    let mut writer = BitWriter::new();

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
            "DECIMAL",
            "BITS IN",
            "XOR",
            "BITS OUT"
        );
        println!(
            "{:<23}  {:64}  {:64}  {}",
            floats[0],
            format_f64_bits(floats[0].to_bits()),
            "",
            format_f64_bits(floats[0].to_bits())
        );
    }

    writer.write_bits(floats.len() as u64, 64);
    writer.write_bits(floats[0].to_bits(), 64);
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
            writer.write_zero();
            bits_string.push_str("\x1b[1;31m0\x1b[0m");
        } else {
            let significant_bits = 64 - leading_zeros - trailing_zeros;
            if leading_zeros >= last_leading_zeros && trailing_zeros >= last_trailing_zeros && (regret < max_regret || significant_bits == last_significant_bits) {
                writer.write_one();
                writer.write_zero();
                bits_string.push_str("\x1b[1;31m10\x1b[0m");
                let xor = xor >> last_trailing_zeros;
                writer.write_bits(xor, last_significant_bits);
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

                writer.write_one();
                writer.write_one();
                bits_string.push_str("\x1b[1;31m11\x1b[0m");
                writer.write_bits(leading_zeros as u64, 5);
                bits_string.push_str(&format!("\x1b[1;32m{:05b}\x1b[0m", leading_zeros));
                writer.write_bits(significant_bits as u64, 6);
                bits_string.push_str(&format!("\x1b[1;34m{:06b}\x1b[0m", significant_bits));
                let xor = xor >> last_trailing_zeros;
                writer.write_bits(xor, significant_bits);
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
    let compressed = writer.close();
    println!(
        "Compression ratio of {:.2} for {name} (max_regret={max_regret})",
        uncompressed_size as f64 / compressed.len() as f64,
    );
    compressed
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
    use crate::test_data::FLOATS;
    use super::{encode, decode};

    #[test]
    fn test_xor_float_encode_decode() {
        for &(floats, _) in FLOATS {
            for max_regret in [0, 30, 100, 100] {
                let encoded = encode(floats, max_regret, None);
                let decoded = decode(&encoded).unwrap();
                assert_eq!(floats, decoded);
            }
        }
    }
}
