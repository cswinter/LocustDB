use bitbuffer::{BigEndian, BitWriteStream};

pub fn encode(floats: &[f32], max_regret: u32, mantissa: Option<u32>) -> Vec<u8> {
    let mut write_bytes = vec![];
    let mut writer = BitWriteStream::new(&mut write_bytes, BigEndian);
    writer.write_int(floats.len(), 64).unwrap();
    match floats.first() {
        Some(first) => writer.write_int(first.to_bits(), 32).unwrap(),
        None => return write_bytes,
    }
    let mut last_value = floats[0];
    let mut last_leading_zeros = 65;
    let mut last_trailing_zeros = 65;
    let mut last_significant_bits = 0;
    let mut regret = 0;
    let mask = match mantissa {
        Some(mantissa) => {
            assert!(mantissa <= 23, "f32 has at most 23 bits of mantissa");
            u32::MAX - ((1 << (32 - mantissa)) - 1)
        }
        None => u32::MAX,
    };
    for &f in floats.iter().skip(1) {
        let xor = f.to_bits() ^ last_value.to_bits() & mask;
        let leading_zeros = xor.leading_zeros();
        let trailing_zeros = xor.trailing_zeros();
        if trailing_zeros == 32 {
            writer.write_int(0, 1).unwrap();
        } else {
            let significant_bits = 32 - leading_zeros - trailing_zeros;
            if leading_zeros >= last_leading_zeros
                && trailing_zeros >= last_trailing_zeros
                && (regret < max_regret || significant_bits == last_significant_bits)
            {
                writer.write_int(0b10, 2).unwrap();
                let xor = xor >> last_trailing_zeros;
                writer.write_int(xor, last_significant_bits as usize).unwrap();
                regret += last_significant_bits - significant_bits;
            } else {
                last_leading_zeros = leading_zeros;
                last_trailing_zeros = trailing_zeros;
                last_significant_bits = significant_bits;
                regret = 0;
                writer.write_int(0b11, 2).unwrap();
                writer.write_int(leading_zeros as u64, 5).unwrap();
                writer.write_int(significant_bits as u64 - 1, 6).unwrap();
                let xor = xor >> last_trailing_zeros;
                writer.write_int(xor, significant_bits as usize).unwrap();
            }
        }
        last_value = f;
    }
    write_bytes
}


pub fn verbose_encode(name: &str, floats: &[f32], max_regret: u32, mantissa: Option<u32>, verbose: bool) -> Vec<u8> {
    let mut write_bytes = vec![];
    let mut writer = BitWriteStream::new(&mut write_bytes, BigEndian);

    let mask = match mantissa {
        Some(mantissa) => {
            assert!(mantissa <= 23, "f32 has at most 23 bits of mantissa");
            let mask = u32::MAX - ((1 << (23 - mantissa)) - 1);
            if verbose {
                println!("MASK: {:032b}", mask);
            }
            mask
        }
        None => u32::MAX,
    };

    // ANSI bold escape code
    if verbose {
        println!(
            "\x1b[1m{:23}\x1b[0m  \x1b[1m{:32}\x1b[0m  \x1b[1m{:32}\x1b[0m  \x1b[1m{:10}\x1b[0m",
            "DECIMAL",
            "BITS IN",
            "XOR",
            "BITS OUT"
        );
        println!(
            "{:<23}  {:32}  {:32}  {}",
            floats[0],
            format_f32_bits(floats[0].to_bits()),
            "",
            format_f32_bits(floats[0].to_bits())
        );
    }

    writer.write_int(floats.len(), 64).unwrap();
    writer.write_int(floats[0].to_bits(), 32).unwrap();
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
        if trailing_zeros == 32 {
            writer.write_int(0, 1).unwrap();
            bits_string.push_str("\x1b[1;31m0\x1b[0m");
        } else {
            let significant_bits = 32 - leading_zeros - trailing_zeros;
            if leading_zeros >= last_leading_zeros && trailing_zeros >= last_trailing_zeros && (regret < max_regret || significant_bits == last_significant_bits) {
                writer.write_int(0b10, 2).unwrap();
                bits_string.push_str("\x1b[1;31m10\x1b[0m");
                let xor = xor >> last_trailing_zeros;
                writer.write_int(xor, last_significant_bits as usize).unwrap();
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
                writer.write_int(leading_zeros as u64, 5).unwrap();
                bits_string.push_str(&format!("\x1b[1;32m{:05b}\x1b[0m", leading_zeros));
                writer.write_int(significant_bits as u64 - 1, 6).unwrap();
                bits_string.push_str(&format!("\x1b[1;34m{:05b}\x1b[0m", significant_bits - 1));
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
                "{:<23}  {:32}  {:32}  {:32}",
                f,
                format_f32_bits(f.to_bits()),
                format_f32_bits_highlight_remainder(xor),
                bits_string
            );
        }
        last_value = f;
    }

    // 8 bytes per value and 8 addtional bytes for the length
    let uncompressed_size = floats.len() * std::mem::size_of::<f32>() + 8;
    println!(
        "Compression ratio of {:.2} for {name} (max_regret={max_regret})",
        uncompressed_size as f64 / write_bytes.len() as f64,
    );
    write_bytes
}


fn format_f32_bits(bits: u32) -> String {
    let bits_str = format!("{:032b}", bits);
    // ANSI escape codes for color on sign, exponent, and mantissa
    format!(
        "\x1b[1;31m{}\x1b[0m\x1b[1;32m{}\x1b[0m\x1b[1;34m{}\x1b[0m",
        &bits_str[0..1],
        &bits_str[1..9],
        &bits_str[9..]
    )
}

fn format_f32_bits_highlight_remainder(bits: u32) -> String {
    let mut bits_str = format!("{:032b}", bits);
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
