use clap::{command, Parser};
use locustdb_compression_utils::{test_data, xor_float};
use rand::{Rng, SeedableRng};

#[derive(Parser, Debug)]
#[command(
    name = "Gorilla Time",
    about = "Demonstrates XOR compression of floating point time series data.",
    author = "Clemens Winter <clemenswinter1@gmail.com>",
    version
)]
struct Opt {
    /// The max_regret parameter for the XOR compression algorithm.
    ///
    /// When encoding a run of floating point numbers that fit into
    /// the previously determined range, the algorithm will reset
    /// the range once the sum in differences between the used
    /// range and the range of the previous run exceeds this value.
    /// This prevents use from getting stuck in a bad state where
    /// we chose a very large range which is never reset and wastes
    /// bits on every subsequent value.
    #[clap(short, long, default_value = "100", num_args = 1..=10)]
    max_regret: Vec<u32>,

    /// Lossily compresses by reducing the size of the mantissa to
    /// this many bits, discarding the least significant bits.
    #[clap(long)]
    mantissa: Option<u32>,

    /// Print the bit representation of the floating point numbers,
    /// XOR differences, and the compressed bit string to the console.
    #[clap(short, long)]
    verbose: bool,

    /// Use single precision floating point numbers instead of double
    /// precision.
    #[clap(short, long)]
    single: bool,

    /// Time compression of this many MiB of random data.
    #[clap(short, long)]
    benchmark: Option<usize>,

    /// Filter the test data by name.
    #[clap(long)]
    filter: Option<String>,
}

fn main() {
    let opt = Opt::parse();

    if let Some(mibibytes) = opt.benchmark {
        assert!(!opt.single, "Benchmarking single precision is not supported");
        assert!(opt.max_regret.len() == 1, "Benchmarking multiple max-regret values is not supported");
        // create 1GiB of random floats
        let len = (1 << 20) * mibibytes / 8;
        let mut data = Vec::with_capacity(len);
        println!("Generating {mibibytes} MiB of random data...");
        let start_time = std::time::Instant::now();
        let mut fast_rng = rand::rngs::SmallRng::seed_from_u64(42);
        for _ in 0..len {
            data.push(fast_rng.gen::<f64>());
        }
        println!(
            "Generated {mibibytes} MiB of random data in {:?}",
            start_time.elapsed(),
        );
        let start_time = std::time::Instant::now();
        let encoded = xor_float::double::encode(&data, opt.max_regret[0], opt.mantissa);
        println!(
            "Encoded {mibibytes} MiB of random data in {:?} ({} MiB/s)",
            start_time.elapsed(),
            mibibytes as u128 * 1000 / start_time.elapsed().as_millis(),
        );
        println!("Compressed size: {} GiB", encoded.len() as f64 / (1 << 30) as f64);
        let start_time = std::time::Instant::now();
        let decoded = xor_float::double::decode(&encoded).unwrap();
        println!(
            "Decoded {mibibytes} MiB of random data in {:?} ({} MiB/s)",
            start_time.elapsed(),
            mibibytes as u128 * 1000 / start_time.elapsed().as_millis(),
        );
        for (i, (expected, actual)) in data.iter().zip(decoded.iter()).enumerate() {
            assert_eq!(expected, actual, "Decoded data does not match original data at index {}", i);
        }
    } else {
        for (data, name) in test_data::FLOATS.iter() {
            if let Some(filter) = &opt.filter {
                if !name.contains(filter) {
                    continue;
                }
            }
            for max_regret in &opt.max_regret {
                if opt.single {
                    let data_f32 = data.iter().map(|&f| f as f32).collect::<Vec<_>>();
                    xor_float::single::verbose_encode(
                        name,
                        &data_f32,
                        *max_regret,
                        opt.mantissa,
                        opt.verbose,
                    );
                } else {
                    let encoded = xor_float::double::verbose_encode(
                        name,
                        data,
                        *max_regret,
                        opt.mantissa,
                        opt.verbose,
                    );
                    assert_eq!(data, &xor_float::double::decode(&encoded).unwrap());
                }
            }
        }
    }
}
