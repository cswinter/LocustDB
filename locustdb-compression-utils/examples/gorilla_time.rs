use clap::{command, Parser};
use locustdb_compression_utils::{test_data, xor_float};


#[derive(Parser, Debug)]
#[command(
    name = "Gorilla Time",
    about = "Demonstrates XOR compression of floating point time series data.",
    author = "Clemens Winter <clemenswinter1@gmail.com>",
    version,
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
}

fn main() {
    let opt = Opt::parse();

    for (data, name) in test_data::FLOATS.iter() {
        for max_regret in &opt.max_regret {
            if opt.single {
                let data_f32 = data.iter().map(|&f| f as f32).collect::<Vec<_>>();
                xor_float::single::verbose_encode(name, &data_f32, *max_regret, opt.mantissa, opt.verbose);
            } else {
                xor_float::double::verbose_encode(name, data, *max_regret, opt.mantissa, opt.verbose);
            }
        }
    }
}

