use std::fmt;


pub fn bite(quantity: usize) -> UnitFormatter {
    UnitFormatter::new(quantity as f64, 1024, 0, IEC_PREFIXES, "B".to_string())
}

pub fn ns(quantity: usize) -> UnitFormatter {
    UnitFormatter::new(quantity as f64, 1000, 5, SI_PREFIXES, "s".to_string())
}

pub fn byte(quantity: f64) -> UnitFormatter {
    UnitFormatter::new(quantity, 1000, 8, SI_PREFIXES, "B".to_string())
}

pub fn second(quantity: usize) -> UnitFormatter {
    UnitFormatter::new(quantity as f64, 1000, 8, SI_PREFIXES, "s".to_string())
}

pub fn billion(quantity: f64) -> UnitFormatter {
    short_scale(quantity * 1000000000.0)
}

pub fn short_scale(quantity: f64) -> UnitFormatter {
    UnitFormatter::new(quantity, 1000, 0, SHORT_SCALE, "".to_string())
}

pub fn percent(quantity: f64) -> UnitFormatter {
    UnitFormatter::new(quantity, 2, 0, NO_PREFIX, "%".to_string())
}


pub struct UnitFormatter {
    quantity: f64,
    power: usize,
    ratio: usize,
    prefixes: &'static [&'static str],
    suffix: String,
}

impl UnitFormatter {
    pub fn new(quantity: f64,
               ratio: usize,
               power: usize,
               prefixes: &'static [&'static str],
               suffix: String) -> UnitFormatter {
        UnitFormatter { quantity, power, ratio, prefixes, suffix }
    }
}

impl fmt::Display for UnitFormatter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let sig_figs = f.precision().unwrap_or(3);
        let mut i = self.power;
        let mut ratio = 1.0;
        while (self.quantity / ratio) < 1.0 && i > 0 {
            ratio /= self.ratio as f64;
            i -= 1;
        }
        while (self.quantity / ratio).log10() > sig_figs as f64 && i + 1 < self.prefixes.len() {
            ratio *= self.ratio as f64;
            i += 1;
        }
        let quantity = self.quantity / ratio;
        let digits = quantity.log10().floor() as isize + 1;
        let precision = if digits < 0 {
            sig_figs
        } else if digits < sig_figs as isize {
            (sig_figs as isize - digits) as usize
        } else { 0 };
        write!(f, "{:.prec$}{}{}",
               quantity,
               self.prefixes[i],
               &self.suffix,
               prec = precision)
    }
}

const NO_PREFIX: &[&str] = &[
    "",
];

const SI_PREFIXES: &[&str] = &[
    "y",
    "z",
    "a",
    "f",
    "p",
    "n",
    "μ",
    "m",
    "",
    "k",
    "M",
    "G",
    "T",
    "P",
    "E",
    "Z",
    "Y",
];

const IEC_PREFIXES: &[&str] = &[
    "",
    "Ki",
    "Mi",
    "Gi",
    "Ti",
    "Pi",
    "Ei",
    "Zi",
    "Yi",
];

const SHORT_SCALE: &[&str] = &[
    "",
    " thousand",
    " million",
    " billion",
    " trillion",
    " quadrillion",
    " quintillion",
    " sextillion",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format() {
        assert_eq!(
            &format!("{}", bite(53413)),
            "52.2KiB");

        assert_eq!(
            &format!("{:.2}", bite(1024 * 1024)),
            "1.0MiB");
    }
}
