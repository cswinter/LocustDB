// Implementation of [Walker's Alias method](https://en.wikipedia.org/wiki/Alias_method)
//
// The Walker's Alias method algorithm is principally useful when you need to
// random sampling with replacement by `O(1)`.
//
// # Examples
//
// ```rust
// use aliasmethod::{new_alias_table, alias_method}
//
// let weights = vec![1.0, 1.0, 8.0];
// match new_alias_table(weights) {
//     Err(e) => {
//         println!(false, "error : {}", e);
//     }
//     Ok(alias_table) => {
//         let n = alias_method().random(&alias_table);
//         assert!(0 <= n && n <= weights.length);
// }
// ```
extern crate rand;

use std::fmt;
use self::rand::Rng;


pub struct AliasMethod<RNG: Rng> {
    rng: RNG,
}


#[derive(Debug)]
pub struct AliasTable {
    len: i32,
    prob: Vec<f64>,
    alias: Vec<usize>,
}

#[derive(Debug)]
pub enum AliasMethodError {
    ZeroTotalWeights,
    Internal {
        text: String,
    },
}

impl fmt::Display for AliasMethodError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AliasMethodError::ZeroTotalWeights => write!(f, "Total of weights is 0."),
            AliasMethodError::Internal { ref text } => write!(f, "Internal error: {}", text),
        }
    }
}

impl<RNG: Rng> AliasMethod<RNG> {
    /// Creates a new AliasMethod struct.
    pub fn new(rng: RNG) -> Self {
        AliasMethod { rng }
    }

    /// Chooses a index.
    pub fn random(&mut self, alias_table: &AliasTable) -> usize {
        let u = self.rng.gen::<f64>();
        let n = self.rng.gen_range(0, alias_table.len) as usize;

        if u <= alias_table.prob[n] {
            n
        } else {
            alias_table.alias[n]
        }
    }
}


/// Creates a new AliasTable struct.
pub fn new_alias_table(weights: &[f64]) -> Result<AliasTable, AliasMethodError> {
    let n = weights.len() as i32;

    let sum = weights.iter().fold(0.0, |acc, x| acc + x);
    if sum == 0.0 {
        return Err(AliasMethodError::ZeroTotalWeights);
    }

    let mut prob = weights.iter().map(|w| w * f64::from(n) / sum).collect::<Vec<f64>>();
    let mut h = 0;
    let mut l = n - 1;
    let mut hl: Vec<usize> = vec![0; n as usize];

    for (i, p) in prob.iter().enumerate() {
        if *p < 1.0 {
            hl[l as usize] = i;
            l -= 1;
        }
        if 1.0 < *p {
            hl[h as usize] = i;
            h += 1;
        }
    }

    let mut a: Vec<usize> = vec![0; n as usize];

    while h != 0 && l != n - 1 {
        let j = hl[(l + 1) as usize];
        let k = hl[(h - 1) as usize];

        if 1.0 < prob[j] {
            return Err(AliasMethodError::Internal { text: format!("MUST: {} <= 1", prob[j]) });
        }
        if prob[k] < 1.0 {
            return Err(AliasMethodError::Internal { text: format!("MUST: 1 <= {}", prob[k]) });
        }

        a[j] = k;
        prob[k] -= 1.0 - prob[j];   // - residual weight
        l += 1;
        if prob[k] < 1.0 {
            hl[l as usize] = k;
            l -= 1;
            h -= 1;
        }
    }

    Ok(AliasTable {
        len: n,
        prob,
        alias: a,
    })
}


#[test]
fn test_new_alias_table() {
    let params = [vec![1.0, 1.0], vec![1.0, 1.0, 8.0]];
    for sample_weights in params.into_iter() {
        let alias_table = new_alias_table(&sample_weights);
        match alias_table {
            Ok(AliasTable { prob, .. }) => {
                assert_eq!(prob.len(), sample_weights.len());
            }
            Err(e) => {
                assert!(false, "error : {}", e);
            }
        }
    }
}
