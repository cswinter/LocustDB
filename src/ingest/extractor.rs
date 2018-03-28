use ingest::chrono::prelude::*;

pub type Extractor = fn(&str) -> i64;

pub fn multiply_by_100(field: &str) -> i64 {
    if let Ok(int) = field.parse::<i64>() {
        int * 100
    } else if let Ok(float) = field.parse::<f64>() {
        (float as i64) * 100
    } else {
        panic!("invalid field {}", &field)
    }
}

pub fn date_time(field: &str) -> i64 {
    Utc.datetime_from_str(field, "%Y-%m-%d %H:%M:%S")
        .expect(&format!("Failed to parse {} as date time", &field))
        .timestamp()
}