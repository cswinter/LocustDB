use chrono::prelude::*;

pub type Extractor = fn(&str) -> i64;

pub fn multiply_by_100(field: &str) -> i64 {
    if let Ok(int) = field.parse::<i64>() {
        int * 100
    } else if let Ok(float) = field.parse::<f64>() {
        (float * 100.0) as i64
    } else if field == "" {
        0
    } else {
        panic!("invalid field {}", &field)
    }
}

pub fn multiply_by_1000(field: &str) -> i64 {
    if let Ok(int) = field.parse::<i64>() {
        int * 1000
    } else if let Ok(float) = field.parse::<f64>() {
        (float * 1000.0) as i64
    } else if field == "" {
        0
    } else {
        panic!("invalid field {}", &field)
    }
}

pub fn int(field: &str) -> i64 {
    if let Ok(int) = field.parse::<i64>() {
        int
    } else if field == "" {
        0
    } else {
        panic!("can't parse {} as integer", &field)
    }
}

pub fn date_time(field: &str) -> i64 {
    Utc.datetime_from_str(field, "%Y-%m-%d %H:%M:%S")
        .expect(&format!("Failed to parse {} as date time", &field))
        .timestamp()
}