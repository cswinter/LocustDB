use extractor;
use ingest::csv_loader::Options;

pub fn nyc_colnames() -> Vec<String> {
    vec![
        "trip_id".to_string(),
        "vendor_id".to_string(),
        "pickup_datetime".to_string(),
        "dropoff_datetime".to_string(),
        "store_and_fwd_flag".to_string(),
        "rate_code_id".to_string(),
        "pickup_longitude".to_string(),
        "pickup_latitude".to_string(),
        "dropoff_longitude".to_string(),
        "dropoff_latitude".to_string(),
        "passenger_count".to_string(),
        "trip_distance".to_string(),
        "fare_amount".to_string(),
        "extra".to_string(),
        "mta_tax".to_string(),
        "tip_amount".to_string(),
        "tolls_amount".to_string(),
        "ehail_fee".to_string(),
        "improvement_surcharge".to_string(),
        "total_amount".to_string(),
        "payment_type".to_string(),
        "trip_type".to_string(),
        "pickup".to_string(),
        "dropoff".to_string(),
        "cab_type".to_string(),
        "precipitation".to_string(),
        "snow_depth".to_string(),
        "snowfall".to_string(),
        "max_temperature".to_string(),
        "min_temperature".to_string(),
        "average_wind_speed".to_string(),
        "pickup_nyct2010_gid".to_string(),
        "pickup_ctlabel".to_string(),
        "pickup_borocode".to_string(),
        "pickup_boroname".to_string(),
        "pickup_ct2010".to_string(),
        "pickup_boroct2010".to_string(),
        "pickup_cdeligibil".to_string(),
        "pickup_ntacode".to_string(),
        "pickup_ntaname".to_string(),
        "pickup_puma".to_string(),
        "dropoff_nyct2010_gid".to_string(),
        "dropoff_ctlabel".to_string(),
        "dropoff_borocode".to_string(),
        "dropoff_boroname".to_string(),
        "dropoff_ct2010".to_string(),
        "dropoff_boroct2010".to_string(),
        "dropoff_cdeligibil".to_string(),
        "dropoff_ntacode".to_string(),
        "dropoff_ntaname".to_string(),
        "dropoff_puma".to_string(),
    ]
}

pub fn nyc_extractors() -> Vec<(&'static str, extractor::Extractor)> {
    vec![
        ("pickup_datetime", extractor::date_time),
        ("dropoff_datetime", extractor::date_time),
        ("trip_distance", extractor::multiply_by_1000),
        ("fare_amount", extractor::multiply_by_100),
        ("extra", extractor::multiply_by_100),
        ("mta_tax", extractor::multiply_by_100),
        ("tip_amount", extractor::multiply_by_100),
        ("tolls_amount", extractor::multiply_by_100),
        ("ehail_fee", extractor::multiply_by_100),
        ("improvement_surcharge", extractor::multiply_by_100),
        ("total_amount", extractor::multiply_by_100),
        ("precipitation", extractor::multiply_by_1000),
        ("snow_depth", extractor::multiply_by_1000),
        ("snowfall", extractor::multiply_by_1000),
        ("average_wind_speed", extractor::multiply_by_1000),
        ("pickup_puma", extractor::int),
        ("dropoff_puma", extractor::int),
    ]
}

pub fn dropped_cols() -> Vec<String> {
    vec![
        "pickup".to_string(),
        "dropoff".to_string(),
        "max_temperature".to_string(),
        "min_temperature".to_string(),
        "dropoff_borocode".to_string(),
        "dropoff_boroname".to_string(),
        "dropoff_cdeligibil".to_string(),
        "dropoff_ct2010".to_string(),
        "dropoff_ctlabel".to_string(),
        "dropoff_latitude".to_string(),
        "dropoff_longitude".to_string(),
        "dropoff_ntacode".to_string(),
        "dropoff_ntaname".to_string(),
        "dropoff_nyct2010_gid".to_string(),
        "pickup_borocode".to_string(),
        "pickup_boroname".to_string(),
        "pickup_boroct2010".to_string(),
        "pickup_ct2010".to_string(),
        "pickup_ctlabel".to_string(),
        "pickup_latitude".to_string(),
        "pickup_longitude".to_string(),
        "pickup_ntacode".to_string(),
        "pickup_nyct2010_gid".to_string(),
        "dropoff_boroct2010".to_string(),
        "dropoff_datetime".to_string(),
        "tolls_amount".to_string(),
        "fare_amount".to_string(),
        "tip_amount".to_string(),
        "extra".to_string(),
        "average_wind_speed".to_string(),
        "snow_depth".to_string(),
        "precipitation".to_string(),
    ]
}

pub fn ingest_file(file_path: &str, tablename: &str) -> Options {
    Options::new(file_path, tablename)
        .with_column_names(nyc_colnames())
        .with_extractors(&nyc_extractors())
        .with_always_string(&["vendor_id", "store_and_fwd_flag", "payment_type", "pickup_ntaname"])
}

pub fn ingest_reduced_file(file_path: &str, tablename: &str) -> Options {
    Options::new(file_path, tablename)
        .with_column_names(nyc_colnames())
        .with_extractors(&nyc_extractors())
        .with_ignore_cols(&dropped_cols())
        .with_always_string(&["vendor_id", "store_and_fwd_flag", "payment_type", "pickup_ntaname"])
}

pub fn ingest_passenger_count(file_path: &str, tablename: &str) -> Options {
    let drop = nyc_colnames().iter()
        .map(|x| x.to_string())
        .filter(|x| x != "passenger_count")
        .collect::<Vec<_>>();
    Options::new(file_path, tablename)
        .with_column_names(nyc_colnames())
        .with_ignore_cols(&drop)
}
