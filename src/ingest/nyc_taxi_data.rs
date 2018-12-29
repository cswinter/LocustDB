use ingest::csv_loader::Options;

pub fn reduced_nyc_schema() -> String {
    "trip_id:i,\
    vendor_id:s,\
    pickup_datetime:i.date,\
    dropoff_datetime:,\
    store_and_fwd_flag:s,\
    rate_code_id:s,\
    pickup_longitude:,\
    pickup_latitude:,\
    dropoff_longitude:,\
    dropoff_latitude:,\
    passenger_count:i,\
    trip_distance:i.1000,\
    fare_amount:,\
    extra:,\
    mta_tax:i.100,\
    tip_amount:,\
    tolls_amount:,\
    ehail_fee:i.100,\
    improvement_surcharge:i.100,\
    total_amount:i.100,\
    payment_type:s,\
    trip_type:s,\
    pickup:,\
    dropoff:,\
    cab_type:s,\
    precipitation:,\
    snow_depth:,\
    snowfall:i.1000,\
    max_temperature:,\
    min_temperature:,\
    average_wind_speed:,\
    pickup_nyct2010_gid:,\
    pickup_ctlabel:,\
    pickup_borocode:,\
    pickup_boroname:,\
    pickup_ct2010:,\
    pickup_boroct2010:,\
    pickup_cdeligibil:s,\
    pickup_ntacode:,\
    pickup_ntaname:s,\
    pickup_puma:i,\
    dropoff_nyct2010_gid:,\
    dropoff_ctlabel:,\
    dropoff_borocode:,\
    dropoff_boroname:,\
    dropoff_ct2010:,\
    dropoff_boroct2010:,\
    dropoff_cdeligibil:,\
    dropoff_ntacode:,\
    dropoff_ntaname:,\
    dropoff_puma:i".to_string()
}

pub fn nyc_schema() -> String {
    "trip_id:i,\
    vendor_id:s,\
    pickup_datetime:i.date,\
    dropoff_datetime:i.date,\
    store_and_fwd_flag:s,\
    rate_code_id:s,\
    pickup_longitude:s,\
    pickup_latitude:s,\
    dropoff_longitude:s,\
    dropoff_latitude:s,\
    passenger_count:i,\
    trip_distance:i.1000,\
    fare_amount:i.100,\
    extra:i.100,\
    mta_tax:i.100,\
    tip_amount:i.100,\
    tolls_amount:i.100,\
    ehail_fee:i.100,\
    improvement_surcharge:i.100,\
    total_amount:i.100,\
    payment_type:s,\
    trip_type:s,\
    pickup:s,\
    dropoff:s,\
    cab_type:s,\
    precipitation:i.1000,\
    snow_depth:i.1000,\
    snowfall:i.1000,\
    max_temperature:s,\
    min_temperature:s,\
    average_wind_speed:i.1000,\
    pickup_nyct2010_gid:s,\
    pickup_ctlabel:s,\
    pickup_borocode:s,\
    pickup_boroname:s,\
    pickup_ct2010:s,\
    pickup_boroct2010:s,\
    pickup_cdeligibil:s,\
    pickup_ntacode:s,\
    pickup_ntaname:s,\
    pickup_puma:i,\
    dropoff_nyct2010_gid:s,\
    dropoff_ctlabel:s,\
    dropoff_borocode:s,\
    dropoff_boroname:s,\
    dropoff_ct2010:s,\
    dropoff_boroct2010:s,\
    dropoff_cdeligibil:s,\
    dropoff_ntacode:s,\
    dropoff_ntaname:s,\
    dropoff_puma:i".to_string()
}

pub fn ingest_file(file_path: &str, tablename: &str) -> Options {
    Options::new(file_path, tablename)
        .with_schema(&nyc_schema())
}

pub fn ingest_reduced_file(file_path: &str, tablename: &str) -> Options {
    Options::new(file_path, tablename)
        .with_schema(&reduced_nyc_schema())
}

