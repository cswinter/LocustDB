use actix_web::dev::ServerHandle;
use locustdb::logging_client::BufferFullPolicy;
use pretty_assertions::assert_eq;
use tempfile::tempdir;

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use locustdb::{value_syntax::*, QueryOutput};
use locustdb::{BasicTypeColumn, LocustDB};
use rand::{Rng, SeedableRng};

// Need multiple threads since dropping logging client blocks main thread and prevents logging worker from flushing buffers
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ingestion() {
    let _ = env_logger::try_init();

    let db_path: PathBuf = tempdir().unwrap().path().into();
    log::info!("Creating LocustDB at {:?}", db_path);
    let opts = locustdb::Options {
        db_path: Some(db_path),
        ..locustdb::Options::default()
    };
    let port = 8888;
    let (db, handle) = create_locustdb(&opts, port);

    let tables = (0..20)
        .map(|i| format!("table_{:02}", i))
        .collect::<Vec<String>>();

    let mut total_rows = 0;
    ingest(total_rows, 127, 10, &tables);
    total_rows += 127;
    log::info!("completed ingestion");

    for (i, table) in tables.iter().enumerate() {
        let id_sum = query(
            &db,
            &format!("SELECT table_id, SUM(row), COUNT(1) FROM {}", table),
        )
        .await;

        assert_eq!(
            id_sum.rows.unwrap(),
            vec![[
                Float(i as f64),
                Float((total_rows * (total_rows - 1) / 2) as f64),
                Int(total_rows as i64)
            ],]
        );
    }

    let old_all = query(&db, &format!("SELECT * FROM {}", &tables[7])).await;
    assert_eq!(old_all.rows.as_ref().unwrap().len(), total_rows);
    // row, table_id, _timestamp, 10 random columns
    assert_eq!(old_all.rows.unwrap()[0].len(), 13);

    handle.stop(true).await;
    drop(db);
    let (mut db, mut handle) = create_locustdb(&opts, port);
    let new_all = query(&db, &format!("SELECT * FROM {}", &tables[7])).await;
    assert_eq!(new_all.rows.unwrap().len(), total_rows);
    assert_eq!(old_all.colnames.len(), new_all.colnames.len());
    let row_col = &new_all
        .columns
        .iter()
        .find(|(name, _)| name == "row")
        .unwrap()
        .1;
    assert_eq!(
        *row_col,
        BasicTypeColumn::Float((0..total_rows).map(|i| i as f64).collect())
    );
    let old_columns: HashMap<_, _> = old_all.columns.into_iter().collect();
    for (name, column) in &new_all.columns {
        assert_eq!(old_columns[name], *column, "Mismatch in column {}", name);
    }

    for i in 0..30 {
        let start_time = Instant::now();
        handle.stop(true).await;
        log::info!("Stopped server in {:?}", start_time.elapsed());

        let start_time = Instant::now();
        drop(db);
        log::info!("Dropped db in {:?}", start_time.elapsed());

        let start_time = Instant::now();
        (db, handle) = create_locustdb(&opts, port);
        log::info!("Created db in {:?}", start_time.elapsed());

        let start_time = Instant::now();
        let new_rows = 21 + 11 * i;
        ingest(total_rows, new_rows, i, &tables);
        log::info!("Ingested {} rows in {:?}", new_rows, start_time.elapsed());

        let start_time = Instant::now();
        total_rows += new_rows;
        test_db(&db, total_rows, &tables).await;
        log::info!("Tested db in {:?}", start_time.elapsed());

        if i % 7 == 0 {
            let start_time = Instant::now();
            db.force_flush();
            log::info!("Forced flush in {:?}", start_time.elapsed());
        }
    }

    test_db(&db, total_rows, &tables).await;

    let old_all = query(&db, &format!("SELECT * FROM {}", &tables[7])).await;
    handle.stop(true).await;
    drop(db);
    let (db, _) = create_locustdb(&opts, port);
    let new_all = query(&db, &format!("SELECT * FROM {}", &tables[7])).await;
    assert_eq!(new_all.rows.unwrap().len(), total_rows);
    assert_eq!(old_all.colnames.len(), new_all.colnames.len());
    let row_col = &new_all
        .columns
        .iter()
        .find(|(name, _)| name == "row")
        .unwrap()
        .1;
    assert_eq!(
        *row_col,
        BasicTypeColumn::Float((0..total_rows).map(|i| i as f64).collect())
    );
    let old_columns: HashMap<_, _> = old_all.columns.into_iter().collect();
    for (name, column) in &new_all.columns {
        assert_eq!(old_columns[name], *column, "Mismatch in column {}", name);
    }
}

async fn test_db(db: &LocustDB, nrow: usize, tables: &[String]) {
    for (i, table) in tables.iter().enumerate() {
        let id_sum = query(
            db,
            &format!("SELECT table_id, SUM(row), COUNT(1) FROM {}", table),
        )
        .await;

        assert_eq!(
            id_sum.rows.unwrap(),
            vec![[
                Float(i as f64),
                Float((nrow * (nrow - 1) / 2) as f64),
                Int(nrow as i64)
            ],]
        );
    }
}

fn ingest(offset: usize, rows: usize, random_cols: usize, tables: &[String]) {
    let start_time = Instant::now();
    log::info!("Ingesting {rows} rows into {} tables", tables.len());
    let addr = "http://localhost:8888";
    let mut log = locustdb::logging_client::LoggingClient::new(
        Duration::from_secs(1),
        addr,
        64 * (1 << 20),
        BufferFullPolicy::Block,
    );
    let mut rng = rand::rngs::SmallRng::seed_from_u64(0);
    for row in 0..rows {
        for (i, table) in tables.iter().enumerate() {
            let mut row = vec![
                ("row".to_string(), (row + offset) as f64),
                ("table_id".to_string(), i as f64),
            ];
            for c in 0..random_cols {
                row.push((format!("col_{c}"), rng.gen::<f64>()));
            }
            log.log(table, row);
        }
    }
    log::info!(
        "Logged {} rows in {:?}",
        rows * tables.len(),
        start_time.elapsed()
    );
}

async fn query(db: &LocustDB, query: &str) -> QueryOutput {
    db.run_query(query, false, true, vec![])
        .await
        .unwrap()
        .unwrap()
}

fn create_locustdb(opts: &locustdb::Options, port: u16) -> (Arc<locustdb::LocustDB>, ServerHandle) {
    let db = Arc::new(locustdb::LocustDB::new(opts));
    let _locustdb = db.clone();
    let (handle, _) =
        locustdb::server::run(_locustdb, false, vec![], format!("localhost:{port}")).unwrap();
    (db, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ingest_sparse_nullable() {
    let _ = env_logger::try_init();

    let db_path: PathBuf = tempdir().unwrap().path().into();
    let opts = locustdb::Options {
        db_path: Some(db_path),
        threads: 1,
        batch_size: 8,
        ..locustdb::Options::default()
    };
    let port = 8889;
    let (db, _handle) = create_locustdb(&opts, port);

    let addr = format!("http://localhost:{port}");
    let mut log = locustdb::logging_client::LoggingClient::new(
        Duration::from_micros(10),
        &addr,
        // Set max buffer size to 0 to ensure we ingest one row at a time
        0,
        BufferFullPolicy::Block,
    );
    let mut rng = rand::rngs::SmallRng::seed_from_u64(0);
    let mut vals = vec![];
    let interval = 7;
    for i in 0..15 {
        let mut row = vec![("row".to_string(), i as f64)];
        if i % interval == 0 {
            let val = rng.gen::<f64>();
            vals.push(val);
            row.push(("sparse_float".to_string(), val));
        }
        log.log("default", row);
    }
    drop(log);

    let query = "SELECT row, sparse_float FROM default WHERE row IS NOT NULL AND (sparse_float IS NOT NULL)";
    let query2 = "SELECT row, sparse_float FROM default WHERE (sparse_float IS NOT NULL)";
    let show = if env::var("DEBUG_TESTS").is_ok() {
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    } else {
        vec![]
    };
    let all_nonzero = db
        .run_query(query, false, true, show.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        all_nonzero.rows.as_ref().unwrap(),
        &vals
            .iter()
            .enumerate()
            .map(|(i, &v)| vec![Float((i * interval) as f64), Float(v)])
            .collect::<Vec<_>>()
    );
    let all_nonzero2 = db
        .run_query(query2, false, true, show)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(all_nonzero.rows.unwrap(), all_nonzero2.rows.unwrap(),);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_persist_meta_tables() {
    let _ = env_logger::try_init();

    let db_path: PathBuf = tempdir().unwrap().path().into();
    let opts = locustdb::Options {
        db_path: Some(db_path),
        threads: 1,
        ..locustdb::Options::default()
    };
    let port = 8890;
    let (db, _handle) = create_locustdb(&opts, port);

    let addr = format!("http://localhost:{port}");
    let mut log = locustdb::logging_client::LoggingClient::new(
        Duration::from_micros(10),
        &addr,
        0,
        BufferFullPolicy::Block,
    );
    log.log("qwerty", [("value".to_string(), 1.0)]);
    log.log("asdf", [("value".to_string(), 1.0)]);
    drop(log);
    drop(db);
    _handle.stop(true).await;

    let (db, _handle) = create_locustdb(&opts, port);
    let query = "SELECT name FROM _meta_tables";
    let show = if env::var("DEBUG_TESTS").is_ok() {
        vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    } else {
        vec![]
    };
    let _meta_tables = db
        .run_query(query, false, true, show.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        _meta_tables.rows.as_ref().unwrap(),
        &[[Str("qwerty")], [Str("asdf")]],
    );
}
