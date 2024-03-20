use actix_web::dev::ServerHandle;
use tempfile::tempdir;
use pretty_assertions::assert_eq;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use locustdb::{BasicTypeColumn, LocustDB};
use locustdb::{value_syntax::*, QueryOutput};
use rand::{Rng, SeedableRng};

// Need multiple threads since dropping logging client blocks main thread and prevents logging worker from flushing buffers
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ingestion() {
    env_logger::init();

    let db_path: PathBuf = tempdir().unwrap().path().into();
    log::info!("Creating LocustDB at {:?}", db_path);
    let (db, handle) = create_locustdb(db_path.clone());

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
            vec![[Float(i as f64), Float((total_rows * (total_rows - 1) / 2) as f64), Int(total_rows as i64)],]
        );
    }

    let old_all = query(&db, &format!("SELECT * FROM {}", &tables[7])).await;
    assert_eq!(old_all.rows.as_ref().unwrap().len(), total_rows);
    // row, table_id, _timestamp, 10 random columns
    assert_eq!(old_all.rows.unwrap()[0].len(), 13);

    handle.stop(true).await;
    drop(db);
    let (mut db, mut handle) = create_locustdb(db_path.clone());
    let new_all = query(&db, &format!("SELECT * FROM {}", &tables[7])).await;
    assert_eq!(new_all.rows.unwrap().len(), total_rows);
    assert_eq!(old_all.colnames.len(), new_all.colnames.len());
    let row_col = &new_all.columns.iter().find(|(name, _)| name == "row").unwrap().1;
    assert_eq!(*row_col, BasicTypeColumn::Float((0..total_rows).map(|i| i as f64).collect()));
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
        (db, handle) = create_locustdb(db_path.clone());
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
    let (db, _) = create_locustdb(db_path.clone());
    let new_all = query(&db, &format!("SELECT * FROM {}", &tables[7])).await;
    assert_eq!(new_all.rows.unwrap().len(), total_rows);
    assert_eq!(old_all.colnames.len(), new_all.colnames.len());
    let row_col = &new_all.columns.iter().find(|(name, _)| name == "row").unwrap().1;
    assert_eq!(*row_col, BasicTypeColumn::Float((0..total_rows).map(|i| i as f64).collect()));
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
    let mut log =
        locustdb::logging_client::LoggingClient::new(Duration::from_secs(1), addr, 64 * (1 << 20));
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
    log::info!("Logged {} rows in {:?}", rows * tables.len(), start_time.elapsed());
}

async fn query(db: &LocustDB, query: &str) -> QueryOutput {
    db.run_query(query, false, true, vec![])
        .await
        .unwrap()
        .unwrap()
}

fn create_locustdb(db_path: PathBuf) -> (Arc<locustdb::LocustDB>, ServerHandle) {
    let options = locustdb::Options {
        db_path: Some(db_path),
        ..locustdb::Options::default()
    };
    let db = Arc::new(locustdb::LocustDB::new(&options));
    let _locustdb = db.clone();
    let (handle, _) =
        locustdb::server::run(_locustdb, false, vec![], "localhost:8888".to_string()).unwrap();
    (db, handle)
}
