use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;
use std::thread;

use actix_cors::Cors;
use actix_web::dev::ServerHandle;
use actix_web::web::{Bytes, Data};
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use futures::channel::oneshot::Canceled;
use itertools::Itertools;
use locustdb_compression_utils::column;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tera::{Context, Tera};
use tokio::sync::oneshot;

use crate::{logging_client, BasicTypeColumn, LocustDB};
use crate::{QueryError, QueryOutput, Value};

lazy_static! {
    pub static ref TEMPLATES: Tera = {
        let mut tera = match Tera::new("templates/**/*") {
            Ok(t) => t,
            Err(e) => {
                println!("Parsing error(s): {}", e);
                ::std::process::exit(1);
            }
        };
        tera.autoescape_on(vec!["html", ".sql"]);
        // tera.register_filter("do_nothing", do_nothing_filter);
        tera
    };
}

#[derive(Serialize, Deserialize, Debug)]
struct DataBatch {
    pub table: String,
    pub rows: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Clone)]
struct AppState {
    db: Arc<LocustDB>,
}

#[derive(Serialize, Deserialize, Debug)]
struct QueryRequest {
    query: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ColumnNameRequest {
    tables: Vec<String>,
    pattern: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
struct MultiQueryRequest {
    queries: Vec<String>,
    encoding_opts: Option<EncodingOpts>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub columns: HashMap<String, column::Column>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EncodingOpts {
    pub xor_float_compression: bool,
    pub mantissa: Option<u32>,
    pub full_precision_cols: HashSet<String>,
}

#[get("/")]
async fn index(data: web::Data<AppState>) -> impl Responder {
    let mut context = Context::new();
    let mut ts: Vec<String> = data
        .db
        .table_stats()
        .await
        .unwrap()
        .into_iter()
        .map(|ts| ts.name)
        .collect::<Vec<_>>();
    ts.sort();
    context.insert("tables", &ts);
    let body = TEMPLATES.render("index.html", &context).unwrap();
    HttpResponse::Ok()
        .content_type("text/html; charset=utf8")
        .body(body)
}

#[get("/plot")]
async fn plot(_data: web::Data<AppState>) -> impl Responder {
    let context = Context::new();
    let body = TEMPLATES.render("plot.html", &context).unwrap();
    HttpResponse::Ok()
        .content_type("text/html; charset=utf8")
        .body(body)
}

#[get("/table/{tablename}")]
async fn table_handler(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    // TODO: sql injection
    let cols = data
        .db
        .run_query(
            &format!("SELECT * FROM \"{}\" LIMIT 0", path.as_str()),
            false,
            true,
            vec![],
        )
        .await
        .unwrap()
        .unwrap()
        .colnames;

    let mut context = Context::new();
    context.insert("columns", &cols.join(", "));
    context.insert("table", path.as_str());
    let body = TEMPLATES.render("table.html", &context).unwrap();

    HttpResponse::Ok()
        .content_type("text/html; charset=utf8")
        .body(body)
}

#[get("/tables")]
async fn tables(data: web::Data<AppState>) -> impl Responder {
    println!("Requesting table stats");
    let stats = data.db.table_stats().await.unwrap();

    let mut total_buffer_bytes = 0;
    let mut total_bytes = 0;
    let mut total_rows = 0;
    for table in &stats {
        total_buffer_bytes += table.buffer_bytes;
        total_bytes += table.batches_bytes + table.buffer_bytes;
        total_rows += table.rows;
    }

    let mut body = String::new();
    writeln!(body, "Total rows: {}", total_rows).unwrap();
    writeln!(body, "Total bytes: {}", total_bytes).unwrap();
    writeln!(body, "Total buffer bytes: {}", total_buffer_bytes).unwrap();
    for table in &stats {
        writeln!(body, "{}", table.name).unwrap();
        writeln!(body, "  Rows: {}", table.rows).unwrap();
        writeln!(body, "  Batches: {}", table.batches).unwrap();
        writeln!(body, "  Batches bytes: {}", table.batches_bytes).unwrap();
        writeln!(body, "  Buffer length: {}", table.buffer_length).unwrap();
        writeln!(body, "  Buffer bytes: {}", table.buffer_bytes).unwrap();
        //writeln!(body, "  Size per column: {}", table.size_per_column).unwrap();
    }
    HttpResponse::Ok().body(body)
}

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

#[get("/query_data")]
async fn query_data(_data: web::Data<AppState>) -> impl Responder {
    let response = json!({
        "cols": ["time", "cpu"],
        "series": [
            [1640025197013.0, 1640025198013.0, 1640025199013.0, 1640025200013.0, 1640025201013.0, 1640025202113.0, 1640025203113.0, 1640025204113.0, 1640025205113.0],
            [0.3, 0.4, 0.5, 0.2, 0.1, 0.3, 0.4, 0.5, 0.2]
        ]
    });
    HttpResponse::Ok().json(response)
}

#[post("/query")]
async fn query(data: web::Data<AppState>, req_body: web::Json<QueryRequest>) -> impl Responder {
    log::debug!("Query: {:?}", req_body);
    let result = data
        .db
        .run_query(&req_body.query, false, true, vec![])
        .await
        .unwrap()
        .unwrap();

    let response = json!({
        "colnames": result.colnames,
        "rows": result.rows.unwrap().iter().map(|row| row.iter().map(|val| match val {
            Value::Int(int) => json!(int),
            Value::Str(str) => json!(str),
            Value::Null => json!(null),
            Value::Float(float) => json!(float.0),
        }).collect::<Vec<_>>()).collect::<Vec<_>>(),
        "stats": result.stats,
    });
    HttpResponse::Ok().json(response)
}

#[post("/query_cols")]
async fn query_cols(
    data: web::Data<AppState>,
    req_body: web::Json<QueryRequest>,
) -> impl Responder {
    log::debug!("Query: {:?}", req_body);
    let x = data
        .db
        .run_query(&req_body.query, false, false, vec![])
        .await;
    match flatmap_err_response(x) {
        Ok(result) => {
            let response = query_output_to_json_cols(result);
            HttpResponse::Ok().json(response)
        }
        Err(err) => err,
    }
}

#[post("/multi_query_cols")]
async fn multi_query_cols(
    data: web::Data<AppState>,
    req_body: web::Json<MultiQueryRequest>,
) -> impl Responder {
    log::debug!("Multi Query: {:?}", req_body);
    let mut futures = vec![];
    for q in &req_body.queries {
        // Run query starts executing immediately even without awaiting future
        let result = data.db.run_query(q, false, false, vec![]);
        futures.push(result);
    }
    let mut results = vec![];
    for future in futures {
        let result = match flatmap_err_response(future.await) {
            Ok(result) => result,
            Err(err) => return err,
        };
        results.push(result);
    }
    match &req_body.encoding_opts {
        Some(encoding_opts) => {
            let full_precision = EncodingOpts {
                mantissa: None,
                ..encoding_opts.clone()
            };
            let mut query_responses = vec![];
            for result in results {
                query_responses.push(QueryResponse {
                    columns: result
                        .columns
                        .into_iter()
                        .map(|(colname, data)| {
                            let use_full_precision =
                                encoding_opts.full_precision_cols.contains(&colname);
                            (
                                colname,
                                encode_column(
                                    data,
                                    if use_full_precision {
                                        &full_precision
                                    } else {
                                        encoding_opts
                                    },
                                ),
                            )
                        })
                        .collect(),
                });
            }
            let serialized = bincode::serialize(&query_responses).unwrap();
            HttpResponse::Ok().body(serialized)
        }
        None => {
            let json_results = results
                .into_iter()
                .map(query_output_to_json_cols)
                .collect::<Vec<_>>();
            HttpResponse::Ok().json(json_results)
        }
    }
}

#[post("/columns")]
async fn columns(
    data: web::Data<AppState>,
    req_body: web::Json<ColumnNameRequest>,
) -> impl Responder {
    let mut cols = HashSet::new();
    let pattern = req_body.pattern.clone().unwrap_or("".to_string());
    for table in &req_body.tables {
        cols.extend(data.db.search_column_names(table, &pattern));
    }
    let len = cols.len();
    let limit = req_body.limit.unwrap_or(usize::MAX);
    let offset = req_body.offset.unwrap_or(0).min(len.saturating_sub(limit));
    HttpResponse::Ok().json(json!({
        "columns": cols.iter().cloned().sorted().into_iter().skip(offset).take(limit).collect::<Vec<_>>(),
        "offset": offset,
        "len": len,
    }))
}

fn flatmap_err_response(
    err: Result<Result<QueryOutput, QueryError>, Canceled>,
) -> Result<QueryOutput, HttpResponse> {
    match err {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(QueryError::NotImplemented(msg))) => Err(HttpResponse::NotImplemented().json(msg)),
        Ok(Err(QueryError::FatalError(msg, bt))) => {
            Err(HttpResponse::InternalServerError().json((msg, bt.to_string())))
        }
        Ok(Err(err)) => Err(HttpResponse::BadRequest().json(err.to_string())),
        Err(err) => Err(HttpResponse::InternalServerError().json(err.to_string())),
    }
}

// TODO: even more efficient, push all data-conversions into client
#[post("/insert_bin")]
async fn insert_bin(data: web::Data<AppState>, req_body: Bytes) -> impl Responder {
    // PRINT FIRST 64 BYTES
    let mut bytes = req_body.clone();
    let mut s = String::new();
    for _ in 0..64 {
        if bytes.is_empty() {
            break;
        }
        s.push_str(&format!("{:02x}", bytes[0]));
        bytes = bytes.slice(1..);
    }
    log::debug!("Inserting bytes: {} ({})", s, req_body.len());

    data.db
        .perf_counter()
        .network_read_ingestion(req_body.len() as u64);

    let events: logging_client::EventBuffer = match bincode::deserialize(&req_body) {
        Ok(events) => events,
        Err(e) => {
            log::error!("Failed to deserialize /insert_bin request: {}", e);
            return HttpResponse::BadRequest()
                .json(format!("Failed to deserialize request: {}", e));
        }
    };
    log::info!(
        "Received request data for {} events",
        events
            .tables
            .values()
            .map(|t| t.columns.values().next().map(|c| c.data.len()).unwrap_or(0))
            .sum::<usize>()
    );
    data.db.ingest_efficient(events).await;
    HttpResponse::Ok().json(r#"{"status": "ok"}"#)
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

fn query_output_to_json_cols(result: QueryOutput) -> serde_json::Value {
    let mut cols: HashMap<String, serde_json::Value> = HashMap::default();
    for (colname, data) in result.columns {
        let json_data = match data {
            BasicTypeColumn::Int(xs) => json!(xs),
            BasicTypeColumn::Float(xs) => json!(xs),
            BasicTypeColumn::String(xs) => json!(xs),
            BasicTypeColumn::Null(xs) => json!(xs),
            BasicTypeColumn::Mixed(xs) => json!(xs
                .into_iter()
                .map(|val| match val {
                    Value::Int(int) => json!(int),
                    Value::Str(str) => json!(str),
                    Value::Null => json!(null),
                    Value::Float(f) => json!(f.0),
                })
                .collect::<Vec<_>>()),
        };
        cols.insert(colname, json_data);
    }
    json!({
        "colnames": result.colnames,
        "cols": cols,
        "stats": result.stats,
    })
}

pub fn run(
    db: Arc<LocustDB>,
    cors_allow_all: bool,
    cors_allow_origin: Vec<String>,
    addrs: String,
) -> std::io::Result<(ServerHandle, oneshot::Receiver<()>)> {
    let server = HttpServer::new(move || {
        let cors = if cors_allow_all {
            Cors::permissive()
        } else {
            let mut cors = Cors::default()
                .allowed_methods(vec!["GET", "POST", "OPTIONS"])
                .allowed_headers(vec!["Authorization", "Accept"])
                .allowed_header(actix_web::http::header::CONTENT_TYPE)
                .max_age(3600);
            for origin in &cors_allow_origin {
                cors = cors.allowed_origin(origin);
            }
            cors
        };
        let app_state = AppState { db: db.clone() };
        App::new()
            .wrap(cors)
            .app_data(Data::new(app_state))
            .app_data(Data::new(web::PayloadConfig::new(512 * 1024 * 1024)))
            .service(index)
            .service(echo)
            .service(tables)
            .service(query)
            .service(table_handler)
            // .service(insert)
            .service(insert_bin)
            .service(query_data)
            .service(query_cols)
            .service(multi_query_cols)
            .service(columns)
            .service(plot)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind(&addrs)?
    .run();

    let (tx, rx) = oneshot::channel();

    let handle = server.handle();
    thread::spawn(move || {
        actix_web::rt::System::new().block_on(server).unwrap();
        let _ = tx.send(());
    });

    Ok((handle, rx))
}

fn encode_column(col: BasicTypeColumn, encode_opts: &EncodingOpts) -> column::Column {
    match col {
        BasicTypeColumn::Int(xs) => column::Column::Int(xs),
        BasicTypeColumn::Float(xs) => {
            //let xs = unsafe { mem::transmute::<Vec<f64>, Vec<OrderedFloat<f64>>>(xs) };
            if encode_opts.xor_float_compression {
                column::Column::compress(xs, encode_opts.mantissa)
            } else {
                column::Column::Float(xs)
            }
        }
        BasicTypeColumn::String(xs) => column::Column::String(xs),
        BasicTypeColumn::Null(xs) => column::Column::Null(xs),
        BasicTypeColumn::Mixed(xs) => column::Column::Mixed(
            xs.into_iter()
                .map(|val| match val {
                    Value::Int(int) => column::Mixed::Int(int),
                    Value::Str(str) => column::Mixed::Str(str),
                    Value::Null => column::Mixed::Null,
                    Value::Float(float) => column::Mixed::Float(float.0),
                })
                .collect(),
        ),
    }
}
