use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use actix_cors::Cors;
use actix_web::web::{Bytes, Data};
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use futures::channel::oneshot::Canceled;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tera::{Context, Tera};

use crate::ingest::raw_val::RawVal;
use crate::{logging_client, LocustDB};
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
struct MultiQueryRequest {
    queries: Vec<String>,
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
        .run_query(&req_body.query, false, vec![])
        .await
        .unwrap()
        .unwrap();

    let response = json!({
        "colnames": result.colnames,
        "rows": result.rows.iter().map(|row| row.iter().map(|val| match val {
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
    let x = data.db.run_query(&req_body.query, false, vec![]).await;
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
    let mut results = vec![];
    for q in &req_body.queries {
        // Run query immediately starts executing without awaiting future
        let result = data.db.run_query(q, false, vec![]);
        results.push(result);
    }
    let mut json_results = vec![];
    for result in results {
        let result = match flatmap_err_response(result.await) {
            Ok(result) => result,
            Err(err) => return err,
        };
        json_results.push(query_output_to_json_cols(result));
    }
    HttpResponse::Ok().json(json_results)
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

#[post("/insert")]
async fn insert(data: web::Data<AppState>, req_body: web::Json<DataBatch>) -> impl Responder {
    let total_network_bytes = req_body.0.rows.iter().fold(0, |acc, row| {
        acc + row
            .iter()
            .fold(0, |acc, (k, v)| acc + k.len() + v.to_string().len())
    });
    data.db
        .perf_counter()
        .network_read_ingestion(total_network_bytes as u64);
    let DataBatch { table, rows } = req_body.0;
    log::debug!("Inserting {} rows into {}", rows.len(), table);
    data.db
        .ingest(
            &table,
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|(colname, val)| {
                            let val = match val {
                                serde_json::Value::Null => RawVal::Null,
                                serde_json::Value::Number(n) => {
                                    if n.is_i64() {
                                        RawVal::Int(n.as_i64().unwrap())
                                    } else if n.is_f64() {
                                        RawVal::Float(OrderedFloat(n.as_f64().unwrap()))
                                    } else {
                                        panic!("Unsupported number {}", n)
                                    }
                                }
                                serde_json::Value::String(s) => RawVal::Str(s),
                                _ => panic!("Unsupported value: {:?}", val),
                            };
                            (colname, val)
                        })
                        .collect()
                })
                .collect(),
        )
        .await;
    log::debug!("Succesfully appended to {}", table);
    HttpResponse::Ok().json(r#"{"status": "ok"}"#)
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

    let mut events: logging_client::EventBuffer = bincode::deserialize(&req_body).unwrap();
    for (name, table) in &mut events.tables {
        let len = table.len;
        log::debug!("Inserting {} rows into {}", table.len, name);

        // TODO: efficiency
        let mut rows = Vec::new();
        for i in 0..table.len {
            let mut row = Vec::new();
            for (colname, col) in &mut table.columns {
                match &mut col.data {
                    logging_client::ColumnData::Dense(data) => {
                        row.push((colname.to_string(), RawVal::Float(OrderedFloat(data.pop().unwrap()))))
                    }
                    logging_client::ColumnData::Sparse(data) => {
                        if data.last().map(|(j, _)| *j == len - i - 1).unwrap_or(false) {
                            row.push((colname.to_string(), RawVal::Float(OrderedFloat(data.pop().unwrap().1))));
                        }
                    }
                }
            }
            rows.push(row);
        }
        rows.reverse();

        data.db.ingest(name, rows).await;
        log::debug!("Succesfully appended to {}", name);
    }
    HttpResponse::Ok().json(r#"{"status": "ok"}"#)
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

fn query_output_to_json_cols(result: QueryOutput) -> serde_json::Value {
    let mut cols: HashMap<String, Vec<serde_json::Value>> = HashMap::default();
    for col in &result.colnames {
        cols.insert(col.to_string(), vec![]);
    }
    for row in result.rows {
        for (val, colname) in row.iter().zip(result.colnames.iter()) {
            cols.get_mut(colname).unwrap().push(match val {
                Value::Int(int) => json!(int),
                Value::Str(str) => json!(str),
                Value::Null => json!(null),
                Value::Float(f) => json!(f.0),
            });
        }
    }
    json!({
        "colnames": result.colnames,
        "cols": cols,
        "stats": result.stats,
    })
}

pub async fn run(db: Arc<LocustDB>) -> std::io::Result<()> {
    HttpServer::new(move || {
        let app_state = AppState { db: db.clone() };
        let cors = Cors::default()
            .allowed_origin("http://localhost:5173")
            .allowed_origin("http://localhost:8080")
            .allowed_methods(vec!["GET", "POST", "OPTIONS"]) // Ensure OPTIONS is allowed
            .allowed_headers(vec!["Authorization", "Accept"])
            .allowed_header(actix_web::http::header::CONTENT_TYPE)
            .max_age(3600);
        App::new()
            .wrap(cors)
            .app_data(Data::new(app_state))
            .app_data(Data::new(web::PayloadConfig::new(512 * 1024 * 1024)))
            .service(index)
            .service(echo)
            .service(tables)
            .service(query)
            .service(table_handler)
            .service(insert)
            .service(insert_bin)
            .service(query_data)
            .service(query_cols)
            .service(multi_query_cols)
            .service(plot)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
