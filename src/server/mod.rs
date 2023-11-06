use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use actix_cors::Cors;
use actix_web::web::{Bytes, Data};
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tera::{Context, Tera};

use crate::ingest::raw_val::RawVal;
use crate::{Value, QueryOutput};
use crate::{logging_client, LocustDB};

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
    let cols = data
        .db
        .run_query(
            &format!("SELECT * FROM {} LIMIT 0", path.as_str()),
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
    log::info!("Query: {:?}", req_body);
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
    log::info!("Query: {:?}", req_body);
    let result = data
        .db
        .run_query(&req_body.query, false, vec![])
        .await
        .unwrap()
        .unwrap();

    let response = query_output_to_json_cols(result);
    HttpResponse::Ok().json(response)
}

#[post("/multi_query_cols")]
async fn multi_query_cols(
    data: web::Data<AppState>,
    req_body: web::Json<MultiQueryRequest>,
) -> impl Responder {
    log::info!("Multi Query: {:?}", req_body);
    let mut results = vec![];
    for q in &req_body.queries {
        // Run query immediately starts executing without awaiting future
        let result = data
            .db
            .run_query(q, false, vec![]);
        results.push(result);
    }
    let mut json_results = vec![];
    for result in results {
        let result = result.await.unwrap().unwrap();
        json_results.push(query_output_to_json_cols(result));
    }
    HttpResponse::Ok().json(json_results)
}

// TODO: efficient endpoint
#[post("/insert")]
async fn insert(data: web::Data<AppState>, req_body: web::Json<DataBatch>) -> impl Responder {
    let DataBatch { table, rows } = req_body.0;
    log::info!("Inserting {} rows into {}", rows.len(), table);
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
    log::info!("Succesfully appended to {}", table);
    HttpResponse::Ok().json(r#"{"status": "ok"}"#)
}

// TODO: more efficient columnar endpoint
#[post("/insert_bin")]
async fn insert_bin(data: web::Data<AppState>, req_body: Bytes) -> impl Responder {
    // PRINT FIRST 64 BYTES
    let mut bytes = req_body.clone();
    let mut s = String::new();
    for _ in 0..64 {
        s.push_str(&format!("{:02x}", bytes[0]));
        bytes = bytes.slice(1..);
    }
    log::info!("Inserting bytes: {} ({})", s, req_body.len());

    let logging_client::DataBatch { table, rows } = bincode::deserialize(&req_body).unwrap();
    log::info!("Inserting {} rows into {}", rows.len(), table);
    data.db
        .ingest(
            &table,
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|(k, v)| (k, RawVal::Float(OrderedFloat(v))))
                        .collect()
                })
                .collect(),
        )
        .await;
    log::info!("Succesfully appended to {}", table);
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


pub async fn run(db: LocustDB) -> std::io::Result<()> {
    let db = Arc::new(db);
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
            .app_data(Data::new(web::PayloadConfig::new(100 * 1024 * 1024)))
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
