#[macro_use] extern crate log;

extern crate blob;
extern crate certs;
extern crate config;
extern crate dlog;
extern crate eventual;
extern crate exclog;
extern crate exclog_pusher;
extern crate grpc;
extern crate http;
extern crate matrix;
extern crate proto_ruba;
extern crate rpc;
extern crate rufio;
extern crate rufio_dns;
extern crate rustc_serialize;
extern crate susanin;
extern crate time;
extern crate util;
extern crate pb;

extern crate ruba;


use blob::BlobPool;
use certs::{Credentials, CredentialsOptions};
use config::ConfigOptions;
use eventual::Async;
use exclog::errors::DropboxResult;
use grpc::Transport;
use http::options::ServerOptions;
use matrix::sdagent::SdAgentOptions;
use matrix::server::{MatrixServer, MatrixServerOptions};
use proto_ruba::service::*;
use proto_ruba::types::*;
use rpc::server::RpcServer;
use rpc::server::RpcService;
use std::convert::From;
use std::default::Default;
use std::sync::Arc;
use std::thread;
use susanin::{SusaninClient, SusaninClientOptions};
use util::options::OptionSet;
use util::options;
use rustc_serialize::json::Json;
use ruba::query_engine::QueryResult;
use ruba::mem_store::ingest::RawVal;

static SERVICE_NAME: &'static str = "ruba.leaf";
static RPC_SERVICE_NAME: &'static str = "ruba.Ruba";

static USAGE: &'static str = "
Usage:
    ruba -h | --help
    ruba --db-path=<path> [options]

General Options:
    -h, --help      Show this help message
    --db-path=<path>
                    Path to database
";

#[derive(RustcDecodable, Debug)]
struct RubaOptions {
    flag_db_path: String,
}


fn try_main() -> DropboxResult<()> {
    rufio::Reactor::run(|ctx| {

        // Setup options
        let mut log_opts = dlog::Options::default();
        let mut matrix_opts = MatrixServerOptions::default();
        let mut sdagent_opts = SdAgentOptions::default();
        let mut config_opts = ConfigOptions::default();
        let mut server_opts = ServerOptions::default();
        let mut susanin_opts = SusaninClientOptions::default();
        let mut creds_opts = CredentialsOptions::default();

        let ruba_options: RubaOptions = {
            let mut opts: Vec<&mut OptionSet> = vec![
                &mut log_opts,
                &mut matrix_opts,
                &mut sdagent_opts,
                &mut config_opts,
                &mut server_opts,
                &mut susanin_opts,
                &mut creds_opts,
            ];
            options::parse(USAGE, &mut opts).unwrap_or_else(|err| err.exit())
        };

        // TODO(clemens): Create grpc identity for Ruba and add to config file.
        if creds_opts.identity == "" {
            creds_opts.identity = "dropbox".to_string();
        }

        if matrix_opts.service_name == "" {
            matrix_opts.service_name = String::from(SERVICE_NAME);
        }

        // The default global_dev config isn't available
        // in the test environment. This allows us to pass
        // an empty string to circumvent config `init`.
        //if !config_opts.path.as_os_str().is_empty() {
        let config = config::init(config_opts);
        //}

        let logger = dlog::init(log_opts);
        init_exclog_trace();

        let hostname = util::net::hostname().unwrap();
        info!("Hostname: {}", hostname);

        let ruba_service = RubaRpcService::new(&ruba_options.flag_db_path);

        let mut rpc = RpcServer::new(ctx.clone(), None, &server_opts, BlobPool::empty());
        rpc.register(ruba_service);


        let (transport, susanin_client) = match matrix_opts.susanin_discoverable {
            true => {
                let transport = Arc::new(Transport::new());
                let credentials = Credentials::new(creds_opts)
                    .expect("Failed to initialize Credentials");
                let susanin_client = SusaninClient::new(transport.clone(), credentials, susanin_opts)
                    .expect("Failed to initialize SusaninClient");
                (Some(transport), Some(susanin_client))
            },
            false => (None, None),
        };

        info!("Starting Ruba Leaf...");
        let matrix_server = MatrixServer::new(
            ctx.clone(), rpc, &matrix_opts, &sdagent_opts, susanin_client
        ).expect("Failed to initialize Matrix Server...");


        // Initialize DNS Resolver.
        let resolver = rufio_dns::posix_resolver()
            .ok()
            .expect("Could not create resolver??");

        // Start the reporter last, so that it doesn't hang trying to push exceptions
        // when we've unwound the reactor::run stack!
        let exclog_reporter =
            exclog_pusher::PeriodicPusher::new(&config, ctx.clone(), resolver.clone());
        exclog::GLOBAL.add_named_tag("group_prefix", Json::String(SERVICE_NAME.to_owned()));
        exclog::GLOBAL.add_tag(&format!("service:{}", SERVICE_NAME));

        util::terminator::TERMINATOR.wait()
            .and_then(move |sig| {
                warn!("Received signal: {} Shutting down...", sig);
                drop(logger);
                drop(matrix_server);
                drop(transport);
                drop(exclog_reporter);
                ctx.quit();
            })
            .fire();
    });

    Ok(())
}

fn main() {
    match try_main() {
        Ok(_) => {
            println!("Bye!");
        },
        Err(err) => {
            println!("Ruba has crashed: {}", err);
            ::std::process::exit(1);
        },
    }
}


fn init_exclog_trace() {
    let mut trace_cfg = exclog::errors::TraceConfig::default();
    for http_crate in vec!["http", "rufio", "mio"] {
        trace_cfg
            .crate_groups
            .insert(http_crate.to_owned(), "http".to_owned());
    }
    for ignored_crate in vec!["stdlib", "syncbox", "eventual"] {
        trace_cfg.ignored_groups.insert(ignored_crate.to_owned());
    }
    assert!(exclog::errors::set_trace_config(trace_cfg))
}


#[derive(Clone)]
pub struct RubaRpcService {
    runtime: Arc<ruba::Ruba>,
}


impl RubaRpcService {
    pub fn new(db_path: &str) -> RpcService {
        let handler = RubaRpcService { runtime: Arc::new(ruba::Ruba::new(db_path, false)) };

        let mut service = RpcService::new(RPC_SERVICE_NAME, None);


        // asynchronously load data from db
        let runtime = handler.runtime.clone();
        thread::spawn(move || { runtime.load_table_data() });

        // Register Handlers
        let lh = handler.clone();
        service.register("Query", move |req| lh.handle_query(req), None);

        let lh = handler.clone();
        service.register("IngestEvents", move |req| lh.handle_ingest_events(req), None);

        let lh = handler.clone();
        service.register("IngestBatches", move |req| lh.handle_ingest_batches(req), None);

        let lh = handler.clone();
        service.register("Stats", move |req| lh.handle_stats(req), None);

        service
    }

    pub fn handle_query(&self, req: QueryRequest) -> DropboxResult<QueryResponse> {
        println!("Received RPC: {:?}", req);
        let result = self.runtime.run_query(&req.query);

        let mut log_msg = Vec::new();
        log_msg.push(("timestamp".to_string(), RawVal::Int(time::now().to_timespec().sec)));
        log_msg.push(("query".to_string(), RawVal::Str(req.query.to_string())));
        match result {
            Ok(ref query_result) => {
                log_msg.push(("runtime_ns".to_string(), RawVal::Int(query_result.stats.runtime_ns as i64)));
                log_msg.push(("rows_scanned".to_string(), RawVal::Int(query_result.stats.rows_scanned as i64)));
            },
            Err(ref msg) => {
                log_msg.push(("error".to_string(), RawVal::Str(msg.to_string())));
            },
        }
        self.runtime.ingest("_meta_queries", log_msg);

        match result {
            Ok(query_result) => {
                println!("Query ok");
                Ok(query_result_to_proto(query_result))
            },
            Err(msg) => {
                println!("Query error: {}", &msg);
                let mut response = QueryResponse::default();
                response.errormsg = msg.to_string();
                Ok(response)
            }
        }
    }

    pub fn handle_ingest_events(&self, req: IngestEventsRequest) -> DropboxResult<IngestEventsResponse> {
        for event in req.events {
            let mut row = Vec::with_capacity(event.fields.len());
            row.clear();
            for event in event.fields {
                let val = match event.value {
                    Some(value) => match value.value {
                        Some(Val_Value::Stringval(string)) => ruba::mem_store::ingest::RawVal::Str(string),
                        Some(Val_Value::Integer(int)) => ruba::mem_store::ingest::RawVal::Int(int),
                        None => ruba::mem_store::ingest::RawVal::Null,
                    },
                    None => ruba::mem_store::ingest::RawVal::Null,
                };
                row.push((event.colname, val));
            }
            self.runtime.ingest(&event.tablename, row);
        }
        Ok(IngestEventsResponse::default())
    }

    pub fn handle_ingest_batches(&self, req: IngestBatchesRequest) -> DropboxResult<IngestBatchesResponse> {
        println!("Received RPC: {:?}", req);
        Ok(IngestBatchesResponse::default())
    }

    pub fn handle_stats(&self, req: StatsRequest) -> DropboxResult<StatsResponse> {
        println!("{:?}", self.runtime.stats());
        Ok(StatsResponse::default())
    }
}


fn query_result_to_proto(result: QueryResult) -> QueryResponse {
    let mut pb = QueryResponse::default();
    let mut cols = result.colnames.iter().map(|colname| {
        let mut col = Column::default();
        col.name = colname.to_string();
        col
    }).collect::<Vec<_>>();

    for row in result.rows.into_iter() {
        for (i, val) in row.into_iter().enumerate() {
            cols[i].values.push(raw_val_to_proto(val));
        }
    }
    pb.columns = cols;

    let mut stats = QueryStats::default();
    stats.rows_scanned = result.stats.rows_scanned;
    stats.runtime_ns = result.stats.runtime_ns;
    pb.stats = Some(stats);

    pb
}

fn raw_val_to_proto(val: RawVal) -> Val {
    match val {
        RawVal::Null => Val { value: None },
        RawVal::Int(i) => Val { value: Some(Val_Value::Integer(i)) },
        RawVal::Str(s) => Val { value: Some(Val_Value::Stringval(s)) },
    }
}