use std::mem;
use std::time::Duration;

use structopt::StructOpt;
use systemstat::{Platform, System};
use tokio::time;

#[derive(StructOpt, Debug)]
#[structopt(
    name = "LocustDB Logger Test",
    about = "Log basic system stats to LocustDB.",
    author = "Clemens Winter <clemenswinter1@gmail.com>"
)]
struct Opt {
    /// Address of LocustDB server
    #[structopt(long, name = "ADDR", default_value = "http://localhost:8080")]
    addr: String,

    /// Logging interval in milliseconds
    #[structopt(long, name = "INTERVAL", default_value = "100")]
    interval: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let Opt { addr, interval } = Opt::from_args();
    let mut log = locustdb::logging_client::LoggingClient::new(Duration::from_secs(1), &addr, 1 << 50);
    let mut interval = time::interval(Duration::from_millis(interval));
    let sys = System::new();
    let mut cpu_watcher = sys.cpu_load_aggregate().unwrap();
    loop {
        interval.tick().await;
        let cpu = mem::replace(&mut cpu_watcher, sys.cpu_load_aggregate().unwrap())
            .done()
            .unwrap();
        log.log(
            "system_stats",
            [("cpu".to_string(), cpu.user as f64)].iter().cloned(),
        );
    }
}
