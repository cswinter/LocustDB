use rand::Rng;
use std::mem;
use std::time::Duration;

use locustdb::logging_client::BufferFullPolicy;
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

struct RandomWalk {
    name: String,
    curr_value: f64,
    interval: u64,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let Opt { addr, interval } = Opt::from_args();
    let mut log = locustdb::logging_client::LoggingClient::new(
        Duration::from_secs(1),
        &addr,
        1 << 50,
        BufferFullPolicy::Block,
    );
    let mut rng = rand::thread_rng();
    let mut random_walks = (0..5)
        .map(|i| RandomWalk {
            name: format!("random_walk_{}", i),
            curr_value: 0.0,
            interval: rng.gen_range(1, 10),
        })
        .collect::<Vec<_>>();
    let mut interval = time::interval(Duration::from_millis(interval));
    let sys = System::new();
    let mut cpu_watcher = sys.cpu_load_aggregate().unwrap();
    for i in 0..u64::MAX {
        interval.tick().await;
        let cpu = mem::replace(&mut cpu_watcher, sys.cpu_load_aggregate().unwrap())
            .done()
            .unwrap();
        log.log(
            "system_stats",
            [("cpu".to_string(), cpu.user as f64)].iter().cloned(),
        );
        for walk in random_walks.iter_mut() {
            if i % walk.interval == 0 {
                walk.curr_value += rng.gen_range(-1.0, 1.0);
                log.log(
                    &walk.name,
                    [
                        ("value".to_string(), walk.curr_value),
                        ("step".to_string(), (i / walk.interval) as f64),
                    ]
                    .iter()
                    .cloned(),
                );
            }
        }
    }
}
