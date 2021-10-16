mod log_parser;

use clap::{App, Arg};
use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, Registry, TextEncoder, default_registry, gather};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use std::path::Path;

use log_parser::LogParser;

struct LogCollector {
    log_parser: LogParser,
}

impl LogCollector {
    fn new(log_parser: LogParser, filename: &Path) -> LogCollector {
        LogCollector { log_parser }
    }
}

impl Collector for LogCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        vec![]
    }
}

async fn serve_req(_req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
    let encoder = TextEncoder::new();

    let metric_families = gather();
    let mut buffer = vec![];
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(Body::from(buffer))
        .unwrap();

    Ok(response)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Standard log format:
    // log_format combined '$remote_addr - $remote_user [$time_local] '
    //                     '"$request" $status $body_bytes_sent '
    //                     '"$http_referer" "$http_user_agent"';
    // With host:
    // $host ...
    // app.taguette.org 216.165.95.135 - - [14/Oct/2021:19:51:30 +0000] "GET /api/project/32/events HTTP/2.0" 200 0 "https://app.taguette.org/project/32" "Firefox/93.0"
    let cli = App::new("access-log-to-prometheus-metrics")
        .bin_name("access-log-to-prometheus-metrics")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("FILE")
                .help("The log file to watch")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("LOG_FORMAT")
                .help("The nginx log_format setting")
                .required(true)
                .takes_value(true),
        );
    let matches = cli.get_matches();

    let parser = LogParser::from_format(matches.value_of("LOG_FORMAT").unwrap())?;
    let collector = LogCollector::new(parser, Path::new(matches.value_of_os("FILE").unwrap()));

    let registry: &Registry = default_registry();
    registry.register(Box::new(collector)).expect("register collector");

    let addr = "127.0.0.1:9898".parse().unwrap();
    eprintln!("Starting server at {}", addr);
    Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_req))
    })).await?;

    Ok(())
}
