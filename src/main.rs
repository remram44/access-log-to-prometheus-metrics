use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, Registry, TextEncoder, default_registry, gather};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;

struct LogCollector;

impl LogCollector {
    fn new() -> LogCollector {
        LogCollector
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

#[tokio::main]
async fn main() {
    // Standard log format:
    // log_format combined '$remote_addr - $remote_user [$time_local] '
    //                     '"$request" $status $body_bytes_sent '
    //                     '"$http_referer" "$http_user_agent"';
    // With host:
    // $host ...
    // app.taguette.org 216.165.95.135 - - [14/Oct/2021:19:51:30 +0000] "GET /api/project/32/events HTTP/2.0" 200 0 "https://app.taguette.org/project/32" "Firefox/93.0"
    let collector = LogCollector::new();

    let registry: &Registry = default_registry();
    registry.register(Box::new(collector)).expect("register collector");

    let addr = "127.0.0.1:9898".parse().unwrap();
    let serve_future = Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_req))
    }));

    if let Err(err) = serve_future.await {
        eprintln!("server error: {}", err);
    }
}
