mod log_parser;

use clap::{App, Arg};
use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use notify::{RecommendedWatcher, Watcher};
use prometheus::{Encoder, Registry, TextEncoder, default_registry, gather};
use prometheus::{IntCounterVec, Opts};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use std::borrow::Cow::*;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use log_parser::{LogValue, LogParser};

struct LogCollector {
    data: Arc<Mutex<Data>>,
    desc: Vec<Desc>,
}

struct Data {
    request_count: IntCounterVec,
}

fn watch_log(filename: &Path, log_parser: &LogParser, data: Arc<Mutex<Data>>) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = std::fs::OpenOptions::new().read(true).open(&filename)?;

    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher: RecommendedWatcher = RecommendedWatcher::new_raw(tx)?;
    watcher.watch(&filename, notify::RecursiveMode::NonRecursive)?;
    let mut offset = file.seek(SeekFrom::End(0))?;

    let mut buffer = String::new();

    // Wait for events
    loop {
        let event: notify::RawEvent = rx.recv()?;

        eprintln!("event: {:?}", event);

        let reopen = match event.op {
            Ok(op) if op.contains(notify::op::Op::RENAME | notify::op::Op::REMOVE) => {
                eprintln!("File moved, restarting watch");
                true
            }
            Err(e) => return Err(e.into()),
            _ => false,
        };

        if reopen {
            return Ok(());
        }

        // Check size
        let size = file.seek(SeekFrom::End(0))?;
        if size < offset {
            eprintln!("Truncation detected ({} -> {})", offset, size);
            offset = size;
        }

        // Read
        file.seek(SeekFrom::Start(offset))?;
        let res = file.read_to_string(&mut buffer)? as u64;
        offset += res;

        // Split into lines
        let mut read_to = 0;
        while let Some(ln) = buffer[read_to..].find('\n') {
            let line = &buffer[read_to..read_to + ln];
            eprintln!("line: {:?}", line);
            read_to += ln + 1;

            let values = log_parser.parse(line)?;
            let mut remote_user = None;
            let mut status = None;
            let mut vhost: Option<String> = None;
            for value in values {
                match value {
                    LogValue::RemoteUser(s) => remote_user = Some(s),
                    LogValue::Request(_) => {}
                    LogValue::Status(i) => status = Some(i),
                    LogValue::Host(s) => vhost = Some(s),
                    LogValue::BodyBytesSent(_) => {}
                    LogValue::Other(_, _) => {}
                }
            }

            let data = data.lock().unwrap();
            data.request_count.with_label_values(&[
                &status.map(|i| Owned(format!("{}", i))).unwrap_or(Borrowed("unk")),
                vhost.as_ref().map(|s| -> &str { s }).unwrap_or("unk"),
            ]).inc();
        }

        // Discard the lines from the buffer
        buffer.drain(0..read_to);
    }
}

impl LogCollector {
    fn new(log_parser: LogParser, filename: PathBuf) -> Result<LogCollector, notify::Error> {
        let data = Data {
            request_count: IntCounterVec::new(
                Opts::new("requests", "The total number of requests per HTTP status code and virtual host name"),
                &["status", "vhost"],
            ).unwrap(),
        };
        let desc = data.request_count.desc().iter().cloned().cloned().collect();

        let data = Arc::new(Mutex::new(data));

        let data_rc = data.clone();
        std::thread::spawn(move || {
            loop {
                match watch_log(&filename, &log_parser, data_rc.clone()) {
                    Ok(()) => {}
                    Err(e) => {
                        eprintln!("{}", e);
                        std::process::exit(1);
                    }
                }
            }
        });

        Ok(LogCollector {
            desc,
            data,
        })
    }
}

impl Collector for LogCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.desc.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        self.data.lock().unwrap().request_count.collect()
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
    let collector = LogCollector::new(parser, Path::new(matches.value_of_os("FILE").unwrap()).to_owned())?;

    let registry: &Registry = default_registry();
    registry.register(Box::new(collector)).expect("register collector");

    let addr = "127.0.0.1:9898".parse().unwrap();
    eprintln!("Starting server at {}", addr);
    Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_req))
    })).await?;

    Ok(())
}
