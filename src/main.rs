mod log_parser;

use clap::{App, Arg};
use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use log::{debug, info, warn};
use notify::{RecommendedWatcher, Watcher};
use prometheus::{Encoder, Registry, TextEncoder, default_registry, gather};
use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use std::borrow::Cow;
use std::borrow::Cow::*;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use log_parser::{LogValue, LogParser, ParseError};

struct Filter {
    field_index: usize,
    func: FilterFunc,
}

enum FilterFunc {
    #[cfg(feature = "re")]
    Regex {
        regex: regex::Regex,
    },
}

impl Filter {
    fn filter(&self, value: &str) -> bool {
        match &self.func {
            #[cfg(feature = "re")]
            FilterFunc::Regex { regex } => {
                regex.is_match(value)
            }
            // Can't happen, but "references are always considered inhabited"
            #[allow(unreachable_patterns)]
            _ => true,
        }
    }
}

struct Extractor {
    label: Option<(String, usize)>,
    field_index: usize,
    func: ExtractorFunc,
}

enum ExtractorFunc {
    User,
    Status,
    Duration,
    Host,
    ResponseBodySize,
    #[cfg(feature = "re")]
    Regex {
        target: String,
        regex: regex::Regex,
    }
}

impl Extractor {
    fn extract<'a>(&'a self, value: &'a str, labels: &mut [Cow<'a, str>], duration: &mut Option<f32>, response_body_size: &mut Option<u64>) -> Result<(), ParseError> {
        let mut set_label = |label: Cow<'a, str>| {
            let label_index = match self.label {
                Some((_, idx)) => idx,
                None => panic!("Extractor with no target label tried to set a label"),
            };
            labels[label_index] = label;
        };

        match &self.func {
            ExtractorFunc::User => {
                if value != "-" {
                    set_label(Borrowed("yes"))
                } else {
                    set_label(Borrowed("no"))
                }
            }
            ExtractorFunc::Status => {
                set_label(Owned(value.parse().map_err(|_| ParseError("Invalid status code".to_owned()))?))
            }
            ExtractorFunc::Duration => {
                let seconds: f32 = value.parse().map_err(|_| ParseError("Invalid duration".to_owned()))?;
                *duration = Some(seconds);
            }
            ExtractorFunc::Host => {
                set_label(Borrowed(value));
            }
            ExtractorFunc::ResponseBodySize => {
                let size = value.parse().map_err(|_| ParseError("Invalid number of bytes".to_owned()))?;
                *response_body_size = Some(size);
            }
            #[cfg(feature = "re")]
            ExtractorFunc::Regex { ref target, ref regex } => {
                let target_value = regex.replace(value, target);
                set_label(target_value);
            }
        }

        Ok(())
    }
}

struct LogData {
    active: bool,
    request_count: IntCounterVec,
    request_duration: HistogramVec,
    response_body_size: HistogramVec,
    error_count: IntCounter,
}

struct LogProcessor {
    data: Arc<Mutex<LogData>>,
    filename: PathBuf,
    log_parser: LogParser,
    labels: Vec<String>,
    filters: Vec<Filter>,
    extractors: Vec<Extractor>,
}

impl LogProcessor {
    fn start_thread(self) {
        std::thread::spawn(move || {
            loop {
                match self.watch_log() {
                    Ok(()) => {}
                    Err(e) => {
                        eprintln!("{}", e);
                        std::process::exit(1);
                    }
                }
                std::thread::sleep(std::time::Duration::from_secs(2));
            }
        });
    }

    fn watch_log(&self) -> Result<(), Box<dyn std::error::Error>> {
        let data: &Mutex<LogData> = &self.data;

        let mut file = match std::fs::OpenOptions::new().read(true).open(&self.filename) {
            Ok(f) => f,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    info!("File is missing, retrying...");
                    return Ok(());
                } else {
                    return Err(e.into());
                }
            }
        };

        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher: RecommendedWatcher = RecommendedWatcher::new_raw(tx)?;
        watcher.watch(&self.filename, notify::RecursiveMode::NonRecursive)?;
        let mut offset = file.seek(SeekFrom::End(0))?;

        data.lock().unwrap().active = true;
        info!("Watch established");

        let mut buffer = String::new();

        // Wait for events
        loop {
            let event: notify::RawEvent = rx.recv()?;

            debug!("event: {:?}", event);

            let reopen = match event.op {
                Ok(op) if !(notify::op::Op::WRITE | notify::op::Op::CLOSE_WRITE).contains(op) => {
                    info!("Restarting watch");
                    true
                }
                Err(e) => return Err(e.into()),
                _ => false,
            };

            if reopen {
                data.lock().unwrap().active = false;
                return Ok(());
            }

            // Check size
            let size = file.seek(SeekFrom::End(0))?;
            if size < offset {
                info!("Truncation detected ({} -> {})", offset, size);
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
                debug!("line: {:?}", line);
                read_to += ln + 1;

                let data = data.lock().unwrap();

                let mut label_values = vec![Borrowed("unk"); self.labels.len()];
                let mut duration: Option<f32> = None;
                let mut response_body_size: Option<u64> = None;

                match self.process_line(line, &mut label_values, &mut duration, &mut response_body_size) {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        warn!("{}", e);
                        data.error_count.inc();
                        continue;
                    }
                };

                debug!("{}", line);
                for (key, value) in self.labels.iter().zip(&label_values) {
                    debug!("    {}: {}", key, value);
                }

                let label_refs: Vec<&str> = label_values.iter().map(|v| -> &str { &v }).collect();

                data.request_count.with_label_values(&label_refs).inc();
                if let Some(d) = duration {
                    data.request_duration.with_label_values(&label_refs).observe(d.into());
                }
                if let Some(s) = response_body_size {
                    data.response_body_size.with_label_values(&label_refs).observe(s as f64);
                }
            }

            // Discard the lines from the buffer
            buffer.drain(0..read_to);
        }
    }

    fn process_line<'a>(
        &'a self,
        line: &'a str,
        label_values: &mut [Cow<'a, str>],
        duration: &mut Option<f32>,
        response_body_size: &mut Option<u64>,
    ) -> Result<bool, ParseError> {
        let values = match self.log_parser.parse(line) {
            Ok(v) => v,
            Err(e) => return Err(e),
        };

        let mut extractor_index = 0;
        let mut filter_index = 0;

        for (field_index, value) in values.iter().enumerate() {
            let LogValue { value, .. } = value;

            // Run filters
            while filter_index < self.filters.len() && self.filters[filter_index].field_index == field_index {
                if !self.filters[filter_index].filter(value) {
                    debug!("Skipping because of filter on {}", self.log_parser.fields()[field_index]);
                    return Ok(false);
                }

                filter_index += 1;
            }

            // Run extractors
            while extractor_index < self.extractors.len() && self.extractors[extractor_index].field_index == field_index {
                self.extractors[extractor_index].extract(value, label_values, duration, response_body_size)?;

                extractor_index += 1;
            }
        }

        Ok(true)
    }
}

struct LogCollectorBuilder {
    log_parser: LogParser,
    filename: PathBuf,
    filters: Vec<Filter>,
    extractors: Vec<Extractor>,
    labels: Vec<String>,
}

impl LogCollectorBuilder {
    /// Get the index of the label in the array, adding it if it's not there.
    fn label(labels: &mut Vec<String>, label: &str) -> usize {
        match labels.iter().position(|l| l == &label) {
            Some(i) => i,
            None => {
                labels.push(label.to_owned());
                labels.len() - 1
            }
        }
    }

    fn new(log_parser: LogParser, filename: PathBuf) -> LogCollectorBuilder {
        let mut labels = Vec::new();

        // Add extractors for the fields that are recognized
        let mut extractors = Vec::new();
        let mut add_extractor = |field_index: usize, label: Option<&str>, func: ExtractorFunc| {
            extractors.push(Extractor {
                label: match label {
                    Some(l) => Some((l.to_owned(), Self::label(&mut labels, l))),
                    None => None,
                },
                field_index,
                func,
            });
        };
        for (field_index, field) in log_parser.fields().iter().enumerate() {
            if field == "remote_user" {
                add_extractor(field_index, Some("user"), ExtractorFunc::User);
            } else if field == "status" {
                add_extractor(field_index, Some("status"), ExtractorFunc::Status);
            } else if field == "request_time" {
                add_extractor(field_index, None, ExtractorFunc::Duration);
            } else if field == "host" {
                add_extractor(field_index, Some("vhost"), ExtractorFunc::Host);
            } else if field == "body_bytes_sent" {
                add_extractor(field_index, None, ExtractorFunc::ResponseBodySize);
            }
        }

        LogCollectorBuilder {
            log_parser,
            filename,
            filters: Vec::new(),
            extractors,
            labels,
        }
    }

    fn add_filter(&mut self, field: String, func: FilterFunc) -> Result<(), ()> {
        let field_index = match self.log_parser.fields().iter().position(|f| f == &field) {
            Some(i) => i,
            None => {
                return Err(());
            }
        };
        self.filters.push(Filter {
            field_index,
            func,
        });
        Ok(())
    }

    fn add_extractor(&mut self, label: Option<String>, field: String, func: ExtractorFunc) -> Result<(), ()> {
        let label = match label {
            Some(label) => {
                let label_index = Self::label(&mut self.labels, &label);
                Some((label, label_index))
            }
            None => None,
        };
        let field_index = match self.log_parser.fields().iter().position(|f| f == &field) {
            Some(i) => i,
            None => {
                return Err(());
            }
        };
        self.extractors.push(Extractor {
            label,
            field_index,
            func,
        });
        Ok(())
    }

    fn build(self) -> Result<LogCollector, notify::Error> {
        let labels = self.labels.clone();
        let label_refs: Vec<&str> = self.labels.iter().map(|v| -> &str { &v }).collect();

        let mut filters = self.filters;
        filters.sort_by(|a, b| a.field_index.cmp(&b.field_index));
        let mut extractors = self.extractors;
        extractors.sort_by(|a, b| a.field_index.cmp(&b.field_index));

        let data = LogData {
            active: false,
            request_count: IntCounterVec::new(
                Opts::new("requests", "The total number of requests per HTTP status code and virtual host name"),
                &label_refs,
            ).unwrap(),
            request_duration: HistogramVec::new(
                HistogramOpts::new("request_duration", "Duration of HTTP requests in seconds per HTTP status code and virtual host name"),
                &label_refs,
            ).unwrap(),
            response_body_size: HistogramVec::new(
                HistogramOpts::new("response_body_size", "Size of responses' bodies in bytes HTTP status code and virtual host name")
                .buckets(prometheus::exponential_buckets(100.0, 5.0, 10).unwrap()),
                &label_refs,
            ).unwrap(),
            error_count: IntCounter::new("errors", "The total number of log lines that failed parsing").unwrap(),
        };
        let mut desc: Vec<Desc> = Vec::new();
        desc.extend(data.request_count.desc().into_iter().cloned());
        desc.extend(data.request_duration.desc().into_iter().cloned());
        desc.extend(data.response_body_size.desc().into_iter().cloned());
        desc.extend(data.error_count.desc().into_iter().cloned());

        let data = Arc::new(Mutex::new(data));

        let log_processor = LogProcessor {
            data: data.clone(),
            filename: self.filename,
            log_parser: self.log_parser,
            labels,
            filters,
            extractors,
        };
        log_processor.start_thread();

        Ok(LogCollector {
            desc,
            data,
        })
    }
}

struct LogCollector {
    data: Arc<Mutex<LogData>>,
    desc: Vec<Desc>,
}

impl Collector for LogCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.desc.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let data = self.data.lock().unwrap();
        if data.active {
            let mut metrics = Vec::new();
            metrics.extend(data.request_count.collect());
            metrics.extend(data.request_duration.collect());
            metrics.extend(data.response_body_size.collect());
            metrics.extend(data.error_count.collect());
            metrics
        } else {
            Vec::new()
        }
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
    // End the process if any thread panics
    // https://stackoverflow.com/a/36031130
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // invoke the default handler and exit the process
        orig_hook(panic_info);
        std::process::exit(1);
    }));

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
        )
        .arg(
            Arg::with_name("bind")
                .long("bind")
                .short("b")
                .help("The address:port to listen on")
                .required(false)
                .takes_value(true)
                .default_value("127.0.0.1:9898")
        )
        .arg(
            Arg::with_name("match")
                .long("match")
                .short("m")
                .help("Only lines where <field> matches <regex>")
                .required(false)
                .multiple(true)
                .takes_value(true)
                .number_of_values(1)
        )
        .arg(
            Arg::with_name("label")
                .long("label")
                .short("l")
                .help("Set <label> to <value> from <field> with <regex>")
                .required(false)
                .multiple(true)
                .takes_value(true)
                .number_of_values(1)
        );
    let matches = cli.get_matches();

    {
        let mut logger_builder = env_logger::Builder::from_default_env();
        logger_builder.init();
    }

    let parser = LogParser::from_format(matches.value_of("LOG_FORMAT").unwrap())?;
    let collector = LogCollectorBuilder::new(parser, Path::new(matches.value_of_os("FILE").unwrap()).to_owned());

    #[cfg(feature = "re")]
    let collector = {
        let mut collector = collector;

        if let Some(v) = matches.values_of("match") {
            for s in v {
                let parts: Vec<&str> = s.splitn(2, ':').collect();
                if parts.len() != 2 {
                    eprintln!("--match needs 2 arguments separated by ':'");
                    std::process::exit(1);
                }
                if let Err(()) = collector.add_filter(
                    parts[0].to_owned(),
                    FilterFunc::Regex { regex: regex::Regex::new(parts[1])? },
                ) {
                    eprintln!("No field {:?}, can't add filter", parts[0]);
                    std::process::exit(1);
                }
            }
        }

        if let Some(v) = matches.values_of("label") {
            for s in v {
                let parts: Vec<&str> = s.splitn(4, ':').collect();
                if parts.len() != 4 {
                    eprintln!("--label needs 4 arguments separated by ':'");
                    std::process::exit(1);
                }
                if let Err(()) = collector.add_extractor(
                    Some(parts[0].to_owned()),
                    parts[2].to_owned(),
                    ExtractorFunc::Regex {
                        target: parts[1].to_owned(),
                        regex: regex::Regex::new(&format!("^.*{}.*$", parts[3]))?,
                    },
                ) {
                    eprintln!("No field {:?}, can't add extractor", parts[2]);
                    std::process::exit(1);
                }
            }
        }

        collector
    };
    #[cfg(not(feature = "re"))]
    {
        if let Some(mut v) = matches.values_of("match") {
            if let Some(_) = v.next() {
                eprintln!("Support for --match and --label was not compiled in");
                std::process::exit(1);
            }
        }
        if let Some(mut v) = matches.values_of("label") {
            if let Some(_) = v.next() {
                eprintln!("Support for --match and --label was not compiled in");
                std::process::exit(1);
            }
        }
    }

    let collector = collector.build()?;

    let registry: &Registry = default_registry();
    registry.register(Box::new(collector)).expect("register collector");

    let addr = match matches.value_of("bind").unwrap().parse() {
        Ok(a) => a,
        Err(_) => {
            eprintln!("Invalid address: use ip:port format, for example 127.0.0.1:9898");
            std::process::exit(1);
        }
    };
    info!("Starting server at {}", addr);
    Server::bind(&addr).serve(make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(serve_req))
    })).await?;

    Ok(())
}
