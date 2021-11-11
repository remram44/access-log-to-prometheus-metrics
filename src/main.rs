mod collector;
mod log_parser;
mod processor;

use clap::{App, Arg};
use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use log::info;
use prometheus::{Encoder, Registry, TextEncoder, default_registry, gather};
use std::path::Path;

use crate::collector::LogCollectorBuilder;
use crate::log_parser::LogParser;

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
        use crate::processor::{FilterFunc, ExtractorFunc};

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
