use prometheus::{HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts};
use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::log_parser::LogParser;
use crate::processor::{Filter, FilterFunc, Extractor, ExtractorFunc, LogProcessor};

pub struct LogData {
    pub active: bool,
    pub request_count: IntCounterVec,
    pub request_duration: HistogramVec,
    pub response_body_size: HistogramVec,
    pub error_count: IntCounter,
}

impl LogData {
    fn new(labels: &[&str]) -> LogData {
        LogData {
            active: false,
            request_count: IntCounterVec::new(
                Opts::new("requests", "The total number of requests per HTTP status code and virtual host name"),
                &labels,
            ).unwrap(),
            request_duration: HistogramVec::new(
                HistogramOpts::new("request_duration", "Duration of HTTP requests in seconds per HTTP status code and virtual host name"),
                &labels,
            ).unwrap(),
            response_body_size: HistogramVec::new(
                HistogramOpts::new("response_body_size", "Size of responses' bodies in bytes HTTP status code and virtual host name")
                .buckets(prometheus::exponential_buckets(100.0, 5.0, 10).unwrap()),
                &labels,
            ).unwrap(),
            error_count: IntCounter::new("errors", "The total number of log lines that failed parsing").unwrap(),
        }
    }
}

pub struct LogCollectorBuilder {
    log_parser: LogParser,
    filename: PathBuf,
    filters: Vec<Filter>,
    extractors: Vec<Extractor>,
    labels: Vec<String>,
}

impl LogCollectorBuilder {
    /// Get the index of the label in the array, adding it if it's not there.
    pub fn label(labels: &mut Vec<String>, label: &str) -> usize {
        match labels.iter().position(|l| l == &label) {
            Some(i) => i,
            None => {
                labels.push(label.to_owned());
                labels.len() - 1
            }
        }
    }

    pub fn new(log_parser: LogParser, filename: PathBuf) -> LogCollectorBuilder {
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

    pub fn add_filter(&mut self, field: String, func: FilterFunc) -> Result<(), ()> {
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

    pub fn add_extractor(&mut self, label: Option<String>, field: String, func: ExtractorFunc) -> Result<(), ()> {
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

    pub fn build_processor(self, data: Arc<Mutex<LogData>>) -> LogProcessor {
        let labels = self.labels.clone();

        let mut filters = self.filters;
        filters.sort_by(|a, b| a.field_index.cmp(&b.field_index));
        let mut extractors = self.extractors;
        extractors.sort_by(|a, b| a.field_index.cmp(&b.field_index));

        LogProcessor {
            data: data.clone(),
            filename: self.filename,
            log_parser: self.log_parser,
            labels,
            filters,
            extractors,
        }
    }

    pub fn build_data(&self) -> LogData {
        let label_refs: Vec<&str> = self.labels.iter().map(|v| -> &str { &v }).collect();
        LogData::new(&label_refs)
    }

    pub fn build(self) -> Result<LogCollector, notify::Error> {
        let data = self.build_data();
        let mut desc: Vec<Desc> = Vec::new();
        desc.extend(data.request_count.desc().into_iter().cloned());
        desc.extend(data.request_duration.desc().into_iter().cloned());
        desc.extend(data.response_body_size.desc().into_iter().cloned());
        desc.extend(data.error_count.desc().into_iter().cloned());

        let data = Arc::new(Mutex::new(data));

        let log_processor = self.build_processor(data.clone());
        log_processor.start_thread();

        Ok(LogCollector {
            desc,
            data,
        })
    }
}

pub struct LogCollector {
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
