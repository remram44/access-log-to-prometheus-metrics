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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::collector::LogCollectorBuilder;
    use crate::log_parser::LogParser;
    use crate::processor::LogProcessor;

    fn test_parse(processor: &LogProcessor, line: &str, expected: Option<(&[&str], Option<f32>, Option<u64>)>) {
            let mut label_values = vec![std::borrow::Cow::Borrowed("unk"); processor.labels.len()];
            let mut duration = None;
            let mut response_body_size = None;
            let matched = processor.process_line(
                line,
                &mut label_values,
                &mut duration,
                &mut response_body_size,
            ).unwrap();
            match (matched, expected) {
                (false, None) => {}
                (false, Some(_)) => panic!("Line was filtered unexpectedly"),
                (true, None) => panic!("Line was not filtered"),
                (true, Some((v, d, s))) => {
                    assert_eq!(label_values, v);
                    assert_eq!(duration, d);
                    assert_eq!(response_body_size, s);
                }
            }
    }

    #[test]
    fn test_process() {
        let log_parser = LogParser::from_format(
            r#"$host $remote_addr - $remote_user [$time_local] "$request" $status $request_time $body_bytes_sent "$http_referer" "$http_user_agent""#,
        ).unwrap();
        let collector_builder = LogCollectorBuilder::new(log_parser, "/tmp/access.log".into());
        let data = Arc::new(Mutex::new(collector_builder.build_data()));
        let processor = collector_builder.build_processor(data);

        test_parse(
            &processor,
            r#"example.org 1.2.3.4 - - [11/Nov/2021:02:34:39 +0000] "GET /api/v4/pets/1 HTTP/1.1" 200 0.092 263 "-" "Mozilla/5.0 (Linux)""#,
            Some((
                &["example.org", "no", "200"],
                Some(0.092),
                Some(263),
            )),
        );
        test_parse(
            &processor,
            r#"remram.fr 8.8.8.8 - person [11/Nov/2021:02:34:41 +0000] "POST /api/v4/pets HTTP/1.1" 201 0.132 14 "-" "Mozilla/5.0 (Linux)""#,
            Some((
                &["remram.fr", "yes", "201"],
                Some(0.132),
                Some(14),
            )),
        );
    }

    #[cfg(feature = "re")]
    #[test]
    fn test_process_re() {
        use crate::processor::{FilterFunc, ExtractorFunc};

        let log_parser = LogParser::from_format(
            r#"$host $remote_addr - $remote_user [$time_local] "$request" $status $request_time $body_bytes_sent "$http_referer" "$http_user_agent""#,
        ).unwrap();
        let mut collector_builder = LogCollectorBuilder::new(log_parser, "/tmp/access.log".into());
        // -m 'status:^200$'
        collector_builder.add_filter(
            "status".to_owned(),
            FilterFunc::Regex { regex: regex::Regex::new("^200$").unwrap() },
        ).unwrap();
        // -l 'api_version:$1:request:^[A-Z]+ /api/(v[0-9]+)/'
        collector_builder.add_extractor(
            Some("api_version".to_owned()),
            "request".to_owned(),
            ExtractorFunc::Regex {
                target: "$1".to_owned(),
                regex: regex::Regex::new("^.*[A-Z]+ /api/(v[0-9]+)/.*$").unwrap(),
            },
        ).unwrap();
        let data = Arc::new(Mutex::new(collector_builder.build_data()));
        let processor = collector_builder.build_processor(data);

        test_parse(
            &processor,
            r#"example.org 1.2.3.4 - - [11/Nov/2021:02:34:39 +0000] "GET /api/v4/pets/1 HTTP/1.1" 200 0.092 263 "-" "Mozilla/5.0 (Linux)""#,
            Some((
                &["example.org", "no", "200", "v4"],
                Some(0.092),
                Some(263),
            )),
        );
        test_parse(
            &processor,
            r#"remram.fr 8.8.8.8 - person [11/Nov/2021:02:34:41 +0000] "POST /api/v4/pets HTTP/1.1" 201 0.132 14 "-" "Mozilla/5.0 (Linux)""#,
            None,
        );
    }
}
