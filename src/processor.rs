use log::{debug, info, warn};
use notify::{RecommendedWatcher, Watcher};
use std::borrow::Cow;
use std::borrow::Cow::*;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::collector::LogData;
use crate::log_parser::{LogValue, LogParser, ParseError};

pub struct Filter {
    pub(crate) field_index: usize,
    pub(crate) func: FilterFunc,
}

pub enum FilterFunc {
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

pub struct Extractor {
    pub(crate) label: Option<(String, usize)>,
    pub(crate) field_index: usize,
    pub(crate) func: ExtractorFunc,
}

pub enum ExtractorFunc {
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

pub struct LogProcessor {
    pub(crate) data: Arc<Mutex<LogData>>,
    pub(crate) filename: PathBuf,
    pub(crate) log_parser: LogParser,
    pub(crate) labels: Vec<String>,
    pub(crate) filters: Vec<Filter>,
    pub(crate) extractors: Vec<Extractor>,
}

impl LogProcessor {
    pub fn start_thread(self) {
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

    pub fn process_line<'a>(
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
