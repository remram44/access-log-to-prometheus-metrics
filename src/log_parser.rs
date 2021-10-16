#[derive(Debug)]
pub struct ParseError(String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Parse error: {}", self.0)
    }
}

impl std::error::Error for ParseError {}

trait PeekableCharIndicesPosExt {
    fn pos(&mut self) -> Option<usize>;
}

impl<'a> PeekableCharIndicesPosExt for std::iter::Peekable<std::str::CharIndices<'a>> {
    fn pos(&mut self) -> Option<usize> {
        match self.peek() {
            None => None,
            Some((i, _)) => Some(*i),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LogToken {
    Str(String),
    Field(LogField),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LogField {
    RemoteAddr,
    RemoteUser,
    TimeLocal,
    Request,
    Status,
    BodyBytesSent,
    HttpReferer,
    HttpUserAgent,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogValue {
    RemoteAddr(String),
    RemoteUser(String),
    TimeLocal(String),
    Request(String),
    Status(u16),
    BodyBytesSent(u64),
    HttpReferer(String),
    HttpUserAgent(String),
    Other(String, String),
}

pub struct LogParser {
    fields: Vec<LogToken>,
}

impl LogParser {
    pub fn from_format(format: &str) -> Result<LogParser, ParseError> {
        let fields = LogFormatParser::new(format).parse()?;
        Ok(LogParser { fields })
    }

    pub fn parse(&self, log: &str) -> Result<Vec<LogValue>, ParseError> {
        LogParserInner::new(&self.fields, log).parse()
    }
}

struct LogParserInner<'a> {
    fields: &'a [LogToken],
    log: &'a str,
    iter: std::iter::Peekable<std::str::CharIndices<'a>>,
    values: Vec<LogValue>,
}

impl<'a> LogParserInner<'a> {
    fn new(fields: &'a [LogToken], log: &'a str) -> LogParserInner<'a> {
        LogParserInner {
            fields,
            log,
            iter: log.char_indices().peekable(),
            values: Vec::new(),
        }
    }

    fn parse(mut self) -> Result<Vec<LogValue>, ParseError> {
        for i in 0..self.fields.len() {
            let field = &self.fields[i];
            eprintln!("Matching field {:?}", field);

            match field {
                &LogToken::Str(ref s) => {
                    let start = self.iter.pos().unwrap_or(self.log.len());
                    let mut it = s.chars();
                    loop {
                        match (it.next(), self.iter.peek()) {
                            (None, None) => break,
                            (Some(e), Some(&(i, a))) => {
                                if e == a {
                                    self.iter.next();
                                } else {
                                    return Err(ParseError(format!("Expected {:?}, found {:?}", s, &self.log[start..i])));
                                }
                            }
                            (None, Some(_)) => break,
                            (Some(_), None) => return Err(ParseError(format!("Expected {:?}, found {:?}", s, &self.log[start..]))),
                        }
                    }
                }
                &LogToken::Field(ref f) => {
                    let next = match self.fields.get(i + 1) {
                        None => None,
                        Some(&LogToken::Str(ref s)) => Some(s.chars().next().unwrap()),
                        Some(n) => return Err(ParseError(format!("Can't parse, no separator between {:?} and {:?}", f, n))),
                    };

                    let value = match next {
                        Some(sep) => {
                            eprintln!("Reading to separator {:?}", sep);
                            match self.iter.pos() {
                                Some(start) => {
                                    loop {
                                        match self.iter.peek() {
                                            Some(&(i, c)) => {
                                                if c == sep {
                                                    break &self.log[start..i];
                                                } else {
                                                    self.iter.next();
                                                }
                                            }
                                            None => return Err(ParseError(format!("Missing separator {:?}", sep))),
                                        }
                                    }.to_owned()
                                }
                                None => {
                                    String::new()
                                }
                            }
                        }
                        None => {
                            eprintln!("Last field, reading to end");
                            match self.iter.pos() {
                                Some(i) => self.log[i..].to_owned(),
                                None => String::new(),
                            }
                        }
                    };

                    match f {
                        LogField::RemoteAddr => self.values.push(LogValue::RemoteAddr(value)),
                        LogField::RemoteUser => self.values.push(LogValue::RemoteUser(value)),
                        LogField::TimeLocal => self.values.push(LogValue::TimeLocal(value)),
                        LogField::Request => self.values.push(LogValue::Request(value)),
                        LogField::Status => self.values.push(LogValue::Status(value.parse().map_err(|_| ParseError("Invalid status code".to_owned()))?)),
                        LogField::BodyBytesSent => self.values.push(LogValue::BodyBytesSent(value.parse().map_err(|_| ParseError("Invalid status code".to_owned()))?)),
                        LogField::HttpReferer => self.values.push(LogValue::HttpReferer(value)),
                        LogField::HttpUserAgent => self.values.push(LogValue::HttpUserAgent(value)),
                        LogField::Other(ref s) => self.values.push(LogValue::Other(s.clone(), value)),
                    }
                }
            }
        }
        Ok(self.values)
    }
}

struct LogFormatParser<'a> {
    format: &'a str,
    iter: std::iter::Peekable<std::str::CharIndices<'a>>,
    fields: Vec<LogToken>,
}

impl<'a> LogFormatParser<'a> {
    fn new(format: &'a str) -> LogFormatParser<'a> {
        LogFormatParser {
            format,
            iter: format.char_indices().peekable(),
            fields: Vec::new(),
        }
    }

    fn parse(mut self) -> Result<Vec<LogToken>, ParseError> {
        self.skip_whitespace();
        if self.iter.peek().is_none() {
            return Err(ParseError("Empty string".to_owned()));
        }
        if self.maybe_consume("log_format") {
            eprintln!("Starts with log_format");
            self.skip_whitespace();
            if self.maybe_consume("combined") {
                self.skip_whitespace();
            }
            match self.iter.next() {
                Some((_, '\'')) => {}
                _ => return Err(ParseError("Missing \'".to_owned())),
            }
            self.parse_format()?;
            eprintln!("Finishing up: \"{}\"", if let Some(i) = self.iter.pos() { &self.format[i..] } else { "" });
            match self.iter.next() {
                Some((_, '\'')) => {},
                _ => return Err(ParseError("Missing final '".to_owned())),
            }
            match self.iter.next() {
                None => {}
                Some((_, ';')) => {
                    self.skip_whitespace();
                    if self.iter.next().is_some() {
                        return Err(ParseError("Unexpected characters at the end".to_owned()));
                    }
                }
                Some(_) => {
                    return Err(ParseError("Unexpected characters at the end".to_owned()));
                }
            }
        } else {
            self.parse_format()?;
            if self.iter.next().is_some() {
                return Err(ParseError("Unexpected characters at the end".to_owned()));
            }
        }
        Ok(self.fields)
    }

    fn parse_format(&mut self) -> Result<(), ParseError> {
        eprintln!("Parsing");
        while let Some(&(_, c)) = self.iter.peek() {
            if c == '\'' {
                break;
            } else if c == '$' {
                eprintln!("Found variable");
                self.iter.next();
                let var = self.read_identifier()?;
                eprintln!("Read identifier: {}", var);
                if var == "remote_addr" {
                    self.fields.push(LogToken::Field(LogField::RemoteAddr));
                } else if var == "remote_user" {
                    self.fields.push(LogToken::Field(LogField::RemoteUser));
                } else if var == "time_local" {
                    self.fields.push(LogToken::Field(LogField::TimeLocal));
                } else if var == "request" {
                    self.fields.push(LogToken::Field(LogField::Request));
                } else if var == "status" {
                    self.fields.push(LogToken::Field(LogField::Status));
                } else if var == "body_bytes_sent" {
                    self.fields.push(LogToken::Field(LogField::BodyBytesSent));
                } else if var == "http_referer" {
                    self.fields.push(LogToken::Field(LogField::HttpReferer));
                } else if var == "http_user_agent" {
                    self.fields.push(LogToken::Field(LogField::HttpUserAgent));
                } else {
                    self.fields.push(LogToken::Field(LogField::Other(var.to_owned())));
                }
            } else {
                eprintln!("Found character {:?}", c);
                self.iter.next();
                match self.fields.last_mut() {
                    Some(LogToken::Str(ref mut s)) => s.push(c),
                    _ => {
                        let mut s = String::new();
                        s.push(c);
                        self.fields.push(LogToken::Str(s));
                    }
                }
            }
        }
        Ok(())
    }

    fn skip_whitespace(&mut self) {
        loop {
            match self.iter.peek() {
                None => {
                    self.iter.next();
                    return;
                }
                Some((_, c)) => {
                    if c.is_whitespace() {
                        self.iter.next();
                    } else {
                        return;
                    }
                }
            }
        }
    }

    fn maybe_consume(&mut self, s: &str) -> bool {
        let mut previous = self.iter.clone();
        let mut s_iter = s.chars();
        loop {
            match (s_iter.next(), self.iter.peek()) {
                (None, None) => return true,
                (Some(e), Some(&(_, a))) => {
                    eprintln!("{:?} {:?} {:?}", e, a, e == a);
                    if e == a {
                        self.iter.next();
                    } else {
                        break;
                    }
                }
                (None, _) => return true,
                _ => return false,
            }
        }
        std::mem::swap(&mut previous, &mut self.iter);
        false
    }

    fn read_identifier(&mut self) -> Result<&'a str, ParseError> {
        let start = self.iter.pos().unwrap_or(self.format.len());
        let identifier = loop {
            match self.iter.peek() {
                Some(&(i, c)) => {
                    if ('a' <= c && c <= 'z')
                        || ('0' <= c && c <= '9')
                        || c == '_' {
                        self.iter.next();
                    } else {
                        break &self.format[start..i];
                    }
                }
                None => break &self.format[start..],
            }
        };
        if identifier.is_empty() {
            return Err(ParseError("Expected identifier".to_owned()));
        }
        Ok(identifier)
    }
}

#[test]
fn test_format_parser() {
    use LogToken::Field;
    use LogField::*;

    fn s(r: &str) -> LogToken {
        LogToken::Str(r.to_owned())
    }

    assert_eq!(
        LogFormatParser::new("log_format combined '$remote_addr - $remote_user [$time_local]';").parse().unwrap(),
        vec![Field(RemoteAddr), s(" - "), Field(RemoteUser), s(" ["), Field(TimeLocal), s("]")],
    );
    assert_eq!(
        LogFormatParser::new("    log_format '$remote_addr - $remote_user [$time_local]';  ").parse().unwrap(),
        vec![Field(RemoteAddr), s(" - "), Field(RemoteUser), s(" ["), Field(TimeLocal), s("]")],
    );
    assert_eq!(
        LogFormatParser::new("$remote_addr - $remote_user [$time_local]").parse().unwrap(),
        vec![Field(RemoteAddr), s(" - "), Field(RemoteUser), s(" ["), Field(TimeLocal), s("]")],
    );
}

#[test]
fn test_parser() {
    use LogToken::Field;
    use LogField::*;

    fn s(r: &str) -> LogToken {
        LogToken::Str(r.to_owned())
    }

    let parser = LogParser { fields: vec![Field(RemoteAddr), s(" - "), Field(RemoteUser), s(" ["), Field(TimeLocal), s("]")] };

    assert_eq!(
        parser.parse("216.165.95.86 - remi [15/Oct/2021:15:39:52 +0000]").unwrap(),
        vec![LogValue::RemoteAddr("216.165.95.86".to_owned()), LogValue::RemoteUser("remi".to_owned()), LogValue::TimeLocal("15/Oct/2021:15:39:52 +0000".to_owned())],
    );
}
