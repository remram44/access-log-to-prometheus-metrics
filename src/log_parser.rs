use log::debug;

#[derive(Debug)]
pub struct ParseError(pub String);

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
    Field(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogValue<'a> {
    pub variable: &'a str,
    pub value: &'a str,
}

pub struct LogParser {
    tokens: Vec<LogToken>,
    fields: Vec<String>,
}

impl LogParser {
    pub fn from_format(format: &str) -> Result<LogParser, ParseError> {
        let tokens = LogFormatParser::new(format).parse()?;
        let fields = tokens.iter().filter_map(|token| match token {
            LogToken::Str(_) => None,
            LogToken::Field(s) => Some(s.clone()),
        }).collect();
        Ok(LogParser {
            tokens,
            fields,
        })
    }

    pub fn parse<'a>(&'a self, log: &'a str) -> Result<Vec<LogValue<'a>>, ParseError> {
        LogParserInner::new(&self.tokens, log).parse()
    }

    pub fn fields(&self) -> &[String] {
        &self.fields
    }
}

struct LogParserInner<'a> {
    tokens: &'a [LogToken],
    log: &'a str,
    iter: std::iter::Peekable<std::str::CharIndices<'a>>,
    values: Vec<LogValue<'a>>,
}

impl<'a> LogParserInner<'a> {
    fn new(tokens: &'a [LogToken], log: &'a str) -> LogParserInner<'a> {
        LogParserInner {
            tokens,
            log,
            iter: log.char_indices().peekable(),
            values: Vec::new(),
        }
    }

    fn parse(mut self) -> Result<Vec<LogValue<'a>>, ParseError> {
        for i in 0..self.tokens.len() {
            let token = &self.tokens[i];
            debug!("Matching token {:?}", token);

            match token {
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
                    let next = match self.tokens.get(i + 1) {
                        None => None,
                        Some(&LogToken::Str(ref s)) => Some(s.chars().next().unwrap()),
                        Some(n) => return Err(ParseError(format!("Can't parse, no separator between {:?} and {:?}", f, n))),
                    };

                    let value = match next {
                        Some(sep) => {
                            debug!("Reading to separator {:?}", sep);
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
                                    }
                                }
                                None => {
                                    ""
                                }
                            }
                        }
                        None => {
                            debug!("Last token, reading to end");
                            match self.iter.pos() {
                                Some(i) => &self.log[i..],
                                None => "",
                            }
                        }
                    };

                    self.values.push(LogValue { variable: f, value });
                }
            }
        }
        Ok(self.values)
    }
}

struct LogFormatParser<'a> {
    format: &'a str,
    iter: std::iter::Peekable<std::str::CharIndices<'a>>,
    tokens: Vec<LogToken>,
}

impl<'a> LogFormatParser<'a> {
    fn new(format: &'a str) -> LogFormatParser<'a> {
        LogFormatParser {
            format,
            iter: format.char_indices().peekable(),
            tokens: Vec::new(),
        }
    }

    fn parse(mut self) -> Result<Vec<LogToken>, ParseError> {
        self.skip_whitespace();
        if self.iter.peek().is_none() {
            return Err(ParseError("Empty string".to_owned()));
        }
        if self.maybe_consume("log_format") {
            debug!("Starts with log_format");
            self.skip_whitespace();
            if self.maybe_consume("combined") {
                self.skip_whitespace();
            }
            match self.iter.next() {
                Some((_, '\'')) => {}
                _ => return Err(ParseError("Missing \'".to_owned())),
            }
            self.parse_format()?;
            debug!("Finishing up: \"{}\"", if let Some(i) = self.iter.pos() { &self.format[i..] } else { "" });
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
        Ok(self.tokens)
    }

    fn parse_format(&mut self) -> Result<(), ParseError> {
        debug!("Parsing");
        while let Some(&(_, c)) = self.iter.peek() {
            if c == '\'' {
                break;
            } else if c == '$' {
                debug!("Found variable");
                self.iter.next();
                let var = self.read_identifier()?;
                debug!("Read identifier: {}", var);
                self.tokens.push(LogToken::Field(var.to_owned()));
            } else {
                debug!("Found character {:?}", c);
                self.iter.next();
                match self.tokens.last_mut() {
                    Some(LogToken::Str(ref mut s)) => s.push(c),
                    _ => {
                        let mut s = String::new();
                        s.push(c);
                        self.tokens.push(LogToken::Str(s));
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
                    debug!("{:?} {:?} {:?}", e, a, e == a);
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
    fn f(n: &str) -> LogToken {
        LogToken::Field(n.to_owned())
    }
    fn s(r: &str) -> LogToken {
        LogToken::Str(r.to_owned())
    }

    assert_eq!(
        LogFormatParser::new("log_format combined '$remote_addr - $remote_user [$time_local]';").parse().unwrap(),
        vec![f("remote_addr"), s(" - "), f("remote_user"), s(" ["), f("time_local"), s("]")],
    );
    assert_eq!(
        LogFormatParser::new("    log_format '$remote_addr - $remote_user [$time_local]';  ").parse().unwrap(),
        vec![f("remote_addr"), s(" - "), f("remote_user"), s(" ["), f("time_local"), s("]")],
    );
    assert_eq!(
        LogFormatParser::new("$remote_addr - $remote_user [$time_local]").parse().unwrap(),
        vec![f("remote_addr"), s(" - "), f("remote_user"), s(" ["), f("time_local"), s("]")],
    );
}

#[test]
fn test_parser() {
    fn f(n: &str) -> LogToken {
        LogToken::Field(n.to_owned())
    }
    fn s(r: &str) -> LogToken {
        LogToken::Str(r.to_owned())
    }
    fn v(n: &'static str, d: &'static str) -> LogValue<'static> {
        LogValue {
            variable: n,
            value: d,
        }
    }

    let parser = LogParser {
        tokens: vec![f("remote_addr"), s(" - "), f("remote_user"), s(" "), f("request_time"), s(" ["), f("time_local"), s("]")],
        fields: vec!["remote_addr".to_owned(), "remote_user".to_owned(), "request_time".to_owned(), "time_local".to_owned()],
    };

    assert_eq!(
        parser.parse("216.165.95.86 - remi 0.012 [15/Oct/2021:15:39:52 +0000]").unwrap(),
        vec![v("remote_addr", "216.165.95.86"), v("remote_user", "remi"), v("request_time", "0.012"), v("time_local", "15/Oct/2021:15:39:52 +0000")],
    );
}
