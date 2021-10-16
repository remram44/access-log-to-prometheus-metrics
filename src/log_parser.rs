#[derive(Debug)]
pub struct ParseError(String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "Parse error: {}", self.0)
    }
}

impl std::error::Error for ParseError {}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LogField {
    Character(char),
    RemoteAddr,
    RemoteUser,
    TimeLocal,
    Request,
    Status,
    BodyBytesSent,
    HttpReferer,
    HttpUserAgent,
}

pub struct LogParser {
    fields: Vec<LogField>,
}

impl LogParser {
    fn from_format(format: &str) -> Result<LogParser, ParseError> {
        let fields = LogFormatParser::new(format).parse()?;
        Ok(LogParser { fields })
    }
}

struct LogFormatParser<'a> {
    format: &'a str,
    iter: std::iter::Peekable<std::str::CharIndices<'a>>,
    fields: Vec<LogField>,
}

impl<'a> LogFormatParser<'a> {
    fn new(format: &'a str) -> LogFormatParser<'a> {
        LogFormatParser {
            format,
            iter: format.char_indices().peekable(),
            fields: Vec::new(),
        }
    }

    fn parse(mut self) -> Result<Vec<LogField>, ParseError> {
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
            eprintln!("Finishing up: \"{}\"", &self.format[self.pos()..]);
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
        while let Some(&(i, c)) = self.iter.peek() {
            if c == '\'' {
                break;
            } else if c == '$' {
                eprintln!("Found variable");
                self.iter.next();
                let var = self.read_identifier()?;
                eprintln!("Read identifier: {}", var);
                if var == "remote_addr" {
                    self.fields.push(LogField::RemoteAddr);
                } else if var == "remote_user" {
                    self.fields.push(LogField::RemoteUser);
                } else if var == "time_local" {
                    self.fields.push(LogField::TimeLocal);
                } else if var == "request" {
                    self.fields.push(LogField::Request);
                } else if var == "status" {
                    self.fields.push(LogField::Status);
                } else if var == "body_bytes_sent" {
                    self.fields.push(LogField::BodyBytesSent);
                } else if var == "http_referer" {
                    self.fields.push(LogField::HttpReferer);
                } else if var == "http_user_agent" {
                    self.fields.push(LogField::HttpUserAgent);
                } else {
                    return Err(ParseError(format!("Unknown variable: {}", var)));
                }
            } else {
                eprintln!("Found character {:?}", c);
                self.iter.next();
                self.fields.push(LogField::Character(c));
            }
        }
        Ok(())
    }

    fn pos(&mut self) -> usize {
        match self.iter.peek() {
            None => self.format.len(),
            Some((i, _)) => *i,
        }
    }

    fn skip_whitespace(&mut self) {
        loop {
            match self.iter.peek() {
                None => {
                    self.iter.next();
                    return;
                }
                Some((i, c)) => {
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
        let start = self.pos();
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
    assert_eq!(
        LogFormatParser::new("log_format combined '$remote_addr - $remote_user [$time_local]';").parse().unwrap(),
        vec![LogField::RemoteAddr, LogField::Character(' '), LogField::Character('-'), LogField::Character(' '), LogField::RemoteUser, LogField::Character(' '), LogField::Character('['), LogField::TimeLocal, LogField::Character(']')],
    );
    assert_eq!(
        LogFormatParser::new("    log_format '$remote_addr - $remote_user [$time_local]';  ").parse().unwrap(),
        vec![LogField::RemoteAddr, LogField::Character(' '), LogField::Character('-'), LogField::Character(' '), LogField::RemoteUser, LogField::Character(' '), LogField::Character('['), LogField::TimeLocal, LogField::Character(']')],
    );
    assert_eq!(
        LogFormatParser::new("$remote_addr - $remote_user [$time_local]").parse().unwrap(),
        vec![LogField::RemoteAddr, LogField::Character(' '), LogField::Character('-'), LogField::Character(' '), LogField::RemoteUser, LogField::Character(' '), LogField::Character('['), LogField::TimeLocal, LogField::Character(']')],
    );
}