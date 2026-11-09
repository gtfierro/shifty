//! Tokenizer for Shapeshift text.
//!
//! Newlines are tokens: inside a shape body a line ends at a newline or `;`.
//! The parser skips them where they carry no meaning (headers, between
//! targets, between the blocks of a boolean frame). Comments are tokens too,
//! so the parser can hand them to the formatter.

use shapeshift_macro::ast::Span;

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    /// `<...>`
    Iri(String),
    /// `"..."` with an optional `@lang`.
    Str {
        value: String,
        lang: Option<String>,
    },
    /// Unsigned integer.
    Int(u64),
    /// Any other number (signed, decimal, exponent), lexical form.
    Num(String),
    /// `prefix:local` (either part may be empty).
    PName {
        prefix: String,
        local: String,
    },
    /// Identifier or keyword; may be dotted (`alias.Shape`).
    Name(String),
    LBrace,
    RBrace,
    LParen,
    RParen,
    Comma,
    Semi,
    Caret,
    Slash,
    Star,
    Plus,
    Question,
    Newline,
    /// `-- text` (text without the dashes, trimmed).
    Comment(String),
    Eof,
}

impl Tok {
    pub fn describe(&self) -> String {
        match self {
            Tok::Iri(i) => format!("<{i}>"),
            Tok::Str { value, .. } => format!("\"{value}\""),
            Tok::Int(n) => n.to_string(),
            Tok::Num(n) => n.clone(),
            Tok::PName { prefix, local } => format!("`{prefix}:{local}`"),
            Tok::Name(n) => format!("`{n}`"),
            Tok::LBrace => "`{`".into(),
            Tok::RBrace => "`}`".into(),
            Tok::LParen => "`(`".into(),
            Tok::RParen => "`)`".into(),
            Tok::Comma => "`,`".into(),
            Tok::Semi => "`;`".into(),
            Tok::Caret => "`^`".into(),
            Tok::Slash => "`/`".into(),
            Tok::Star => "`*`".into(),
            Tok::Plus => "`+`".into(),
            Tok::Question => "`?`".into(),
            Tok::Newline => "end of line".into(),
            Tok::Comment(_) => "a comment".into(),
            Tok::Eof => "end of file".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub tok: Tok,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LexError {
    pub message: String,
    pub span: Span,
}

fn is_name_start(c: char) -> bool {
    c.is_alphabetic() || c == '_'
}

fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-'
}

fn is_local_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '%'
}

pub fn lex(src: &str) -> (Vec<Token>, Vec<LexError>) {
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0;
    let mut line = 1u32;
    let mut col = 1u32;
    let mut out = Vec::new();
    let mut errors = Vec::new();

    macro_rules! bump {
        () => {{
            let c = chars[i];
            i += 1;
            if c == '\n' {
                line += 1;
                col = 1;
            } else {
                col += 1;
            }
            c
        }};
    }

    while i < chars.len() {
        let c = chars[i];
        let span = Span::new(line, col);

        match c {
            ' ' | '\t' | '\r' => {
                bump!();
            }
            '\n' => {
                bump!();
                out.push(Token {
                    tok: Tok::Newline,
                    span,
                });
            }
            '-' if chars.get(i + 1).copied() == Some('-') => {
                bump!();
                bump!();
                let mut text = String::new();
                while i < chars.len() && chars[i] != '\n' {
                    text.push(bump!());
                }
                out.push(Token {
                    tok: Tok::Comment(text.trim().to_string()),
                    span,
                });
            }
            '{' | '}' | '(' | ')' | ',' | ';' | '^' | '/' | '*' | '+' | '?' => {
                bump!();
                let tok = match c {
                    '{' => Tok::LBrace,
                    '}' => Tok::RBrace,
                    '(' => Tok::LParen,
                    ')' => Tok::RParen,
                    ',' => Tok::Comma,
                    ';' => Tok::Semi,
                    '^' => Tok::Caret,
                    '/' => Tok::Slash,
                    '*' => Tok::Star,
                    '+' => Tok::Plus,
                    _ => Tok::Question,
                };
                out.push(Token { tok, span });
            }
            '<' => {
                bump!();
                let mut iri = String::new();
                let mut closed = false;
                while i < chars.len() {
                    let c = bump!();
                    if c == '>' {
                        closed = true;
                        break;
                    }
                    if c == '\n' || c == ' ' {
                        break;
                    }
                    iri.push(c);
                }
                if !closed {
                    errors.push(LexError {
                        message: "unterminated IRI: expected `>`".into(),
                        span,
                    });
                }
                out.push(Token {
                    tok: Tok::Iri(iri),
                    span,
                });
            }
            '"' => {
                let triple = chars.get(i + 1).copied() == Some('"')
                    && chars.get(i + 2).copied() == Some('"');
                bump!();
                if triple {
                    bump!();
                    bump!();
                }
                let mut value = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if triple {
                        if chars[i] == '"'
                            && chars.get(i + 1).copied() == Some('"')
                            && chars.get(i + 2).copied() == Some('"')
                        {
                            bump!();
                            bump!();
                            bump!();
                            closed = true;
                            break;
                        }
                    } else if chars[i] == '"' {
                        bump!();
                        closed = true;
                        break;
                    } else if chars[i] == '\n' {
                        break;
                    }
                    let c = bump!();
                    if c == '\\' && i < chars.len() {
                        let e = bump!();
                        value.push(match e {
                            'n' => '\n',
                            't' => '\t',
                            'r' => '\r',
                            other => other,
                        });
                    } else {
                        value.push(c);
                    }
                }
                if !closed {
                    errors.push(LexError {
                        message: "unterminated string: expected a closing `\"`".into(),
                        span,
                    });
                }
                let mut lang = None;
                if i < chars.len() && chars[i] == '@' {
                    bump!();
                    let mut l = String::new();
                    while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '-') {
                        l.push(bump!());
                    }
                    lang = Some(l);
                }
                out.push(Token {
                    tok: Tok::Str { value, lang },
                    span,
                });
            }
            c if c.is_ascii_digit()
                || ((c == '-' || c == '.')
                    && chars
                        .get(i + 1)
                        .copied()
                        .is_some_and(|d| d.is_ascii_digit())) =>
            {
                let mut text = String::new();
                text.push(bump!());
                while i < chars.len() {
                    let d = chars[i];
                    let prev = text.chars().last().unwrap();
                    let exp_sign = (d == '-' || d == '+') && (prev == 'e' || prev == 'E');
                    if d.is_ascii_digit()
                        || exp_sign
                        || ((d == 'e' || d == 'E') && !text.contains(['e', 'E']))
                        || (d == '.'
                            && !text.contains('.')
                            && chars.get(i + 1).is_some_and(|x| x.is_ascii_digit()))
                    {
                        text.push(bump!());
                    } else {
                        break;
                    }
                }
                let tok = match text.parse::<u64>() {
                    Ok(n) => Tok::Int(n),
                    Err(_) => Tok::Num(text),
                };
                out.push(Token { tok, span });
            }
            ':' => {
                bump!();
                let mut local = String::new();
                while i < chars.len() && is_local_char(chars[i]) {
                    local.push(bump!());
                }
                while local.ends_with('.') {
                    local.pop();
                    i -= 1;
                    col -= 1;
                }
                out.push(Token {
                    tok: Tok::PName {
                        prefix: String::new(),
                        local,
                    },
                    span,
                });
            }
            c if is_name_start(c) => {
                let mut name = String::new();
                while i < chars.len() {
                    let d = chars[i];
                    // `--` starts a comment; a `.` joins dotted names (`m.Shape`).
                    let in_name =
                        is_name_char(d) && !(d == '-' && chars.get(i + 1).copied() == Some('-'));
                    let dotted = d == '.' && chars.get(i + 1).is_some_and(|x| is_name_start(*x));
                    if in_name || dotted {
                        name.push(bump!());
                    } else {
                        break;
                    }
                }
                if i < chars.len() && chars[i] == ':' && !name.contains('.') {
                    bump!();
                    let mut local = String::new();
                    while i < chars.len() && is_local_char(chars[i]) {
                        local.push(bump!());
                    }
                    while local.ends_with('.') {
                        local.pop();
                        i -= 1;
                        col -= 1;
                    }
                    out.push(Token {
                        tok: Tok::PName {
                            prefix: name,
                            local,
                        },
                        span,
                    });
                } else {
                    out.push(Token {
                        tok: Tok::Name(name),
                        span,
                    });
                }
            }
            other => {
                bump!();
                errors.push(LexError {
                    message: format!("unexpected character `{other}`"),
                    span,
                });
            }
        }
    }
    out.push(Token {
        tok: Tok::Eof,
        span: Span::new(line, col),
    });
    (out, errors)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(s: &str) -> Vec<Tok> {
        lex(s).0.into_iter().map(|t| t.tok).collect()
    }

    #[test]
    fn prefixed_names_and_labels() {
        assert_eq!(
            toks("supplyTemp: brick:hasPoint"),
            vec![
                Tok::PName {
                    prefix: "supplyTemp".into(),
                    local: "".into()
                },
                Tok::PName {
                    prefix: "brick".into(),
                    local: "hasPoint".into()
                },
                Tok::Eof
            ]
        );
    }

    #[test]
    fn dotted_names_comments_and_numbers() {
        assert_eq!(
            toks("include VAV.fedBy -- note\n-1.5 3"),
            vec![
                Tok::Name("include".into()),
                Tok::Name("VAV.fedBy".into()),
                Tok::Comment("note".into()),
                Tok::Newline,
                Tok::Num("-1.5".into()),
                Tok::Int(3),
                Tok::Eof
            ]
        );
    }

    #[test]
    fn local_name_does_not_swallow_trailing_dot() {
        assert_eq!(
            toks("ex:a."),
            vec![
                Tok::PName {
                    prefix: "ex".into(),
                    local: "a".into()
                },
                Tok::Eof
            ]
        );
    }
}
