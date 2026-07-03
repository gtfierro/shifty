//! Parse a SPARQL 1.1 property path expression (the `Path` production, SPARQL
//! 1.1 §18.2 grammar rules [88]-[94]) into the [`Path`] algebra.
//!
//! This exists as its own small recursive-descent parser rather than reusing
//! `spargebra`'s query parser: `spargebra` lowers a parsed `SELECT ?s <path>
//! ?o` straight to algebra, and for "simple" paths (no alternation/Kleene
//! forms) that lowering silently rewrites the path into a chain of joined
//! triple patterns rather than keeping a `PropertyPathExpression` tree —
//! there is no reliable way to recover an arbitrary [`Path`] from the result.
//! The `Path` grammar production itself is small and has been stable since
//! SPARQL 1.1, so parsing it directly is the more robust option.
//!
//! Supported: sequence (`/`), alternation (`|`), inverse (`^`), the Kleene
//! forms (`*`, `+`, `?`), grouping (`( … )`), and the `a` keyword for
//! `rdf:type`. Negated property sets (`!…`) have no equivalent in the [`Path`]
//! algebra and are rejected with a clear error.

use crate::graph::Loaded;
use crate::vocab;
use oxrdf::NamedNode;
use shifty_algebra::Path;

/// Parse a SPARQL 1.1 property path expression, resolving prefixed names
/// (`prefix:local`) against `shapes`' declared `@prefix`es and `a` against
/// `rdf:type`. `<absolute IRI>` forms are also accepted.
pub fn parse_property_path(expr: &str, shapes: &Loaded) -> Result<Path, String> {
    let tokens = lex(expr)?;
    let mut parser = Parser {
        tokens: &tokens,
        pos: 0,
        prefixes: &shapes.prefixes,
    };
    let path = parser.parse_path_alternative()?;
    if parser.pos != parser.tokens.len() {
        return Err(format!(
            "unexpected trailing input in property path {expr:?} at token {}",
            parser.pos
        ));
    }
    Ok(path)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// From `<...>` — already an absolute IRI, used as-is.
    AbsoluteIri(String),
    /// From a bare `prefix:local` — must resolve through `prefixes`.
    PrefixedName(String),
    A,
    Caret,
    Slash,
    Pipe,
    Star,
    Plus,
    Question,
    Bang,
    LParen,
    RParen,
}

fn lex(expr: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let mut chars = expr.char_indices().peekable();
    while let Some(&(i, c)) = chars.peek() {
        match c {
            c if c.is_whitespace() => {
                chars.next();
            }
            '^' => {
                tokens.push(Token::Caret);
                chars.next();
            }
            '/' => {
                tokens.push(Token::Slash);
                chars.next();
            }
            '|' => {
                tokens.push(Token::Pipe);
                chars.next();
            }
            '*' => {
                tokens.push(Token::Star);
                chars.next();
            }
            '+' => {
                tokens.push(Token::Plus);
                chars.next();
            }
            '?' => {
                tokens.push(Token::Question);
                chars.next();
            }
            '!' => {
                tokens.push(Token::Bang);
                chars.next();
            }
            '(' => {
                tokens.push(Token::LParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RParen);
                chars.next();
            }
            '<' => {
                chars.next();
                let start = i + 1;
                let mut end = expr.len();
                let mut closed = false;
                for (j, c2) in chars.by_ref() {
                    if c2 == '>' {
                        end = j;
                        closed = true;
                        break;
                    }
                }
                if !closed {
                    return Err(format!("unterminated <IRI> in property path {expr:?}"));
                }
                tokens.push(Token::AbsoluteIri(expr[start..end].to_string()));
            }
            c if is_name_char(c) => {
                let start = i;
                let mut end = expr.len();
                while let Some(&(j, c2)) = chars.peek() {
                    if is_name_char(c2) {
                        chars.next();
                    } else {
                        end = j;
                        break;
                    }
                }
                let word = &expr[start..end];
                if word == "a" {
                    tokens.push(Token::A);
                } else if word.contains(':') {
                    tokens.push(Token::PrefixedName(word.to_string()));
                } else {
                    return Err(format!(
                        "expected 'prefix:local' or 'a' in property path, got {word:?}"
                    ));
                }
            }
            other => {
                return Err(format!(
                    "unexpected character {other:?} in property path {expr:?}"
                ));
            }
        }
    }
    Ok(tokens)
}

fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | ':')
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
    prefixes: &'a [(String, String)],
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn eat(&mut self, token: &Token) -> bool {
        if self.peek() == Some(token) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, token: Token) -> Result<(), String> {
        if self.eat(&token) {
            Ok(())
        } else {
            Err(format!(
                "expected {token:?} in property path, got {:?}",
                self.peek()
            ))
        }
    }

    // PathAlternative ::= PathSequence ( '|' PathSequence )*
    fn parse_path_alternative(&mut self) -> Result<Path, String> {
        let mut parts = vec![self.parse_path_sequence()?];
        while self.eat(&Token::Pipe) {
            parts.push(self.parse_path_sequence()?);
        }
        Ok(Path::alt(parts))
    }

    // PathSequence ::= PathEltOrInverse ( '/' PathEltOrInverse )*
    fn parse_path_sequence(&mut self) -> Result<Path, String> {
        let mut parts = vec![self.parse_path_elt_or_inverse()?];
        while self.eat(&Token::Slash) {
            parts.push(self.parse_path_elt_or_inverse()?);
        }
        Ok(Path::seq(parts))
    }

    // PathEltOrInverse ::= PathElt | '^' PathElt
    fn parse_path_elt_or_inverse(&mut self) -> Result<Path, String> {
        if self.eat(&Token::Caret) {
            Ok(self.parse_path_elt()?.inverse())
        } else {
            self.parse_path_elt()
        }
    }

    // PathElt ::= PathPrimary PathMod?
    fn parse_path_elt(&mut self) -> Result<Path, String> {
        let primary = self.parse_path_primary()?;
        if self.eat(&Token::Star) {
            Ok(primary.star())
        } else if self.eat(&Token::Plus) {
            Ok(primary.one_or_more())
        } else if self.eat(&Token::Question) {
            Ok(primary.zero_or_one())
        } else {
            Ok(primary)
        }
    }

    // PathPrimary ::= iri | 'a' | '!' PathNegatedPropertySet | '(' Path ')'
    fn parse_path_primary(&mut self) -> Result<Path, String> {
        match self.tokens.get(self.pos).cloned() {
            Some(Token::AbsoluteIri(iri)) => {
                self.pos += 1;
                let named =
                    NamedNode::new(&iri).map_err(|e| format!("invalid IRI <{iri}>: {e}"))?;
                Ok(Path::Pred(named))
            }
            Some(Token::PrefixedName(name)) => {
                self.pos += 1;
                let named = resolve_prefixed_name(&name, self.prefixes)?;
                Ok(Path::Pred(named))
            }
            Some(Token::A) => {
                self.pos += 1;
                Ok(Path::Pred(vocab::rdf_type()))
            }
            Some(Token::LParen) => {
                self.pos += 1;
                let inner = self.parse_path_alternative()?;
                self.expect(Token::RParen)?;
                Ok(inner)
            }
            Some(Token::Bang) => Err(
                "negated property sets ('!...') have no equivalent in the path algebra".to_string(),
            ),
            other => Err(format!("unexpected token {other:?} in property path")),
        }
    }
}

/// Resolve a bare `prefix:local` token against `prefixes`. Unlike `<...>`,
/// this never falls back to treating the token as an already-absolute IRI:
/// `prefix:local` is only ever a CURIE here, and an undeclared prefix (e.g. a
/// typo) must be a hard error rather than silently accepted as some other
/// IRI scheme (`nope:p` is, syntactically, itself a valid absolute IRI).
fn resolve_prefixed_name(token: &str, prefixes: &[(String, String)]) -> Result<NamedNode, String> {
    let (prefix, local) = token
        .split_once(':')
        .expect("PrefixedName tokens always contain ':'");
    let (_, namespace) = prefixes
        .iter()
        .find(|(p, _)| p == prefix)
        .ok_or_else(|| format!("undeclared prefix {prefix:?} in property path {token:?}"))?;
    NamedNode::new(format!("{namespace}{local}"))
        .map_err(|e| format!("invalid IRI from {token:?}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Loaded;

    fn shapes_with_prefixes() -> Loaded {
        Loaded::from_turtle(
            b"@prefix ex: <http://ex/> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . ex:s ex:p ex:o .",
            None,
        )
        .unwrap()
    }

    fn pred(iri: &str) -> Path {
        Path::Pred(NamedNode::new(iri).unwrap())
    }

    #[test]
    fn single_predicate() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("ex:p", &shapes).unwrap(),
            pred("http://ex/p")
        );
    }

    #[test]
    fn absolute_iri() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("<http://ex/p>", &shapes).unwrap(),
            pred("http://ex/p")
        );
    }

    #[test]
    fn sequence() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("ex:p/rdfs:label", &shapes).unwrap(),
            Path::Seq(vec![
                pred("http://ex/p"),
                pred("http://www.w3.org/2000/01/rdf-schema#label")
            ])
        );
    }

    #[test]
    fn inverse() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("^ex:p", &shapes).unwrap(),
            pred("http://ex/p").inverse()
        );
    }

    #[test]
    fn inverse_then_sequence() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("^ex:p/rdfs:label", &shapes).unwrap(),
            Path::Seq(vec![
                pred("http://ex/p").inverse(),
                pred("http://www.w3.org/2000/01/rdf-schema#label")
            ])
        );
    }

    #[test]
    fn alternation() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("ex:p|ex:q", &shapes).unwrap(),
            Path::Alt(vec![pred("http://ex/p"), pred("http://ex/q")])
        );
    }

    #[test]
    fn zero_or_more() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("ex:p*", &shapes).unwrap(),
            pred("http://ex/p").star()
        );
    }

    #[test]
    fn one_or_more() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("ex:p+", &shapes).unwrap(),
            pred("http://ex/p").one_or_more()
        );
    }

    #[test]
    fn zero_or_one() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("ex:p?", &shapes).unwrap(),
            pred("http://ex/p").zero_or_one()
        );
    }

    #[test]
    fn grouping_changes_precedence() {
        let shapes = shapes_with_prefixes();
        // Without grouping, / binds tighter than |, so this is p/(q|r).
        assert_eq!(
            parse_property_path("ex:p/(ex:q|ex:r)", &shapes).unwrap(),
            Path::Seq(vec![
                pred("http://ex/p"),
                Path::Alt(vec![pred("http://ex/q"), pred("http://ex/r")]),
            ])
        );
    }

    #[test]
    fn a_keyword_is_rdf_type() {
        let shapes = shapes_with_prefixes();
        assert_eq!(
            parse_property_path("a", &shapes).unwrap(),
            pred("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        );
    }

    #[test]
    fn unknown_prefix_is_an_error() {
        let shapes = shapes_with_prefixes();
        assert!(parse_property_path("nope:p", &shapes).is_err());
    }

    #[test]
    fn negated_property_set_is_rejected() {
        let shapes = shapes_with_prefixes();
        let err = parse_property_path("!ex:p", &shapes).unwrap_err();
        assert!(err.contains("negated property set"), "{err}");
    }

    #[test]
    fn trailing_garbage_is_an_error() {
        let shapes = shapes_with_prefixes();
        assert!(parse_property_path("ex:p )", &shapes).is_err());
    }
}
