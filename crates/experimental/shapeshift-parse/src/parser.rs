//! Recursive-descent parser: tokens to [`Module`].
//!
//! The grammar has three rigid frames (the shape block, the field line, the
//! boolean block), so the parser is a handful of small functions, one per
//! frame. Errors are recovered at line granularity: a bad line is reported
//! and skipped, and parsing continues with the next one, so one typo does not
//! hide every other diagnostic in the file.

use crate::lexer::{Tok, Token, lex};
use shapeshift_macro::ast::*;
use shapeshift_macro::build::DATATYPE_KEYWORDS;
use shapeshift_macro::diagnostic::Diagnostic;
use shifty_algebra::SourceLocation;

/// Words with a fixed meaning; they cannot be used as names.
pub const KEYWORDS: &[&str] = &[
    "a",
    "above",
    "and",
    "any",
    "as",
    "at",
    "base",
    "below",
    "between",
    "blank",
    "datatype",
    "different",
    "doc",
    "each",
    "every",
    "exactly",
    "false",
    "flags",
    "for",
    "from",
    "import",
    "iri",
    "is",
    "language",
    "least",
    "length",
    "literal",
    "matches",
    "matching",
    "message",
    "most",
    "node",
    "none",
    "of",
    "one",
    "only",
    "optional",
    "or",
    "path",
    "pattern",
    "prefix",
    "properties",
    "required",
    "same",
    "self",
    "severity",
    "shape",
    "sparql",
    "then",
    "these",
    "to",
    "true",
    "unique",
    "up",
    "value",
    "when",
    "with",
];

pub struct ParseOutput {
    pub module: Module,
    pub diagnostics: Vec<Diagnostic>,
}

impl ParseOutput {
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }
}

/// Parse `src`; `label` names the file in diagnostics.
pub fn parse(src: &str, label: &str) -> ParseOutput {
    let (tokens, lex_errors) = lex(src);
    let mut p = Parser {
        toks: tokens,
        pos: 0,
        label: label.to_string(),
        diags: Vec::new(),
        comments: Vec::new(),
    };
    for e in lex_errors {
        let loc = p.loc(e.span);
        p.diags.push(Diagnostic::error(e.message, loc));
    }
    let module = p.module();
    ParseOutput {
        module,
        diagnostics: p.diags,
    }
}

struct Fail;
type R<T> = Result<T, Fail>;

struct Parser {
    toks: Vec<Token>,
    pos: usize,
    label: String,
    diags: Vec<Diagnostic>,
    /// Comments seen since the last construct, waiting to attach to the next.
    comments: Vec<String>,
}

fn is_kw(t: &Tok, kw: &str) -> bool {
    matches!(t, Tok::Name(n) if n == kw)
}

impl Parser {
    fn loc(&self, span: Span) -> Option<SourceLocation> {
        Some(SourceLocation {
            file: self.label.clone(),
            line: span.line,
            column: span.column,
        })
    }

    fn peek(&self) -> &Tok {
        &self.toks[self.pos].tok
    }

    fn peek_at(&self, k: usize) -> &Tok {
        let i = (self.pos + k).min(self.toks.len() - 1);
        &self.toks[i].tok
    }

    fn span(&self) -> Span {
        self.toks[self.pos].span
    }

    fn bump(&mut self) -> Token {
        let t = self.toks[self.pos].clone();
        if self.pos < self.toks.len() - 1 {
            self.pos += 1;
        }
        t
    }

    fn at_kw(&self, kw: &str) -> bool {
        is_kw(self.peek(), kw)
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.at_kw(kw) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn fail<T>(&mut self, msg: impl Into<String>) -> R<T> {
        let span = self.span();
        let loc = self.loc(span);
        self.diags.push(Diagnostic::error(msg, loc));
        Err(Fail)
    }

    fn fail_hint<T>(&mut self, msg: impl Into<String>, hint: impl Into<String>) -> R<T> {
        let span = self.span();
        let loc = self.loc(span);
        self.diags
            .push(Diagnostic::error(msg, loc).with_hint(hint.into()));
        Err(Fail)
    }

    /// Report a spelling the language no longer has, and the one it has
    /// instead. Parsing goes on: the line means what it meant.
    fn removed(&mut self, span: Span, msg: impl Into<String>, hint: impl Into<String>) {
        let loc = self.loc(span);
        self.diags
            .push(Diagnostic::error(msg, loc).with_hint(hint.into()));
    }

    fn expect_kw(&mut self, kw: &str, context: &str) -> R<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            let found = self.peek().describe();
            self.fail(format!("expected `{kw}` {context}, found {found}"))
        }
    }

    /// Skip newlines and comments (collecting comments).
    fn skip_nl(&mut self) {
        loop {
            match self.peek().clone() {
                Tok::Newline => {
                    self.bump();
                }
                Tok::Comment(c) => {
                    self.comments.push(c);
                    self.bump();
                }
                _ => break,
            }
        }
    }

    /// Look past newlines/comments without consuming: the next real token.
    fn peek_past_nl(&self) -> &Tok {
        let mut i = self.pos;
        while i < self.toks.len() - 1 && matches!(self.toks[i].tok, Tok::Newline | Tok::Comment(_))
        {
            i += 1;
        }
        &self.toks[i].tok
    }

    fn take_comments(&mut self) -> Vec<String> {
        std::mem::take(&mut self.comments)
    }

    /// Recover: skip to the end of the current line (or a closing brace at
    /// this depth).
    fn recover_line(&mut self) {
        let mut depth = 0i32;
        loop {
            match self.peek() {
                Tok::Eof => return,
                Tok::Newline | Tok::Semi if depth <= 0 => return,
                Tok::RBrace if depth <= 0 => return,
                Tok::LBrace => depth += 1,
                Tok::RBrace => depth -= 1,
                _ => {}
            }
            self.bump();
        }
    }

    /// Recover at top level: skip to the next `shape`/header keyword at the
    /// start of a line, outside braces.
    fn recover_top(&mut self) {
        let mut depth = 0i32;
        let mut line_start = false;
        loop {
            match self.peek() {
                Tok::Eof => return,
                Tok::LBrace => depth += 1,
                Tok::RBrace => {
                    depth -= 1;
                    if depth <= 0 {
                        self.bump();
                        return;
                    }
                }
                Tok::Newline => {
                    line_start = true;
                    self.bump();
                    continue;
                }
                Tok::Name(n)
                    if depth <= 0
                        && line_start
                        && matches!(
                            n.as_str(),
                            "shape" | "path" | "prefix" | "import" | "base" | "doc"
                        ) =>
                {
                    return;
                }
                _ => {}
            }
            line_start = false;
            self.bump();
        }
    }

    fn name(&mut self, what: &str) -> R<String> {
        match self.peek().clone() {
            Tok::Name(n) => {
                if KEYWORDS.contains(&n.as_str()) {
                    return self.fail(format!("`{n}` is a keyword and cannot be used as {what}"));
                }
                self.bump();
                Ok(n)
            }
            other => self.fail(format!("expected {what}, found {}", other.describe())),
        }
    }

    fn string(&mut self, what: &str) -> R<String> {
        match self.peek().clone() {
            Tok::Str { value, .. } => {
                self.bump();
                Ok(value)
            }
            other => self.fail(format!(
                "expected {what} (a quoted string), found {}",
                other.describe()
            )),
        }
    }

    // ----------------------------------------------------------------- file

    fn module(&mut self) -> Module {
        let mut m = Module::default();
        let mut pending = Annotations::default();
        let mut first = true;
        loop {
            self.skip_nl();
            if first {
                // Comments before a blank line belong to the file, not to
                // the first declaration.
                first = false;
                if !self.comments.is_empty() && matches!(self.peek(), Tok::Eof) {
                    m.leading_comments = self.take_comments();
                }
            }
            let span = self.span();
            let r: R<()> = match self.peek().clone() {
                Tok::Eof => break,
                Tok::Name(k) if k == "base" => {
                    self.bump();
                    match self.peek().clone() {
                        Tok::Iri(i) => {
                            self.bump();
                            if m.base.is_some() {
                                let loc = self.loc(span);
                                self.diags
                                    .push(Diagnostic::error("`base` is declared twice", loc));
                            }
                            m.leading_comments.extend(self.take_comments());
                            m.base = Some(i);
                            m.header_order.push(HeaderRef::Base);
                            self.end_header()
                        }
                        other => self.fail(format!(
                            "expected an IRI after `base`, found {}",
                            other.describe()
                        )),
                    }
                }
                Tok::Name(k) if k == "prefix" => {
                    self.bump();
                    let name = match self.peek().clone() {
                        Tok::PName { prefix, local } if local.is_empty() => {
                            self.bump();
                            Ok(prefix)
                        }
                        other => self.fail_hint(
                            format!(
                                "expected a prefix name like `brick:` after `prefix`, found {}",
                                other.describe()
                            ),
                            "write `prefix brick: <https://brickschema.org/schema/Brick#>`",
                        ),
                    };
                    match (name, self.peek().clone()) {
                        (Ok(name), Tok::Iri(iri)) => {
                            self.bump();
                            let comments = self.take_comments();
                            m.header_order.push(HeaderRef::Prefix(m.prefixes.len()));
                            m.prefixes.push(PrefixDecl {
                                name,
                                iri,
                                span,
                                comments,
                            });
                            self.end_header()
                        }
                        (Ok(_), other) => self.fail(format!(
                            "expected the namespace IRI, found {}",
                            other.describe()
                        )),
                        (Err(e), _) => Err(e),
                    }
                }
                Tok::Name(k) if k == "import" => {
                    self.bump();
                    let source = match self.peek().clone() {
                        Tok::Iri(i) => {
                            self.bump();
                            Ok(ImportTarget::Iri(i))
                        }
                        Tok::Str { value, .. } => {
                            self.bump();
                            Ok(ImportTarget::Path(value))
                        }
                        other => self.fail(format!(
                            "expected an IRI or a quoted path after `import`, found {}",
                            other.describe()
                        )),
                    };
                    match source {
                        Ok(source) => {
                            let alias = if self.eat_kw("as") {
                                self.name("an import alias").ok()
                            } else {
                                None
                            };
                            let comments = self.take_comments();
                            let trailing_comment = self.trailing_comment();
                            m.header_order.push(HeaderRef::Import(m.imports.len()));
                            m.imports.push(ImportDecl {
                                source,
                                alias,
                                span,
                                comments,
                                trailing_comment,
                            });
                            self.end_header()
                        }
                        Err(e) => Err(e),
                    }
                }
                Tok::Name(k) if matches!(k.as_str(), "doc" | "severity" | "message") => {
                    self.annotation(&mut pending)
                }
                Tok::Name(k) if k == "path" => {
                    let annotations = std::mem::take(&mut pending);
                    match self.path_decl(annotations) {
                        Ok(l) => {
                            m.header_order.push(HeaderRef::Path(m.paths.len()));
                            m.paths.push(l);
                            self.end_header()
                        }
                        Err(e) => Err(e),
                    }
                }
                Tok::Name(k) if k == "shape" => match self.shape(std::mem::take(&mut pending)) {
                    Ok(s) => {
                        m.shapes.push(s);
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                other => self.fail_hint(
                    format!(
                        "expected `shape`, `path`, `prefix`, `import`, or `base`, found {}",
                        other.describe()
                    ),
                    "every rule starts with `shape Name { ... }`",
                ),
            };
            if r.is_err() {
                self.recover_top();
            }
        }
        if !pending.is_empty() {
            let loc = self.loc(self.span());
            self.diags.push(Diagnostic::error(
                "a `doc`, `severity`, or `message` line must be followed by the shape or line it describes",
                loc,
            ));
        }
        m.trailing_comments = self.take_comments();
        m
    }

    fn trailing_comment(&mut self) -> Option<String> {
        if let Tok::Comment(c) = self.peek().clone() {
            self.bump();
            Some(c)
        } else {
            None
        }
    }

    fn end_header(&mut self) -> R<()> {
        match self.peek() {
            Tok::Newline | Tok::Eof | Tok::Semi | Tok::Comment(_) => Ok(()),
            other => {
                let d = other.describe();
                self.fail(format!("unexpected {d} after declaration"))
            }
        }
    }

    fn annotation(&mut self, a: &mut Annotations) -> R<()> {
        let kw = match self.bump().tok {
            Tok::Name(k) => k,
            _ => unreachable!(),
        };
        match kw.as_str() {
            "doc" => {
                let s = self.string("a docstring")?;
                a.doc = Some(match a.doc.take() {
                    Some(prev) => format!("{prev}\n{s}"),
                    None => s,
                });
            }
            "message" => {
                let s = self.string("a message")?;
                a.messages.push(s);
            }
            "severity" => {
                let level = match self.peek().clone() {
                    Tok::Name(w) if w == "info" => SeverityLevel::Info,
                    Tok::Name(w) if w == "warning" => SeverityLevel::Warning,
                    Tok::Name(w) if w == "violation" => SeverityLevel::Violation,
                    other => {
                        return self.fail_hint(
                            format!("unknown severity {}", other.describe()),
                            "use `severity info`, `severity warning`, or `severity violation`",
                        );
                    }
                };
                self.bump();
                a.severity = Some(level);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    // ---------------------------------------------------------------- shape

    fn shape(&mut self, annotations: Annotations) -> R<ShapeDecl> {
        let span = self.span();
        let comments = self.take_comments();
        self.bump(); // shape
        let name = self.name("a shape name")?;
        let params = self.params("shape", &name, span)?;
        let mut targets = Vec::new();
        loop {
            // targets may each sit on their own line
            if matches!(self.peek_past_nl(), Tok::Name(k) if k == "for") {
                self.skip_nl();
                let tspan = self.span();
                self.bump();
                let kind = self.target()?;
                let trailing_comment = self.trailing_comment();
                targets.push(TargetDecl {
                    kind,
                    span: tspan,
                    trailing_comment,
                });
            } else {
                break;
            }
        }
        self.skip_nl();
        let body_comments = self.take_comments();
        if !matches!(self.peek(), Tok::LBrace) {
            let found = self.peek().describe();
            return self.fail_hint(
                format!("expected `{{` to start the body of `{name}`, found {found}"),
                "targets are written `for every <class>`, `for every node with <p>`, `for every value of <p>`, `for node <n>`, `for every node matching <Shape>`, or `for sparql \"...\"`",
            );
        }
        let mut comments = comments;
        comments.extend(body_comments);
        let (body, trailing_comments) = self.body()?;
        Ok(ShapeDecl {
            name,
            params,
            targets,
            body,
            annotations,
            span,
            comments,
            trailing_comments,
        })
    }

    fn target(&mut self) -> R<Target> {
        if self.eat_kw("every") {
            if self.at_kw("node") {
                self.bump();
                if self.eat_kw("with") {
                    return Ok(Target::SubjectsOf(self.node()?));
                }
                if self.eat_kw("matching") {
                    return Ok(Target::Matching(self.call()?));
                }
                let found = self.peek().describe();
                return self.fail_hint(
                    format!("expected `with` or `matching` after `for every node`, found {found}"),
                    "`for every node with <property>` targets subjects; `for every node matching <Shape>` targets conforming nodes",
                );
            }
            if self.at_kw("value") {
                self.bump();
                self.expect_kw("of", "after `for every value`")?;
                return Ok(Target::ObjectsOf(self.node()?));
            }
            return Ok(Target::Class(self.node()?));
        }
        if self.eat_kw("node") {
            return Ok(Target::Node(self.value()?));
        }
        if self.eat_kw("sparql") {
            return Ok(Target::Sparql(self.string("a SPARQL query")?));
        }
        let found = self.peek().describe();
        self.fail_hint(
            format!("expected `every`, `node`, or `sparql` after `for`, found {found}"),
            "write `for every brick:VAV` to target every instance of a class",
        )
    }

    /// `{ line* }` — returns the lines and any comments before the `}`.
    fn body(&mut self) -> R<(Vec<Line>, Vec<String>)> {
        let open = self.span();
        self.bump(); // {
        let mut lines = Vec::new();
        let mut pending = Annotations::default();
        loop {
            // separators
            loop {
                match self.peek().clone() {
                    Tok::Newline | Tok::Semi => {
                        self.bump();
                    }
                    Tok::Comment(c) => {
                        self.comments.push(c);
                        self.bump();
                    }
                    _ => break,
                }
            }
            match self.peek() {
                Tok::RBrace => {
                    self.bump();
                    break;
                }
                Tok::Eof => {
                    let loc = self.loc(open);
                    self.diags.push(
                        Diagnostic::error("this `{` is never closed", loc)
                            .with_hint("add a `}` after the last line of the block"),
                    );
                    return Err(Fail);
                }
                _ => {}
            }
            if matches!(self.peek(), Tok::Name(k) if matches!(k.as_str(), "doc" | "severity" | "message"))
            {
                if self.annotation(&mut pending).is_err() {
                    self.recover_line();
                }
                continue;
            }
            let comments = self.take_comments();
            match self.line(std::mem::take(&mut pending), comments) {
                Ok(l) => {
                    lines.push(l);
                    match self.peek() {
                        Tok::Newline | Tok::Semi | Tok::RBrace | Tok::Eof => {}
                        other => {
                            let d = other.describe();
                            let _ = self.fail::<()>(format!(
                                "unexpected {d}; each line holds one field, shape, or block"
                            ));
                            self.recover_line();
                        }
                    }
                }
                Err(_) => self.recover_line(),
            }
        }
        if !pending.is_empty() {
            let loc = self.loc(self.toks[self.pos.saturating_sub(1)].span);
            self.diags.push(Diagnostic::error(
                "a `doc`, `severity`, or `message` line must be followed by the line it describes",
                loc,
            ));
        }
        Ok((lines, self.take_comments()))
    }

    fn line(&mut self, annotations: Annotations, comments: Vec<String>) -> R<Line> {
        let span = self.span();
        let mut annotations = annotations;
        let kind = match self.peek().clone() {
            Tok::Name(k)
                if k == "include"
                    && matches!(
                        self.peek_at(1),
                        Tok::Name(_) | Tok::PName { .. } | Tok::Iri(_)
                    ) =>
            {
                let at = self.span();
                self.bump();
                let c = self.call()?;
                let text = shapeshift_macro::display::call(&c);
                self.removed(
                    at,
                    "`include` is not part of the language",
                    format!("a shape alone on a line composes it: write `{text}`"),
                );
                LineKind::Include(c)
            }
            Tok::Name(k) if k == "all" && is_kw(self.peek_at(1), "of") => {
                let at = self.span();
                self.bump();
                self.bump();
                let _ = self.boolean(Frame::AnyOf)?;
                self.removed(
                    at,
                    "`all of` is not part of the language",
                    "the lines of a block all hold already: write them as lines, or as one block inside another frame",
                );
                return Err(Fail);
            }
            Tok::Name(k) if k == "only" => {
                self.bump();
                self.expect_kw("these", "in `only these properties`")?;
                self.expect_kw("properties", "in `only these properties`")?;
                LineKind::Closed
            }
            Tok::Name(k) if k == "any" || k == "none" => {
                self.bump();
                self.expect_kw("of", &format!("after `{k}`"))?;
                let frame = match k.as_str() {
                    "any" => Frame::AnyOf,
                    _ => Frame::NoneOf,
                };
                LineKind::Boolean(self.boolean(frame)?)
            }
            Tok::Name(k) if k == "exactly" => {
                self.bump();
                self.expect_kw(
                    "one",
                    "after `exactly` at the start of a line (`exactly one of`)",
                )?;
                self.expect_kw("of", "in `exactly one of`")?;
                LineKind::Boolean(self.boolean(Frame::ExactlyOneOf)?)
            }
            Tok::Name(k) if k == "at" => {
                let at = self.span();
                self.bump();
                let least = if self.eat_kw("least") {
                    true
                } else if self.eat_kw("most") {
                    false
                } else {
                    let found = self.peek().describe();
                    return self.fail(format!(
                        "expected `least` or `most` in `at least n of` or `at most n of`, found {found}"
                    ));
                };
                let word = if least { "least" } else { "most" };
                let n = self.int_lit()?;
                self.expect_kw("of", &format!("in `at {word} {n} of`"))?;
                let b = self.boolean(if least {
                    Frame::AtLeast(n)
                } else {
                    Frame::AtMost(n)
                })?;
                match (least, n) {
                    (true, 0) => {
                        self.removed(at, "`at least 0 of` always holds", "remove the line")
                    }
                    (true, 1) => {
                        self.removed(at, "`at least 1 of` is written `any of`", "write `any of`")
                    }
                    (false, 0) => {
                        self.removed(at, "`at most 0 of` is written `none of`", "write `none of`")
                    }
                    _ => {}
                }
                LineKind::Boolean(b)
            }
            Tok::Name(k) if k == "when" => {
                self.bump();
                let cond = self.block()?;
                if !matches!(self.peek_past_nl(), Tok::Name(t) if t == "then") {
                    let found = self.peek_past_nl().describe();
                    return self.fail_hint(
                        format!("expected `then` after the `when` block, found {found}"),
                        "write `when { condition } then { requirement }`",
                    );
                }
                self.skip_nl();
                self.bump(); // then
                let body = self.block()?;
                LineKind::Boolean(Boolean {
                    frame: Frame::When,
                    blocks: vec![cond, body],
                })
            }
            Tok::Name(k) if k == "then" => {
                return self.fail("`then` without a `when` before it");
            }
            Tok::Name(k) if k == "is" => {
                self.bump();
                let at = self.span();
                let explicit = self.at_kw("matches");
                let t = self.test()?;
                match &t {
                    Test::Matches(c) if explicit => {
                        let text = shapeshift_macro::display::call(c);
                        self.removed(
                            at,
                            "`is matches` is not part of the language",
                            format!("a shape alone on a line composes it: write `{text}`"),
                        );
                    }
                    Test::Shape(_) => self.removed(
                        at,
                        "`is { ... }` is not part of the language",
                        "write the block's lines in place",
                    ),
                    _ => {}
                }
                LineKind::Is(t)
            }
            Tok::PName { prefix, local }
                if local.is_empty()
                    && !matches!(
                        self.peek_at(1),
                        Tok::Newline | Tok::Semi | Tok::RBrace | Tok::Eof
                    ) =>
            {
                // `name: path ...` or `name: { ... }`. A label may be a
                // keyword (`value:`, `length:`): the colon sets it apart.
                self.bump();
                if matches!(self.peek(), Tok::LBrace) {
                    let f = self.field_block(prefix, &mut annotations)?;
                    let text = shapeshift_macro::display::field_text(&f);
                    self.removed(
                        span,
                        "a field is written on one line",
                        format!(
                            "write `{text}`, with its `doc`, `message`, and `severity` on lines before it"
                        ),
                    );
                    LineKind::Field(f)
                } else {
                    self.path_line(Some(prefix))?
                }
            }
            _ => self.path_line(None)?,
        };
        let trailing_comment = self.trailing_comment();
        Ok(Line {
            kind,
            annotations,
            span,
            comments,
            trailing_comment,
        })
    }

    /// `(a, b = default, ...)`, if present.
    fn params(&mut self, what: &str, name: &str, span: Span) -> R<Vec<Param>> {
        let mut params: Vec<Param> = Vec::new();
        if !matches!(self.peek(), Tok::LParen) {
            return Ok(params);
        }
        self.bump();
        loop {
            self.skip_nl();
            if matches!(self.peek(), Tok::RParen) {
                self.bump();
                break;
            }
            let pname = self.name("a parameter name")?;
            let default = if matches!(self.peek(), Tok::Eq) {
                self.bump();
                Some(self.arg()?)
            } else {
                if let Some(prev) = params.iter().rev().find(|p| p.default.is_some()) {
                    let prev = prev.name.clone();
                    return self.fail_hint(
                        format!(
                            "parameter `{pname}` needs a default, since `{prev}` before it has one"
                        ),
                        "put the parameters with defaults last",
                    );
                }
                None
            };
            params.push(Param {
                name: pname,
                default,
            });
            self.skip_nl();
            match self.peek() {
                Tok::Comma => {
                    self.bump();
                }
                Tok::RParen => {
                    self.bump();
                    break;
                }
                other => {
                    let d = other.describe();
                    return self.fail(format!(
                        "expected `,` or `)` in the parameter list, found {d}"
                    ));
                }
            }
        }
        if params.is_empty() {
            let loc = self.loc(span);
            self.diags.push(
                Diagnostic::error(
                    format!("{what} `{name}()` has an empty parameter list"),
                    loc,
                )
                .with_hint(format!("write `{what} {name}` without parentheses")),
            );
        }
        Ok(params)
    }

    // ---------------------------------------------------------- paths, groups

    /// `path NAME = PATH`
    fn path_decl(&mut self, annotations: Annotations) -> R<PathDecl> {
        let span = self.span();
        let comments = self.take_comments();
        self.bump(); // path
        let name = self.name("a path name")?;
        let params = self.params("path", &name, span)?;
        if !matches!(self.peek(), Tok::Eq) {
            let found = self.peek().describe();
            return self.fail_hint(
                format!("expected `=` after `path {name}`, found {found}"),
                format!("write `path {name} = brick:hasPoint`"),
            );
        }
        self.bump(); // =
        let path = self.path()?;
        let trailing_comment = self.trailing_comment();
        Ok(PathDecl {
            name,
            params,
            path,
            annotations,
            span,
            comments,
            trailing_comment,
        })
    }

    /// The rows of a group, from its `{` through the matching `}`.
    fn group_rows(&mut self, head: &str) -> R<(Vec<Row>, Vec<String>)> {
        let open = self.span();
        self.bump(); // {
        let mut rows = Vec::new();
        let mut pending = Annotations::default();
        loop {
            loop {
                match self.peek().clone() {
                    Tok::Newline | Tok::Semi => {
                        self.bump();
                    }
                    Tok::Comment(c) => {
                        self.comments.push(c);
                        self.bump();
                    }
                    _ => break,
                }
            }
            match self.peek() {
                Tok::RBrace => {
                    self.bump();
                    break;
                }
                Tok::Eof => {
                    let loc = self.loc(open);
                    self.diags.push(
                        Diagnostic::error(format!("the `{head}` group is never closed"), loc)
                            .with_hint("add a `}` after the last row"),
                    );
                    return Err(Fail);
                }
                _ => {}
            }
            if matches!(self.peek(), Tok::Name(k) if matches!(k.as_str(), "doc" | "severity" | "message"))
            {
                if self.annotation(&mut pending).is_err() {
                    self.recover_line();
                }
                continue;
            }
            let comments = self.take_comments();
            match self.row(std::mem::take(&mut pending), comments) {
                Ok(r) => {
                    rows.push(r);
                    match self.peek() {
                        Tok::Newline | Tok::Semi | Tok::RBrace | Tok::Eof => {}
                        other => {
                            let d = other.describe();
                            let _ = self.fail::<()>(format!(
                                "unexpected {d}; each row of a group is one line"
                            ));
                            self.recover_line();
                        }
                    }
                }
                Err(_) => self.recover_line(),
            }
        }
        if !pending.is_empty() {
            let loc = self.loc(self.toks[self.pos.saturating_sub(1)].span);
            self.diags.push(Diagnostic::error(
                "a `doc`, `severity`, or `message` line must be followed by the row it describes",
                loc,
            ));
        }
        Ok((rows, self.take_comments()))
    }

    /// `label: ITEM COUNT? (doc "..." | message "..." | severity L)* { line* }?`
    fn row(&mut self, annotations: Annotations, comments: Vec<String>) -> R<Row> {
        let span = self.span();
        let mut annotations = annotations;
        let mut label = None;
        if let Tok::PName { prefix, local } = self.peek().clone()
            && local.is_empty()
            && !matches!(
                self.peek_at(1),
                Tok::Newline | Tok::Semi | Tok::RBrace | Tok::Eof
            )
        {
            // as for a field, a keyword may label a row
            self.bump();
            label = Some(prefix);
        }
        let item = match self.peek().clone() {
            Tok::PName { local, .. } if !local.is_empty() => Item::Class(self.node()?),
            Tok::Iri(_) => Item::Class(self.node()?),
            Tok::Name(n) if !KEYWORDS.contains(&n.as_str()) => Item::Shape(self.call()?),
            other => {
                return self.fail_hint(
                    format!("expected a class or a shape in this row, found {}", other.describe()),
                    "a row reads `label: brick:Fan_Status`, `label: SupplyFan`, or `label: brick:Damper { ... }`",
                );
            }
        };
        let mut count = None;
        let mut body = None;
        while !self.at_line_end() {
            if count.is_none()
                && body.is_none()
                && let Some(c) = self.try_count()?
            {
                count = Some(c);
                continue;
            }
            if let Tok::Name(k) = self.peek().clone()
                && matches!(k.as_str(), "doc" | "severity" | "message")
            {
                let at = self.span();
                self.annotation(&mut annotations)?;
                self.removed(
                    at,
                    format!("`{k}` goes on the line before the row it describes"),
                    format!("move `{k} ...` to its own line above the row"),
                );
                continue;
            }
            if body.is_none() && matches!(self.peek(), Tok::LBrace) {
                body = Some(self.block()?);
                continue;
            }
            let found = self.peek().describe();
            return self.fail_hint(
                format!("unexpected {found} in a group row"),
                "after the class or shape a row may have a count (`optional`, `at least 2`) and a `{ ... }` block",
            );
        }
        let trailing_comment = self.trailing_comment();
        Ok(Row {
            label,
            item,
            count,
            body,
            annotations,
            span,
            comments,
            trailing_comment,
        })
    }

    fn block(&mut self) -> R<Block> {
        self.skip_nl();
        let span = self.span();
        if !matches!(self.peek(), Tok::LBrace) {
            let found = self.peek().describe();
            return self.fail(format!("expected a `{{ ... }}` block, found {found}"));
        }
        let (lines, trailing_comments) = self.body()?;
        Ok(Block {
            lines,
            span,
            trailing_comments,
        })
    }

    fn boolean(&mut self, frame: Frame) -> R<Boolean> {
        let mut blocks = vec![self.block()?];
        while matches!(self.peek_past_nl(), Tok::LBrace) {
            blocks.push(self.block()?);
        }
        if let Frame::AtLeast(n) = frame
            && n as usize > blocks.len()
        {
            let loc = self.loc(blocks[0].span);
            self.diags.push(Diagnostic::warning(
                format!(
                    "`at least {n} of` has only {} blocks, so it can never hold",
                    blocks.len()
                ),
                loc,
            ));
        }
        if let Frame::AtMost(n) = frame
            && n as usize >= blocks.len()
        {
            let loc = self.loc(blocks[0].span);
            self.diags.push(Diagnostic::warning(
                format!(
                    "`at most {n} of` has only {} blocks, so it always holds",
                    blocks.len()
                ),
                loc,
            ));
        }
        Ok(Boolean { frame, blocks })
    }

    // ---------------------------------------------------------------- field

    fn at_line_end(&self) -> bool {
        matches!(
            self.peek(),
            Tok::Newline | Tok::Semi | Tok::RBrace | Tok::Eof | Tok::Comment(_)
        )
    }

    /// `PATH COUNT? qualifiers` (a field) or `PATH COUNT? { row* }` (a group).
    fn path_line(&mut self, name: Option<String>) -> R<LineKind> {
        let path_span = self.span();
        let path = self.path()?;
        let count = self.try_count()?;
        if matches!(self.peek(), Tok::LBrace) {
            let head = shapeshift_macro::display::path(&path);
            if let Some(n) = name {
                let loc = self.loc(path_span);
                self.diags.push(
                    Diagnostic::error(
                        format!("a group has no name of its own; `{n}:` would name nothing"),
                        loc,
                    )
                    .with_hint(format!(
                        "name its rows instead (`{head} {{ {n}: brick:Point }}`), or write `{n}: {head} each {{ ... }}` for values that match an inline shape"
                    )),
                );
                let _ = self.group_rows(&head);
                return Err(Fail);
            }
            let (rows, trailing_comments) = self.group_rows(&head)?;
            return Ok(LineKind::Group(Group {
                path,
                count,
                rows,
                trailing_comments,
            }));
        }
        let mut f = self.field_rest(path, count)?;
        f.name = name;
        Ok(LineKind::Field(f))
    }

    fn field_rest(&mut self, path: PathExpr, count: Option<Count>) -> R<Field> {
        let mut count = count;
        let mut qualifiers = Vec::new();
        let mut first = count.is_none();
        while !self.at_line_end() {
            if !first && matches!(self.peek(), Tok::Comma) {
                self.bump();
            }
            first = false;
            if count.is_none()
                && qualifiers.is_empty()
                && let Some(c) = self.try_count()?
            {
                count = Some(c);
                continue;
            }
            qualifiers.push(self.qualifier()?);
        }
        Ok(Field {
            name: None,
            path,
            count,
            qualifiers,
        })
    }

    fn field_block(&mut self, name: String, annotations: &mut Annotations) -> R<Field> {
        let open = self.span();
        self.bump(); // {
        let mut path = None;
        let mut count = None;
        let mut qualifiers = Vec::new();
        loop {
            while matches!(self.peek(), Tok::Newline | Tok::Semi | Tok::Comment(_)) {
                self.bump();
            }
            match self.peek() {
                Tok::RBrace => {
                    self.bump();
                    break;
                }
                Tok::Eof => {
                    let loc = self.loc(open);
                    self.diags.push(Diagnostic::error(
                        format!("the block of field `{name}` is never closed"),
                        loc,
                    ));
                    return Err(Fail);
                }
                _ => {}
            }
            let r: R<()> = (|| {
                if matches!(self.peek(), Tok::Name(k) if matches!(k.as_str(), "doc" | "severity" | "message"))
                {
                    return self.annotation(annotations);
                }
                if path.is_none() {
                    path = Some(self.path()?);
                    return Ok(());
                }
                if count.is_none()
                    && let Some(c) = self.try_count()?
                {
                    count = Some(c);
                    return Ok(());
                }
                qualifiers.push(self.qualifier()?);
                Ok(())
            })();
            if r.is_err() {
                self.recover_line();
            }
        }
        let Some(path) = path else {
            let loc = self.loc(open);
            self.diags.push(
                Diagnostic::error(format!("field `{name}` has no property"), loc)
                    .with_hint("the first line of a field block is its property path"),
            );
            return Err(Fail);
        };
        Ok(Field {
            name: Some(name),
            path,
            count,
            qualifiers,
        })
    }

    fn int_lit(&mut self) -> R<u64> {
        match self.peek().clone() {
            Tok::Int(n) => {
                self.bump();
                Ok(n)
            }
            other => self.fail(format!(
                "expected a whole number, found {}",
                other.describe()
            )),
        }
    }

    fn int_arg(&mut self) -> R<IntArg> {
        match self.peek().clone() {
            Tok::Int(n) => {
                self.bump();
                Ok(IntArg::Lit(n))
            }
            Tok::Name(n) if !KEYWORDS.contains(&n.as_str()) => {
                self.bump();
                Ok(IntArg::Param(n))
            }
            other => self.fail(format!(
                "expected a whole number, found {}",
                other.describe()
            )),
        }
    }

    /// A count, if the next tokens are one (and not a qualified count). A
    /// count with a word of its own (`at least 1` for `required`) is
    /// reported.
    fn try_count(&mut self) -> R<Option<Count>> {
        let at = self.span();
        let c = self.try_count_raw()?;
        let lit = |i: &IntArg| match i {
            IntArg::Lit(n) => Some(*n),
            IntArg::Param(_) => None,
        };
        let better = match &c {
            Some(Count::AtLeast(n)) if lit(n) == Some(1) => Some("required".to_string()),
            Some(Count::AtMost(n)) if lit(n) == Some(1) => Some("optional".to_string()),
            Some(Count::Between(a, b)) => match (lit(a), lit(b)) {
                (Some(0), Some(1)) => Some("optional".to_string()),
                (Some(a), Some(b)) if a == b => Some(format!("exactly {a}")),
                (Some(0), Some(b)) => Some(format!("at most {b}")),
                _ => None,
            },
            _ => None,
        };
        if let (Some(c), Some(word)) = (&c, better) {
            let text = shapeshift_macro::display::count(c);
            self.removed(
                at,
                format!("`{text}` is written `{word}`"),
                format!("write `{word}`"),
            );
        }
        Ok(c)
    }

    fn try_count_raw(&mut self) -> R<Option<Count>> {
        let t0 = self.peek().clone();
        let qualified = |t: &Tok| is_kw(t, "is") || is_kw(t, "matches");
        let int_like = |t: &Tok| {
            matches!(t, Tok::Int(_)) || matches!(t, Tok::Name(n) if !KEYWORDS.contains(&n.as_str()))
        };
        if is_kw(&t0, "required") {
            self.bump();
            return Ok(Some(Count::Required));
        }
        if is_kw(&t0, "optional") {
            self.bump();
            return Ok(Some(Count::Optional));
        }
        if is_kw(&t0, "exactly") && int_like(self.peek_at(1)) && !qualified(self.peek_at(2)) {
            self.bump();
            return Ok(Some(Count::Exactly(self.int_arg()?)));
        }
        if is_kw(&t0, "at")
            && (is_kw(self.peek_at(1), "least") || is_kw(self.peek_at(1), "most"))
            && int_like(self.peek_at(2))
            && !qualified(self.peek_at(3))
        {
            self.bump();
            let least = is_kw(self.peek(), "least");
            self.bump();
            let n = self.int_arg()?;
            return Ok(Some(if least {
                Count::AtLeast(n)
            } else {
                Count::AtMost(n)
            }));
        }
        if is_kw(&t0, "between") {
            self.bump();
            let a = self.int_arg()?;
            self.expect_kw("and", "in `between a and b`")?;
            let b = self.int_arg()?;
            return Ok(Some(Count::Between(a, b)));
        }
        Ok(None)
    }

    fn qualifier(&mut self) -> R<Qualifier> {
        let t0 = self.peek().clone();
        let at = self.span();
        if is_kw(&t0, "each") {
            self.bump();
            return Ok(Qualifier::Each(self.test()?));
        }
        if is_kw(&t0, "exactly") || is_kw(&t0, "at") {
            let bound = if is_kw(&t0, "exactly") {
                self.bump();
                QualifiedBound::Exactly
            } else {
                self.bump();
                if self.eat_kw("least") {
                    QualifiedBound::AtLeast
                } else if self.eat_kw("most") {
                    if matches!(self.peek(), Tok::PName { .. } | Tok::Iri(_)) {
                        let n = self.node()?;
                        let q = display_node(&n);
                        self.removed(
                            at,
                            format!("`at most {q}` is written `up to {q}`"),
                            format!("write `up to {q}`"),
                        );
                        return Ok(Qualifier::UpTo(n));
                    }
                    QualifiedBound::AtMost
                } else {
                    let found = self.peek().describe();
                    return self.fail(format!(
                        "expected `least` or `most` after `at`, found {found}"
                    ));
                }
            };
            let n = self.int_arg()?;
            let test_at = self.span();
            let test = if self.eat_kw("matches") {
                Test::Matches(self.call()?)
            } else if self.eat_kw("is") {
                let explicit = self.at_kw("matches");
                let t = self.test()?;
                if let Test::Matches(c) = &t
                    && explicit
                {
                    let b = match bound {
                        QualifiedBound::AtLeast => "at least",
                        QualifiedBound::AtMost => "at most",
                        QualifiedBound::Exactly => "exactly",
                    };
                    let text = format!(
                        "{b} {} matches {}",
                        shapeshift_macro::display::int_arg(&n),
                        shapeshift_macro::display::call(c)
                    );
                    self.removed(
                        test_at,
                        "`is matches` is not part of the language",
                        format!("write `{text}`"),
                    );
                }
                t
            } else {
                let found = self.peek().describe();
                return self.fail_hint(
                    format!("expected `is` or `matches` in a qualified count, found {found}"),
                    "a qualified count reads `at least 1 is a brick:Sensor` or `at least 1 matches sensor`; a plain count goes right after the property",
                );
            };
            if matches!(test, Test::Class(_) | Test::Matches(_))
                && matches!(self.peek(), Tok::LBrace)
            {
                let t = shapeshift_macro::display::test(&test);
                return self.fail_hint(
                    format!("a `{{ ... }}` after `{t}` is a test of every value, not of these"),
                    format!("for values that are `{t}` and satisfy the lines, write `is {{ is {t}; ... }}`"),
                );
            }
            return Ok(Qualifier::Qualified { bound, n, test });
        }
        if is_kw(&t0, "same") {
            self.bump();
            self.expect_kw("as", "in `same as`")?;
            return Ok(Qualifier::SameAs(self.node()?));
        }
        if is_kw(&t0, "different") {
            self.bump();
            self.expect_kw("from", "in `different from`")?;
            return Ok(Qualifier::DifferentFrom(self.node()?));
        }
        if matches!(&t0, Tok::Name(n) if n == "less") && is_kw(self.peek_at(1), "than") {
            self.bump();
            self.bump();
            let n = self.node()?;
            let q = display_node(&n);
            self.removed(
                at,
                format!("`less than {q}` is written `below {q}`"),
                format!("write `below {q}`"),
            );
            return Ok(Qualifier::Below(n));
        }
        // `below q`, `up to q`: compare with another property's values
        let compare = if is_kw(&t0, "below") {
            Some(("below", 1))
        } else if is_kw(&t0, "up") && is_kw(self.peek_at(1), "to") {
            Some(("up to", 2))
        } else if is_kw(&t0, "above") {
            Some(("above", 1))
        } else if is_kw(&t0, "from") {
            Some(("from", 1))
        } else {
            None
        };
        let operand = |t: &Tok| match t {
            Tok::PName { .. } | Tok::Iri(_) => true,
            Tok::Name(n) => !KEYWORDS.contains(&n.as_str()),
            _ => false,
        };
        if let Some((word, len)) = compare
            && operand(self.peek_at(len))
        {
            for _ in 0..len {
                self.bump();
            }
            let n = self.node()?;
            return match word {
                "below" => Ok(Qualifier::Below(n)),
                "up to" => Ok(Qualifier::UpTo(n)),
                _ => {
                    let q = display_node(&n);
                    let flip = if word == "above" { "below" } else { "up to" };
                    self.fail_hint(
                        format!("a property is compared with `below` or `up to`, not `{word}`"),
                        format!(
                            "SHACL compares the other way round: write the comparison on `{q}`, as `{flip}` this property"
                        ),
                    )
                }
            };
        }
        if is_kw(&t0, "unique") {
            self.bump();
            self.expect_kw("language", "in `unique language`")?;
            return Ok(Qualifier::UniqueLanguage);
        }
        if is_kw(&t0, "is") {
            // `ex:p is "x"`: each value is the literal
            self.bump();
            if self.at_literal() {
                let v = self.value()?;
                let text = shapeshift_macro::display::value(&v);
                self.removed(
                    at,
                    "a property's values are tested with `each`",
                    format!("write `each one of {text}`"),
                );
                return Ok(Qualifier::Each(Test::OneOf(vec![v])));
            }
            self.removed(
                at,
                "a property's values are tested with `each`",
                "write `each` and the test: `each a brick:AHU`, `each one of ex:on`",
            );
            return Ok(Qualifier::Each(self.test()?));
        }
        if self.at_test_start() {
            let word = match &t0 {
                Tok::LBrace => "{ ... }".to_string(),
                Tok::Name(n) if n == "up" => "up to ...".to_string(),
                Tok::Name(n) => format!("{n} ..."),
                _ => "...".to_string(),
            };
            self.removed(
                at,
                format!(
                    "a test of the values goes after `each`, found {}",
                    t0.describe()
                ),
                format!("write `each {word}`"),
            );
            return Ok(Qualifier::Each(self.test()?));
        }
        let found = t0.describe();
        if matches!(t0, Tok::PName { .. } | Tok::Iri(_))
            || matches!(&t0, Tok::Name(n) if !KEYWORDS.contains(&n.as_str()))
        {
            let term = match &t0 {
                Tok::PName { prefix, local } => format!("{prefix}:{local}"),
                Tok::Iri(i) => format!("<{i}>"),
                Tok::Name(n) => n.clone(),
                _ => unreachable!(),
            };
            let each = if matches!(t0, Tok::Name(_)) {
                format!("each matches {term}")
            } else {
                format!("each a {term}")
            };
            return self.fail_hint(
                format!("unexpected {found} in a field line"),
                format!(
                    "for values that are `{term}`, write `{each}`; to follow another property, write a path like `ex:p/{term}`"
                ),
            );
        }
        self.fail_hint(
            format!("unexpected {found} in a field line"),
            "a field reads `<property> <count>, <qualifier>`, e.g. `brick:hasPoint required, each a brick:Point`",
        )
    }

    /// Whether the next token starts a test (a word only tests begin with).
    fn at_test_start(&self) -> bool {
        match self.peek() {
            Tok::LBrace => true,
            Tok::Name(k) => {
                matches!(
                    k.as_str(),
                    "a" | "matches"
                        | "one"
                        | "datatype"
                        | "iri"
                        | "literal"
                        | "blank"
                        | "length"
                        | "from"
                        | "above"
                        | "up"
                        | "below"
                        | "pattern"
                        | "language"
                ) || DATATYPE_KEYWORDS.iter().any(|(w, _)| w == k)
            }
            _ => false,
        }
    }

    fn at_literal(&self) -> bool {
        matches!(
            self.peek(),
            Tok::Str { .. } | Tok::Int(_) | Tok::Num(_) | Tok::Temporal { .. }
        ) || matches!(self.peek(), Tok::Name(n) if n == "true" || n == "false")
    }

    /// A test: what `each`, `is`, and a qualified count apply to a value.
    /// Every test starts with the word that says what kind it is.
    fn test(&mut self) -> R<Test> {
        let at = self.span();
        if matches!(self.peek(), Tok::LBrace) {
            return Ok(Test::Shape(self.block()?));
        }
        if self.at_literal() {
            let v = self.value()?;
            let text = shapeshift_macro::display::value(&v);
            self.removed(
                at,
                format!("`{text}` alone is not a test"),
                format!("for this value, write `one of {text}`"),
            );
            return Ok(Test::OneOf(vec![v]));
        }
        match self.peek().clone() {
            Tok::PName { .. } | Tok::Iri(_) => {
                let n = self.node()?;
                let text = display_node(&n);
                self.removed(
                    at,
                    format!("`{text}` alone is not a test"),
                    format!(
                        "write `a {text}` for instances of the class, or `one of {text}` for the node itself"
                    ),
                );
                return Ok(Test::Class(vec![n]));
            }
            Tok::Name(n)
                if !KEYWORDS.contains(&n.as_str())
                    && !DATATYPE_KEYWORDS.iter().any(|(w, _)| *w == n) =>
            {
                let c = self.call()?;
                let text = shapeshift_macro::display::call(&c);
                self.removed(
                    at,
                    format!("`{text}` alone is not a test"),
                    format!(
                        "write `matches {text}` for a shape, or `a {text}` for a class parameter"
                    ),
                );
                return Ok(Test::Matches(c));
            }
            _ => {}
        }
        let t0 = self.peek().clone();
        let Tok::Name(k) = &t0 else {
            let found = t0.describe();
            return self.fail_hint(format!("expected a test, found {found}"), TESTS_HINT);
        };
        let k = k.clone();
        if !self.at_test_start() && k != "is" {
            let hint = if matches!(
                k.as_str(),
                "at" | "exactly" | "between" | "required" | "optional"
            ) {
                "a count goes right after the property (`ex:p at least 2`); for a range of values, write `from 5`, `above 0`, `up to 10`, or `below 1`"
            } else {
                TESTS_HINT
            };
            return self.fail_hint(format!("expected a test, found `{k}`"), hint);
        }
        self.bump();
        Ok(match k.as_str() {
            "a" => {
                let mut classes = vec![self.node()?];
                while self.eat_kw("or") {
                    classes.push(self.node()?);
                }
                Test::Class(classes)
            }
            "matches" => Test::Matches(self.call()?),
            "is" => {
                self.removed(
                    at,
                    "`is` here is not part of the language",
                    "write the test directly: `each a brick:AHU`, `each matches sensor`, `each one of ex:on ex:off`",
                );
                return self.test();
            }
            "one" => {
                self.expect_kw("of", "in `one of`")?;
                let mut vs = vec![self.value()?];
                while matches!(
                    self.peek(),
                    Tok::Iri(_)
                        | Tok::PName { .. }
                        | Tok::Str { .. }
                        | Tok::Int(_)
                        | Tok::Num(_)
                        | Tok::Temporal { .. }
                ) || matches!(self.peek(), Tok::Name(n) if n == "true" || n == "false" || !KEYWORDS.contains(&n.as_str()))
                {
                    vs.push(self.value()?);
                }
                Test::OneOf(vs)
            }
            "datatype" => Test::Datatype(DatatypeRef::Node(self.node()?)),
            "iri" => Test::Kind(NodeKind::Iri),
            "literal" => Test::Kind(NodeKind::Literal),
            "blank" => Test::Kind(NodeKind::Blank),
            "length" => self.length(at)?,
            "from" | "above" => {
                let lo = RangeEnd {
                    value: self.bound()?,
                    inclusive: k == "from",
                };
                let hi = if self.eat_kw("to") {
                    Some(RangeEnd {
                        value: self.bound()?,
                        inclusive: true,
                    })
                } else if self.eat_kw("below") {
                    Some(RangeEnd {
                        value: self.bound()?,
                        inclusive: false,
                    })
                } else if self.at_kw("up") {
                    return self.fail_hint(
                        format!("`{k} ... up to` is written `{k} ... to`"),
                        format!(
                            "write `{k} {} to ...`",
                            shapeshift_macro::display::value(&lo.value)
                        ),
                    );
                } else {
                    None
                };
                Test::Range { lo: Some(lo), hi }
            }
            "up" | "below" => {
                if k == "up" {
                    self.expect_kw("to", "in `up to n`")?;
                }
                Test::Range {
                    lo: None,
                    hi: Some(RangeEnd {
                        value: self.bound()?,
                        inclusive: k == "up",
                    }),
                }
            }
            "pattern" => {
                let regex = self.string("a regular expression")?;
                let flags = if self.eat_kw("flags") {
                    self.string("regex flags")?
                } else {
                    String::new()
                };
                Test::Pattern { regex, flags }
            }
            "language" => {
                let mut ls = vec![self.string("a language tag")?];
                while let Tok::Str { value, .. } = self.peek().clone() {
                    self.bump();
                    ls.push(value);
                }
                Test::Language(ls)
            }
            w if DATATYPE_KEYWORDS.iter().any(|(d, _)| *d == w) => {
                Test::Datatype(DatatypeRef::Builtin(w.to_string()))
            }
            _ => unreachable!("checked by at_test_start"),
        })
    }

    /// After `length`: `n`, `from a`, `up to b`, or `from a to b`.
    fn length(&mut self, at: Span) -> R<Test> {
        if self.eat_kw("from") {
            let min = Some(self.int_lit()?);
            let max = if self.eat_kw("to") {
                Some(self.int_lit()?)
            } else {
                None
            };
            return Ok(Test::Length { min, max });
        }
        if self.eat_kw("up") {
            self.expect_kw("to", "in `length up to n`")?;
            return Ok(Test::Length {
                min: None,
                max: Some(self.int_lit()?),
            });
        }
        if self.eat_kw("at") {
            let least = self.eat_kw("least");
            if !least {
                self.expect_kw("most", "in `length up to n`")?;
            }
            let n = self.int_lit()?;
            let word = if least { "from" } else { "up to" };
            self.removed(
                at,
                format!(
                    "`length at {}` is written `length {word}`",
                    if least { "least" } else { "most" }
                ),
                format!("write `length {word} {n}`"),
            );
            return Ok(if least {
                Test::Length {
                    min: Some(n),
                    max: None,
                }
            } else {
                Test::Length {
                    min: None,
                    max: Some(n),
                }
            });
        }
        let a = self.int_lit()?;
        if self.eat_kw("to") {
            let b = self.int_lit()?;
            self.removed(
                at,
                format!("`length {a} to {b}` is written `length from {a} to {b}`"),
                format!("write `length from {a} to {b}`"),
            );
            return Ok(Test::Length {
                min: Some(a),
                max: Some(b),
            });
        }
        Ok(Test::Length {
            min: Some(a),
            max: Some(a),
        })
    }

    /// A range's end: a number, a date, a dateTime, or a time.
    fn bound(&mut self) -> R<Value> {
        match self.peek().clone() {
            Tok::Int(_) | Tok::Num(_) | Tok::Temporal { .. } => self.value(),
            other => self.fail_hint(
                format!(
                    "expected a number, a date, or a time, found {}",
                    other.describe()
                ),
                "a range's ends are numbers (`from 0 to 1.5`), dates (`from 2024-01-01`), dateTimes (`below 2024-01-01T00:00:00Z`), or times (`from 08:00:00 to 18:00:00`)",
            ),
        }
    }

    // ---------------------------------------------------------- nodes/paths

    fn node(&mut self) -> R<NodeRef> {
        match self.peek().clone() {
            Tok::Iri(i) => {
                self.bump();
                Ok(NodeRef::Iri(i))
            }
            Tok::PName { prefix, local } => {
                self.bump();
                Ok(NodeRef::Prefixed { prefix, local })
            }
            Tok::Name(n) if !KEYWORDS.contains(&n.as_str()) => {
                self.bump();
                Ok(NodeRef::Name(n))
            }
            other => self.fail_hint(
                format!("expected a node, found {}", other.describe()),
                "write a prefixed name like `brick:AHU` or a full IRI like `<https://...>`",
            ),
        }
    }

    fn value(&mut self) -> R<Value> {
        match self.peek().clone() {
            Tok::Str { value, lang } => {
                self.bump();
                Ok(Value::String { value, lang })
            }
            Tok::Int(n) => {
                self.bump();
                Ok(Value::Number(Number {
                    lexical: n.to_string(),
                }))
            }
            Tok::Num(s) => {
                self.bump();
                Ok(Value::Number(Number { lexical: s }))
            }
            Tok::Temporal { kind, lexical } => {
                self.bump();
                Ok(Value::Temporal(Temporal { kind, lexical }))
            }
            Tok::Name(n) if n == "true" || n == "false" => {
                self.bump();
                Ok(Value::Boolean(n == "true"))
            }
            _ => Ok(Value::Node(self.node()?)),
        }
    }

    fn call(&mut self) -> R<Call> {
        let span = self.span();
        let name = match self.peek().clone() {
            Tok::Name(n) if !KEYWORDS.contains(&n.as_str()) => {
                self.bump();
                n
            }
            Tok::PName { prefix, local } => {
                self.bump();
                format!("{prefix}:{local}")
            }
            Tok::Iri(i) => {
                self.bump();
                format!("<{i}>")
            }
            other => {
                return self.fail(format!("expected a shape name, found {}", other.describe()));
            }
        };
        let mut args = Vec::new();
        let mut named: Vec<NamedArg> = Vec::new();
        if matches!(self.peek(), Tok::LParen) {
            self.bump();
            loop {
                self.skip_nl();
                if matches!(self.peek(), Tok::RParen) {
                    self.bump();
                    break;
                }
                if let Tok::Name(n) = self.peek().clone()
                    && matches!(self.peek_at(1), Tok::Eq)
                {
                    self.bump();
                    self.bump();
                    let value = self.arg()?;
                    named.push(NamedArg { name: n, value });
                } else {
                    if let Some(n) = named.last() {
                        let n = n.name.clone();
                        return self.fail_hint(
                            format!("a positional argument after the named argument `{n}`"),
                            "put positional arguments first",
                        );
                    }
                    args.push(self.arg()?);
                }
                self.skip_nl();
                match self.peek() {
                    Tok::Comma => {
                        self.bump();
                    }
                    Tok::RParen => {
                        self.bump();
                        break;
                    }
                    other => {
                        let d = other.describe();
                        return self.fail(format!(
                            "expected `,` or `)` in the argument list, found {d}"
                        ));
                    }
                }
            }
        }
        Ok(Call {
            name,
            args,
            named,
            span,
        })
    }

    fn arg(&mut self) -> R<Arg> {
        match self.peek().clone() {
            Tok::Int(n) => {
                self.bump();
                Ok(Arg::Int(n))
            }
            Tok::Str { .. } | Tok::Num(_) | Tok::Temporal { .. } => Ok(Arg::Literal(self.value()?)),
            Tok::Name(n) if n == "true" || n == "false" => Ok(Arg::Literal(self.value()?)),
            Tok::LBrace => Ok(Arg::Shape(self.block()?)),
            _ => {
                // `T(x)` is a shape or a path template; which one is known
                // once names are resolved.
                let p = self.path()?;
                Ok(match p {
                    PathExpr::Node(n) => Arg::Node(n),
                    PathExpr::Call(c) => Arg::Call(c),
                    other => Arg::Path(other),
                })
            }
        }
    }

    fn path(&mut self) -> R<PathExpr> {
        let mut alts = vec![self.path_seq()?];
        while self.eat_kw("or") {
            alts.push(self.path_seq()?);
        }
        Ok(if alts.len() == 1 {
            alts.pop().unwrap()
        } else {
            PathExpr::Alt(alts)
        })
    }

    fn path_seq(&mut self) -> R<PathExpr> {
        let mut parts = vec![self.path_unary()?];
        while matches!(self.peek(), Tok::Slash) {
            self.bump();
            parts.push(self.path_unary()?);
        }
        Ok(if parts.len() == 1 {
            parts.pop().unwrap()
        } else {
            PathExpr::Seq(parts)
        })
    }

    fn path_unary(&mut self) -> R<PathExpr> {
        if matches!(self.peek(), Tok::Caret) {
            self.bump();
            return Ok(PathExpr::Inverse(Box::new(self.path_unary()?)));
        }
        let mut p = match self.peek().clone() {
            Tok::LParen => {
                self.bump();
                let inner = self.path()?;
                if !matches!(self.peek(), Tok::RParen) {
                    let found = self.peek().describe();
                    return self.fail(format!("expected `)` to close the path, found {found}"));
                }
                self.bump();
                inner
            }
            Tok::Name(n) if n == "self" => {
                self.bump();
                PathExpr::SelfPath
            }
            Tok::Name(n) if KEYWORDS.contains(&n.as_str()) => {
                return self.fail_hint(
                    format!("expected a property, found the keyword `{n}`"),
                    "a field starts with the property it constrains, e.g. `brick:hasPoint required`",
                );
            }
            Tok::Name(_) if matches!(self.peek_at(1), Tok::LParen) => PathExpr::Call(self.call()?),
            _ => PathExpr::Node(self.node()?),
        };
        loop {
            match self.peek() {
                Tok::Star => p = PathExpr::Star(Box::new(p)),
                Tok::Plus => p = PathExpr::Plus(Box::new(p)),
                Tok::Question => p = PathExpr::Optional(Box::new(p)),
                _ => break,
            }
            self.bump();
        }
        Ok(p)
    }
}

fn display_node(n: &NodeRef) -> String {
    shapeshift_macro::display::node(n)
}

const TESTS_HINT: &str = "a test is `a <class>`, `matches <shape>`, `one of ...`, a datatype like `string`, `iri`, `length ...`, a range like `from 0 to 100`, `pattern`, `language`, or a `{ ... }` block";
