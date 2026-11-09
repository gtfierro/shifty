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
    "all",
    "and",
    "any",
    "as",
    "at",
    "base",
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
    "include",
    "iri",
    "is",
    "language",
    "least",
    "length",
    "less",
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
    "pattern",
    "prefix",
    "properties",
    "required",
    "same",
    "self",
    "severity",
    "shape",
    "sparql",
    "than",
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
                        && matches!(n.as_str(), "shape" | "prefix" | "import" | "base" | "doc") =>
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
                Tok::Name(k) if k == "shape" => match self.shape(std::mem::take(&mut pending)) {
                    Ok(s) => {
                        m.shapes.push(s);
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                other => self.fail_hint(
                    format!(
                        "expected `shape`, `prefix`, `import`, or `base`, found {}",
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
        let mut params = Vec::new();
        if matches!(self.peek(), Tok::LParen) {
            self.bump();
            loop {
                self.skip_nl();
                if matches!(self.peek(), Tok::RParen) {
                    self.bump();
                    break;
                }
                params.push(self.name("a parameter name")?);
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
                    Diagnostic::error(format!("shape `{name}()` has an empty parameter list"), loc)
                        .with_hint(format!("write `shape {name}` without parentheses")),
                );
            }
        }
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
                                "unexpected {d}; each line holds one field, include, or block"
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
            Tok::Name(k) if k == "include" => {
                self.bump();
                LineKind::Include(self.call()?)
            }
            Tok::Name(k) if k == "only" => {
                self.bump();
                self.expect_kw("these", "in `only these properties`")?;
                self.expect_kw("properties", "in `only these properties`")?;
                LineKind::Closed
            }
            Tok::Name(k) if k == "any" || k == "all" || k == "none" => {
                self.bump();
                self.expect_kw("of", &format!("after `{k}`"))?;
                let frame = match k.as_str() {
                    "any" => Frame::AnyOf,
                    "all" => Frame::AllOf,
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
                self.bump();
                self.expect_kw("least", "in `at least n of`")?;
                let n = self.int_lit()?;
                self.expect_kw("of", &format!("in `at least {n} of`"))?;
                LineKind::Boolean(self.boolean(Frame::AtLeast(n))?)
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
            Tok::PName { prefix, local }
                if local.is_empty()
                    && !matches!(
                        self.peek_at(1),
                        Tok::Newline | Tok::Semi | Tok::RBrace | Tok::Eof
                    ) =>
            {
                // `name: path ...` or `name: { ... }`
                self.bump();
                if KEYWORDS.contains(&prefix.as_str()) {
                    return self.fail(format!("`{prefix}` is a keyword and cannot name a field"));
                }
                if matches!(self.peek(), Tok::LBrace) {
                    LineKind::Field(self.field_block(prefix, &mut annotations)?)
                } else {
                    let mut f = self.field_line()?;
                    f.name = Some(prefix);
                    LineKind::Field(f)
                }
            }
            _ => LineKind::Field(self.field_line()?),
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
        Ok(Boolean { frame, blocks })
    }

    // ---------------------------------------------------------------- field

    fn at_line_end(&self) -> bool {
        matches!(
            self.peek(),
            Tok::Newline | Tok::Semi | Tok::RBrace | Tok::Eof | Tok::Comment(_)
        )
    }

    fn field_line(&mut self) -> R<Field> {
        let path = self.path()?;
        let mut count = None;
        let mut qualifiers = Vec::new();
        let mut first = true;
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
            block: false,
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
            block: true,
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

    /// A count, if the next tokens are one (and not a qualified count).
    fn try_count(&mut self) -> R<Option<Count>> {
        let t0 = self.peek().clone();
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
        if is_kw(&t0, "exactly") && int_like(self.peek_at(1)) && !is_kw(self.peek_at(2), "is") {
            self.bump();
            return Ok(Some(Count::Exactly(self.int_arg()?)));
        }
        if is_kw(&t0, "at")
            && (is_kw(self.peek_at(1), "least") || is_kw(self.peek_at(1), "most"))
            && int_like(self.peek_at(2))
            && !is_kw(self.peek_at(3), "is")
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
                    // `at most q` (a property): sh:lessThanOrEquals
                    if matches!(self.peek(), Tok::PName { .. } | Tok::Iri(_)) {
                        return Ok(Qualifier::AtMostProperty(self.node()?));
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
            if !self.eat_kw("is") {
                let found = self.peek().describe();
                return self.fail_hint(
                    format!("expected `is` in a qualified count, found {found}"),
                    "a qualified count reads `at least 1 is a brick:Sensor`; a plain count goes right after the property",
                );
            }
            let test = self.test_or_value()?;
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
        if is_kw(&t0, "less") {
            self.bump();
            self.expect_kw("than", "in `less than`")?;
            return Ok(Qualifier::LessThan(self.node()?));
        }
        if is_kw(&t0, "unique") {
            self.bump();
            self.expect_kw("language", "in `unique language`")?;
            return Ok(Qualifier::UniqueLanguage);
        }
        if self.at_test_start() {
            return Ok(Qualifier::Each(self.test()?));
        }
        let found = t0.describe();
        self.fail_hint(
            format!("unexpected {found} in a field line"),
            "a field reads `<property> <count>, <qualifier>`, e.g. `brick:hasPoint required, each a brick:Point`",
        )
    }

    fn at_test_start(&self) -> bool {
        match self.peek() {
            Tok::Name(k) => {
                matches!(
                    k.as_str(),
                    "a" | "matches"
                        | "is"
                        | "one"
                        | "datatype"
                        | "iri"
                        | "literal"
                        | "blank"
                        | "length"
                        | "from"
                        | "up"
                        | "pattern"
                        | "language"
                ) || DATATYPE_KEYWORDS.iter().any(|(w, _)| w == k)
            }
            _ => false,
        }
    }

    fn test_or_value(&mut self) -> R<Test> {
        if self.at_test_start() {
            self.test()
        } else {
            Ok(Test::Is(self.value()?))
        }
    }

    fn test(&mut self) -> R<Test> {
        let t0 = self.peek().clone();
        let Tok::Name(k) = &t0 else {
            let found = t0.describe();
            return self.fail_hint(
                format!("expected a test, found {found}"),
                "tests are `a <class>`, `matches <Shape>`, `is <node>`, `one of ...`, a datatype like `string`, `iri`, `length`, `from ... to ...`, `pattern`, or `language`",
            );
        };
        let k = k.clone();
        self.bump();
        Ok(match k.as_str() {
            "a" => Test::Class(self.node()?),
            "matches" => Test::Matches(self.call()?),
            "is" => {
                if self.eat_kw("a") {
                    Test::Class(self.node()?)
                } else {
                    Test::Is(self.value()?)
                }
            }
            "one" => {
                self.expect_kw("of", "in `one of`")?;
                let mut vs = vec![self.value()?];
                while matches!(
                    self.peek(),
                    Tok::Iri(_) | Tok::PName { .. } | Tok::Str { .. } | Tok::Int(_) | Tok::Num(_)
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
            "length" => {
                if self.eat_kw("at") {
                    if self.eat_kw("least") {
                        Test::Length {
                            min: Some(self.int_lit()?),
                            max: None,
                        }
                    } else {
                        self.expect_kw("most", "in `length at most n`")?;
                        Test::Length {
                            min: None,
                            max: Some(self.int_lit()?),
                        }
                    }
                } else {
                    let a = self.int_lit()?;
                    if self.eat_kw("to") {
                        Test::Length {
                            min: Some(a),
                            max: Some(self.int_lit()?),
                        }
                    } else {
                        Test::Length {
                            min: Some(a),
                            max: Some(a),
                        }
                    }
                }
            }
            "from" => {
                let from = self.number()?;
                let to = if self.eat_kw("to") {
                    Some(self.number()?)
                } else {
                    None
                };
                Test::Range {
                    from: Some(from),
                    to,
                }
            }
            "up" => {
                self.expect_kw("to", "in `up to n`")?;
                Test::Range {
                    from: None,
                    to: Some(self.number()?),
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
            _ => unreachable!("guarded by at_test_start or the caller"),
        })
    }

    fn number(&mut self) -> R<Number> {
        match self.peek().clone() {
            Tok::Int(n) => {
                self.bump();
                Ok(Number {
                    lexical: n.to_string(),
                })
            }
            Tok::Num(s) => {
                self.bump();
                Ok(Number { lexical: s })
            }
            other => self.fail(format!("expected a number, found {}", other.describe())),
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
        if matches!(self.peek(), Tok::LParen) {
            self.bump();
            loop {
                self.skip_nl();
                if matches!(self.peek(), Tok::RParen) {
                    self.bump();
                    break;
                }
                args.push(self.arg()?);
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
        Ok(Call { name, args, span })
    }

    fn arg(&mut self) -> R<Arg> {
        match self.peek().clone() {
            Tok::Int(n) => {
                self.bump();
                Ok(Arg::Int(n))
            }
            Tok::Str { .. } | Tok::Num(_) => Ok(Arg::Literal(self.value()?)),
            Tok::Name(n) if n == "true" || n == "false" => Ok(Arg::Literal(self.value()?)),
            Tok::Name(_) if matches!(self.peek_at(1), Tok::LParen) => Ok(Arg::Call(self.call()?)),
            _ => {
                let p = self.path()?;
                Ok(match p {
                    PathExpr::Node(n) => Arg::Node(n),
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
