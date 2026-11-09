//! Render the AST back to Shapeshift text.
//!
//! This is the formatter's back half (the parser is the front half), and it is
//! also how reports quote the line an author wrote: a field's one-line
//! rendering is stored in its provenance record. Output is canonical: one
//! space between tokens, two-space indentation, `each` written out, comments
//! and docstrings preserved.

use crate::ast::*;
use std::fmt::Write;

pub fn format_module(m: &Module) -> String {
    let mut out = String::new();
    for c in &m.leading_comments {
        writeln!(out, "{}", comment(c)).unwrap();
    }
    let order: Vec<HeaderRef> = if m.header_order.is_empty() {
        let mut o = Vec::new();
        if m.base.is_some() {
            o.push(HeaderRef::Base);
        }
        o.extend((0..m.prefixes.len()).map(HeaderRef::Prefix));
        o.extend((0..m.imports.len()).map(HeaderRef::Import));
        o
    } else {
        m.header_order.clone()
    };
    for h in &order {
        match h {
            HeaderRef::Base => {
                if let Some(b) = &m.base {
                    writeln!(out, "base <{b}>").unwrap();
                }
            }
            HeaderRef::Prefix(i) => {
                let p = &m.prefixes[*i];
                for c in &p.comments {
                    writeln!(out, "{}", comment(c)).unwrap();
                }
                writeln!(out, "prefix {}: <{}>", p.name, p.iri).unwrap();
            }
            HeaderRef::Import(i) => {
                let imp = &m.imports[*i];
                for c in &imp.comments {
                    writeln!(out, "{}", comment(c)).unwrap();
                }
                let src = match &imp.source {
                    ImportTarget::Iri(i) => format!("<{i}>"),
                    ImportTarget::Path(p) => string_lit(p),
                };
                let mut line = format!("import {src}");
                if let Some(a) = &imp.alias {
                    write!(line, " as {a}").unwrap();
                }
                if let Some(c) = &imp.trailing_comment {
                    write!(line, "  {}", comment(c)).unwrap();
                }
                writeln!(out, "{line}").unwrap();
            }
        }
    }
    for s in &m.shapes {
        if !out.is_empty() {
            out.push('\n');
        }
        format_shape(&mut out, s);
    }
    if !m.trailing_comments.is_empty() {
        out.push('\n');
        for c in &m.trailing_comments {
            writeln!(out, "{}", comment(c)).unwrap();
        }
    }
    out
}

fn comment(c: &str) -> String {
    if c.is_empty() {
        "--".into()
    } else {
        format!("-- {c}")
    }
}

fn annotations(out: &mut String, a: &Annotations, indent: &str) {
    if let Some(d) = &a.doc {
        writeln!(out, "{indent}doc {}", string_lit(d)).unwrap();
    }
    if let Some(s) = &a.severity {
        writeln!(out, "{indent}severity {}", s.word()).unwrap();
    }
    for m in &a.messages {
        writeln!(out, "{indent}message {}", string_lit(m)).unwrap();
    }
}

fn format_shape(out: &mut String, s: &ShapeDecl) {
    for c in &s.comments {
        writeln!(out, "{}", comment(c)).unwrap();
    }
    annotations(out, &s.annotations, "");
    let mut head = format!("shape {}", s.name);
    if !s.params.is_empty() {
        write!(head, "({})", s.params.join(", ")).unwrap();
    }
    let inline_target = s.targets.len() == 1 && s.targets[0].trailing_comment.is_none();
    if inline_target {
        write!(head, " {}", target(&s.targets[0].kind)).unwrap();
        out.push_str(&head);
    } else {
        out.push_str(&head);
        for t in &s.targets {
            write!(out, "\n  {}", target(&t.kind)).unwrap();
            if let Some(c) = &t.trailing_comment {
                write!(out, "  {}", comment(c)).unwrap();
            }
        }
        if !s.targets.is_empty() {
            out.push('\n');
        }
    }
    if s.body.is_empty() && s.trailing_comments.is_empty() {
        out.push_str(if !s.targets.is_empty() && !inline_target {
            "{ }\n"
        } else {
            " { }\n"
        });
        return;
    }
    out.push_str(if !s.targets.is_empty() && !inline_target {
        "{\n"
    } else {
        " {\n"
    });
    lines(out, &s.body, 1);
    for c in &s.trailing_comments {
        writeln!(out, "  {}", comment(c)).unwrap();
    }
    out.push_str("}\n");
}

fn lines(out: &mut String, ls: &[Line], depth: usize) {
    for l in ls {
        line(out, l, depth);
    }
}

fn line(out: &mut String, l: &Line, depth: usize) {
    let indent = "  ".repeat(depth);
    for c in &l.comments {
        writeln!(out, "{indent}{}", comment(c)).unwrap();
    }
    let block_field = matches!(&l.kind, LineKind::Field(f) if f.block && f.name.is_some());
    if !block_field {
        annotations(out, &l.annotations, &indent);
    }
    let trailing = l
        .trailing_comment
        .as_ref()
        .map(|c| format!("  {}", comment(c)))
        .unwrap_or_default();
    match &l.kind {
        LineKind::Field(f) if block_field => {
            writeln!(out, "{indent}{}: {{{trailing}", f.name.as_deref().unwrap()).unwrap();
            let inner = "  ".repeat(depth + 1);
            writeln!(out, "{inner}{}", path(&f.path)).unwrap();
            if let Some(c) = &f.count {
                writeln!(out, "{inner}{}", count(c)).unwrap();
            }
            for q in &f.qualifiers {
                writeln!(out, "{inner}{}", qualifier(q)).unwrap();
            }
            annotations(out, &l.annotations, &inner);
            writeln!(out, "{indent}}}").unwrap();
        }
        LineKind::Field(f) => {
            writeln!(out, "{indent}{}{trailing}", field_text(f)).unwrap();
        }
        LineKind::Include(c) => {
            writeln!(out, "{indent}include {}{trailing}", call(c)).unwrap();
        }
        LineKind::Closed => {
            writeln!(out, "{indent}only these properties{trailing}").unwrap();
        }
        LineKind::Boolean(b) => {
            boolean(out, b, depth, &trailing);
        }
    }
}

fn inline_block(b: &Block) -> Option<String> {
    if !b.trailing_comments.is_empty() {
        return None;
    }
    let parts: Option<Vec<String>> = b
        .lines
        .iter()
        .map(|l| {
            if !l.comments.is_empty() || l.trailing_comment.is_some() || !l.annotations.is_empty() {
                return None;
            }
            match &l.kind {
                LineKind::Field(f) if !f.block => Some(field_text(f)),
                LineKind::Include(c) => Some(format!("include {}", call(c))),
                LineKind::Closed => Some("only these properties".into()),
                _ => None,
            }
        })
        .collect();
    let parts = parts?;
    let text = if parts.is_empty() {
        "{ }".to_string()
    } else {
        format!("{{ {} }}", parts.join("; "))
    };
    (text.len() <= 72).then_some(text)
}

fn block_text(out: &mut String, b: &Block, depth: usize) {
    if let Some(t) = inline_block(b) {
        out.push_str(&t);
        return;
    }
    out.push_str("{\n");
    lines(out, &b.lines, depth + 1);
    for c in &b.trailing_comments {
        writeln!(out, "{}{}", "  ".repeat(depth + 1), comment(c)).unwrap();
    }
    write!(out, "{}}}", "  ".repeat(depth)).unwrap();
}

fn boolean(out: &mut String, b: &Boolean, depth: usize, trailing: &str) {
    let indent = "  ".repeat(depth);
    match b.frame {
        Frame::When => {
            write!(out, "{indent}when ").unwrap();
            block_text(out, &b.blocks[0], depth);
            write!(out, "\n{indent}then ").unwrap();
            block_text(out, &b.blocks[1], depth);
            writeln!(out, "{trailing}").unwrap();
        }
        frame => {
            let phrase = match frame {
                Frame::AllOf => "all of".to_string(),
                Frame::AnyOf => "any of".to_string(),
                Frame::ExactlyOneOf => "exactly one of".to_string(),
                Frame::NoneOf => "none of".to_string(),
                Frame::AtLeast(n) => format!("at least {n} of"),
                Frame::When => unreachable!(),
            };
            write!(out, "{indent}{phrase}").unwrap();
            for blk in &b.blocks {
                out.push(' ');
                block_text(out, blk, depth);
            }
            writeln!(out, "{trailing}").unwrap();
        }
    }
}

fn target(t: &Target) -> String {
    match t {
        Target::Class(n) => format!("for every {}", node(n)),
        Target::SubjectsOf(n) => format!("for every node with {}", node(n)),
        Target::ObjectsOf(n) => format!("for every value of {}", node(n)),
        Target::Node(v) => format!("for node {}", value(v)),
        Target::Matching(c) => format!("for every node matching {}", call(c)),
        Target::Sparql(q) => format!("for sparql {}", string_lit(q)),
    }
}

/// A field in one-line form, as reports quote it.
pub fn field_text(f: &Field) -> String {
    let mut s = String::new();
    if let Some(n) = &f.name {
        write!(s, "{n}: ").unwrap();
    }
    s.push_str(&path(&f.path));
    let mut first = true;
    if let Some(c) = &f.count {
        write!(s, " {}", count(c)).unwrap();
        first = false;
    }
    for q in &f.qualifiers {
        if first {
            s.push(' ');
            first = false;
        } else {
            s.push_str(", ");
        }
        s.push_str(&qualifier(q));
    }
    s
}

pub fn int_arg(i: &IntArg) -> String {
    match i {
        IntArg::Lit(n) => n.to_string(),
        IntArg::Param(p) => p.clone(),
    }
}

pub fn count(c: &Count) -> String {
    match c {
        Count::Required => "required".into(),
        Count::Optional => "optional".into(),
        Count::Exactly(n) => format!("exactly {}", int_arg(n)),
        Count::AtLeast(n) => format!("at least {}", int_arg(n)),
        Count::AtMost(n) => format!("at most {}", int_arg(n)),
        Count::Between(a, b) => format!("between {} and {}", int_arg(a), int_arg(b)),
    }
}

pub fn qualifier(q: &Qualifier) -> String {
    match q {
        Qualifier::Each(t) => format!("each {}", test(t)),
        Qualifier::Qualified { bound, n, test: t } => {
            let b = match bound {
                QualifiedBound::AtLeast => "at least",
                QualifiedBound::AtMost => "at most",
                QualifiedBound::Exactly => "exactly",
            };
            let t = match t {
                Test::Is(v) => value(v),
                other => test(other),
            };
            format!("{b} {} is {t}", int_arg(n))
        }
        Qualifier::SameAs(n) => format!("same as {}", node(n)),
        Qualifier::DifferentFrom(n) => format!("different from {}", node(n)),
        Qualifier::LessThan(n) => format!("less than {}", node(n)),
        Qualifier::AtMostProperty(n) => format!("at most {}", node(n)),
        Qualifier::UniqueLanguage => "unique language".into(),
    }
}

pub fn test(t: &Test) -> String {
    match t {
        Test::Class(n) => format!("a {}", node(n)),
        Test::Matches(c) => format!("matches {}", call(c)),
        Test::Is(v) => format!("is {}", value(v)),
        Test::OneOf(vs) => format!(
            "one of {}",
            vs.iter().map(value).collect::<Vec<_>>().join(" ")
        ),
        Test::Datatype(DatatypeRef::Builtin(w)) => w.clone(),
        Test::Datatype(DatatypeRef::Node(n)) => format!("datatype {}", node(n)),
        Test::Kind(NodeKind::Iri) => "iri".into(),
        Test::Kind(NodeKind::Literal) => "literal".into(),
        Test::Kind(NodeKind::Blank) => "blank".into(),
        Test::Length { min, max } => match (min, max) {
            (Some(a), Some(b)) if a == b => format!("length {a}"),
            (Some(a), Some(b)) => format!("length {a} to {b}"),
            (Some(a), None) => format!("length at least {a}"),
            (None, Some(b)) => format!("length at most {b}"),
            (None, None) => "length at least 0".into(),
        },
        Test::Range { from, to } => match (from, to) {
            (Some(a), Some(b)) => format!("from {} to {}", a.lexical, b.lexical),
            (Some(a), None) => format!("from {}", a.lexical),
            (None, Some(b)) => format!("up to {}", b.lexical),
            (None, None) => "from 0 to 0".into(),
        },
        Test::Pattern { regex, flags } => {
            if flags.is_empty() {
                format!("pattern {}", string_lit(regex))
            } else {
                format!("pattern {} flags {}", string_lit(regex), string_lit(flags))
            }
        }
        Test::Language(ls) => format!(
            "language {}",
            ls.iter()
                .map(|l| string_lit(l))
                .collect::<Vec<_>>()
                .join(" ")
        ),
    }
}

pub fn node(n: &NodeRef) -> String {
    match n {
        NodeRef::Iri(i) => format!("<{i}>"),
        NodeRef::Prefixed { prefix, local } => format!("{prefix}:{local}"),
        NodeRef::Name(n) => n.clone(),
    }
}

pub fn value(v: &Value) -> String {
    match v {
        Value::Node(n) => node(n),
        Value::String { value, lang } => match lang {
            Some(l) => format!("{}@{l}", string_lit(value)),
            None => string_lit(value),
        },
        Value::Number(n) => n.lexical.clone(),
        Value::Boolean(b) => b.to_string(),
    }
}

pub fn call(c: &Call) -> String {
    if c.args.is_empty() {
        c.name.clone()
    } else {
        format!(
            "{}({})",
            c.name,
            c.args.iter().map(arg).collect::<Vec<_>>().join(", ")
        )
    }
}

pub fn arg(a: &Arg) -> String {
    match a {
        Arg::Node(n) => node(n),
        Arg::Literal(v) => value(v),
        Arg::Path(p) => path(p),
        Arg::Int(n) => n.to_string(),
        Arg::Call(c) => call(c),
    }
}

fn prec(p: &PathExpr) -> u8 {
    match p {
        PathExpr::Alt(_) => 0,
        PathExpr::Seq(_) => 1,
        _ => 2,
    }
}

pub fn path(p: &PathExpr) -> String {
    fn wrap(p: &PathExpr, min: u8) -> String {
        if prec(p) < min {
            format!("({})", path(p))
        } else {
            path(p)
        }
    }
    match p {
        PathExpr::Node(n) => node(n),
        PathExpr::SelfPath => "self".into(),
        PathExpr::Inverse(i) => format!("^{}", wrap(i, 2)),
        PathExpr::Seq(ps) => ps.iter().map(|q| wrap(q, 2)).collect::<Vec<_>>().join("/"),
        PathExpr::Alt(ps) => ps
            .iter()
            .map(|q| wrap(q, 1))
            .collect::<Vec<_>>()
            .join(" or "),
        PathExpr::Star(i) => format!("{}*", wrap(i, 2)),
        PathExpr::Plus(i) => format!("{}+", wrap(i, 2)),
        PathExpr::Optional(i) => format!("{}?", wrap(i, 2)),
    }
}

pub fn string_lit(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
