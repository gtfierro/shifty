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
        o.extend((0..m.paths.len()).map(HeaderRef::Path));
        o
    } else {
        let mut o = m.header_order.clone();
        // Paths declared by hand (not parsed) may be missing from the order.
        for i in 0..m.paths.len() {
            if !o.contains(&HeaderRef::Path(i)) {
                o.push(HeaderRef::Path(i));
            }
        }
        o
    };
    let mut last_was_path = false;
    for h in &order {
        let is_path = matches!(h, HeaderRef::Path(_));
        let documented = match h {
            HeaderRef::Path(i) => {
                !m.paths[*i].annotations.is_empty() || !m.paths[*i].comments.is_empty()
            }
            _ => false,
        };
        if (is_path != last_was_path || (is_path && documented)) && !out.is_empty() {
            out.push('\n');
        }
        last_was_path = is_path;
        match h {
            HeaderRef::Path(i) => {
                let l = &m.paths[*i];
                for c in &l.comments {
                    writeln!(out, "{}", comment(c)).unwrap();
                }
                annotations(&mut out, &l.annotations, "");
                let mut line = if l.params.is_empty() {
                    format!("path {} = {}", l.name, path(&l.path))
                } else {
                    format!("path {}({}) = {}", l.name, params(&l.params), path(&l.path))
                };
                if let Some(c) = &l.trailing_comment {
                    write!(line, "  {}", comment(c)).unwrap();
                }
                writeln!(out, "{line}").unwrap();
            }
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
    let mut prev_bare = false;
    for s in &m.shapes {
        // Undocumented one-line shapes in a row stay together, like a table.
        let bare = s.annotations.is_empty() && one_line_body(s).is_some();
        if !out.is_empty() && !(bare && prev_bare && s.comments.is_empty()) {
            out.push('\n');
        }
        prev_bare = bare;
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
        write!(head, "({})", params(&s.params)).unwrap();
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
    if let Some(t) = one_line_body(s) {
        // `head` is already written
        writeln!(out, " {{ {t} }}").unwrap();
        return;
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

/// The body of a shape that fits on its header line: `shape S { is a C }`.
fn one_line_body(s: &ShapeDecl) -> Option<String> {
    let inline_target = s.targets.len() == 1 && s.targets[0].trailing_comment.is_none();
    if !s.trailing_comments.is_empty() || !(s.targets.is_empty() || inline_target) {
        return None;
    }
    let [only] = s.body.as_slice() else {
        return None;
    };
    let t = inline_line(only)?;
    let mut head = format!("shape {}", s.name);
    if !s.params.is_empty() {
        write!(head, "({})", params(&s.params)).unwrap();
    }
    if inline_target {
        write!(head, " {}", target(&s.targets[0].kind)).unwrap();
    }
    (head.len() + t.len() + 5 <= 88).then_some(t)
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
        LineKind::Field(Field {
            path: PathExpr::Call(c),
            name,
            count: None,
            qualifiers,
            ..
        }) if qualifiers.is_empty() => {
            let mut head = indent.clone();
            if let Some(n) = name {
                write!(head, "{n}: ").unwrap();
            }
            out.push_str(&head);
            call_text(out, c, depth, head.len());
            writeln!(out, "{trailing}").unwrap();
        }
        LineKind::Field(f) => field_line(out, f, depth, &trailing),
        LineKind::Include(c) => {
            write!(out, "{indent}").unwrap();
            call_text(out, c, depth, indent.len());
            writeln!(out, "{trailing}").unwrap();
        }
        LineKind::Closed => {
            writeln!(out, "{indent}only these properties{trailing}").unwrap();
        }
        LineKind::Boolean(b) => {
            boolean(out, b, depth, &trailing);
        }
        LineKind::Is(t) => {
            writeln!(out, "{indent}{}{trailing}", is_line(t)).unwrap();
        }
        LineKind::Group(g) => group(out, g, depth, &trailing),
    }
}

/// The widest a formatted line should be.
const WIDTH: usize = 88;

/// A field on one line if it fits; otherwise with the inline shape that
/// ends it opened over the lines below:
///
/// ```text
/// in_zone: downstream at least n is {
///   any of { ... } { ... }
/// }
/// ```
fn field_line(out: &mut String, f: &Field, depth: usize, trailing: &str) {
    let indent = "  ".repeat(depth);
    if let Some(t) = field_inline(f)
        && indent.len() + t.len() + trailing.len() <= WIDTH
    {
        writeln!(out, "{indent}{t}{trailing}").unwrap();
        return;
    }
    let last = f.qualifiers.last().and_then(|q| match q {
        Qualifier::Each(Test::Shape(b))
        | Qualifier::Qualified {
            test: Test::Shape(b),
            ..
        } => Some(b),
        _ => None,
    });
    let Some(b) = last else {
        writeln!(out, "{indent}{}{trailing}", field_text(f)).unwrap();
        return;
    };
    // the line up to the shape: render it empty and drop the `{ }`
    let mut head = f.clone();
    if let Some(Qualifier::Each(t) | Qualifier::Qualified { test: t, .. }) =
        head.qualifiers.last_mut()
    {
        *t = Test::Shape(Block {
            lines: Vec::new(),
            span: b.span,
            trailing_comments: Vec::new(),
        });
    }
    let head = field_text(&head);
    let head = head.strip_suffix("{ }").unwrap_or(&head);
    write!(out, "{indent}{head}").unwrap();
    open_block(out, b, depth);
    writeln!(out, "{trailing}").unwrap();
}

/// A field in one-line form, unless an inline shape in it holds comments
/// that one line would lose.
fn field_inline(f: &Field) -> Option<String> {
    if f.block {
        return None;
    }
    for q in &f.qualifiers {
        if let Qualifier::Each(Test::Shape(b))
        | Qualifier::Qualified {
            test: Test::Shape(b),
            ..
        } = q
        {
            inline_block_within(b, usize::MAX)?;
        }
    }
    Some(field_text(f))
}

fn is_line(t: &Test) -> String {
    match t {
        Test::Is(v @ Value::Node(_)) => format!("is one of {}", value(v)),
        Test::Is(v) => format!("is {}", value(v)),
        other => format!("is {}", test(other)),
    }
}

/// A call starting at column `col`: on one line if it fits; else with an
/// inline shape passed last opened below it; else one argument per line.
fn call_text(out: &mut String, c: &Call, depth: usize, col: usize) {
    let args: Vec<(Option<&str>, &Arg)> = c
        .args
        .iter()
        .map(|a| (None, a))
        .chain(c.named.iter().map(|n| (Some(n.name.as_str()), &n.value)))
        .collect();
    let opens =
        |a: &Arg| matches!(a, Arg::Shape(b) if inline_block_within(b, usize::MAX).is_none());
    let one = call(c);
    if !args.iter().any(|(_, a)| opens(a)) && col + one.len() <= WIDTH {
        out.push_str(&one);
        return;
    }
    // `W(x, {` ... `})`
    if let Some(((name, Arg::Shape(b)), rest)) = args.split_last()
        && !rest.iter().any(|(_, a)| opens(a))
    {
        let mut head = format!("{}(", c.name);
        for (n, a) in rest {
            if let Some(n) = n {
                write!(head, "{n} = ").unwrap();
            }
            write!(head, "{}, ", arg(a)).unwrap();
        }
        if let Some(n) = name {
            write!(head, "{n} = ").unwrap();
        }
        if col + head.len() < WIDTH {
            out.push_str(&head);
            open_block(out, b, depth);
            out.push(')');
            return;
        }
    }
    let inner = "  ".repeat(depth + 1);
    writeln!(out, "{}(", c.name).unwrap();
    for (n, a) in args {
        out.push_str(&inner);
        if let Some(n) = n {
            write!(out, "{n} = ").unwrap();
        }
        match a {
            Arg::Shape(b) => block_text(out, b, depth + 1),
            a => out.push_str(&arg(a)),
        }
        out.push_str(",\n");
    }
    write!(out, "{})", "  ".repeat(depth)).unwrap();
}

/// `PATH COUNT?`, the head of a group.
fn group_head(g: &Group) -> String {
    let mut head = path(&g.path);
    if let Some(c) = &g.count {
        write!(head, " {}", count(c)).unwrap();
    }
    head
}

fn group(out: &mut String, u: &Group, depth: usize, trailing: &str) {
    let indent = "  ".repeat(depth);
    let head = group_head(u);
    if let Some(t) = group_inline(u) {
        writeln!(out, "{indent}{t}{trailing}").unwrap();
        return;
    }
    writeln!(out, "{indent}{head} {{{trailing}").unwrap();
    let inner = "  ".repeat(depth + 1);
    // Align the items of labelled rows, as point lists are usually written.
    let width = u
        .rows
        .iter()
        .filter_map(|r| r.label.as_ref().map(|l| l.len() + 1))
        .max()
        .unwrap_or(0);
    for r in &u.rows {
        for c in &r.comments {
            writeln!(out, "{inner}{}", comment(c)).unwrap();
        }
        let mut text = String::new();
        if let Some(l) = &r.label {
            write!(text, "{:<width$} ", format!("{l}:")).unwrap();
        }
        text.push_str(&row_tail(r));
        out.push_str(&inner);
        out.push_str(&text);
        if let Some(b) = &r.body {
            out.push(' ');
            block_text(out, b, depth + 1);
        }
        if let Some(c) = &r.trailing_comment {
            write!(out, "  {}", comment(c)).unwrap();
        }
        out.push('\n');
    }
    for c in &u.trailing_comments {
        writeln!(out, "{inner}{}", comment(c)).unwrap();
    }
    writeln!(out, "{indent}}}").unwrap();
}

/// A row without its body: `label: ITEM COUNT annotations`.
fn row_head(r: &Row) -> String {
    match &r.label {
        Some(l) => format!("{l}: {}", row_tail(r)),
        None => row_tail(r),
    }
}

fn row_tail(r: &Row) -> String {
    let mut text = row_item(&r.item);
    if let Some(c) = &r.count {
        write!(text, " {}", count(c)).unwrap();
    }
    let a = &r.annotations;
    if let Some(s) = &a.severity {
        write!(text, " severity {}", s.word()).unwrap();
    }
    if let Some(d) = &a.doc {
        write!(text, " doc {}", string_lit(d)).unwrap();
    }
    for m in &a.messages {
        write!(text, " message {}", string_lit(m)).unwrap();
    }
    text
}

pub fn row_item(i: &Item) -> String {
    match i {
        Item::Class(n) => node(n),
        Item::Shape(c) => call(c),
    }
}

/// `a, b = ex:x`
pub fn params(ps: &[Param]) -> String {
    ps.iter()
        .map(|p| match &p.default {
            Some(d) => format!("{} = {}", p.name, arg(d)),
            None => p.name.clone(),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A line in one-line form (comments dropped), for inline shapes in reports.
pub fn line_text(l: &Line) -> String {
    match &l.kind {
        LineKind::Field(f) => field_text(f),
        LineKind::Include(c) => call(c),
        LineKind::Closed => "only these properties".into(),
        LineKind::Is(t) => is_line(t),
        LineKind::Boolean(b) => {
            let blocks: Vec<String> = b.blocks.iter().map(block_one_line).collect();
            match b.frame {
                Frame::When => format!("when {} then {}", blocks[0], blocks[1]),
                Frame::AllOf => format!("all of {}", blocks.join(" ")),
                Frame::AnyOf => format!("any of {}", blocks.join(" ")),
                Frame::ExactlyOneOf => format!("exactly one of {}", blocks.join(" ")),
                Frame::NoneOf => format!("none of {}", blocks.join(" ")),
                Frame::AtLeast(n) => format!("at least {n} of {}", blocks.join(" ")),
            }
        }
        LineKind::Group(u) => {
            let head = group_head(u);
            let rows: Vec<String> = u
                .rows
                .iter()
                .map(|r| {
                    let mut t = String::new();
                    if let Some(l) = &r.label {
                        write!(t, "{l}: ").unwrap();
                    }
                    t.push_str(&row_item(&r.item));
                    if let Some(c) = &r.count {
                        write!(t, " {}", count(c)).unwrap();
                    }
                    if let Some(b) = &r.body {
                        write!(t, " {}", block_one_line(b)).unwrap();
                    }
                    t
                })
                .collect();
            if rows.is_empty() {
                format!("{head} {{ }}")
            } else {
                format!("{head} {{ {} }}", rows.join("; "))
            }
        }
    }
}

fn block_one_line(b: &Block) -> String {
    if b.lines.is_empty() {
        "{ }".into()
    } else {
        format!(
            "{{ {} }}",
            b.lines.iter().map(line_text).collect::<Vec<_>>().join("; ")
        )
    }
}

/// A line in one-line form, if it reads well that way (no comments, no
/// annotations, nothing that is better as a table).
fn inline_line(l: &Line) -> Option<String> {
    if !l.comments.is_empty() || l.trailing_comment.is_some() || !l.annotations.is_empty() {
        return None;
    }
    match &l.kind {
        LineKind::Field(f) => field_inline(f),
        LineKind::Include(c) => Some(call(c)),
        LineKind::Closed => Some("only these properties".into()),
        LineKind::Is(t) => Some(is_line(t)),
        LineKind::Group(g) => group_inline(g),
        _ => None,
    }
}

/// `inputs { SAT_AVG; MAT_AVG }`: a group stays on one line when it is
/// short and is not a table of labelled rows.
fn group_inline(u: &Group) -> Option<String> {
    if !u.trailing_comments.is_empty() {
        return None;
    }
    let labelled = u.rows.iter().filter(|r| r.label.is_some()).count();
    if labelled > 1 {
        return None;
    }
    let mut rows = Vec::new();
    for r in &u.rows {
        if !r.comments.is_empty() || r.trailing_comment.is_some() {
            return None;
        }
        let mut t = row_head(r);
        if let Some(b) = &r.body {
            t.push(' ');
            t.push_str(&inline_block(b)?);
        }
        rows.push(t);
    }
    let head = group_head(u);
    let text = if rows.is_empty() {
        format!("{head} {{ }}")
    } else {
        format!("{head} {{ {} }}", rows.join("; "))
    };
    (text.len() <= 72).then_some(text)
}

fn inline_block(b: &Block) -> Option<String> {
    inline_block_within(b, 72)
}

fn inline_block_within(b: &Block, max: usize) -> Option<String> {
    if !b.trailing_comments.is_empty() {
        return None;
    }
    let parts: Option<Vec<String>> = b.lines.iter().map(inline_line).collect();
    let parts = parts?;
    let text = if parts.is_empty() {
        "{ }".to_string()
    } else {
        format!("{{ {} }}", parts.join("; "))
    };
    (text.len() <= max).then_some(text)
}

fn block_text(out: &mut String, b: &Block, depth: usize) {
    if let Some(t) = inline_block(b) {
        out.push_str(&t);
        return;
    }
    open_block(out, b, depth);
}

/// `{`, the block's lines one level in, and `}`.
fn open_block(out: &mut String, b: &Block, depth: usize) {
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
            let pad = indent.len() + phrase.len() + 1;
            let inline: Option<Vec<String>> = b
                .blocks
                .iter()
                .map(|blk| inline_block_within(blk, 88usize.saturating_sub(pad)))
                .collect();
            if let Some(blocks) = inline {
                let one = format!("{indent}{phrase} {}", blocks.join(" "));
                if one.len() <= 88 || blocks.len() < 2 {
                    writeln!(out, "{one}{trailing}").unwrap();
                } else {
                    // any of { ... }
                    //        { ... }
                    let pad = " ".repeat(pad);
                    write!(out, "{indent}{phrase} {}", blocks[0]).unwrap();
                    for blk in &blocks[1..] {
                        write!(out, "\n{pad}{blk}").unwrap();
                    }
                    writeln!(out, "{trailing}").unwrap();
                }
                return;
            }
            // one branch needs lines of its own, so all of them get them,
            // and the alternatives read alike
            write!(out, "{indent}{phrase}").unwrap();
            for blk in &b.blocks {
                out.push(' ');
                open_block(out, blk, depth);
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
                Test::Is(v @ Value::Node(_)) => format!("one of {}", value(v)),
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
        // a bare node is a class; node equality is `one of`
        Test::Is(v @ Value::Node(_)) => format!("one of {}", value(v)),
        Test::Is(v) => format!("is {}", value(v)),
        Test::Item(i) => row_item(i),
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
        Test::Shape(b) => block_one_line(b),
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

/// `T(a, b = c)`.
pub fn call(c: &Call) -> String {
    if c.args.is_empty() && c.named.is_empty() {
        return c.name.clone();
    }
    let args = c.args.iter().map(arg).chain(
        c.named
            .iter()
            .map(|n| format!("{} = {}", n.name, arg(&n.value))),
    );
    format!("{}({})", c.name, args.collect::<Vec<_>>().join(", "))
}

pub fn arg(a: &Arg) -> String {
    match a {
        Arg::Node(n) => node(n),
        Arg::Literal(v) => value(v),
        Arg::Path(p) => path(p),
        Arg::Int(n) => n.to_string(),
        Arg::Call(c) => call(c),
        Arg::Shape(b) => block_one_line(b),
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
        PathExpr::Call(c) => call(c),
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
