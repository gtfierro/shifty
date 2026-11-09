//! Path aliases and field groups.
//!
//! Point lists are fields that share a path:
//!
//! ```text
//! path points = brick:hasPoint
//!
//! shape SupplyFan {
//!   is a brick:Supply_Fan
//!   points {
//!     speed:      brick:Frequency_Command
//!     start_stop: brick:Start_Stop_Command
//!     status:     brick:Fan_Status optional
//!   }
//! }
//! ```
//!
//! Two features, each usable on its own:
//!
//! - `path NAME = PATH` names a property path. The name stands for the path
//!   anywhere one is written: a field (`points each a brick:Point`), a group,
//!   a template argument, or (when the path is a single property) `same as`
//!   and the `for every node with` targets.
//! - `PATH COUNT? { row* }` is a group. Each row is a named field along
//!   `PATH` that counts the values that are instances of the row's class (or
//!   match the row's shape): `exactly 1` unless the row or the group gives a
//!   count. `required` means at least one; `optional` means at most one.
//!
//! Neither adds anything to the kernel. Before building, aliases are
//! replaced by their paths and every group is rewritten into ordinary field
//! lines, so a row is a field in every respect: `speed` exports as the
//! property shape `SupplyFan.speed`, reports quote it, and
//! `include SupplyFan.speed` pulls in that one row. The row above becomes
//!
//! ```text
//! speed: brick:hasPoint exactly 1 is a brick:Frequency_Command
//! ```
//!
//! A row may name a shape instead of a class (`fan: SupplyFan`); a row that
//! is only a shape name is labelled by it, so a glossary of point roles reads
//! as `inputs { SAT_AVG; MAT_AVG }`.

use crate::Binding;
use crate::ast::*;
use crate::build::{DEFAULT_PREFIXES, Program};
use crate::diagnostic::Diagnostic;
use shifty_algebra::SourceLocation;
use std::collections::{HashMap, HashSet};

/// Replace path aliases by their paths and rewrite every group in
/// `program` into field lines.
pub fn desugar(program: &Program) -> (Program, Vec<Diagnostic>) {
    let mut out = program.clone();
    let mut diags = Vec::new();
    let mut cycles = HashSet::new();
    for (m, unit) in program.modules.iter().enumerate() {
        let mut seen: HashMap<&str, &PathDecl> = HashMap::new();
        for d in &unit.module.paths {
            if let Some(prev) = seen.insert(&d.name, d) {
                diags.push(
                    Diagnostic::error(
                        format!("path `{}` is declared twice", d.name),
                        loc(program, m, d.span),
                    )
                    .with_note(format!("first declared on line {}", prev.span.line)),
                );
            }
            if let Some(s) = unit.module.shapes.iter().find(|s| s.name == d.name) {
                diags.push(
                    Diagnostic::error(
                        format!("`{}` names both a path and a shape", d.name),
                        loc(program, m, d.span),
                    )
                    .with_note(format!("the shape is declared on line {}", s.span.line))
                    .with_hint("rename one of them"),
                );
            }
        }
    }
    for m in 0..program.modules.len() {
        let paths: Vec<PathDecl> = program.modules[m]
            .module
            .paths
            .iter()
            .map(|d| {
                let mut cx = Cx {
                    p: program,
                    m,
                    params: d.params.iter().map(|p| p.name.clone()).collect(),
                    stack: vec![(m, d.name.clone())],
                    cycles: &mut cycles,
                    diags: &mut diags,
                };
                PathDecl {
                    path: cx.path(&d.path, d.span),
                    ..d.clone()
                }
            })
            .collect();
        out.modules[m].module.paths = paths;
        for (i, decl) in program.modules[m].module.shapes.iter().enumerate() {
            let mut cx = Cx {
                p: program,
                m,
                params: decl.param_names(),
                stack: Vec::new(),
                cycles: &mut cycles,
                diags: &mut diags,
            };
            let targets = decl
                .targets
                .iter()
                .map(|t| {
                    let mut t = t.clone();
                    match &mut t.kind {
                        Target::SubjectsOf(n) => {
                            *n = cx.property(n, t.span, "`for every node with`");
                        }
                        Target::ObjectsOf(n) => {
                            *n = cx.property(n, t.span, "`for every value of`");
                        }
                        Target::Matching(c) => cx.call(c, t.span),
                        _ => {}
                    }
                    t
                })
                .collect();
            let body = cx.lines(&decl.body);
            let shape = &mut out.modules[m].module.shapes[i];
            shape.targets = targets;
            shape.body = body;
        }
    }
    (out, diags)
}

/// Path aliases visible in module `m`, as it would write them.
pub fn visible_paths(p: &Program, m: usize) -> Vec<String> {
    let unit = &p.modules[m];
    let mut names: Vec<String> = unit.module.paths.iter().map(|d| d.name.clone()).collect();
    for b in &unit.open {
        if let Binding::Module(o) = b {
            names.extend(p.modules[*o].module.paths.iter().map(|d| d.name.clone()));
        }
    }
    for (alias, b) in &unit.bindings {
        if let Binding::Module(o) = b {
            names.extend(
                p.modules[*o]
                    .module
                    .paths
                    .iter()
                    .map(|d| format!("{alias}.{}", d.name)),
            );
        }
    }
    names
}

fn loc(p: &Program, m: usize, span: Span) -> Option<SourceLocation> {
    Some(SourceLocation {
        file: p.modules[m].label.clone(),
        line: span.line,
        column: span.column,
    })
}

struct Cx<'a> {
    p: &'a Program,
    m: usize,
    params: Vec<String>,
    /// Aliases being expanded, to report one defined in terms of itself.
    stack: Vec<(usize, String)>,
    /// Aliases already reported as cyclic.
    cycles: &'a mut HashSet<(usize, String)>,
    diags: &'a mut Vec<Diagnostic>,
}

impl Cx<'_> {
    fn is_param(&self, name: &str) -> bool {
        self.params.iter().any(|p| p == name)
    }

    fn error(&mut self, span: Span, message: impl Into<String>, hint: Option<String>) {
        let mut d = Diagnostic::error(message.into(), loc(self.p, self.m, span));
        if let Some(h) = hint {
            d = d.with_hint(h);
        }
        self.diags.push(d);
    }

    fn lines(&mut self, lines: &[Line]) -> Vec<Line> {
        let mut out = Vec::with_capacity(lines.len());
        for l in lines {
            match &l.kind {
                LineKind::Group(g) => self.group(l, g, &mut out),
                _ => {
                    let mut l = l.clone();
                    self.descend(&mut l.kind, l.span);
                    out.push(l);
                }
            }
        }
        out
    }

    /// Expand aliases in a line, and groups nested in its blocks.
    fn descend(&mut self, kind: &mut LineKind, span: Span) {
        match kind {
            LineKind::Boolean(b) => {
                for blk in &mut b.blocks {
                    blk.lines = self.lines(&blk.lines);
                }
            }
            LineKind::Field(f) => {
                // `HasPoint(brick:X)` alone on a line composes that shape;
                // a path template there would be a field with no constraint.
                if f.name.is_none()
                    && f.count.is_none()
                    && f.qualifiers.is_empty()
                    && let PathExpr::Call(c) = &f.path
                    && self.resolve(&c.name).is_none()
                {
                    let mut c = c.clone();
                    self.call(&mut c, span);
                    *kind = LineKind::Include(c);
                    return;
                }
                f.path = self.path(&f.path, span);
                for q in &mut f.qualifiers {
                    self.qualifier(q, span);
                }
            }
            LineKind::Include(c) => self.call(c, span),
            LineKind::Is(t) => self.test(t, span),
            LineKind::Closed | LineKind::Group(_) => {}
        }
    }

    fn qualifier(&mut self, q: &mut Qualifier, span: Span) {
        match q {
            Qualifier::Each(t) | Qualifier::Qualified { test: t, .. } => self.test(t, span),
            Qualifier::SameAs(n) => *n = self.property(n, span, "`same as`"),
            Qualifier::DifferentFrom(n) => *n = self.property(n, span, "`different from`"),
            Qualifier::LessThan(n) => *n = self.property(n, span, "`less than`"),
            Qualifier::AtMostProperty(n) => *n = self.property(n, span, "`at most`"),
            Qualifier::UniqueLanguage => {}
        }
    }

    fn test(&mut self, t: &mut Test, span: Span) {
        match t {
            Test::Shape(b) => b.lines = self.lines(&b.lines),
            Test::Matches(c) | Test::Item(Item::Shape(c)) => self.call(c, span),
            _ => {}
        }
    }

    /// Expand aliases and path templates passed to a shape, and groups in
    /// the blocks it is given.
    fn call(&mut self, c: &mut Call, span: Span) {
        for a in &mut c.args {
            self.arg(a, span);
        }
        for n in &mut c.named {
            self.arg(&mut n.value, span);
        }
        if let Some(b) = &mut c.block {
            b.lines = self.lines(&b.lines);
        }
    }

    fn arg(&mut self, a: &mut Arg, span: Span) {
        match a {
            Arg::Node(NodeRef::Name(n)) if self.is_alias(n) => {
                let p = self.path(&PathExpr::Node(NodeRef::Name(n.clone())), span);
                *a = path_arg(p);
            }
            Arg::Path(p) => *p = self.path(p, span),
            Arg::Call(c) if !self.is_local_shape(&c.name) && self.resolve(&c.name).is_some() => {
                if let Some(p) = self.expand_path(c, span) {
                    *a = path_arg(p);
                }
            }
            Arg::Call(inner) => self.call(inner, span),
            Arg::Shape(b) => b.lines = self.lines(&b.lines),
            Arg::Node(_) | Arg::Literal(_) | Arg::Int(_) => {}
        }
    }

    fn is_local_shape(&self, name: &str) -> bool {
        self.p.modules[self.m]
            .module
            .shapes
            .iter()
            .any(|s| s.name == name)
    }

    /// A bare name that refers to a path alias here: not a parameter, and
    /// not a shape of this module (shapes win in argument positions).
    fn is_alias(&self, name: &str) -> bool {
        !self.is_param(name) && !self.is_local_shape(name) && self.resolve(name).is_some()
    }

    /// `p` with every alias and path template replaced by its path.
    fn path(&mut self, p: &PathExpr, span: Span) -> PathExpr {
        let rec = |cx: &mut Self, q: &PathExpr| Box::new(cx.path(q, span));
        match p {
            PathExpr::Node(NodeRef::Name(n)) if !self.is_param(n) => {
                if self.resolve(n).is_none() {
                    return p.clone();
                }
                let call = Call {
                    name: n.clone(),
                    args: Vec::new(),
                    named: Vec::new(),
                    block: None,
                    span,
                };
                self.expand_path(&call, span).unwrap_or_else(|| p.clone())
            }
            PathExpr::Call(c) => self.expand_path(c, span).unwrap_or_else(|| p.clone()),
            PathExpr::Node(_) | PathExpr::SelfPath => p.clone(),
            PathExpr::Inverse(i) => PathExpr::Inverse(rec(self, i)),
            PathExpr::Seq(ps) => PathExpr::Seq(ps.iter().map(|q| self.path(q, span)).collect()),
            PathExpr::Alt(ps) => PathExpr::Alt(ps.iter().map(|q| self.path(q, span)).collect()),
            PathExpr::Star(i) => PathExpr::Star(rec(self, i)),
            PathExpr::Plus(i) => PathExpr::Plus(rec(self, i)),
            PathExpr::Optional(i) => PathExpr::Optional(rec(self, i)),
        }
    }

    /// The path a `path` declaration stands for, with the call's arguments
    /// (or the declaration's defaults) in place of its parameters.
    fn expand_path(&mut self, c: &Call, span: Span) -> Option<PathExpr> {
        let span = if c.span == Span::default() {
            span
        } else {
            c.span
        };
        let Some((dm, decl)) = self.resolve(&c.name) else {
            let known = visible_paths(self.p, self.m);
            let hint = match crate::diagnostic::nearest(&c.name, known.iter().map(String::as_str)) {
                Some(n) => format!("did you mean the path `{n}`?"),
                None => format!(
                    "to compose the shape `{}`, write it alone on a line; a path is declared with `path {}(p) = ...`",
                    c.name, c.name
                ),
            };
            self.error(span, format!("no path named `{}`", c.name), Some(hint));
            return None;
        };
        let decl = decl.clone();
        let key = (dm, decl.name.clone());
        if self.cycles.contains(&key) {
            return None;
        }
        if let Some(at) = self.stack.iter().position(|k| *k == key) {
            let mut chain: Vec<&str> = self.stack[at..].iter().map(|k| k.1.as_str()).collect();
            chain.push(&decl.name);
            self.diags.push(
                Diagnostic::error(
                    format!("path `{}` is defined in terms of itself", decl.name),
                    loc(self.p, dm, decl.span),
                )
                .with_note(format!("`{}`", chain.join("` uses `")))
                .with_hint("write the property it stands for, e.g. `brick:hasPoint`"),
            );
            for k in &self.stack[at..] {
                self.cycles.insert(k.clone());
            }
            return None;
        }
        let matched = match crate::args::match_args(&c.name, &decl.params, c) {
            Ok(m) => m,
            Err(e) => {
                self.error(span, e.message, e.hint);
                return None;
            }
        };
        // The arguments, as paths of this module.
        let mut given = Vec::with_capacity(matched.len());
        for a in &matched {
            given.push(match a {
                Some(a) => Some(self.arg_path(a, span)?),
                None => None,
            });
        }
        // The body and the defaults, expanded where they were written.
        let mut inner = Cx {
            p: self.p,
            m: dm,
            params: decl.params.iter().map(|p| p.name.clone()).collect(),
            stack: std::mem::take(&mut self.stack),
            cycles: &mut *self.cycles,
            diags: &mut *self.diags,
        };
        inner.stack.push(key);
        let body = inner.path(&decl.path, decl.span);
        let mut defaults = Vec::with_capacity(decl.params.len());
        for p in &decl.params {
            defaults.push(match &p.default {
                Some(d) => inner.arg_path(d, decl.span),
                None => None,
            });
        }
        inner.stack.pop();
        self.stack = std::mem::take(&mut inner.stack);
        let mut env: HashMap<String, PathExpr> = HashMap::new();
        for ((p, g), d) in decl.params.iter().zip(given).zip(defaults) {
            let v = match g {
                Some(v) => v,
                None => subst(&self.portable_path(dm, &d?), &env),
            };
            env.insert(p.name.clone(), v);
        }
        Some(subst(&self.portable_path(dm, &body), &env))
    }

    /// An argument where a path is expected.
    fn arg_path(&mut self, a: &Arg, span: Span) -> Option<PathExpr> {
        match a {
            Arg::Node(n) => Some(self.path(&PathExpr::Node(n.clone()), span)),
            Arg::Path(p) => Some(self.path(p, span)),
            Arg::Call(c) => self.expand_path(c, span),
            Arg::Literal(_) | Arg::Int(_) => {
                self.error(span, "expected a path, found a literal", None);
                None
            }
            Arg::Shape(_) => {
                self.error(span, "expected a path, found a block", None);
                None
            }
        }
    }

    /// An alias where SHACL wants a single property (`sh:equals`, ...).
    fn property(&mut self, n: &NodeRef, span: Span, what: &str) -> NodeRef {
        let NodeRef::Name(name) = n else {
            return n.clone();
        };
        if self.is_param(name) || self.resolve(name).is_none() {
            return n.clone();
        }
        match self.path(&PathExpr::Node(n.clone()), span) {
            PathExpr::Node(x) => x,
            p => {
                self.diags.push(
                    Diagnostic::error(
                        format!(
                            "{what} needs a single property, but `{name}` is the path `{}`",
                            crate::display::path(&p)
                        ),
                        loc(self.p, self.m, span),
                    )
                    .with_note("SHACL compares values along one property here, not a path"),
                );
                n.clone()
            }
        }
    }

    /// Find an alias by the name a module uses for it: its own, one from an
    /// import without an alias, or `alias.name`.
    fn resolve(&self, name: &str) -> Option<(usize, &PathDecl)> {
        let find = |m: usize, n: &str| {
            self.p.modules[m]
                .module
                .paths
                .iter()
                .find(|d| d.name == n)
                .map(|d| (m, d))
        };
        if let Some(hit) = find(self.m, name) {
            return Some(hit);
        }
        let unit = &self.p.modules[self.m];
        if let Some((alias, rest)) = name.split_once('.')
            && let Some(Binding::Module(other)) = unit.bindings.get(alias)
        {
            return find(*other, rest);
        }
        unit.open.iter().find_map(|b| match b {
            Binding::Module(other) => find(*other, name),
            Binding::Rdf(_) => None,
        })
    }

    fn group(&mut self, line: &Line, g: &Group, out: &mut Vec<Line>) {
        let path = self.path(&g.path, line.span);
        let default = g.count.clone().unwrap_or(Count::Exactly(IntArg::Lit(1)));
        // Comments and annotations written on the group line go to its first row.
        let mut lead_comments = line.comments.clone();
        let mut lead_annotations = line.annotations.clone();
        for r in &g.rows {
            let test = self.row_test(r);
            let qualifiers = bounds(r.count.as_ref().unwrap_or(&default))
                .into_iter()
                .map(|(bound, n)| Qualifier::Qualified {
                    bound,
                    n,
                    test: test.clone(),
                })
                .collect();
            let mut annotations = r.annotations.clone();
            if !lead_annotations.is_empty() {
                let a = std::mem::take(&mut lead_annotations);
                annotations.severity = annotations.severity.or(a.severity);
                annotations.doc = annotations.doc.or(a.doc);
                annotations.messages.extend(a.messages);
            }
            let mut comments = std::mem::take(&mut lead_comments);
            comments.extend(r.comments.iter().cloned());
            out.push(Line {
                kind: LineKind::Field(Field {
                    name: r.label.clone().or_else(|| self.implicit_label(r)),
                    path: path.clone(),
                    count: None,
                    qualifiers,
                    block: false,
                }),
                annotations,
                span: r.span,
                comments,
                trailing_comment: r.trailing_comment.clone(),
            });
        }
    }

    /// A row that names a shape is labelled by it: `inputs { SAT_AVG }`
    /// defines the field `SAT_AVG`.
    fn implicit_label(&self, r: &Row) -> Option<String> {
        match &r.item {
            Item::Shape(c)
                if c.args.is_empty() && c.named.is_empty() && !self.is_param(&c.name) =>
            {
                Some(c.name.rsplit('.').next().unwrap_or(&c.name).to_string())
            }
            _ => None,
        }
    }

    /// A row's item as a test. Its `{ ... }` is the block of a shape item
    /// (the last argument of a template, more lines for a shape), and more
    /// lines for a class.
    fn row_test(&mut self, r: &Row) -> Test {
        let mut item = r.item.clone();
        if let Item::Shape(c) = &mut item {
            self.call(c, r.span);
        }
        let Some(body) = &r.body else {
            return Test::Item(item);
        };
        let lines = self.lines(&body.lines);
        match item {
            Item::Shape(mut c) if c.block.is_none() => {
                c.block = Some(Block {
                    lines,
                    ..body.clone()
                });
                Test::Item(Item::Shape(c))
            }
            item => {
                let first = Line {
                    kind: LineKind::Is(Test::Item(item)),
                    annotations: Annotations::default(),
                    span: r.span,
                    comments: Vec::new(),
                    trailing_comment: None,
                };
                let mut all = vec![first];
                all.extend(lines);
                Test::Shape(Block {
                    lines: all,
                    span: body.span,
                    trailing_comments: Vec::new(),
                })
            }
        }
    }

    /// A path from module `dm`, with prefixes this module may not share
    /// spelled out as IRIs.
    fn portable_path(&self, dm: usize, p: &PathExpr) -> PathExpr {
        if dm == self.m {
            return p.clone();
        }
        let ns = |m: usize, prefix: &str| -> Option<String> {
            self.p.modules[m]
                .module
                .prefixes
                .iter()
                .find(|d| d.name == prefix)
                .map(|d| d.iri.clone())
                .or_else(|| {
                    DEFAULT_PREFIXES
                        .iter()
                        .find(|(n, _)| *n == prefix)
                        .map(|(_, i)| i.to_string())
                })
        };
        let fix = |n: &NodeRef| -> NodeRef {
            match n {
                NodeRef::Prefixed { prefix, local } => match (ns(dm, prefix), ns(self.m, prefix)) {
                    (Some(a), Some(b)) if a == b => n.clone(),
                    (Some(a), _) => NodeRef::Iri(format!("{a}{local}")),
                    _ => n.clone(),
                },
                other => other.clone(),
            }
        };
        fn map(p: &PathExpr, f: &dyn Fn(&NodeRef) -> NodeRef) -> PathExpr {
            match p {
                PathExpr::Node(n) => PathExpr::Node(f(n)),
                PathExpr::SelfPath => PathExpr::SelfPath,
                PathExpr::Inverse(i) => PathExpr::Inverse(Box::new(map(i, f))),
                PathExpr::Seq(ps) => PathExpr::Seq(ps.iter().map(|q| map(q, f)).collect()),
                PathExpr::Alt(ps) => PathExpr::Alt(ps.iter().map(|q| map(q, f)).collect()),
                PathExpr::Star(i) => PathExpr::Star(Box::new(map(i, f))),
                PathExpr::Plus(i) => PathExpr::Plus(Box::new(map(i, f))),
                PathExpr::Optional(i) => PathExpr::Optional(Box::new(map(i, f))),
                PathExpr::Call(c) => PathExpr::Call(c.clone()),
            }
        }
        map(p, &fix)
    }
}

/// `p` with each parameter name replaced by its value.
fn subst(p: &PathExpr, env: &HashMap<String, PathExpr>) -> PathExpr {
    let rec = |q: &PathExpr| Box::new(subst(q, env));
    match p {
        PathExpr::Node(NodeRef::Name(n)) => env.get(n).cloned().unwrap_or_else(|| p.clone()),
        PathExpr::Node(_) | PathExpr::SelfPath | PathExpr::Call(_) => p.clone(),
        PathExpr::Inverse(i) => PathExpr::Inverse(rec(i)),
        PathExpr::Seq(ps) => PathExpr::Seq(ps.iter().map(|q| subst(q, env)).collect()),
        PathExpr::Alt(ps) => PathExpr::Alt(ps.iter().map(|q| subst(q, env)).collect()),
        PathExpr::Star(i) => PathExpr::Star(rec(i)),
        PathExpr::Plus(i) => PathExpr::Plus(rec(i)),
        PathExpr::Optional(i) => PathExpr::Optional(rec(i)),
    }
}

/// A path as a template argument.
fn path_arg(p: PathExpr) -> Arg {
    match p {
        PathExpr::Node(x) => Arg::Node(x),
        p => Arg::Path(p),
    }
}

/// A row count as qualified bounds on the row's class.
fn bounds(c: &Count) -> Vec<(QualifiedBound, IntArg)> {
    match c {
        Count::Required => vec![(QualifiedBound::AtLeast, IntArg::Lit(1))],
        Count::Optional => vec![(QualifiedBound::AtMost, IntArg::Lit(1))],
        Count::Exactly(n) => vec![(QualifiedBound::Exactly, n.clone())],
        Count::AtLeast(n) => vec![(QualifiedBound::AtLeast, n.clone())],
        Count::AtMost(n) => vec![(QualifiedBound::AtMost, n.clone())],
        Count::Between(a, b) => vec![
            (QualifiedBound::AtLeast, a.clone()),
            (QualifiedBound::AtMost, b.clone()),
        ],
    }
}
