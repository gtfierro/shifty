//! Raising: a built Shapeshift schema back to SHACL RDF.
//!
//! **Experimental**: this crate may change in any release, without a
//! deprecation period. It is part of the implementation of
//! [`shifty-experimental`](https://docs.rs/shifty-experimental), the entry
//! point for Shapeshift (`shifty_experimental::shapeshift`): depend on that
//! instead.
//!
//! This is the inverse of shifty's lowering. The goal is SHACL a person would
//! have written, not canonical algebra poured into `sh:` vocabulary, so the
//! raiser works on the *authored* arena (before normalization erases the
//! readable forms) and matches arena patterns back to SHACL idioms:
//!
//! | Arena pattern                           | Emitted as                              |
//! |-----------------------------------------|-----------------------------------------|
//! | `Count{p, min, max, ⊤}`                 | property shape, `sh:minCount/maxCount`  |
//! | `Count{p, ≤0, ¬φ}`                      | property shape with φ's constraints     |
//! | `Count{p, ≥1, test(c)}`                 | `sh:hasValue c`                         |
//! | `Count{p, min/max, φ}` otherwise        | `sh:qualifiedValueShape` + counts       |
//! | `Count{rdf:type/rdfs:subClassOf*, ≥1, test(X)}` | `sh:class X`                    |
//! | several path constraints on one path    | merged into one property shape          |
//! | `Or` from `when` / `exactly one of`     | `sh:or ( [ sh:not C ] B )` / `sh:xone`  |
//! | `Or` of constants                       | `sh:in ( ... )`                         |
//! | named slot                              | `sh:NodeShape`/`sh:PropertyShape` IRI   |
//! | `HasPath(id, S)` selector               | universal SPARQL target + `when`        |
//!
//! Provenance is what restores `when` and `exactly one of`: after expansion
//! both are plain disjunctions; the frame origin says which idiom to emit.
//!
//! Correctness property (tested): lowering the raised graph with
//! `shifty-parse` yields a schema whose statements are structurally equal,
//! up to transparent annotations and the order of conjuncts and disjuncts, to
//! the builder's.

pub mod turtle;

use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple};
use shapeshift_macro::BuildOutput;
use shapeshift_macro::build::{OWL, RDF, RDFS, SH, XSD, class_path};
use shifty_algebra::{
    BooleanFrame, NodeKindSet, OriginKind, Path, Selector, Severity, Shape, ShapeId, ValueType,
};
use std::collections::{BTreeSet, HashMap, HashSet};

/// The Shapeshift annotation vocabulary.
pub const SS: &str = "https://shifty.gtf.fyi/ns/shapeshift#";

/// The universal SPARQL target used to encode shape-based targets portably.
pub const UNIVERSAL_TARGET: &str =
    "SELECT DISTINCT ?this WHERE { { ?this ?p ?o } UNION { ?s ?p ?this } }";

#[derive(Debug, Clone, Default)]
pub struct RaiseOptions {
    /// Pure SHACL Core: refuse anything needing SHACL-SPARQL.
    pub core: bool,
}

#[derive(Debug, Clone)]
pub struct Raised {
    /// Triples in emission order (the Turtle writer keeps it).
    pub triples: Vec<Triple>,
    pub prefixes: Vec<(String, String)>,
    pub warnings: Vec<String>,
}

impl Raised {
    pub fn graph(&self) -> oxrdf::Graph {
        let mut g = oxrdf::Graph::new();
        for t in &self.triples {
            g.insert(t);
        }
        g
    }

    pub fn to_turtle(&self) -> String {
        turtle::Writer::new(&self.triples, &self.prefixes).write()
    }
}

#[derive(Debug, Clone)]
pub struct RaiseError(pub Vec<String>);

impl std::fmt::Display for RaiseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("\n"))
    }
}

impl std::error::Error for RaiseError {}

fn nn(s: &str) -> NamedNode {
    NamedNode::new_unchecked(s)
}
fn sh(local: &str) -> NamedNode {
    nn(&format!("{SH}{local}"))
}
fn ss(local: &str) -> NamedNode {
    nn(&format!("{SS}{local}"))
}
fn rdf(local: &str) -> NamedNode {
    nn(&format!("{RDF}{local}"))
}
fn int_lit(n: u64) -> Term {
    Term::Literal(Literal::new_typed_literal(
        n.to_string(),
        nn(&format!("{XSD}integer")),
    ))
}
fn bool_lit(b: bool) -> Term {
    Term::Literal(Literal::new_typed_literal(
        b.to_string(),
        nn(&format!("{XSD}boolean")),
    ))
}
fn str_lit(s: &str) -> Term {
    Term::Literal(Literal::new_simple_literal(s))
}

/// Per-subject bookkeeping for single-valued SHACL predicates.
#[derive(Default)]
struct NodeState {
    used: HashSet<&'static str>,
    closed: Option<BTreeSet<NamedNode>>,
    direct_props: BTreeSet<NamedNode>,
    /// Is the subject a node shape (no `sh:path`)?
    node_shape: bool,
}

struct Raiser<'a> {
    b: &'a BuildOutput,
    triples: Vec<Triple>,
    refs: HashMap<ShapeId, NamedOrBlankNode>,
    queue: Vec<ShapeId>,
    emitted: HashSet<ShapeId>,
    errors: Vec<String>,
    warnings: Vec<String>,
    bnodes: usize,
    opts: RaiseOptions,
}

/// A group of constraints that share one path and fit in one property shape.
#[derive(Default)]
struct Group {
    card: Option<(Option<u64>, Option<u64>)>,
    forall: Vec<ShapeId>,
    has_value: Vec<Term>,
    qualified: Option<(Option<u64>, Option<u64>, ShapeId)>,
    pairs: Vec<(&'static str, NamedNode)>,
    unique_lang: bool,
}

pub fn raise(b: &BuildOutput, opts: RaiseOptions) -> Result<Raised, RaiseError> {
    let mut r = Raiser {
        b,
        triples: Vec::new(),
        refs: HashMap::new(),
        queue: Vec::new(),
        emitted: HashSet::new(),
        errors: Vec::new(),
        warnings: Vec::new(),
        bnodes: 0,
        opts,
    };
    r.run();
    if !r.errors.is_empty() {
        return Err(RaiseError(r.errors));
    }
    let mut prefixes: Vec<(String, String)> = Vec::new();
    if let Some(base) = &b.base {
        let ns = if base.ends_with('#') || base.ends_with('/') {
            base.clone()
        } else {
            format!("{base}#")
        };
        prefixes.push((String::new(), ns));
    }
    for (p, ns) in b.schema.prefixes.declarations() {
        if !prefixes.iter().any(|(q, _)| q == p) {
            prefixes.push((p.clone(), ns.clone()));
        }
    }
    prefixes.push(("shapeshift".into(), SS.into()));
    Ok(Raised {
        triples: r.triples,
        prefixes,
        warnings: r.warnings,
    })
}

fn is_class_path(p: &Path) -> bool {
    *p == class_path()
}

impl Raiser<'_> {
    fn schema(&self) -> &shifty_algebra::Schema {
        &self.b.schema
    }

    fn add(&mut self, s: &NamedOrBlankNode, p: NamedNode, o: Term) {
        self.triples.push(Triple::new(s.clone(), p, o));
    }

    fn bnode(&mut self) -> NamedOrBlankNode {
        self.bnodes += 1;
        NamedOrBlankNode::BlankNode(BlankNode::new_unchecked(format!("b{}", self.bnodes)))
    }

    fn list(&mut self, items: Vec<Term>) -> Term {
        if items.is_empty() {
            return Term::NamedNode(rdf("nil"));
        }
        let cells: Vec<NamedOrBlankNode> = items.iter().map(|_| self.bnode()).collect();
        for (i, item) in items.into_iter().enumerate() {
            let cell = cells[i].clone();
            self.add(&cell, rdf("first"), item);
            let rest = cells
                .get(i + 1)
                .map(|c| Term::from(c.clone()))
                .unwrap_or(Term::NamedNode(rdf("nil")));
            self.add(&cell, rdf("rest"), rest);
        }
        Term::from(cells[0].clone())
    }

    fn named(&self, id: ShapeId) -> Option<&str> {
        self.schema().name_of(id)
    }

    fn run(&mut self) {
        let schema = &self.b.schema;
        // Ontology header.
        if let Some(base) = &self.b.base {
            let h = NamedOrBlankNode::NamedNode(nn(base));
            self.add(
                &h,
                rdf("type"),
                Term::NamedNode(nn(&format!("{OWL}Ontology"))),
            );
            for iri in self.b.rdf_imports.clone() {
                self.add(&h, nn(&format!("{OWL}imports")), Term::NamedNode(nn(&iri)));
            }
            let p = self.b.portability.clone();
            let recursive = shifty_opt::analyze(&schema.arena)
                .recursive()
                .any(|s| s.shapes.iter().any(|id| !self.b.imported_rdf.contains(id)));
            if recursive || p.recursive {
                self.add(&h, ss("recursive"), bool_lit(true));
                self.warnings.push(
                    "the schema is recursive; its meaning is portable only to engines that pin greatest-fixpoint semantics (as shifty does)".into(),
                );
            }
            if p.shape_targets {
                self.add(&h, ss("shapeTargets"), bool_lit(true));
            }
            if p.sparql_targets || p.shape_targets {
                self.add(&h, ss("sparqlTargets"), bool_lit(true));
            }
        } else {
            self.warnings.push(
                "the file has no `base`; shapes are exported under a urn:shapeshift: placeholder IRI".into(),
            );
        }

        // Every named, non-imported slot in arena order, then targets.
        let mut named: Vec<ShapeId> = (0..schema.arena.len())
            .map(|i| ShapeId(i as u32))
            .filter(|id| !schema.names_of(*id).is_empty() && !self.b.imported_rdf.contains(id))
            .collect();
        named.sort();
        for id in named {
            self.emit_named(id);
        }
        let statements = schema.statements.clone();
        for (i, st) in statements.iter().enumerate() {
            if self.b.imported_statements.contains(&i) {
                continue;
            }
            let subj = self.shape_ref(st.shape);
            self.target(&subj, st.shape, &st.selector);
        }
        while let Some(id) = self.queue.pop() {
            self.emit_named(id);
        }
    }

    fn target(&mut self, subj: &NamedOrBlankNode, shape: ShapeId, sel: &Selector) {
        match sel {
            Selector::HasOut(p) => {
                self.add(subj, sh("targetSubjectsOf"), Term::NamedNode(p.clone()))
            }
            Selector::HasIn(p) => self.add(subj, sh("targetObjectsOf"), Term::NamedNode(p.clone())),
            Selector::IsConst(t) => self.add(subj, sh("targetNode"), t.clone()),
            Selector::HasPath(p, q) if is_class_path(p) => {
                match self.schema().arena.get(*q).clone() {
                    Shape::TestConst(c) => self.add(subj, sh("targetClass"), c),
                    _ => self.errors.push(
                        "a class target whose class is not a constant cannot be raised".into(),
                    ),
                }
            }
            Selector::HasPath(Path::Id, t) => {
                if self.opts.core {
                    self.errors.push(format!(
                        "`{}` has a shape-based target (`for every node matching ...`), which SHACL Core cannot express; export without --core to use SHACL-SPARQL",
                        self.display(shape)
                    ));
                    return;
                }
                let w = self.bnode();
                self.add(&w, rdf("type"), Term::NamedNode(sh("NodeShape")));
                let tgt = self.bnode();
                self.add(&w, sh("target"), Term::from(tgt.clone()));
                self.add(&tgt, rdf("type"), Term::NamedNode(sh("SPARQLTarget")));
                self.add(&tgt, sh("select"), str_lit(UNIVERSAL_TARGET));
                let t_ref = Term::from(self.shape_ref(*t));
                let neg = self.bnode();
                self.add(&neg, sh("not"), t_ref.clone());
                let s_ref = Term::from(subj.clone());
                let l = self.list(vec![Term::from(neg), s_ref.clone()]);
                self.add(&w, sh("or"), l);
                self.add(&w, ss("targetShape"), t_ref);
                self.add(&w, ss("targetFor"), s_ref);
            }
            Selector::HasPath(..) => self
                .errors
                .push("a path-based target other than a class target cannot be raised".into()),
            Selector::Sparql(q) => {
                if self.opts.core {
                    self.errors.push(format!(
                        "`{}` has a SPARQL target, which SHACL Core cannot express; export without --core",
                        self.display(shape)
                    ));
                    return;
                }
                let tgt = self.bnode();
                self.add(subj, sh("target"), Term::from(tgt.clone()));
                self.add(&tgt, rdf("type"), Term::NamedNode(sh("SPARQLTarget")));
                self.add(&tgt, sh("select"), str_lit(&q.query));
            }
        }
    }

    fn display(&self, id: ShapeId) -> String {
        self.b
            .display
            .get(&id)
            .cloned()
            .or_else(|| self.named(id).map(str::to_string))
            .unwrap_or_else(|| format!("@{}", id.0))
    }

    /// The RDF node that stands for `id` wherever it is referenced.
    fn shape_ref(&mut self, id: ShapeId) -> NamedOrBlankNode {
        if let Some(r) = self.refs.get(&id) {
            return r.clone();
        }
        if let Some(name) = self.named(id) {
            let r = NamedOrBlankNode::NamedNode(nn(name));
            self.refs.insert(id, r.clone());
            if !self.emitted.contains(&id) && !self.b.imported_rdf.contains(&id) {
                self.queue.push(id);
            }
            return r;
        }
        let r = self.bnode();
        self.refs.insert(id, r.clone());
        self.emit_node_shape(&r, id, true);
        r
    }

    fn emit_named(&mut self, id: ShapeId) {
        if !self.emitted.insert(id) {
            return;
        }
        let subj = NamedOrBlankNode::NamedNode(nn(self.named(id).unwrap()));
        self.refs.insert(id, subj.clone());
        let (sev, msgs, body) = match self.schema().arena.get(id).clone() {
            Shape::Annotated {
                severity,
                messages,
                shape,
            } => (severity, messages.to_vec(), shape),
            _ => (Severity::Violation, Vec::new(), id),
        };
        let doc = self.schema().docs.get(&id).cloned();
        if let Some((path, members)) = self.property_group(body) {
            self.add(&subj, rdf("type"), Term::NamedNode(sh("PropertyShape")));
            self.emit_property_body(&subj, &path, &members);
            if let Some(d) = doc {
                self.add(&subj, sh("description"), str_lit(&d));
            }
        } else {
            self.add(&subj, rdf("type"), Term::NamedNode(sh("NodeShape")));
            let mut st = NodeState {
                node_shape: true,
                ..Default::default()
            };
            self.emit_vc(&subj, body, &mut st);
            self.finish_node(&subj, st);
            if let Some(d) = doc {
                self.add(&subj, nn(&format!("{RDFS}comment")), str_lit(&d));
            }
        }
        self.annotations(&subj, &sev, &msgs);
        if let Some(e) = self.b.expansions.iter().find(|e| e.slot == id) {
            let text = format!("{}({})", e.template, e.args.join(", "));
            self.add(&subj, ss("expands"), str_lit(&text));
        }
        if let Some(base) = self.b.defined_in.get(&id).cloned() {
            self.add(&subj, ss("definedIn"), Term::NamedNode(nn(&base)));
        }
    }

    fn annotations(&mut self, subj: &NamedOrBlankNode, sev: &Severity, msgs: &[Term]) {
        if *sev != Severity::Violation {
            self.add(subj, sh("severity"), Term::NamedNode(nn(sev.as_str())));
        }
        for m in msgs {
            self.add(subj, sh("message"), m.clone());
        }
    }

    /// Emit `id` as a (blank or named) node shape on `subj`.
    fn emit_node_shape(&mut self, subj: &NamedOrBlankNode, id: ShapeId, _nested: bool) {
        let mut st = NodeState {
            node_shape: true,
            ..Default::default()
        };
        match self.schema().arena.get(id).clone() {
            Shape::Annotated {
                severity,
                messages,
                shape,
            } => {
                self.annotations(subj, &severity, &messages);
                self.emit_vc(subj, shape, &mut st);
            }
            _ => self.emit_vc(subj, id, &mut st),
        }
        if let Some(d) = self.schema().docs.get(&id).cloned() {
            self.add(subj, nn(&format!("{RDFS}comment")), str_lit(&d));
        }
        self.finish_node(subj, st);
    }

    fn finish_node(&mut self, subj: &NamedOrBlankNode, st: NodeState) {
        if let Some(q) = st.closed {
            if st.node_shape && st.direct_props.is_subset(&q) {
                self.add(subj, sh("closed"), bool_lit(true));
                let ignored: Vec<Term> = q
                    .difference(&st.direct_props)
                    .map(|p| Term::NamedNode(p.clone()))
                    .collect();
                if !ignored.is_empty() {
                    let l = self.list(ignored);
                    self.add(subj, sh("ignoredProperties"), l);
                }
            } else {
                let inner = self.bnode();
                self.add(&inner, sh("closed"), bool_lit(true));
                let all: Vec<Term> = q.iter().map(|p| Term::NamedNode(p.clone())).collect();
                if !all.is_empty() {
                    let l = self.list(all);
                    self.add(&inner, sh("ignoredProperties"), l);
                }
                self.add(subj, sh("node"), Term::from(inner));
            }
        }
    }

    fn is_default_annotation(&self, id: ShapeId) -> Option<ShapeId> {
        match self.schema().arena.get(id) {
            Shape::Annotated {
                severity: Severity::Violation,
                messages,
                shape,
            } if messages.is_empty()
                && self.named(id).is_none()
                && !self.schema().docs.contains_key(&id) =>
            {
                Some(*shape)
            }
            _ => None,
        }
    }

    /// If `id` (through transparent wrappers) is one or more constraints on a
    /// single path that fit one property shape, the path and its members.
    fn property_group(&self, id: ShapeId) -> Option<(Path, Vec<ShapeId>)> {
        let id = self.is_default_annotation(id).unwrap_or(id);
        let members = match self.schema().arena.get(id) {
            Shape::And(cs) => cs.clone(),
            _ => vec![id],
        };
        let mut path: Option<Path> = None;
        let mut g = Group::default();
        for m in &members {
            let p = self.path_of(*m)?;
            if path.as_ref().is_some_and(|q| *q != p) {
                return None;
            }
            if !self.fits(&mut g, *m) {
                return None;
            }
            path = Some(p);
        }
        path.map(|p| (p, members))
    }

    /// The path of a path-level constraint (not class membership, not `id`).
    fn path_of(&self, id: ShapeId) -> Option<Path> {
        if self.named(id).is_some() {
            return None;
        }
        match self.schema().arena.get(id) {
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => {
                if *path == Path::Id || self.class_idiom(path, *min, *max, *qualifier).is_some() {
                    None
                } else {
                    Some(path.clone())
                }
            }
            Shape::Eq(p, _)
            | Shape::Disj(p, _)
            | Shape::Lt(p, _)
            | Shape::Le(p, _)
            | Shape::UniqueLang(p)
                if *p != Path::Id =>
            {
                Some(p.clone())
            }
            _ => None,
        }
    }

    fn class_idiom(
        &self,
        path: &Path,
        min: Option<u64>,
        max: Option<u64>,
        q: ShapeId,
    ) -> Option<Term> {
        if is_class_path(path)
            && min == Some(1)
            && max.is_none()
            && let Shape::TestConst(c) = self.schema().arena.get(q)
        {
            return Some(c.clone());
        }
        None
    }

    /// Try to add a path-level constraint to a group; false if it would
    /// collide with a single-valued SHACL property already used.
    fn fits(&self, g: &mut Group, id: ShapeId) -> bool {
        match self.schema().arena.get(id).clone() {
            Shape::Count {
                min,
                max,
                qualifier,
                ..
            } => {
                let q = self.schema().arena.get(qualifier).clone();
                if matches!(q, Shape::Top) {
                    if g.card.is_some() {
                        return false;
                    }
                    g.card = Some((min, max));
                } else if min.is_none() && max == Some(0) {
                    g.forall.push(qualifier);
                } else if min == Some(1)
                    && max.is_none()
                    && let Shape::TestConst(c) = q
                {
                    g.has_value.push(c);
                } else {
                    if g.qualified.is_some() {
                        return false;
                    }
                    g.qualified = Some((min, max, qualifier));
                }
                true
            }
            Shape::Eq(_, p) => {
                g.pairs.push(("equals", p));
                true
            }
            Shape::Disj(_, p) => {
                g.pairs.push(("disjoint", p));
                true
            }
            Shape::Lt(_, p) => {
                g.pairs.push(("lessThan", p));
                true
            }
            Shape::Le(_, p) => {
                g.pairs.push(("lessThanOrEquals", p));
                true
            }
            Shape::UniqueLang(_) => {
                g.unique_lang = true;
                true
            }
            _ => false,
        }
    }

    /// Emit a property shape's body (path and constraints) on `ps`.
    fn emit_property_body(&mut self, ps: &NamedOrBlankNode, path: &Path, members: &[ShapeId]) {
        let p = self.path_term(path);
        self.add(ps, sh("path"), p);
        let mut g = Group::default();
        for m in members {
            let ok = self.fits(&mut g, *m);
            debug_assert!(ok);
        }
        if let Some((min, max)) = g.card {
            if let Some(n) = min.filter(|n| *n > 0) {
                self.add(ps, sh("minCount"), int_lit(n));
            }
            if let Some(n) = max {
                self.add(ps, sh("maxCount"), int_lit(n));
            }
        }
        let mut st = NodeState::default();
        for q in g.forall {
            match self.schema().arena.get(q).clone() {
                Shape::Not(inner) => self.emit_vc(ps, inner, &mut st),
                _ => {
                    let r = Term::from(self.shape_ref(q));
                    self.add(ps, sh("not"), r);
                }
            }
        }
        self.finish_node(ps, st);
        for v in g.has_value {
            self.add(ps, sh("hasValue"), v);
        }
        if let Some((min, max, q)) = g.qualified {
            let r = Term::from(self.shape_ref(q));
            self.add(ps, sh("qualifiedValueShape"), r);
            if let Some(n) = min {
                self.add(ps, sh("qualifiedMinCount"), int_lit(n));
            }
            if let Some(n) = max {
                self.add(ps, sh("qualifiedMaxCount"), int_lit(n));
            }
        }
        for (k, p) in g.pairs {
            self.add(ps, sh(k), Term::NamedNode(p));
        }
        if g.unique_lang {
            self.add(ps, sh("uniqueLang"), bool_lit(true));
        }
    }

    fn path_term(&mut self, p: &Path) -> Term {
        match p {
            Path::Pred(n) => Term::NamedNode(n.clone()),
            Path::Inverse(i) => {
                let b = self.bnode();
                let inner = self.path_term(i);
                self.add(&b, sh("inversePath"), inner);
                Term::from(b)
            }
            Path::Seq(ps) => {
                let items: Vec<Term> = ps.iter().map(|q| self.path_term(q)).collect();
                self.list(items)
            }
            Path::Alt(ps) => {
                if ps.len() == 2 && ps[1] == Path::Id {
                    let b = self.bnode();
                    let inner = self.path_term(&ps[0]);
                    self.add(&b, sh("zeroOrOnePath"), inner);
                    return Term::from(b);
                }
                let items: Vec<Term> = ps.iter().map(|q| self.path_term(q)).collect();
                let b = self.bnode();
                let l = self.list(items);
                self.add(&b, sh("alternativePath"), l);
                Term::from(b)
            }
            Path::Star(i) => {
                let b = self.bnode();
                let inner = self.path_term(i);
                self.add(&b, sh("zeroOrMorePath"), inner);
                Term::from(b)
            }
            Path::Id => {
                self.errors.push(
                    "`self` can only be used where SHACL allows the focus node itself".into(),
                );
                Term::NamedNode(rdf("nil"))
            }
        }
    }

    /// Reference a nested shape from `subj` with `sh:node` (or
    /// `sh:property` when it is a named property shape).
    fn reference(&mut self, subj: &NamedOrBlankNode, id: ShapeId) {
        let named_property = self.named(id).is_some() && {
            let body = match self.schema().arena.get(id) {
                Shape::Annotated { shape, .. } => *shape,
                _ => id,
            };
            self.property_group(body).is_some()
        };
        let r = Term::from(self.shape_ref(id));
        self.add(
            subj,
            sh(if named_property { "property" } else { "node" }),
            r,
        );
    }

    fn single(
        &mut self,
        subj: &NamedOrBlankNode,
        st: &mut NodeState,
        key: &'static str,
        id: ShapeId,
    ) -> bool {
        if st.used.insert(key) {
            true
        } else {
            // Collides with a single-valued property already on this node.
            let inner = self.bnode();
            self.emit_node_shape(&inner, id, true);
            self.add(subj, sh("node"), Term::from(inner));
            false
        }
    }

    /// Emit `id` as value constraints on `subj`: after this, `subj`'s focus
    /// (node shape) or each value (property shape) must satisfy `id`.
    fn emit_vc(&mut self, subj: &NamedOrBlankNode, id: ShapeId, st: &mut NodeState) {
        if self.named(id).is_some() {
            self.reference(subj, id);
            return;
        }
        if let Some(inner) = self.is_default_annotation(id) {
            self.emit_vc(subj, inner, st);
            return;
        }
        let shape = self.schema().arena.get(id).clone();
        match shape {
            Shape::Top => {}
            Shape::Pending => self
                .errors
                .push("internal: an unfilled slot reached the raiser".into()),
            Shape::Annotated {
                severity,
                messages,
                shape,
            } => {
                // An annotated line: a property shape when it is one, else a
                // nested node shape carrying the annotation.
                if let Some((path, members)) = self.property_group(shape) {
                    let ps = self.bnode();
                    self.emit_property_body(&ps, &path, &members);
                    self.annotations(&ps, &severity, &messages);
                    if let Some(d) = self.schema().docs.get(&id).cloned() {
                        self.add(&ps, sh("description"), str_lit(&d));
                    }
                    if let Path::Pred(p) = &path {
                        st.direct_props.insert(p.clone());
                    }
                    self.add(subj, sh("property"), Term::from(ps));
                } else {
                    let inner = self.bnode();
                    self.refs.insert(id, inner.clone());
                    self.emit_node_shape(&inner, id, true);
                    self.add(subj, sh("node"), Term::from(inner));
                }
            }
            Shape::And(cs) => {
                // Group path-level members by path into property shapes.
                let mut groups: Vec<(Path, Vec<ShapeId>, Group)> = Vec::new();
                let mut rest = Vec::new();
                for c in cs {
                    let c = self.is_default_annotation(c).unwrap_or(c);
                    if let Some(p) = self.path_of(c) {
                        let mut placed = false;
                        for (gp, members, g) in groups.iter_mut() {
                            if *gp == p && self.fits(g, c) {
                                members.push(c);
                                placed = true;
                                break;
                            }
                        }
                        if !placed {
                            let mut g = Group::default();
                            self.fits(&mut g, c);
                            groups.push((p, vec![c], g));
                        }
                    } else {
                        rest.push(c);
                    }
                }
                for (p, members, _) in groups {
                    let ps = self.bnode();
                    self.emit_property_body(&ps, &p, &members);
                    if let Path::Pred(pp) = &p {
                        st.direct_props.insert(pp.clone());
                    }
                    self.add(subj, sh("property"), Term::from(ps));
                }
                for c in rest {
                    self.emit_vc(subj, c, st);
                }
            }
            Shape::Count {
                ref path,
                min,
                max,
                qualifier,
            } => {
                if let Some(c) = self.class_idiom(path, min, max, qualifier) {
                    self.add(subj, sh("class"), c);
                } else if *path == Path::Id {
                    match (min, max, self.schema().arena.get(qualifier).clone()) {
                        (Some(1), None, _) => self.emit_vc(subj, qualifier, st),
                        (None, Some(0), Shape::Not(inner)) => self.emit_vc(subj, inner, st),
                        _ => self.errors.push(
                            "a count over `self` other than `required` or `each` cannot be raised"
                                .into(),
                        ),
                    }
                } else {
                    let ps = self.bnode();
                    self.emit_property_body(&ps, path, &[id]);
                    if let Path::Pred(p) = path {
                        st.direct_props.insert(p.clone());
                    }
                    self.add(subj, sh("property"), Term::from(ps));
                }
            }
            Shape::Eq(..)
            | Shape::Disj(..)
            | Shape::Lt(..)
            | Shape::Le(..)
            | Shape::UniqueLang(_) => match self.path_of(id) {
                Some(p) => {
                    let ps = self.bnode();
                    self.emit_property_body(&ps, &p, &[id]);
                    self.add(subj, sh("property"), Term::from(ps));
                }
                None => self
                    .errors
                    .push("a property comparison on `self` cannot be raised to SHACL".into()),
            },
            Shape::Or(cs) => {
                let frame = self
                    .schema()
                    .origins_of(id)
                    .iter()
                    .find_map(|o| match &o.kind {
                        OriginKind::Frame { frame, branches } => Some((*frame, branches.clone())),
                        _ => None,
                    });
                match frame {
                    Some((BooleanFrame::When, br)) if br.len() == 2 => {
                        let neg = self.bnode();
                        let c = Term::from(self.shape_ref(br[0]));
                        self.add(&neg, sh("not"), c);
                        let body = Term::from(self.shape_ref(br[1]));
                        let l = self.list(vec![Term::from(neg), body]);
                        self.add(subj, sh("or"), l);
                    }
                    Some((BooleanFrame::ExactlyOneOf, br)) => {
                        let items: Vec<Term> =
                            br.iter().map(|b| Term::from(self.shape_ref(*b))).collect();
                        let l = self.list(items);
                        self.add(subj, sh("xone"), l);
                    }
                    _ => {
                        let consts: Option<Vec<Term>> = cs
                            .iter()
                            .map(|c| match self.schema().arena.get(*c) {
                                Shape::TestConst(t) if self.named(*c).is_none() => Some(t.clone()),
                                _ => None,
                            })
                            .collect();
                        if let Some(consts) = consts {
                            let l = self.list(consts);
                            self.add(subj, sh("in"), l);
                        } else {
                            let items: Vec<Term> =
                                cs.iter().map(|c| Term::from(self.shape_ref(*c))).collect();
                            let l = self.list(items);
                            self.add(subj, sh("or"), l);
                        }
                    }
                }
            }
            Shape::Not(c) => {
                let frame = self
                    .schema()
                    .origins_of(id)
                    .iter()
                    .find_map(|o| match &o.kind {
                        OriginKind::Frame {
                            frame: BooleanFrame::NoneOf,
                            branches,
                        } => Some(branches.clone()),
                        _ => None,
                    });
                match frame {
                    Some(br) if br.len() > 1 => {
                        let inner = self.bnode();
                        let items: Vec<Term> =
                            br.iter().map(|b| Term::from(self.shape_ref(*b))).collect();
                        let l = self.list(items);
                        self.add(&inner, sh("or"), l);
                        self.add(subj, sh("not"), Term::from(inner));
                    }
                    _ => {
                        let r = Term::from(self.shape_ref(c));
                        self.add(subj, sh("not"), r);
                    }
                }
            }
            Shape::TestConst(t) => {
                let l = self.list(vec![t]);
                self.add(subj, sh("in"), l);
            }
            Shape::TestKind(k) => {
                let iri = if k == NodeKindSet::IRI {
                    "IRI"
                } else if k == NodeKindSet::LITERAL {
                    "Literal"
                } else if k == NodeKindSet::BLANK_NODE {
                    "BlankNode"
                } else if k == NodeKindSet::BLANK_NODE_OR_IRI {
                    "BlankNodeOrIRI"
                } else if k == NodeKindSet::BLANK_NODE_OR_LITERAL {
                    "BlankNodeOrLiteral"
                } else if k == NodeKindSet::IRI_OR_LITERAL {
                    "IRIOrLiteral"
                } else if k.is_empty() {
                    let t = self.bnode();
                    self.add(subj, sh("not"), Term::from(t));
                    return;
                } else {
                    return;
                };
                if self.single(subj, st, "nodeKind", id) {
                    self.add(subj, sh("nodeKind"), Term::NamedNode(sh(iri)));
                }
            }
            Shape::TestType(vt) => self.value_type(subj, st, id, &vt),
            Shape::Closed(q) => {
                if st.closed.is_some() || !st.node_shape {
                    let inner = self.bnode();
                    let mut ist = NodeState {
                        node_shape: true,
                        closed: Some(q),
                        ..Default::default()
                    };
                    ist.used.insert("closed");
                    self.finish_node(&inner, ist);
                    self.add(subj, sh("node"), Term::from(inner));
                } else {
                    st.closed = Some(q);
                }
            }
            Shape::Sparql(c) => {
                let b = self.bnode();
                let key = match c.kind {
                    shifty_algebra::SparqlQueryKind::Ask => "ask",
                    shifty_algebra::SparqlQueryKind::Select => "select",
                };
                self.add(&b, sh(key), str_lit(&c.query));
                self.add(subj, sh("sparql"), Term::from(b));
            }
            Shape::Expression(_) => self
                .errors
                .push("SHACL-AF node expressions cannot be raised from Shapeshift".into()),
        }
    }

    fn value_type(
        &mut self,
        subj: &NamedOrBlankNode,
        st: &mut NodeState,
        id: ShapeId,
        vt: &ValueType,
    ) {
        match vt {
            ValueType::Any => {}
            ValueType::And(parts) => {
                for p in parts {
                    self.value_type(subj, st, id, p);
                }
            }
            ValueType::Datatype(d) => {
                if self.single(subj, st, "datatype", id) {
                    self.add(subj, sh("datatype"), Term::NamedNode(d.clone()));
                }
            }
            ValueType::NumericRange { lo, hi } => {
                if self.single(subj, st, "range", id) {
                    if let Some(b) = lo {
                        let k = if b.inclusive {
                            "minInclusive"
                        } else {
                            "minExclusive"
                        };
                        self.add(subj, sh(k), Term::Literal(b.value.clone()));
                    }
                    if let Some(b) = hi {
                        let k = if b.inclusive {
                            "maxInclusive"
                        } else {
                            "maxExclusive"
                        };
                        self.add(subj, sh(k), Term::Literal(b.value.clone()));
                    }
                }
            }
            ValueType::Length { min, max } => {
                if self.single(subj, st, "length", id) {
                    if let Some(n) = min {
                        self.add(subj, sh("minLength"), int_lit(*n));
                    }
                    if let Some(n) = max {
                        self.add(subj, sh("maxLength"), int_lit(*n));
                    }
                }
            }
            ValueType::Pattern { regex, flags } => {
                if self.single(subj, st, "pattern", id) {
                    self.add(subj, sh("pattern"), str_lit(regex));
                    if !flags.is_empty() {
                        self.add(subj, sh("flags"), str_lit(flags));
                    }
                }
            }
            ValueType::LangIn(ls) => {
                let items: Vec<Term> = ls.iter().map(|l| str_lit(l)).collect();
                let l = self.list(items);
                self.add(subj, sh("languageIn"), l);
            }
        }
    }
}
