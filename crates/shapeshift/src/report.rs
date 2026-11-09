//! Validation reports in the author's terms.
//!
//! shifty's evidence identifies the algebra node that failed
//! ([`Reason::shape`]); the provenance sidecar maps that node back to the
//! Shapeshift line it came from, through every macro and include it passed
//! through. Hints are advisory: they never change conformance.

use oxrdf::{Graph, NamedNodeRef, Term, TermRef};
use serde::{Deserialize, Serialize};
use shapeshift_macro::BuildOutput;
use shapeshift_macro::build::class_path;
use shapeshift_macro::diagnostic::edit_distance;
use shapeshift_macro::vocab::split_iri;
use shifty_algebra::{OriginKind, Path, Schema, Shape, ShapeId, SourceLocation};
use shifty_engine::{Reason, ValidationOutcome};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Write;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    /// `include HasPointOf(brick:AHU)`, `when`, ...
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    /// Macros, includes, and frames between the shape and the failing line.
    pub via: Vec<Step>,
    /// The line the author wrote, or the algebra if it has no source line.
    pub line: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
    /// The value node the constraint failed at.
    pub value: String,
    pub message: String,
    pub severity: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultEntry {
    pub focus: String,
    pub shape: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<SourceLocation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    pub failures: Vec<Failure>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Report {
    pub conforms: bool,
    pub results: Vec<ResultEntry>,
}

/// Everything a report needs to look at besides the outcome.
pub struct ReportContext<'a> {
    pub built: &'a BuildOutput,
    pub data: &'a Graph,
    /// Imported ontologies and shapes graphs (the shapes view).
    pub shapes: &'a Graph,
}

impl ReportContext<'_> {
    fn schema(&self) -> &Schema {
        &self.built.schema
    }

    fn compact(&self, t: &Term) -> String {
        match t {
            Term::NamedNode(n) => {
                let c = self.schema().prefixes.compact(n.as_str());
                if c.starts_with("http") || c.starts_with("urn") {
                    format!("<{}>", n.as_str())
                } else {
                    c
                }
            }
            other => other.to_string(),
        }
    }

    fn shape_name(&self, id: ShapeId) -> String {
        self.built
            .display
            .get(&id)
            .cloned()
            .or_else(|| {
                self.schema()
                    .name_of(id)
                    .map(|n| self.schema().prefixes.compact(n))
            })
            .unwrap_or_else(|| format!("@{}", id.0))
    }

    fn location(&self, id: ShapeId) -> Option<SourceLocation> {
        self.schema()
            .origins_of(id)
            .iter()
            .find(|o| {
                matches!(
                    o.kind,
                    OriginKind::Shape { .. } | OriginKind::MacroCall { .. }
                )
            })
            .and_then(|o| o.location.clone())
    }

    /// Shortest path of slots from `root` to `target` in the arena.
    fn path_to(&self, root: ShapeId, target: ShapeId) -> Vec<ShapeId> {
        let arena = &self.schema().arena;
        let mut parent: HashMap<ShapeId, ShapeId> = HashMap::new();
        let mut seen = HashSet::from([root]);
        let mut q = VecDeque::from([root]);
        while let Some(id) = q.pop_front() {
            if id == target {
                let mut path = vec![id];
                let mut cur = id;
                while let Some(p) = parent.get(&cur) {
                    path.push(*p);
                    cur = *p;
                }
                path.reverse();
                return path;
            }
            for c in arena.get(id).child_shapes() {
                if seen.insert(c) {
                    parent.insert(c, id);
                    q.push_back(c);
                }
            }
        }
        vec![root, target]
    }

    fn failure(&self, root: ShapeId, r: &Reason) -> Failure {
        let path = self.path_to(root, r.shape);
        let mut via = Vec::new();
        let mut line = None;
        let mut location = None;
        let mut owner = self.shape_name(root);
        for (i, id) in path.iter().enumerate() {
            if i == 0 {
                continue;
            }
            for o in self.schema().origins_of(*id) {
                match &o.kind {
                    OriginKind::Include { from, into } if *into == owner => via.push(Step {
                        text: format!("include {from}"),
                        location: o.location.clone(),
                    }),
                    OriginKind::Frame { frame, .. } => via.push(Step {
                        text: frame.phrase(),
                        location: o.location.clone(),
                    }),
                    OriginKind::Field { text, .. } => {
                        line = Some(text.clone());
                        location = o.location.clone();
                    }
                    _ => {}
                }
            }
            if self.built.display.contains_key(id) && i + 1 < path.len() {
                owner = self.shape_name(*id);
            }
        }
        // `include` of a named field: the included slot is itself the field.
        let line = line.unwrap_or_else(|| {
            shifty_algebra::render::describe_shape_in(
                &self.schema().arena,
                r.shape,
                &self.schema().prefixes,
            )
        });
        let message = r
            .author_message
            .clone()
            .unwrap_or_else(|| r.message.clone());
        let hints = self.hints(r, &line);
        Failure {
            via,
            line,
            location,
            value: self.compact(&r.value),
            message,
            severity: r.severity.label().to_string(),
            hints,
        }
    }

    fn types_of(&self, node: &Term) -> Vec<Term> {
        let Some(s) = node_ref(node) else {
            return Vec::new();
        };
        let ty = NamedNodeRef::new_unchecked(RDF_TYPE);
        self.data
            .objects_for_subject_predicate(s, ty)
            .chain(self.shapes.objects_for_subject_predicate(s, ty))
            .map(TermRef::into_owned)
            .collect()
    }

    fn has_superclass(&self, class: &Term) -> bool {
        let Some(c) = node_ref(class) else {
            return false;
        };
        let sub = NamedNodeRef::new_unchecked(RDFS_SUBCLASS_OF);
        self.data
            .objects_for_subject_predicate(c, sub)
            .next()
            .is_some()
            || self
                .shapes
                .objects_for_subject_predicate(c, sub)
                .next()
                .is_some()
    }

    fn values(&self, focus: &Term, p: &Path) -> Vec<Term> {
        let (Some(s), Path::Pred(pred)) = (node_ref(focus), p) else {
            return Vec::new();
        };
        self.data
            .objects_for_subject_predicate(s, pred.as_ref())
            .map(TermRef::into_owned)
            .collect()
    }

    fn class_of(&self, id: ShapeId) -> Option<Term> {
        match self.schema().arena.get(id) {
            Shape::Count {
                path,
                min: Some(1),
                max: None,
                qualifier,
            } if *path == class_path() => match self.schema().arena.get(*qualifier) {
                Shape::TestConst(c) => Some(c.clone()),
                _ => None,
            },
            Shape::Annotated { shape, .. } => self.class_of(*shape),
            _ => None,
        }
    }

    fn is_instance(&self, node: &Term, class: &Term) -> bool {
        // Subclass closure over data and shapes, like the engine.
        let mut stack = self.types_of(node);
        let mut seen = HashSet::new();
        let sub = NamedNodeRef::new_unchecked(RDFS_SUBCLASS_OF);
        while let Some(c) = stack.pop() {
            if &c == class {
                return true;
            }
            if !seen.insert(c.clone()) {
                continue;
            }
            if let Some(cr) = node_ref(&c) {
                for g in [self.data, self.shapes] {
                    stack.extend(
                        g.objects_for_subject_predicate(cr, sub)
                            .map(TermRef::into_owned),
                    );
                }
            }
        }
        false
    }

    fn missing_ontology_hint(&self, values: &[Term], class: &Term) -> Option<String> {
        let unknown: Vec<String> = values
            .iter()
            .flat_map(|v| self.types_of(v))
            .filter(|t| !self.has_superclass(t) && t != class)
            .map(|t| self.compact(&t))
            .collect();
        if unknown.is_empty() {
            return None;
        }
        let mut u = unknown;
        u.sort();
        u.dedup();
        Some(format!(
            "the values' classes ({}) have no `rdfs:subClassOf` edges here, so subclass reasoning cannot see whether they are {}; the ontology that defines them may not be loaded (see `import`)",
            u.join(", "),
            self.compact(class)
        ))
    }

    fn hints(&self, r: &Reason, _line: &str) -> Vec<String> {
        let mut hints = Vec::new();
        let focus_like = &r.value;
        match &r.constraint {
            // `a X` directly on a value.
            Shape::Count {
                path,
                min: Some(1),
                max: None,
                qualifier,
            } if *path == class_path() => {
                if let Shape::TestConst(class) = self.schema().arena.get(*qualifier) {
                    hints.extend(
                        self.missing_ontology_hint(std::slice::from_ref(focus_like), class),
                    );
                }
            }
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => {
                let values = self.values(focus_like, path);
                let q = self.schema().arena.get(*qualifier).clone();
                // ∀ class: `each a X` failed.
                if min.is_none()
                    && *max == Some(0)
                    && let Shape::Not(inner) = q
                    && let Some(class) = self.class_of(inner)
                {
                    let some = values.iter().any(|v| self.is_instance(v, &class));
                    if some {
                        hints.push(format!(
                            "this line requires *all* values to be {c}; use `at least 1 is a {c}` if one is enough",
                            c = self.compact(&class)
                        ));
                    }
                    hints.extend(self.missing_ontology_hint(&values, &class));
                } else if let Some(class) = self.class_of(*qualifier) {
                    hints.extend(self.missing_ontology_hint(&values, &class));
                }
                // `required` with no values: a lexically similar predicate?
                if matches!(q, Shape::Top)
                    && min.is_some_and(|m| m > 0)
                    && values.is_empty()
                    && let Path::Pred(p) = path
                    && let Some(s) = node_ref(focus_like)
                {
                    let (_, want) = split_iri(p.as_str());
                    let mut best: Option<(usize, String)> = None;
                    for t in self.data.triples_for_subject(s) {
                        let have = t.predicate.as_str();
                        if have == p.as_str() {
                            continue;
                        }
                        let d = edit_distance(want, split_iri(have).1);
                        if d <= (want.len() / 3).max(2) && best.as_ref().is_none_or(|(b, _)| d < *b)
                        {
                            best = Some((d, have.to_string()));
                        }
                    }
                    if let Some((_, have)) = best {
                        hints.push(format!(
                            "did you mean `{}`? the data uses `{}`",
                            self.compact(&Term::NamedNode(p.clone())),
                            self.compact(&Term::NamedNode(oxrdf::NamedNode::new_unchecked(have)))
                        ));
                    }
                }
            }
            Shape::Closed(_)
                if r.message.contains("rdf-syntax-ns#type") || r.message.contains("rdf:type") =>
            {
                hints.push(
                    "add `rdf:type` to the shape (e.g. `rdf:type required`), or remove `only these properties`".into(),
                );
            }
            _ => {}
        }
        hints
    }
}

fn node_ref(t: &Term) -> Option<oxrdf::NamedOrBlankNodeRef<'_>> {
    match t {
        Term::NamedNode(n) => Some(n.as_ref().into()),
        Term::BlankNode(b) => Some(b.as_ref().into()),
        Term::Literal(_) => None,
    }
}

/// Flatten `sh:or` groups so each failed alternative is reported.
fn leaves(r: &Reason, out: &mut Vec<Reason>) {
    if r.sub_reasons.is_empty() {
        out.push(r.clone());
    } else {
        for s in &r.sub_reasons {
            leaves(s, out);
        }
    }
}

/// Build a report from a validation outcome.
pub fn report(ctx: &ReportContext<'_>, outcome: &ValidationOutcome) -> Report {
    let mut results = Vec::new();
    for v in &outcome.violations {
        let Some(st) = ctx.schema().statements.get(v.statement) else {
            continue;
        };
        let root = st.shape;
        let mut failures = Vec::new();
        for r in &v.reasons {
            let mut ls = Vec::new();
            if r.sub_reasons.is_empty() {
                ls.push(r.clone());
            } else {
                // An `any of` / `when` group: report it once, as the frame.
                let mut f = ctx.failure(root, r);
                let mut subs = Vec::new();
                leaves(r, &mut subs);
                f.message = format!(
                    "{} (none of the {} alternatives holds)",
                    f.message,
                    r.sub_reasons.len()
                );
                for s in subs {
                    let sf = ctx.failure(root, &s);
                    f.hints
                        .push(format!("alternative failed: {} — {}", sf.line, sf.message));
                }
                failures.push(f);
                continue;
            }
            for l in ls {
                failures.push(ctx.failure(root, &l));
            }
        }
        results.push(ResultEntry {
            focus: ctx.compact(&v.focus),
            shape: ctx.shape_name(root),
            location: ctx.location(root),
            doc: ctx.schema().docs.get(&root).cloned(),
            failures,
        });
    }
    Report {
        conforms: outcome.conforms,
        results,
    }
}

fn loc(l: &Option<SourceLocation>) -> String {
    match l {
        Some(l) => format!("  ({}:{})", short_file(&l.file), l.line),
        None => String::new(),
    }
}

fn short_file(f: &str) -> &str {
    std::path::Path::new(f)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(f)
}

impl Report {
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        if self.conforms && self.results.is_empty() {
            out.push_str("conforms: every node satisfies every rule\n");
            return out;
        }
        for r in &self.results {
            writeln!(out, "{} fails {}{}", r.focus, r.shape, loc(&r.location)).unwrap();
            if let Some(d) = &r.doc {
                writeln!(out, "  \"{d}\"").unwrap();
            }
            for f in &r.failures {
                for s in &f.via {
                    writeln!(out, "  via {}{}", s.text, loc(&s.location)).unwrap();
                }
                writeln!(out, "  {}{}", f.line, loc(&f.location)).unwrap();
                let at = if f.value != r.focus {
                    format!(" (at {})", f.value)
                } else {
                    String::new()
                };
                let sev = if f.severity != "Violation" {
                    format!("[{}] ", f.severity.to_lowercase())
                } else {
                    String::new()
                };
                writeln!(out, "    {sev}{}{at}", f.message).unwrap();
                for h in &f.hints {
                    writeln!(out, "    hint: {h}").unwrap();
                }
            }
            out.push('\n');
        }
        writeln!(
            out,
            "{}: {} node{} failed",
            if self.conforms {
                "conforms (below threshold)"
            } else {
                "does not conform"
            },
            self.results.len(),
            if self.results.len() == 1 { "" } else { "s" }
        )
        .unwrap();
        out
    }
}
