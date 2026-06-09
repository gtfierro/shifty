//! Lower a loaded shapes graph into the formalism [`Schema`].
//!
//! Every SHACL Core construct collapses into the small IR, applying the sugar
//! rules from the gap analysis (`class → path`, `minCount/maxCount → Count`,
//! per-value constraints wrapped in `∀π = ∃≤0 π.¬φ`, `xone → ∧∨¬`, …). Each
//! shape lowers to a **focus-node predicate** `φ`, so `sh:property`/`sh:node`
//! compose by conjunction. Unsupported AF constructs emit diagnostics.

use crate::diagnostics::{DiagLevel, Diagnostic};
use crate::graph::{term_to_node, Loaded};
use crate::path::parse_path;
use crate::vocab;
use oxrdf::{Literal, NamedOrBlankNode, Term};
use shacl_algebra::{
    Bound, NodeKindSet, Path, Schema, Selector, Shape, ShapeArena, ShapeId, Statement, ValueType,
};
use std::collections::{BTreeSet, HashMap, HashSet};

pub struct Lowered {
    pub schema: Schema,
    pub diagnostics: Vec<Diagnostic>,
}

/// Lower a loaded graph into a schema plus diagnostics.
pub fn lower(g: &Loaded) -> Lowered {
    let mut l = Lowerer {
        g,
        arena: ShapeArena::new(),
        cache: HashMap::new(),
        statements: Vec::new(),
        diags: Vec::new(),
    };
    let shapes = l.discover_shapes();
    for s in &shapes {
        l.lower_shape(s);
    }
    for s in &shapes {
        l.add_statements(s);
    }
    Lowered {
        schema: Schema {
            arena: l.arena,
            statements: l.statements,
            rules: Vec::new(),
        },
        diagnostics: l.diags,
    }
}

struct Lowerer<'a> {
    g: &'a Loaded,
    arena: ShapeArena,
    cache: HashMap<NamedOrBlankNode, ShapeId>,
    statements: Vec<Statement>,
    diags: Vec<Diagnostic>,
}

impl Lowerer<'_> {
    fn diag(&mut self, level: DiagLevel, msg: impl Into<String>, subj: &NamedOrBlankNode) {
        self.diags
            .push(Diagnostic::new(level, msg, Some(subj.to_string())));
    }

    /// Subjects that are declared shapes: typed NodeShape/PropertyShape, or
    /// carrying `sh:path` or a target predicate. Referenced-only shapes are
    /// pulled in on demand during lowering. Sorted for deterministic output.
    fn discover_shapes(&self) -> Vec<NamedOrBlankNode> {
        let mut found: HashSet<NamedOrBlankNode> = HashSet::new();
        for triple in self.g.graph.iter() {
            let p = triple.predicate;
            let is_target = p == vocab::SH_TARGET_NODE
                || p == vocab::SH_TARGET_CLASS
                || p == vocab::SH_TARGET_SUBJECTS_OF
                || p == vocab::SH_TARGET_OBJECTS_OF;
            if p == vocab::SH_PATH || is_target {
                found.insert(triple.subject.into_owned());
            }
            if p == vocab::RDF_TYPE
                && let Term::NamedNode(ty) = triple.object.into_owned()
                && (ty.as_ref() == vocab::SH_NODE_SHAPE || ty.as_ref() == vocab::SH_PROPERTY_SHAPE)
            {
                found.insert(triple.subject.into_owned());
            }
        }
        let mut shapes: Vec<NamedOrBlankNode> = found.into_iter().collect();
        shapes.sort_by_key(|n| n.to_string());
        shapes
    }

    fn lower_shape(&mut self, s: &NamedOrBlankNode) -> ShapeId {
        if let Some(id) = self.cache.get(s) {
            return *id;
        }
        let id = self.arena.reserve();
        self.cache.insert(s.clone(), id);

        if self.bool_prop(s, vocab::SH_DEACTIVATED) {
            self.arena.set(id, Shape::Top);
            return id;
        }

        let path = self.parse_shape_path(s);
        let mut conjuncts: Vec<ShapeId> = Vec::new();

        // Value-scoped constraints: each applies to every value node along the
        // path (or to the focus node directly when there is no path).
        let value = self.collect_value_constraints(s);
        if !value.is_empty() {
            let value_phi = self.arena.and(value);
            match &path {
                Some(p) => {
                    // ∀π.φ  ≡  ∃≤0 π.¬φ
                    let neg = self.arena.not(value_phi);
                    let c = self.arena.count(p.clone(), None, Some(0), neg);
                    conjuncts.push(c);
                }
                None => conjuncts.push(value_phi),
            }
        }

        self.collect_path_constraints(s, path.as_ref(), &mut conjuncts);

        // Nested property shapes compose by conjunction (same focus node).
        for prop in self.g.objects(s, vocab::SH_PROPERTY) {
            if let Some(pn) = term_to_node(&prop) {
                let pid = self.lower_shape(&pn);
                conjuncts.push(pid);
            }
        }

        if self.bool_prop(s, vocab::SH_CLOSED) {
            let q = self.closed_allowed(s);
            let c = self.arena.insert(Shape::Closed(q));
            conjuncts.push(c);
        }

        if self.g.object(s, vocab::SH_SPARQL).is_some() {
            self.diag(DiagLevel::Unsupported, "sh:sparql constraint not yet lowered", s);
        }
        if self.g.object(s, vocab::SH_RULE).is_some() {
            self.diag(DiagLevel::Unsupported, "sh:rule not yet lowered (Layer 6)", s);
        }

        let shape = if conjuncts.is_empty() {
            Shape::Top
        } else if conjuncts.len() == 1 {
            if conjuncts[0] == id {
                Shape::Top
            } else if matches!(self.arena.get(conjuncts[0]), Shape::Pending) {
                // back-reference to an ancestor still being built: keep the ref
                Shape::And(vec![conjuncts[0]])
            } else {
                self.arena.get(conjuncts[0]).clone()
            }
        } else {
            Shape::And(conjuncts)
        };
        self.arena.set(id, shape);
        id
    }

    fn collect_value_constraints(&mut self, s: &NamedOrBlankNode) -> Vec<ShapeId> {
        let mut value: Vec<ShapeId> = Vec::new();

        // sh:class C  ≡  ∃≥1 (rdf:type/rdfs:subClassOf*) . test(C)
        for c in self.g.objects(s, vocab::SH_CLASS) {
            let tn = self.arena.insert(Shape::TestConst(c));
            let cc = self.arena.count(class_path(), Some(1), None, tn);
            value.push(cc);
        }

        // sh:datatype
        for d in self.g.objects(s, vocab::SH_DATATYPE) {
            if let Term::NamedNode(n) = d {
                let id = self.arena.insert(Shape::TestType(ValueType::Datatype(n)));
                value.push(id);
            }
        }

        // sh:nodeKind
        for k in self.g.objects(s, vocab::SH_NODE_KIND) {
            if let Some(set) = map_node_kind(&k) {
                let id = self.arena.insert(Shape::TestKind(set));
                value.push(id);
            } else {
                self.diag(DiagLevel::Warning, "unrecognized sh:nodeKind value", s);
            }
        }

        // numeric range (combine the four bounds into one facet)
        let lo = self
            .lit(s, vocab::SH_MIN_INCLUSIVE)
            .map(|value| Bound { value, inclusive: true })
            .or_else(|| {
                self.lit(s, vocab::SH_MIN_EXCLUSIVE)
                    .map(|value| Bound { value, inclusive: false })
            });
        let hi = self
            .lit(s, vocab::SH_MAX_INCLUSIVE)
            .map(|value| Bound { value, inclusive: true })
            .or_else(|| {
                self.lit(s, vocab::SH_MAX_EXCLUSIVE)
                    .map(|value| Bound { value, inclusive: false })
            });
        if lo.is_some() || hi.is_some() {
            let id = self
                .arena
                .insert(Shape::TestType(ValueType::NumericRange { lo, hi }));
            value.push(id);
        }

        // length
        let min_len = self.int(s, vocab::SH_MIN_LENGTH);
        let max_len = self.int(s, vocab::SH_MAX_LENGTH);
        if min_len.is_some() || max_len.is_some() {
            let id = self.arena.insert(Shape::TestType(ValueType::Length {
                min: min_len,
                max: max_len,
            }));
            value.push(id);
        }

        // pattern (+ flags)
        let flags = self
            .lit(s, vocab::SH_FLAGS)
            .map(|l| l.value().to_string())
            .unwrap_or_default();
        for pat in self.g.objects(s, vocab::SH_PATTERN) {
            if let Term::Literal(l) = pat {
                let id = self.arena.insert(Shape::TestType(ValueType::Pattern {
                    regex: l.value().to_string(),
                    flags: flags.clone(),
                }));
                value.push(id);
            }
        }

        // sh:languageIn
        for li in self.g.objects(s, vocab::SH_LANGUAGE_IN) {
            let langs: Vec<String> = self
                .g
                .read_list(&li)
                .into_iter()
                .filter_map(|m| match m {
                    Term::Literal(l) => Some(l.value().to_string()),
                    _ => None,
                })
                .collect();
            let id = self.arena.insert(Shape::TestType(ValueType::LangIn(langs)));
            value.push(id);
        }

        // sh:in  ≡  ⋁ test(member)
        for inl in self.g.objects(s, vocab::SH_IN) {
            let alts: Vec<ShapeId> = self
                .g
                .read_list(&inl)
                .into_iter()
                .map(|m| self.arena.insert(Shape::TestConst(m)))
                .collect();
            let or = self.arena.or(alts);
            value.push(or);
        }

        // sh:node — value must conform to the referenced shape
        for n in self.g.objects(s, vocab::SH_NODE) {
            if let Some(nn) = term_to_node(&n) {
                let id = self.lower_shape(&nn);
                value.push(id);
            }
        }

        // sh:not
        for n in self.g.objects(s, vocab::SH_NOT) {
            if let Some(nn) = term_to_node(&n) {
                let id = self.lower_shape(&nn);
                let neg = self.arena.not(id);
                value.push(neg);
            }
        }

        // sh:and / sh:or / sh:xone (each object is an rdf:list of shapes)
        for l in self.g.objects(s, vocab::SH_AND) {
            let ids = self.lower_shape_list(&l);
            let a = self.arena.and(ids);
            value.push(a);
        }
        for l in self.g.objects(s, vocab::SH_OR) {
            let ids = self.lower_shape_list(&l);
            let o = self.arena.or(ids);
            value.push(o);
        }
        for l in self.g.objects(s, vocab::SH_XONE) {
            let ids = self.lower_shape_list(&l);
            let x = self.arena.xone(ids);
            value.push(x);
        }

        value
    }

    /// Path-level constraints (cardinality, qualified counts, property pairs,
    /// hasValue, uniqueLang). Most require a path; without one they are ignored
    /// with a diagnostic, except `sh:hasValue` which applies to the focus node.
    fn collect_path_constraints(
        &mut self,
        s: &NamedOrBlankNode,
        path: Option<&Path>,
        conjuncts: &mut Vec<ShapeId>,
    ) {
        let need_path = |me: &mut Self, what: &str| {
            me.diag(DiagLevel::Warning, format!("{what} ignored: no sh:path"), s);
        };

        let min_count = self.int(s, vocab::SH_MIN_COUNT);
        let max_count = self.int(s, vocab::SH_MAX_COUNT);
        if min_count.is_some() || max_count.is_some() {
            match path {
                Some(p) => {
                    let top = self.arena.top();
                    let c = self.arena.count(p.clone(), min_count, max_count, top);
                    conjuncts.push(c);
                }
                None => need_path(self, "sh:minCount/sh:maxCount"),
            }
        }

        // sh:hasValue
        for v in self.g.objects(s, vocab::SH_HAS_VALUE) {
            match path {
                Some(p) => {
                    let tc = self.arena.insert(Shape::TestConst(v));
                    let c = self.arena.count(p.clone(), Some(1), None, tc);
                    conjuncts.push(c);
                }
                None => {
                    let tc = self.arena.insert(Shape::TestConst(v));
                    conjuncts.push(tc);
                }
            }
        }

        // sh:qualifiedValueShape + qualifiedMin/MaxCount
        for q in self.g.objects(s, vocab::SH_QUALIFIED_VALUE_SHAPE) {
            if let Some(qn) = term_to_node(&q) {
                let qmin = self.int(s, vocab::SH_QUALIFIED_MIN_COUNT);
                let qmax = self.int(s, vocab::SH_QUALIFIED_MAX_COUNT);
                match path {
                    Some(p) => {
                        let phi = self.lower_shape(&qn);
                        let c = self.arena.count(p.clone(), qmin, qmax, phi);
                        conjuncts.push(c);
                    }
                    None => need_path(self, "sh:qualifiedValueShape"),
                }
            }
        }

        // property-pair constraints
        let pairs = [
            (vocab::SH_EQUALS, "equals"),
            (vocab::SH_DISJOINT, "disjoint"),
            (vocab::SH_LESS_THAN, "lessThan"),
            (vocab::SH_LESS_THAN_OR_EQUALS, "lessThanOrEquals"),
        ];
        for (pred, name) in pairs {
            for other in self.g.objects(s, pred) {
                let Term::NamedNode(op) = other else { continue };
                match path {
                    Some(p) => {
                        let shape = match name {
                            "equals" => Shape::Eq(p.clone(), op),
                            "disjoint" => Shape::Disj(p.clone(), op),
                            "lessThan" => Shape::Lt(p.clone(), op),
                            _ => Shape::Le(p.clone(), op),
                        };
                        let c = self.arena.insert(shape);
                        conjuncts.push(c);
                    }
                    None => need_path(self, &format!("sh:{name}")),
                }
            }
        }

        // sh:uniqueLang
        if self.bool_prop(s, vocab::SH_UNIQUE_LANG) {
            match path {
                Some(p) => {
                    let c = self.arena.insert(Shape::UniqueLang(p.clone()));
                    conjuncts.push(c);
                }
                None => need_path(self, "sh:uniqueLang"),
            }
        }
    }

    fn add_statements(&mut self, s: &NamedOrBlankNode) {
        let Some(&shape) = self.cache.get(s) else { return };

        for c in self.g.objects(s, vocab::SH_TARGET_NODE) {
            self.statements.push(Statement {
                selector: Selector::IsConst(c),
                shape,
            });
        }
        for c in self.g.objects(s, vocab::SH_TARGET_CLASS) {
            let sel = self.class_selector(c);
            self.statements.push(Statement { selector: sel, shape });
        }
        for p in self.g.objects(s, vocab::SH_TARGET_SUBJECTS_OF) {
            if let Term::NamedNode(n) = p {
                self.statements.push(Statement {
                    selector: Selector::HasOut(n),
                    shape,
                });
            }
        }
        for p in self.g.objects(s, vocab::SH_TARGET_OBJECTS_OF) {
            if let Term::NamedNode(n) = p {
                self.statements.push(Statement {
                    selector: Selector::HasIn(n),
                    shape,
                });
            }
        }

        // implicit class target: a shape that is also an rdfs:Class / owl:Class
        if (self.g.has_type(s, vocab::RDFS_CLASS) || self.g.has_type(s, vocab::OWL_CLASS))
            && let NamedOrBlankNode::NamedNode(n) = s
        {
            let sel = self.class_selector(Term::NamedNode(n.clone()));
            self.statements.push(Statement { selector: sel, shape });
        }

        if self.g.object(s, vocab::SH_TARGET).is_some() {
            self.diag(
                DiagLevel::Unsupported,
                "sh:target (SPARQL/custom target) not yet lowered",
                s,
            );
        }
    }

    /// `∃≥1 (rdf:type/rdfs:subClassOf*) . test(class)` as a selector.
    fn class_selector(&mut self, class: Term) -> Selector {
        let tn = self.arena.insert(Shape::TestConst(class));
        Selector::HasPath(class_path(), tn)
    }

    fn lower_shape_list(&mut self, list_head: &Term) -> Vec<ShapeId> {
        self.g
            .read_list(list_head)
            .into_iter()
            .filter_map(|m| term_to_node(&m))
            .map(|n| self.lower_shape(&n))
            .collect()
    }

    fn parse_shape_path(&mut self, s: &NamedOrBlankNode) -> Option<Path> {
        let term = self.g.object(s, vocab::SH_PATH)?;
        match parse_path(self.g, &term) {
            Ok(p) => Some(p),
            Err(e) => {
                self.diag(DiagLevel::Error, format!("invalid sh:path: {e}"), s);
                None
            }
        }
    }

    fn closed_allowed(&self, s: &NamedOrBlankNode) -> BTreeSet<oxrdf::NamedNode> {
        let mut q = BTreeSet::new();
        for prop in self.g.objects(s, vocab::SH_PROPERTY) {
            if let Some(pn) = term_to_node(&prop)
                && let Some(Term::NamedNode(n)) = self.g.object(&pn, vocab::SH_PATH)
            {
                q.insert(n);
            }
        }
        for ip in self.g.objects(s, vocab::SH_IGNORED_PROPERTIES) {
            for m in self.g.read_list(&ip) {
                if let Term::NamedNode(n) = m {
                    q.insert(n);
                }
            }
        }
        q
    }

    fn bool_prop(&self, s: &NamedOrBlankNode, pred: oxrdf::NamedNodeRef) -> bool {
        matches!(self.g.object(s, pred), Some(Term::Literal(l)) if l.value() == "true")
    }

    fn int(&self, s: &NamedOrBlankNode, pred: oxrdf::NamedNodeRef) -> Option<u64> {
        match self.g.object(s, pred) {
            Some(Term::Literal(l)) => l.value().parse().ok(),
            _ => None,
        }
    }

    fn lit(&self, s: &NamedOrBlankNode, pred: oxrdf::NamedNodeRef) -> Option<Literal> {
        match self.g.object(s, pred) {
            Some(Term::Literal(l)) => Some(l),
            _ => None,
        }
    }
}

fn class_path() -> Path {
    Path::seq(vec![
        Path::Pred(vocab::rdf_type()),
        Path::star(Path::Pred(vocab::rdfs_subclassof())),
    ])
}

fn map_node_kind(term: &Term) -> Option<NodeKindSet> {
    let Term::NamedNode(n) = term else { return None };
    let r = n.as_ref();
    Some(if r == vocab::SH_IRI {
        NodeKindSet::IRI
    } else if r == vocab::SH_BLANK_NODE {
        NodeKindSet::BLANK_NODE
    } else if r == vocab::SH_LITERAL {
        NodeKindSet::LITERAL
    } else if r == vocab::SH_BLANK_NODE_OR_IRI {
        NodeKindSet::BLANK_NODE_OR_IRI
    } else if r == vocab::SH_BLANK_NODE_OR_LITERAL {
        NodeKindSet::BLANK_NODE_OR_LITERAL
    } else if r == vocab::SH_IRI_OR_LITERAL {
        NodeKindSet::IRI_OR_LITERAL
    } else {
        return None;
    })
}
