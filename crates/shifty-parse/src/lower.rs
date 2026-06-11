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
use spargebra::{Query, SparqlParser};
use shifty_algebra::{
    Bound, NodeExpr, NodeKindSet, Path, Rule, RuleHead, Schema, Selector, Shape, ShapeArena,
    ShapeId, SparqlConstraint, SparqlConstruct, SparqlQueryKind, SparqlTarget, Statement,
    ValueType,
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
        rules: Vec::new(),
        diags: Vec::new(),
    };
    let shapes = l.discover_shapes();
    for s in &shapes {
        l.lower_shape(s);
    }
    for s in &shapes {
        // selectors are shared by the shape's statements and its rules
        let selectors = l.target_selectors(s);
        if let Some(shape) = l.cache.get(s).copied() {
            for sel in &selectors {
                l.statements.push(Statement { selector: sel.clone(), shape });
            }
        }
        l.parse_rules(s, &selectors);
    }
    let names = l
        .cache
        .iter()
        .filter_map(|(node, id)| match node {
            NamedOrBlankNode::NamedNode(n) => Some((*id, n.as_str().to_string())),
            NamedOrBlankNode::BlankNode(_) => None,
        })
        .collect();
    Lowered {
        schema: Schema {
            arena: l.arena,
            statements: l.statements,
            rules: l.rules,
            names,
        },
        diagnostics: l.diags,
    }
}

struct Lowerer<'a> {
    g: &'a Loaded,
    arena: ShapeArena,
    cache: HashMap<NamedOrBlankNode, ShapeId>,
    statements: Vec<Statement>,
    rules: Vec<Rule>,
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
                || p == vocab::SH_TARGET_OBJECTS_OF
                || p == vocab::SH_TARGET;
            if p == vocab::SH_PATH || p == vocab::SH_SPARQL || p == vocab::SH_RULE || is_target {
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

        if self.bool_prop(s, vocab::SH_CLOSED) {
            let q = self.closed_allowed(s);
            let c = self.arena.insert(Shape::Closed(q));
            conjuncts.push(c);
        }

        for constraint_term in self.g.objects(s, vocab::SH_SPARQL) {
            let Some(constraint_node) = term_to_node(&constraint_term) else {
                self.diag(DiagLevel::Error, "sh:sparql must reference a resource", s);
                continue;
            };
            let parsed = if let Some(Term::Literal(query)) =
                self.g.object(&constraint_node, vocab::SH_SELECT)
            {
                self.canonical_sparql(&constraint_node, query.value(), ExpectedQuery::Select)
                    .map(|query| (SparqlQueryKind::Select, query))
            } else if let Some(Term::Literal(query)) =
                self.g.object(&constraint_node, vocab::SH_ASK)
            {
                self.canonical_sparql(&constraint_node, query.value(), ExpectedQuery::Ask)
                    .map(|query| (SparqlQueryKind::Ask, query))
            } else {
                self.diag(
                    DiagLevel::Error,
                    "sh:sparql constraint requires sh:select or sh:ask",
                    &constraint_node,
                );
                None
            };
            if let Some((kind, query)) = parsed {
                let shape = Some(match s {
                    NamedOrBlankNode::NamedNode(n) => Term::NamedNode(n.clone()),
                    NamedOrBlankNode::BlankNode(b) => Term::BlankNode(b.clone()),
                });
                // `sh:message` on the SPARQL constraint takes precedence; absent
                // that, fall back to the owning shape's `sh:message` (SHACL §5.2.1).
                let mut messages: Vec<Term> = self.g.objects(&constraint_node, vocab::SH_MESSAGE);
                if messages.is_empty() {
                    messages = self.g.objects(s, vocab::SH_MESSAGE);
                }
                let constraint = SparqlConstraint {
                    kind,
                    query,
                    path: path.clone(),
                    shape,
                    messages,
                };
                conjuncts.push(self.arena.insert(Shape::Sparql(constraint)));
            }
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

        // sh:node — each value node must conform to the referenced shape
        for n in self.g.objects(s, vocab::SH_NODE) {
            if let Some(nn) = term_to_node(&n) {
                let id = self.lower_shape(&nn);
                value.push(id);
            }
        }

        // sh:property — like sh:node, each *value node* must conform to the
        // referenced property shape (so on a property shape it is scoped under
        // ∀path, not applied to the focus node directly).
        for prop in self.g.objects(s, vocab::SH_PROPERTY) {
            if let Some(pn) = term_to_node(&prop) {
                let id = self.lower_shape(&pn);
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

    /// The target selectors of a shape (used by both its statements and rules).
    fn target_selectors(&mut self, s: &NamedOrBlankNode) -> Vec<Selector> {
        let mut sels = Vec::new();

        for c in self.g.objects(s, vocab::SH_TARGET_NODE) {
            sels.push(Selector::IsConst(c));
        }
        for c in self.g.objects(s, vocab::SH_TARGET_CLASS) {
            sels.push(self.class_selector(c));
        }
        for p in self.g.objects(s, vocab::SH_TARGET_SUBJECTS_OF) {
            if let Term::NamedNode(n) = p {
                sels.push(Selector::HasOut(n));
            }
        }
        for p in self.g.objects(s, vocab::SH_TARGET_OBJECTS_OF) {
            if let Term::NamedNode(n) = p {
                sels.push(Selector::HasIn(n));
            }
        }

        // implicit class target: a shape that is also an rdfs:Class / owl:Class
        if (self.g.is_instance_of(s, vocab::RDFS_CLASS)
            || self.g.is_instance_of(s, vocab::OWL_CLASS))
            && let NamedOrBlankNode::NamedNode(n) = s
        {
            sels.push(self.class_selector(Term::NamedNode(n.clone())));
        }

        for target_term in self.g.objects(s, vocab::SH_TARGET) {
            let Some(target_node) = term_to_node(&target_term) else {
                self.diag(DiagLevel::Error, "sh:target must reference a resource", s);
                continue;
            };
            match self.g.object(&target_node, vocab::SH_SELECT) {
                Some(Term::Literal(query)) => {
                    if let Some(query) =
                        self.canonical_sparql(&target_node, query.value(), ExpectedQuery::Select)
                    {
                        sels.push(Selector::Sparql(SparqlTarget { query }));
                    }
                }
                _ => self.diag(
                    DiagLevel::Unsupported,
                    "custom sh:target without sh:select is not yet lowered",
                    &target_node,
                ),
            }
        }

        sels
    }

    /// Lower the `sh:rule`s of a shape (SHACL-AF). A rule fires on the shape's
    /// targets, so we emit one [`Rule`] per selector.
    fn parse_rules(&mut self, s: &NamedOrBlankNode, selectors: &[Selector]) {
        for rule_term in self.g.objects(s, vocab::SH_RULE) {
            let Some(rn) = term_to_node(&rule_term) else { continue };
            let Some(head) = self.parse_rule_head(&rn) else { continue };

            let conditions: Vec<ShapeId> = self
                .g
                .objects(&rn, vocab::SH_CONDITION)
                .iter()
                .filter_map(term_to_node)
                .map(|c| self.lower_shape(&c))
                .collect();
            let order = self.order(&rn);
            let deactivated = self.bool_prop(&rn, vocab::SH_DEACTIVATED);

            for sel in selectors {
                self.rules.push(Rule {
                    selector: sel.clone(),
                    conditions: conditions.clone(),
                    head: head.clone(),
                    order,
                    deactivated,
                });
            }
        }
    }

    fn parse_rule_head(&mut self, rn: &NamedOrBlankNode) -> Option<RuleHead> {
        // sh:SPARQLRule — parse and canonicalize the CONSTRUCT while retaining
        // an opaque algebra leaf for later query rewriting.
        if let Some(Term::Literal(q)) = self.g.object(rn, vocab::SH_CONSTRUCT) {
            let query = self.canonical_sparql(rn, q.value(), ExpectedQuery::Construct)?;
            return Some(RuleHead::Sparql(SparqlConstruct { query }));
        }
        // sh:TripleRule — subject/predicate/object node expressions
        let (subj, pred, obj) = (
            self.g.object(rn, vocab::SH_SUBJECT),
            self.g.object(rn, vocab::SH_PREDICATE),
            self.g.object(rn, vocab::SH_OBJECT),
        );
        if subj.is_none() && pred.is_none() && obj.is_none() {
            self.diag(DiagLevel::Unsupported, "unrecognized sh:rule head", rn);
            return None;
        }
        let (Some(subj), Some(pred), Some(obj)) = (subj, pred, obj) else {
            self.diag(DiagLevel::Error, "sh:TripleRule missing subject/predicate/object", rn);
            return None;
        };
        Some(RuleHead::Triple {
            subject: self.parse_node_expr(subj, rn)?,
            predicate: self.parse_node_expr(pred, rn)?,
            object: self.parse_node_expr(obj, rn)?,
        })
    }

    /// Parse a node expression (SHACL-AF §5). Currently handles `sh:this`,
    /// constants, and path expressions; richer expressions are diagnosed.
    fn parse_node_expr(&mut self, term: Term, owner: &NamedOrBlankNode) -> Option<NodeExpr> {
        match &term {
            Term::NamedNode(n) if n.as_ref() == vocab::SH_THIS => Some(NodeExpr::This),
            Term::NamedNode(_) | Term::Literal(_) => Some(NodeExpr::Constant(term)),
            Term::BlankNode(_) => {
                let node = term_to_node(&term).expect("blank node");
                if let Some(path_term) = self.g.object(&node, vocab::SH_PATH) {
                    match parse_path(self.g, &path_term) {
                        Ok(path) => Some(NodeExpr::Path(path)),
                        Err(e) => {
                            self.diag(DiagLevel::Error, format!("invalid node-expression path: {e}"), owner);
                            None
                        }
                    }
                } else {
                    self.diag(DiagLevel::Unsupported, "complex node expression not yet lowered", owner);
                    None
                }
            }
        }
    }

    fn order(&self, s: &NamedOrBlankNode) -> Option<i64> {
        match self.g.object(s, vocab::SH_ORDER) {
            Some(Term::Literal(l)) => l.value().parse().ok(),
            _ => None,
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

    /// Parse a SHACL SPARQL query once, resolving both document prefixes and
    /// `sh:prefixes` declarations. `Query::to_string` expands prefix names, so
    /// the IR remains self-contained and can be reparsed or rewritten later.
    fn canonical_sparql(
        &mut self,
        owner: &NamedOrBlankNode,
        raw: &str,
        expected: ExpectedQuery,
    ) -> Option<String> {
        let (query, canonical) = match canonical_sparql_query(self.g, owner, raw) {
            Ok(result) => result,
            Err(message) => {
                self.diag(DiagLevel::Error, message, owner);
                return None;
            }
        };
        let actual = match &query {
            Query::Select { .. } => ExpectedQuery::Select,
            Query::Ask { .. } => ExpectedQuery::Ask,
            Query::Construct { .. } => ExpectedQuery::Construct,
            Query::Describe { .. } => ExpectedQuery::Describe,
        };
        if actual != expected {
            self.diag(
                DiagLevel::Error,
                format!("expected SPARQL {expected}, found {actual}"),
                owner,
            );
            return None;
        }
        Some(canonical)
    }
}

/// Build the canonical, prefix-expanded form of a SHACL SPARQL query string.
///
/// Resolves the document base IRI, document-level prefixes, and the
/// `sh:prefixes` / `sh:declare` chains (following `owl:imports`) declared on
/// `owner`, parses `raw`, and returns the parsed query together with its
/// canonical string form. `Query::to_string` expands prefix names, so the
/// result is self-contained and can be reparsed without external declarations.
///
/// Errors are returned as messages so callers can decide how to surface them:
/// the lowerer routes them to diagnostics; the report validator drops the
/// offending constraint, matching the lowering path.
pub fn canonical_sparql_query(
    g: &Loaded,
    owner: &NamedOrBlankNode,
    raw: &str,
) -> Result<(Query, String), String> {
    let mut parser = SparqlParser::new();
    if let Some(base) = &g.base {
        parser = parser
            .with_base_iri(base)
            .map_err(|e| format!("invalid SPARQL base IRI: {e}"))?;
    }
    for (prefix, namespace) in &g.prefixes {
        parser = parser
            .with_prefix(prefix, namespace)
            .map_err(|e| format!("invalid SPARQL prefix declaration {prefix}: {e}"))?;
    }
    let mut prefix_sources: Vec<NamedOrBlankNode> = g
        .objects(owner, vocab::SH_PREFIXES)
        .iter()
        .filter_map(term_to_node)
        .collect();
    let mut seen_sources = HashSet::new();
    while let Some(source) = prefix_sources.pop() {
        if !seen_sources.insert(source.clone()) {
            continue;
        }
        prefix_sources.extend(
            g.objects(&source, vocab::OWL_IMPORTS)
                .iter()
                .filter_map(term_to_node),
        );
        for declaration_term in g.objects(&source, vocab::SH_DECLARE) {
            let Some(declaration) = term_to_node(&declaration_term) else { continue };
            let (Some(Term::Literal(prefix)), Some(Term::Literal(namespace))) = (
                g.object(&declaration, vocab::SH_PREFIX),
                g.object(&declaration, vocab::SH_NAMESPACE),
            ) else {
                continue;
            };
            parser = parser
                .with_prefix(prefix.value(), namespace.value())
                .map_err(|e| format!("invalid SHACL SPARQL prefix declaration: {e}"))?;
        }
    }
    let query = parser
        .parse_query(raw)
        .map_err(|e| format!("invalid SPARQL query: {e}"))?;
    let canonical = query.to_string();
    Ok((query, canonical))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpectedQuery {
    Select,
    Ask,
    Construct,
    Describe,
}

impl std::fmt::Display for ExpectedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Select => "SELECT",
            Self::Ask => "ASK",
            Self::Construct => "CONSTRUCT",
            Self::Describe => "DESCRIBE",
        })
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
