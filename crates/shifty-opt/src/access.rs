//! Data-independent read demand for a compiled shapes document.
//!
//! This is deliberately conservative: an unknown read remains distinguishable
//! from a proven empty read set. Physical indexes are chosen for each dataset,
//! not by this catalog.

use serde::Serialize;
use shifty_algebra::{
    FunctionDef, NamedNode, NodeExpr, Path, RuleHead, Schema, Selector, Shape, ShapeArena, ShapeId,
};
use spargebra::algebra::{
    AggregateExpression, Expression, Function, GraphPattern, OrderExpression,
    PropertyPathExpression,
};
use spargebra::term::{NamedNodePattern, TermPattern};
use spargebra::{Query, SparqlParser};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct QueryId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct PathId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum ReadScope {
    Default,
    Shapes,
    Unknown,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProbeModes {
    pub forward: bool,
    pub reverse: bool,
    pub membership: bool,
    pub open_scan: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AccessRequirement {
    pub predicates: HashSet<NamedNode>,
    pub any_predicate: bool,
    pub probes: ProbeModes,
    pub reads_node_domain: bool,
    pub incomplete: bool,
}

impl AccessRequirement {
    fn merge(&mut self, other: &Self) {
        self.predicates.extend(other.predicates.iter().cloned());
        self.any_predicate |= other.any_predicate;
        self.probes.forward |= other.probes.forward;
        self.probes.reverse |= other.probes.reverse;
        self.probes.membership |= other.probes.membership;
        self.probes.open_scan |= other.probes.open_scan;
        self.reads_node_domain |= other.reads_node_domain;
        self.incomplete |= other.incomplete;
    }

    fn unknown(&mut self) {
        self.any_predicate = true;
        self.incomplete = true;
        self.probes.open_scan = true;
    }

    fn edge(&mut self, predicate: &NamedNode, reverse: bool) {
        self.predicates.insert(predicate.clone());
        if reverse {
            self.probes.reverse = true;
        } else {
            self.probes.forward = true;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum Consumer {
    Statement(usize),
    Rule(usize),
    Function(NamedNode),
    /// Legacy source-oriented report and property lookups awaiting an explicit
    /// read description. Their demand must never be mistaken for an empty set.
    SourceReporting,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerAccess {
    pub consumer: Consumer,
    pub default: AccessRequirement,
    pub shapes: AccessRequirement,
    pub queries: Vec<QueryId>,
    pub paths: Vec<PathId>,
    pub calls: HashSet<NamedNode>,
}

impl ConsumerAccess {
    fn new(consumer: Consumer) -> Self {
        Self {
            consumer,
            default: AccessRequirement::default(),
            shapes: AccessRequirement::default(),
            queries: Vec::new(),
            paths: Vec::new(),
            calls: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryAccess {
    pub text: String,
    pub default: AccessRequirement,
    pub shapes: AccessRequirement,
    pub calls: HashSet<NamedNode>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PathAccess {
    pub path: Path,
    pub requirement: AccessRequirement,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AccessCatalog {
    pub consumers: Vec<ConsumerAccess>,
    pub queries: Vec<QueryAccess>,
    pub paths: Vec<PathAccess>,
}

impl AccessCatalog {
    pub fn compile(schema: &Schema, functions: &[FunctionDef]) -> Self {
        let mut catalog = Self::default();
        let mut query_ids = HashMap::new();
        let mut path_ids = HashMap::new();
        for (index, statement) in schema.statements.iter().enumerate() {
            let mut access = ConsumerAccess::new(Consumer::Statement(index));
            catalog.selector(
                &statement.selector,
                &schema.arena,
                &mut access,
                &mut query_ids,
                &mut path_ids,
            );
            let mut visited = HashSet::new();
            catalog.shape(
                statement.shape,
                &schema.arena,
                &mut access,
                &mut visited,
                &mut query_ids,
                &mut path_ids,
            );
            catalog.consumers.push(access);
        }
        for (index, rule) in schema
            .rules
            .iter()
            .enumerate()
            .filter(|(_, rule)| !rule.deactivated)
        {
            let mut access = ConsumerAccess::new(Consumer::Rule(index));
            catalog.selector(
                &rule.selector,
                &schema.arena,
                &mut access,
                &mut query_ids,
                &mut path_ids,
            );
            let mut visited = HashSet::new();
            for condition in &rule.conditions {
                catalog.shape(
                    *condition,
                    &schema.arena,
                    &mut access,
                    &mut visited,
                    &mut query_ids,
                    &mut path_ids,
                );
            }
            match &rule.head {
                RuleHead::Triple {
                    subject,
                    predicate,
                    object,
                } => {
                    for expression in [subject, predicate, object] {
                        catalog.expression(
                            expression,
                            &schema.arena,
                            &mut access,
                            &mut visited,
                            &mut query_ids,
                            &mut path_ids,
                        );
                    }
                }
                RuleHead::Sparql(query) => catalog.query(&query.query, &mut access, &mut query_ids),
            }
            catalog.consumers.push(access);
        }
        for function in functions {
            let mut access = ConsumerAccess::new(Consumer::Function(function.iri.clone()));
            catalog.query(&function.query, &mut access, &mut query_ids);
            catalog.consumers.push(access);
        }
        let mut reporting = ConsumerAccess::new(Consumer::SourceReporting);
        reporting.default.unknown();
        reporting.shapes.unknown();
        catalog.consumers.push(reporting);
        catalog.resolve_function_calls();
        catalog
    }

    fn resolve_function_calls(&mut self) {
        let mut definitions: HashMap<NamedNode, Vec<usize>> = HashMap::new();
        for (index, consumer) in self.consumers.iter().enumerate() {
            if let Consumer::Function(iri) = &consumer.consumer {
                definitions.entry(iri.clone()).or_default().push(index);
            }
        }
        // Function bodies can call other functions, including cycles. Merge
        // each round from a snapshot until the finite read sets stop growing.
        loop {
            let snapshot: Vec<_> = self
                .consumers
                .iter()
                .map(|consumer| (consumer.default.clone(), consumer.shapes.clone()))
                .collect();
            let mut changed = false;
            for consumer in &mut self.consumers {
                let before = (consumer.default.clone(), consumer.shapes.clone());
                for call in &consumer.calls {
                    if let Some(targets) = definitions.get(call) {
                        for &target in targets {
                            consumer.default.merge(&snapshot[target].0);
                            consumer.shapes.merge(&snapshot[target].1);
                        }
                    } else {
                        consumer.default.unknown();
                        consumer.shapes.unknown();
                    }
                }
                changed |= before != (consumer.default.clone(), consumer.shapes.clone());
            }
            if !changed {
                break;
            }
        }
    }

    fn query(
        &mut self,
        text: &str,
        consumer: &mut ConsumerAccess,
        ids: &mut HashMap<String, QueryId>,
    ) {
        let id = *ids.entry(text.to_owned()).or_insert_with(|| {
            let id = QueryId(self.queries.len());
            self.queries.push(analyze_query(text));
            id
        });
        let query = &self.queries[id.0];
        consumer.default.merge(&query.default);
        consumer.shapes.merge(&query.shapes);
        consumer.calls.extend(query.calls.iter().cloned());
        consumer.queries.push(id);
    }

    fn path(
        &mut self,
        path: &Path,
        consumer: &mut ConsumerAccess,
        ids: &mut HashMap<Path, PathId>,
    ) {
        let id = *ids.entry(path.clone()).or_insert_with(|| {
            let id = PathId(self.paths.len());
            let mut requirement = AccessRequirement::default();
            analyze_path(path, false, &mut requirement);
            self.paths.push(PathAccess {
                path: path.clone(),
                requirement,
            });
            id
        });
        consumer.default.merge(&self.paths[id.0].requirement);
        consumer.paths.push(id);
    }

    fn selector(
        &mut self,
        selector: &Selector,
        arena: &ShapeArena,
        access: &mut ConsumerAccess,
        queries: &mut HashMap<String, QueryId>,
        paths: &mut HashMap<Path, PathId>,
    ) {
        match selector {
            Selector::HasOut(p) => access.default.edge(p, false),
            Selector::HasIn(p) => access.default.edge(p, true),
            Selector::IsConst(_) => {}
            Selector::HasPath(path, shape) => {
                self.path(path, access, paths);
                if nullable(path) {
                    access.default.reads_node_domain = true;
                }
                self.shape(*shape, arena, access, &mut HashSet::new(), queries, paths);
            }
            Selector::Sparql(query) => self.query(&query.query, access, queries),
        }
    }

    fn shape(
        &mut self,
        id: ShapeId,
        arena: &ShapeArena,
        access: &mut ConsumerAccess,
        visited: &mut HashSet<ShapeId>,
        queries: &mut HashMap<String, QueryId>,
        paths: &mut HashMap<Path, PathId>,
    ) {
        if !visited.insert(id) {
            return;
        }
        match arena.get(id) {
            Shape::Annotated { shape, .. } | Shape::Not(shape) => {
                self.shape(*shape, arena, access, visited, queries, paths)
            }
            Shape::Top | Shape::TestConst(_) | Shape::TestType(_) | Shape::TestKind(_) => {}
            Shape::Pending => access.default.unknown(),
            Shape::Closed(_) => {
                access.default.any_predicate = true;
                access.default.probes.forward = true;
            }
            Shape::Eq(path, p) | Shape::Disj(path, p) | Shape::Lt(path, p) | Shape::Le(path, p) => {
                self.path(path, access, paths);
                access.default.edge(p, false);
            }
            Shape::UniqueLang(path) => self.path(path, access, paths),
            Shape::And(children) | Shape::Or(children) => {
                for child in children {
                    self.shape(*child, arena, access, visited, queries, paths);
                }
            }
            Shape::Count {
                path, qualifier, ..
            } => {
                self.path(path, access, paths);
                self.shape(*qualifier, arena, access, visited, queries, paths);
            }
            Shape::Sparql(query) => {
                if let Some(path) = &query.path {
                    self.path(path, access, paths);
                }
                self.query(&query.query, access, queries);
            }
            Shape::Expression(expression) => {
                self.expression(expression, arena, access, visited, queries, paths)
            }
        }
    }

    fn expression(
        &mut self,
        expression: &NodeExpr,
        arena: &ShapeArena,
        access: &mut ConsumerAccess,
        visited: &mut HashSet<ShapeId>,
        queries: &mut HashMap<String, QueryId>,
        paths: &mut HashMap<Path, PathId>,
    ) {
        match expression {
            NodeExpr::This | NodeExpr::Constant(_) => {}
            NodeExpr::Path(path) => self.path(path, access, paths),
            NodeExpr::Filter { input, shape } => {
                self.expression(input, arena, access, visited, queries, paths);
                self.shape(*shape, arena, access, visited, queries, paths);
            }
            NodeExpr::Intersection(parts) | NodeExpr::Union(parts) => {
                for part in parts {
                    self.expression(part, arena, access, visited, queries, paths);
                }
            }
            NodeExpr::Function { iri, args } => {
                for arg in args {
                    self.expression(arg, arena, access, visited, queries, paths);
                }
                access.calls.insert(iri.clone());
            }
        }
    }
}

fn nullable(path: &Path) -> bool {
    match path {
        Path::Id | Path::Star(_) => true,
        Path::Pred(_) => false,
        Path::Inverse(inner) => nullable(inner),
        Path::Seq(parts) => parts.iter().all(nullable),
        Path::Alt(parts) => parts.iter().any(nullable),
    }
}

fn analyze_path(path: &Path, reverse: bool, out: &mut AccessRequirement) {
    match path {
        Path::Id => out.reads_node_domain = true,
        Path::Pred(p) => out.edge(p, reverse),
        Path::Inverse(inner) => analyze_path(inner, !reverse, out),
        Path::Star(inner) => {
            out.reads_node_domain = true;
            analyze_path(inner, reverse, out);
        }
        Path::Seq(parts) | Path::Alt(parts) => {
            for part in parts {
                analyze_path(part, reverse, out);
            }
        }
    }
}

fn analyze_query(text: &str) -> QueryAccess {
    let mut result = QueryAccess {
        text: text.to_owned(),
        default: AccessRequirement::default(),
        shapes: AccessRequirement::default(),
        calls: HashSet::new(),
    };
    let Ok(query) = SparqlParser::new().parse_query(text) else {
        result.default.unknown();
        result.shapes.unknown();
        return result;
    };
    let (pattern, has_dataset_clause) = match &query {
        Query::Select {
            pattern, dataset, ..
        }
        | Query::Ask {
            pattern, dataset, ..
        }
        | Query::Construct {
            pattern, dataset, ..
        }
        | Query::Describe {
            pattern, dataset, ..
        } => (pattern, dataset.is_some()),
    };
    analyze_pattern(pattern, ReadScope::Default, &mut result);
    if has_dataset_clause {
        // FROM/FROM NAMED can redirect reads away from the ordinary default
        // and shapes views. Retain discovered predicates but do not claim a
        // complete graph-scoped description until dataset clauses are modeled.
        result.default.unknown();
        result.shapes.unknown();
    }
    result
}

fn scope(query: &mut QueryAccess, scope: ReadScope) -> &mut AccessRequirement {
    match scope {
        ReadScope::Default => &mut query.default,
        ReadScope::Shapes => &mut query.shapes,
        ReadScope::Unknown => {
            query.default.unknown();
            query.shapes.unknown();
            &mut query.default
        }
    }
}

fn analyze_pattern(pattern: &GraphPattern, graph: ReadScope, out: &mut QueryAccess) {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                let req = scope(out, graph);
                match &triple.predicate {
                    NamedNodePattern::NamedNode(p) => {
                        req.predicates.insert(p.clone());
                        req.probes.forward |= !matches!(triple.subject, TermPattern::Variable(_));
                        req.probes.reverse |= !matches!(triple.object, TermPattern::Variable(_));
                        req.probes.open_scan |= matches!(triple.subject, TermPattern::Variable(_))
                            && matches!(triple.object, TermPattern::Variable(_));
                    }
                    NamedNodePattern::Variable(_) => {
                        req.any_predicate = true;
                        req.probes.forward |= !matches!(triple.subject, TermPattern::Variable(_));
                        req.probes.reverse |= !matches!(triple.object, TermPattern::Variable(_));
                        req.probes.open_scan |= matches!(triple.subject, TermPattern::Variable(_))
                            && matches!(triple.object, TermPattern::Variable(_));
                    }
                }
            }
        }
        GraphPattern::Path { path, .. } => analyze_sparql_path(path, false, scope(out, graph)),
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::Lateral { left, right } => {
            analyze_pattern(left, graph, out);
            analyze_pattern(right, graph, out);
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            analyze_pattern(left, graph, out);
            analyze_pattern(right, graph, out);
            if let Some(expression) = expression {
                analyze_expression(expression, graph, out);
            }
        }
        GraphPattern::Filter { expr, inner } => {
            analyze_pattern(inner, graph, out);
            analyze_expression(expr, graph, out);
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            analyze_pattern(inner, graph, out);
            analyze_expression(expression, graph, out);
        }
        GraphPattern::Graph { name, inner } => {
            let graph = match name {
                NamedNodePattern::NamedNode(n) if n.as_str() == "urn:x-shacl:shapes-graph" => {
                    ReadScope::Shapes
                }
                // $shapesGraph is statically prebound by the executor.
                NamedNodePattern::Variable(v) if v.as_str() == "shapesGraph" => ReadScope::Shapes,
                _ => ReadScope::Unknown,
            };
            analyze_pattern(inner, graph, out);
        }
        GraphPattern::OrderBy { inner, expression } => {
            analyze_pattern(inner, graph, out);
            for order in expression {
                match order {
                    OrderExpression::Asc(expr) | OrderExpression::Desc(expr) => {
                        analyze_expression(expr, graph, out);
                    }
                }
            }
        }
        GraphPattern::Group {
            inner, aggregates, ..
        } => {
            analyze_pattern(inner, graph, out);
            for (_, aggregate) in aggregates {
                if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                    analyze_expression(expr, graph, out);
                }
            }
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. } => analyze_pattern(inner, graph, out),
        GraphPattern::Values { .. } => {}
        GraphPattern::Service { .. } => scope(out, graph).unknown(),
    }
}

fn analyze_expression(expr: &Expression, graph: ReadScope, out: &mut QueryAccess) {
    match expr {
        Expression::Exists(pattern) => analyze_pattern(pattern, graph, out),
        Expression::Not(x) | Expression::UnaryPlus(x) | Expression::UnaryMinus(x) => {
            analyze_expression(x, graph, out)
        }
        Expression::And(a, b)
        | Expression::Or(a, b)
        | Expression::SameTerm(a, b)
        | Expression::Equal(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b) => {
            analyze_expression(a, graph, out);
            analyze_expression(b, graph, out);
        }
        Expression::In(x, list) => {
            analyze_expression(x, graph, out);
            for item in list {
                analyze_expression(item, graph, out);
            }
        }
        Expression::If(a, b, c) => {
            analyze_expression(a, graph, out);
            analyze_expression(b, graph, out);
            analyze_expression(c, graph, out);
        }
        Expression::Coalesce(list) => {
            for item in list {
                analyze_expression(item, graph, out);
            }
        }
        Expression::FunctionCall(function, args) => {
            for arg in args {
                analyze_expression(arg, graph, out);
            }
            if let Function::Custom(iri) = function {
                out.calls.insert(iri.clone());
            }
        }
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
    }
}

fn analyze_sparql_path(path: &PropertyPathExpression, reverse: bool, out: &mut AccessRequirement) {
    match path {
        PropertyPathExpression::NamedNode(p) => out.edge(p, reverse),
        PropertyPathExpression::Reverse(inner) => analyze_sparql_path(inner, !reverse, out),
        PropertyPathExpression::ZeroOrMore(inner) | PropertyPathExpression::ZeroOrOne(inner) => {
            out.reads_node_domain = true;
            analyze_sparql_path(inner, reverse, out);
        }
        PropertyPathExpression::OneOrMore(inner) => analyze_sparql_path(inner, reverse, out),
        PropertyPathExpression::Sequence(a, b) | PropertyPathExpression::Alternative(a, b) => {
            analyze_sparql_path(a, reverse, out);
            analyze_sparql_path(b, reverse, out);
        }
        PropertyPathExpression::NegatedPropertySet(_) => out.unknown(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(s: &str) -> NamedNode {
        NamedNode::new(format!("http://ex/{s}")).unwrap()
    }

    #[test]
    fn graph_scope_and_unknown_predicate_are_distinct() {
        let q = analyze_query(
            "PREFIX ex: <http://ex/> SELECT * WHERE { GRAPH $shapesGraph { ?s ex:p ?o } ?s ?p ?o }",
        );
        assert!(q.shapes.predicates.contains(&named("p")));
        assert!(!q.shapes.any_predicate);
        assert!(q.default.any_predicate);
    }

    #[test]
    fn arbitrary_graph_variables_are_conservative_in_both_scopes() {
        let q = analyze_query("SELECT * WHERE { GRAPH ?g { ?s <http://ex/p> ?o } }");
        assert!(q.default.incomplete);
        assert!(q.default.any_predicate);
        assert!(q.shapes.incomplete);
        assert!(q.shapes.any_predicate);
    }

    #[test]
    fn closed_shapes_demand_unknown_predicate_subject_scans() {
        let mut schema = Schema::new();
        let closed = schema.arena.insert(Shape::Closed(Default::default()));
        schema.statements.push(shifty_algebra::Statement {
            selector: Selector::HasOut(named("seed")),
            shape: closed,
        });
        let catalog = AccessCatalog::compile(&schema, &[]);
        let access = &catalog.consumers[0].default;
        assert!(access.any_predicate);
        assert!(access.probes.forward);
        assert!(!access.incomplete);
    }

    #[test]
    fn paths_record_direction_and_domain() {
        let mut r = AccessRequirement::default();
        analyze_path(
            &Path::Star(Box::new(Path::Inverse(Box::new(Path::Pred(named("p")))))),
            false,
            &mut r,
        );
        assert!(r.probes.reverse);
        assert!(r.reads_node_domain);
    }

    #[test]
    fn variable_predicate_reports_bound_endpoint_demand() {
        let forward = analyze_query("SELECT * WHERE { <http://ex/a> ?p ?o }");
        assert!(forward.default.any_predicate);
        assert!(forward.default.probes.forward);
        let reverse = analyze_query("SELECT * WHERE { ?s ?p <http://ex/b> }");
        assert!(reverse.default.any_predicate);
        assert!(reverse.default.probes.reverse);
    }

    #[test]
    fn function_calls_propagate_reads_through_cycles() {
        let mut schema = Schema::new();
        let top = schema.arena.insert(Shape::Top);
        schema.statements.push(shifty_algebra::Statement {
            selector: Selector::Sparql(shifty_algebra::SparqlTarget {
                query: "SELECT * WHERE { FILTER(<http://ex/a>()) }".into(),
            }),
            shape: top,
        });
        let expression = schema.arena.insert(Shape::Expression(NodeExpr::Function {
            iri: named("a"),
            args: vec![],
        }));
        schema.statements.push(shifty_algebra::Statement {
            selector: Selector::HasOut(named("seed")),
            shape: expression,
        });
        let functions = [
            FunctionDef {
                iri: named("a"),
                params: vec![],
                query: "ASK { FILTER(<http://ex/b>()) }".into(),
                reads_graph: true,
            },
            FunctionDef {
                iri: named("b"),
                params: vec![],
                query: "ASK { ?s <http://ex/p> ?o . FILTER(<http://ex/a>()) }".into(),
                reads_graph: true,
            },
        ];
        let catalog = AccessCatalog::compile(&schema, &functions);
        for consumer in catalog.consumers.iter().take(4) {
            assert!(consumer.default.predicates.contains(&named("p")));
            assert!(!consumer.default.incomplete);
        }
    }

    #[test]
    fn unresolved_function_calls_remain_conservative() {
        let mut schema = Schema::new();
        let top = schema.arena.insert(Shape::Top);
        schema.statements.push(shifty_algebra::Statement {
            selector: Selector::Sparql(shifty_algebra::SparqlTarget {
                query: "SELECT * WHERE { FILTER(<http://ex/missing>()) }".into(),
            }),
            shape: top,
        });
        let catalog = AccessCatalog::compile(&schema, &[]);
        assert!(catalog.consumers[0].default.incomplete);
        assert!(catalog.consumers[0].shapes.incomplete);
    }

    #[test]
    fn order_and_aggregate_expressions_contribute_reads() {
        let ordered = analyze_query(
            "SELECT ?s WHERE { ?s <http://ex/p> ?o } ORDER BY EXISTS { ?s <http://ex/order> ?x }",
        );
        assert!(ordered.default.predicates.contains(&named("order")));
        let grouped = analyze_query(
            "SELECT (GROUP_CONCAT(<http://ex/f>(?o)) AS ?v) WHERE { ?s <http://ex/p> ?o }",
        );
        assert!(grouped.calls.contains(&named("f")));
    }
}
