//! Conservative graph-read dependencies for SHACL-AF rules.
//!
//! The inference engine uses these dependencies for delta scheduling: after a
//! round adds triples, only rules that may observe one of the changed
//! predicates need to run again. Opaque or domain-sensitive constructs use a
//! wildcard dependency and are always rescheduled.

use shifty_algebra::{
    NamedNode, NodeExpr, Path, Rule, RuleHead, Selector, Shape, ShapeArena, ShapeId,
};
use spargebra::algebra::{Expression, GraphPattern, PropertyPathExpression};
use spargebra::term::{NamedNodePattern, TriplePattern};
use spargebra::{Query, SparqlParser};
use std::collections::HashSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleDependencies {
    pub predicates: HashSet<NamedNode>,
    pub wildcard: bool,
}

impl RuleDependencies {
    pub fn affected_by(&self, changed: &HashSet<NamedNode>) -> bool {
        self.wildcard || !self.predicates.is_disjoint(changed)
    }

    fn predicate(&mut self, predicate: &NamedNode) {
        self.predicates.insert(predicate.clone());
    }
}

/// Predicates whose triples may affect a rule's targets, conditions, or head
/// node expressions.
pub fn rule_dependencies(rule: &Rule, arena: &ShapeArena) -> RuleDependencies {
    let mut deps = rule_guard_dependencies(rule, arena);

    match &rule.head {
        RuleHead::Triple {
            subject,
            predicate,
            object,
        } => {
            node_expr_dependencies(subject, arena, &mut deps);
            node_expr_dependencies(predicate, arena, &mut deps);
            node_expr_dependencies(object, arena, &mut deps);
        }
        // Analyze the CONSTRUCT body for the predicates it reads. A rule whose
        // read set is fully known need only rerun when one of those predicates
        // changes, instead of on every pass (wildcard). Anything we cannot
        // analyze soundly (a variable predicate, negated property set, SERVICE,
        // …) falls back to wildcard.
        RuleHead::Sparql(construct) => match sparql_construct_dependencies(&construct.query) {
            Some(predicates) => deps.predicates.extend(predicates),
            None => deps.wildcard = true,
        },
    }
    deps
}

/// Predicates read by a SPARQL `CONSTRUCT` rule's WHERE clause, or `None` when
/// the body contains a construct we cannot analyze soundly — in which case the
/// rule must be treated as a wildcard dependency (rerun whenever anything
/// changes). Returning `None` is always safe; returning too small a set would
/// be unsound (a rule that should rerun would be skipped), so every
/// unrecognized or open-ended construct yields `None`.
fn sparql_construct_dependencies(query: &str) -> Option<HashSet<NamedNode>> {
    let parsed = SparqlParser::new().parse_query(query).ok()?;
    let pattern = match &parsed {
        Query::Construct { pattern, .. } => pattern,
        _ => return None,
    };
    let mut predicates = HashSet::new();
    pattern_read_predicates(pattern, &mut predicates)?;
    Some(predicates)
}

/// Collect the predicates a graph pattern reads. `None` on any unanalyzable
/// construct (variable predicate, negated property set, SERVICE).
fn pattern_read_predicates(pattern: &GraphPattern, out: &mut HashSet<NamedNode>) -> Option<()> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            for triple in patterns {
                triple_read_predicate(triple, out)?;
            }
        }
        GraphPattern::Path { path, .. } => path_read_predicates(path, out)?,
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right }
        | GraphPattern::Lateral { left, right } => {
            pattern_read_predicates(left, out)?;
            pattern_read_predicates(right, out)?;
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            pattern_read_predicates(left, out)?;
            pattern_read_predicates(right, out)?;
            if let Some(expr) = expression {
                expr_read_predicates(expr, out)?;
            }
        }
        GraphPattern::Filter { expr, inner } => {
            pattern_read_predicates(inner, out)?;
            expr_read_predicates(expr, out)?;
        }
        GraphPattern::Extend {
            inner, expression, ..
        } => {
            pattern_read_predicates(inner, out)?;
            expr_read_predicates(expression, out)?;
        }
        // The shapes graph (the only non-default GRAPH target shifty binds) is
        // immutable during inference, so reads there never trigger a rerun;
        // walking the inner pattern is sound regardless of the graph name.
        GraphPattern::Graph { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        // ORDER BY / aggregate expressions operate on already-bound solutions,
        // not on fresh graph reads, and cannot change a CONSTRUCT's result set.
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => pattern_read_predicates(inner, out)?,
        GraphPattern::Values { .. } => {}
        GraphPattern::Service { .. } => return None,
    }
    Some(())
}

fn triple_read_predicate(triple: &TriplePattern, out: &mut HashSet<NamedNode>) -> Option<()> {
    match &triple.predicate {
        NamedNodePattern::NamedNode(predicate) => {
            out.insert(predicate.clone());
            Some(())
        }
        // A variable predicate can match any predicate in the graph.
        NamedNodePattern::Variable(_) => None,
    }
}

fn path_read_predicates(path: &PropertyPathExpression, out: &mut HashSet<NamedNode>) -> Option<()> {
    match path {
        PropertyPathExpression::NamedNode(predicate) => {
            out.insert(predicate.clone());
        }
        PropertyPathExpression::Reverse(inner)
        | PropertyPathExpression::ZeroOrMore(inner)
        | PropertyPathExpression::OneOrMore(inner)
        | PropertyPathExpression::ZeroOrOne(inner) => path_read_predicates(inner, out)?,
        PropertyPathExpression::Sequence(a, b) | PropertyPathExpression::Alternative(a, b) => {
            path_read_predicates(a, out)?;
            path_read_predicates(b, out)?;
        }
        // `!(...)` matches every predicate outside the set, so it reads any.
        PropertyPathExpression::NegatedPropertySet(_) => return None,
    }
    Some(())
}

/// Walk an expression for `EXISTS` / `NOT EXISTS` sub-patterns, whose reads also
/// affect the rule's output. All other expression nodes operate on bound values.
fn expr_read_predicates(expr: &Expression, out: &mut HashSet<NamedNode>) -> Option<()> {
    match expr {
        Expression::Exists(pattern) => pattern_read_predicates(pattern, out)?,
        Expression::NamedNode(_)
        | Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Bound(_) => {}
        Expression::Not(a) | Expression::UnaryPlus(a) | Expression::UnaryMinus(a) => {
            expr_read_predicates(a, out)?
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
            expr_read_predicates(a, out)?;
            expr_read_predicates(b, out)?;
        }
        Expression::In(a, list) => {
            expr_read_predicates(a, out)?;
            for item in list {
                expr_read_predicates(item, out)?;
            }
        }
        Expression::If(a, b, c) => {
            expr_read_predicates(a, out)?;
            expr_read_predicates(b, out)?;
            expr_read_predicates(c, out)?;
        }
        Expression::Coalesce(list) | Expression::FunctionCall(_, list) => {
            for item in list {
                expr_read_predicates(item, out)?;
            }
        }
    }
    Some(())
}

/// Predicates whose triples may change a rule's focus nodes or conditions.
///
/// Keeping guard dependencies separate from head dependencies allows inference
/// to narrow a monotone SPARQL rule to focus nodes derived from the graph delta
/// when its eligibility is known to be unchanged.
pub fn rule_guard_dependencies(rule: &Rule, arena: &ShapeArena) -> RuleDependencies {
    let mut deps = selector_dependencies(&rule.selector, arena);
    let mut visited = HashSet::new();
    for condition in &rule.conditions {
        shape_dependencies(arena, *condition, &mut visited, &mut deps);
    }
    deps
}

/// Predicates whose triples may change the focus nodes selected by `selector`.
pub fn selector_dependencies(selector: &Selector, arena: &ShapeArena) -> RuleDependencies {
    let mut deps = RuleDependencies::default();
    match selector {
        Selector::HasOut(predicate) | Selector::HasIn(predicate) => {
            deps.predicate(predicate);
        }
        Selector::IsConst(_) => {}
        Selector::HasPath(path, qualifier) => {
            path_dependencies(path, &mut deps);
            if path_is_nullable(path) {
                // HasPath scans the graph's node domain. A triple with any
                // predicate can introduce a new focus node for a nullable path.
                deps.wildcard = true;
            }
            let mut visited = HashSet::new();
            shape_dependencies(arena, *qualifier, &mut visited, &mut deps);
        }
        Selector::Sparql(_) => deps.wildcard = true,
    }
    deps
}

fn node_expr_dependencies(expr: &NodeExpr, arena: &ShapeArena, deps: &mut RuleDependencies) {
    match expr {
        NodeExpr::This | NodeExpr::Constant(_) => {}
        NodeExpr::Path(path) => path_dependencies(path, deps),
        NodeExpr::Filter { input, shape } => {
            node_expr_dependencies(input, arena, deps);
            let mut visited = HashSet::new();
            shape_dependencies(arena, *shape, &mut visited, deps);
        }
        NodeExpr::Intersection(expressions) | NodeExpr::Union(expressions) => {
            for expression in expressions {
                node_expr_dependencies(expression, arena, deps);
            }
        }
        NodeExpr::Function { .. } => deps.wildcard = true,
    }
}

fn shape_dependencies(
    arena: &ShapeArena,
    id: ShapeId,
    visited: &mut HashSet<ShapeId>,
    deps: &mut RuleDependencies,
) {
    if !visited.insert(id) {
        return;
    }
    match arena.get(id) {
        Shape::Annotated { shape, .. } => shape_dependencies(arena, *shape, visited, deps),
        Shape::Top
        | Shape::Pending
        | Shape::TestConst(_)
        | Shape::TestType(_)
        | Shape::TestKind(_) => {}
        Shape::Closed(_) | Shape::Sparql(_) => deps.wildcard = true,
        Shape::Expression(e) => node_expr_dependencies(e, arena, deps),
        Shape::Eq(path, predicate)
        | Shape::Disj(path, predicate)
        | Shape::Lt(path, predicate)
        | Shape::Le(path, predicate) => {
            path_dependencies(path, deps);
            deps.predicate(predicate);
        }
        Shape::UniqueLang(path) => path_dependencies(path, deps),
        Shape::Not(child) => shape_dependencies(arena, *child, visited, deps),
        Shape::And(children) | Shape::Or(children) => {
            for child in children {
                shape_dependencies(arena, *child, visited, deps);
            }
        }
        Shape::Count {
            path, qualifier, ..
        } => {
            path_dependencies(path, deps);
            shape_dependencies(arena, *qualifier, visited, deps);
        }
    }
}

fn path_dependencies(path: &Path, deps: &mut RuleDependencies) {
    match path {
        Path::Id => {}
        Path::Pred(predicate) => deps.predicate(predicate),
        Path::Inverse(inner) | Path::Star(inner) => path_dependencies(inner, deps),
        Path::Seq(parts) | Path::Alt(parts) => {
            for part in parts {
                path_dependencies(part, deps);
            }
        }
    }
}

fn path_is_nullable(path: &Path) -> bool {
    match path {
        Path::Id | Path::Star(_) => true,
        Path::Pred(_) => false,
        Path::Inverse(inner) => path_is_nullable(inner),
        Path::Seq(parts) => parts.iter().all(path_is_nullable),
        Path::Alt(parts) => parts.iter().any(path_is_nullable),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shifty_algebra::{RuleHead, Term};

    fn named(local: &str) -> NamedNode {
        NamedNode::new(format!("http://ex/{local}")).unwrap()
    }

    #[test]
    fn collects_selector_condition_and_head_reads() {
        let mut arena = ShapeArena::new();
        let top = arena.top();
        let condition = arena.insert(Shape::Count {
            path: Path::Pred(named("condition")),
            min: Some(1),
            max: None,
            qualifier: top,
        });
        let rule = Rule {
            selector: Selector::HasOut(named("target")),
            conditions: vec![condition],
            head: RuleHead::Triple {
                subject: NodeExpr::This,
                predicate: NodeExpr::Constant(Term::NamedNode(named("result"))),
                object: NodeExpr::Path(Path::Pred(named("value"))),
            },
            order: None,
            deactivated: false,
        };

        let deps = rule_dependencies(&rule, &arena);
        assert!(!deps.wildcard);
        assert_eq!(
            deps.predicates,
            HashSet::from([named("target"), named("condition"), named("value")])
        );
    }

    #[test]
    fn opaque_and_domain_sensitive_constructs_are_wildcards() {
        let mut arena = ShapeArena::new();
        let top = arena.top();
        let rule = Rule {
            selector: Selector::HasPath(Path::Id, top),
            conditions: Vec::new(),
            head: RuleHead::Sparql(shifty_algebra::SparqlConstruct {
                query: "CONSTRUCT {} WHERE {}".to_string(),
            }),
            order: None,
            deactivated: false,
        };

        let deps = rule_dependencies(&rule, &arena);
        assert!(deps.wildcard);
    }

    fn sparql_rule(query: &str) -> Rule {
        Rule {
            selector: Selector::IsConst(Term::NamedNode(named("focus"))),
            conditions: Vec::new(),
            head: RuleHead::Sparql(shifty_algebra::SparqlConstruct {
                query: query.to_string(),
            }),
            order: None,
            deactivated: false,
        }
    }

    #[test]
    fn sparql_construct_reads_become_precise_dependencies() {
        // The WHERE clause reads ex:in (and the EXISTS guard reads ex:has); the
        // template predicate ex:out is written, not read, so it is not a dep.
        let arena = ShapeArena::new();
        let rule = sparql_rule(
            "PREFIX ex: <http://ex/> \
             CONSTRUCT { ?this ex:out ?o } \
             WHERE { ?this ex:in ?o . FILTER NOT EXISTS { ?this ex:has ?x } }",
        );
        let deps = rule_dependencies(&rule, &arena);
        assert!(!deps.wildcard, "analyzable body should not be wildcard");
        assert_eq!(deps.predicates, HashSet::from([named("in"), named("has")]));
        // The head must not leak into the guard dependencies.
        assert_eq!(
            rule_guard_dependencies(&rule, &arena),
            RuleDependencies::default()
        );
    }

    #[test]
    fn sparql_construct_variable_predicate_is_wildcard() {
        let arena = ShapeArena::new();
        let rule = sparql_rule("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }");
        assert!(rule_dependencies(&rule, &arena).wildcard);
    }

    #[test]
    fn sparql_construct_negated_property_set_is_wildcard() {
        let arena = ShapeArena::new();
        let rule = sparql_rule(
            "PREFIX ex: <http://ex/> \
             CONSTRUCT { ?s ex:out ?o } WHERE { ?s !(ex:a|ex:b) ?o }",
        );
        assert!(rule_dependencies(&rule, &arena).wildcard);
    }
}
