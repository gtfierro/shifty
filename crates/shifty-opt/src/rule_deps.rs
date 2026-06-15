//! Conservative graph-read dependencies for SHACL-AF rules.
//!
//! The inference engine uses these dependencies for delta scheduling: after a
//! round adds triples, only rules that may observe one of the changed
//! predicates need to run again. Opaque or domain-sensitive constructs use a
//! wildcard dependency and are always rescheduled.

use shifty_algebra::{
    NamedNode, NodeExpr, Path, Rule, RuleHead, Selector, Shape, ShapeArena, ShapeId,
};
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
        RuleHead::Sparql(_) => deps.wildcard = true,
    }
    deps
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

    #[test]
    fn sparql_head_does_not_make_unchanged_guards_wildcard() {
        let arena = ShapeArena::new();
        let rule = Rule {
            selector: Selector::IsConst(Term::NamedNode(named("focus"))),
            conditions: Vec::new(),
            head: RuleHead::Sparql(shifty_algebra::SparqlConstruct {
                query: "CONSTRUCT {} WHERE {}".to_string(),
            }),
            order: None,
            deactivated: false,
        };

        assert!(rule_dependencies(&rule, &arena).wildcard);
        assert_eq!(
            rule_guard_dependencies(&rule, &arena),
            RuleDependencies::default()
        );
    }
}
