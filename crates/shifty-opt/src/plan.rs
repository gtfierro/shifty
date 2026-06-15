//! Logical → physical planning (Layer 5).
//!
//! A [`PhysicalPlan`] decides *how* each `(selector, φ)` statement is evaluated.
//! This first slice introduces two static (data-independent) optimizations:
//!
//! 1. **Selector seeding** — each selector compiles to a [`FocusSource`] that
//!    enumerates focus nodes directly. The important case is a path selector
//!    whose qualifier is a constant (class targets): instead of scanning every
//!    node and testing `∃π.test(c)`, we seed *backward* from the constant
//!    (`pred(c, π)`).
//! 2. **Cost-based ordering** — `And`/`Or` children are reordered cheapest-first
//!    using a static [cost model](shape_cost), so the short-circuiting evaluator
//!    rejects/accepts on cheap atoms before walking expensive paths.
//!
//! Data-aware selectivity, path compilation, and the plan executor come next.

use serde::{Deserialize, Serialize};
use shifty_algebra::render::{path_to_string, shape_to_string};
use shifty_algebra::{
    NamedNode, Path, Schema, Selector, Shape, ShapeArena, ShapeId, SparqlTarget, Term,
};
use std::collections::BTreeSet;
use std::collections::HashMap;

/// How to enumerate the focus nodes of a statement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FocusSource {
    /// `sh:targetSubjectsOf p` — subjects of `(·, p, ·)`.
    SubjectsOf(NamedNode),
    /// `sh:targetObjectsOf p` — objects of `(·, p, ·)`.
    ObjectsOf(NamedNode),
    /// `sh:targetNode c` — a single node.
    Node(Term),
    /// Path selector with a constant qualifier (e.g. class targets): the focus
    /// set is `pred(target, path)` — nodes reaching `target` along `path`.
    PathToConst { path: Path, target: Term },
    /// General path selector: scan candidate nodes, keep those with a
    /// path-successor satisfying `qualifier`.
    ScanFilter { path: Path, qualifier: ShapeId },
    /// Parsed and canonicalized SPARQL target.
    Sparql(SparqlTarget),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementPlan {
    pub source: FocusSource,
    pub shape: ShapeId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalPlan {
    /// The shape arena with `And`/`Or` children reordered cheapest-first.
    pub arena: ShapeArena,
    pub statements: Vec<StatementPlan>,
    /// IRI names for named shape nodes, copied from the source schema for
    /// profiling and diagnostics (same contents as `Schema::names`).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub names: HashMap<ShapeId, String>,
}

/// Plan a (normalized) schema.
pub fn plan(schema: &Schema) -> PhysicalPlan {
    let mut arena = schema.arena.clone();
    let costs = compute_costs(&arena);

    // reorder And/Or children cheapest-first (ties by id for determinism)
    for i in 0..arena.len() {
        let id = ShapeId(i as u32);
        let reordered = match arena.get(id).clone() {
            Shape::And(cs) => Some(Shape::And(sort_by_cost(cs, &costs))),
            Shape::Or(cs) => Some(Shape::Or(sort_by_cost(cs, &costs))),
            _ => None,
        };
        if let Some(s) = reordered {
            arena.set(id, s);
        }
    }

    let statements = schema
        .statements
        .iter()
        .map(|st| StatementPlan {
            source: plan_selector(&arena, &st.selector),
            shape: st.shape,
        })
        .collect();

    PhysicalPlan {
        arena,
        statements,
        names: schema.names.clone(),
    }
}

fn sort_by_cost(mut cs: Vec<ShapeId>, costs: &[u64]) -> Vec<ShapeId> {
    cs.sort_by_key(|c| (costs[c.0 as usize], c.0));
    cs
}

fn plan_selector(arena: &ShapeArena, sel: &Selector) -> FocusSource {
    match sel {
        Selector::HasOut(p) => FocusSource::SubjectsOf(p.clone()),
        Selector::HasIn(p) => FocusSource::ObjectsOf(p.clone()),
        Selector::IsConst(c) => FocusSource::Node(c.clone()),
        Selector::HasPath(path, qual) => match arena.get(*qual) {
            Shape::TestConst(c) => FocusSource::PathToConst {
                path: path.clone(),
                target: c.clone(),
            },
            _ => FocusSource::ScanFilter {
                path: path.clone(),
                qualifier: *qual,
            },
        },
        Selector::Sparql(target) => FocusSource::Sparql(target.clone()),
    }
}

// ---- static cost model ----

const C_CLOSED: u64 = 4;
const C_PAIR: u64 = 2;
const C_SPARQL: u64 = 100;
const C_STAR: u64 = 10;
const C_RECURSIVE: u64 = 50;

/// Estimated relative cost of checking each arena node at one focus node.
pub fn compute_costs(arena: &ShapeArena) -> Vec<u64> {
    let mut memo = vec![None; arena.len()];
    let mut computing = vec![false; arena.len()];
    for i in 0..arena.len() {
        cost_of(arena, ShapeId(i as u32), &mut memo, &mut computing);
    }
    memo.into_iter().map(|c| c.unwrap_or(0)).collect()
}

fn cost_of(
    arena: &ShapeArena,
    id: ShapeId,
    memo: &mut [Option<u64>],
    computing: &mut [bool],
) -> u64 {
    let i = id.0 as usize;
    if let Some(c) = memo[i] {
        return c;
    }
    if computing[i] {
        return C_RECURSIVE; // back-edge in a recursive shape
    }
    computing[i] = true;
    let cost = match arena.get(id).clone() {
        Shape::Annotated { shape, .. } => cost_of(arena, shape, memo, computing),
        Shape::Top | Shape::Pending => 0,
        Shape::TestConst(_) | Shape::TestKind(_) | Shape::TestType(_) => 1,
        Shape::Closed(_) => C_CLOSED,
        Shape::Eq(p, _) | Shape::Disj(p, _) | Shape::Lt(p, _) | Shape::Le(p, _) => {
            C_PAIR + path_cost(&p)
        }
        Shape::UniqueLang(p) => 1 + path_cost(&p),
        Shape::Not(c) => cost_of(arena, c, memo, computing),
        Shape::And(cs) | Shape::Or(cs) => cs
            .iter()
            .map(|c| cost_of(arena, *c, memo, computing))
            .sum::<u64>()
            .max(1),
        Shape::Count {
            path, qualifier, ..
        } => {
            let q = cost_of(arena, qualifier, memo, computing);
            path_cost(&path) * (1 + q)
        }
        Shape::Sparql(_) => C_SPARQL,
    };
    computing[i] = false;
    memo[i] = Some(cost);
    cost
}

fn path_cost(p: &Path) -> u64 {
    match p {
        Path::Id => 0,
        Path::Pred(_) => 1,
        Path::Inverse(inner) => 1 + path_cost(inner),
        Path::Seq(ps) | Path::Alt(ps) => ps.iter().map(path_cost).sum::<u64>().max(1),
        Path::Star(inner) => C_STAR * (1 + path_cost(inner)),
    }
}

// ---- rendering (inspect --stage plan) ----

/// Render a plan as text for `shacl inspect --stage plan`.
pub fn plan_to_text(plan: &PhysicalPlan) -> String {
    let mut out = String::new();
    out.push_str(&format!("plan: {} statement(s)\n", plan.statements.len()));
    for (i, st) in plan.statements.iter().enumerate() {
        out.push_str(&format!(
            "  [{i}] {}  ⇒  @{}\n",
            focus_to_string(&st.source),
            st.shape.0
        ));
    }

    let costs = compute_costs(&plan.arena);
    let reachable = reachable_shapes(plan);
    out.push_str("shapes (cost-ordered):\n");
    for id in &reachable {
        out.push_str(&format!(
            "  @{} [cost {}] = {}\n",
            id.0,
            costs[id.0 as usize],
            shape_to_string(&plan.arena, *id),
        ));
    }
    out
}

fn focus_to_string(source: &FocusSource) -> String {
    match source {
        FocusSource::SubjectsOf(p) => format!("subjectsOf({p})"),
        FocusSource::ObjectsOf(p) => format!("objectsOf({p})"),
        FocusSource::Node(c) => format!("node({c})"),
        FocusSource::PathToConst { path, target } => {
            format!("seed {target} ⟵ {}", path_to_string(path))
        }
        FocusSource::ScanFilter { path, qualifier } => {
            format!("scan ∃ {} . @{}", path_to_string(path), qualifier.0)
        }
        FocusSource::Sparql(_) => "sparql{…}".to_string(),
    }
}

fn reachable_shapes(plan: &PhysicalPlan) -> BTreeSet<ShapeId> {
    let mut stack: Vec<ShapeId> = Vec::new();
    for st in &plan.statements {
        stack.push(st.shape);
        if let FocusSource::ScanFilter { qualifier, .. } = &st.source {
            stack.push(*qualifier);
        }
    }
    let mut seen = BTreeSet::new();
    while let Some(id) = stack.pop() {
        if seen.insert(id) {
            match plan.arena.get(id) {
                Shape::Annotated { shape, .. } => stack.push(*shape),
                Shape::Not(c) => stack.push(*c),
                Shape::And(cs) | Shape::Or(cs) => stack.extend(cs.iter().copied()),
                Shape::Count { qualifier, .. } => stack.push(*qualifier),
                _ => {}
            }
        }
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;
    use shifty_algebra::{NodeKindSet, Statement};

    fn nn(s: &str) -> NamedNode {
        NamedNode::new(s).unwrap()
    }

    fn schema_with(arena: ShapeArena, selector: Selector, shape: ShapeId) -> Schema {
        Schema {
            arena,
            statements: vec![Statement { selector, shape }],
            rules: Vec::new(),
            names: Default::default(),
        }
    }

    #[test]
    fn reorders_and_cheap_first() {
        // And([expensive count over a star path, cheap nodeKind]) → cheap first
        let mut a = ShapeArena::new();
        let kind = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let top = a.insert(Shape::Top);
        let star = Path::star(Path::Pred(nn("http://ex/p")));
        let count = a.insert(Shape::Count {
            path: star,
            min: Some(1),
            max: None,
            qualifier: top,
        });
        let and = a.insert(Shape::And(vec![count, kind])); // expensive first
        let p = plan(&schema_with(
            a,
            Selector::IsConst(Term::NamedNode(nn("http://ex/x"))),
            and,
        ));
        match p.arena.get(and) {
            Shape::And(cs) => assert_eq!(cs, &vec![kind, count]), // cheap nodeKind moved first
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn class_target_seeds_from_constant() {
        // HasPath(rdf:type/subClassOf*, test(ex:Person)) → PathToConst seed
        let mut a = ShapeArena::new();
        let class = Term::NamedNode(nn("http://ex/Person"));
        let test = a.insert(Shape::TestConst(class.clone()));
        let path = Path::seq(vec![
            Path::Pred(nn("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")),
            Path::star(Path::Pred(nn(
                "http://www.w3.org/2000/01/rdf-schema#subClassOf",
            ))),
        ]);
        let root = a.insert(Shape::TestKind(NodeKindSet::IRI));
        let p = plan(&schema_with(a, Selector::HasPath(path.clone(), test), root));
        assert_eq!(
            p.statements[0].source,
            FocusSource::PathToConst {
                path,
                target: class
            }
        );
    }

    #[test]
    fn simple_selectors_compile() {
        let mut a = ShapeArena::new();
        let root = a.insert(Shape::Top);
        let p = plan(&schema_with(a, Selector::HasOut(nn("http://ex/q")), root));
        assert_eq!(
            p.statements[0].source,
            FocusSource::SubjectsOf(nn("http://ex/q"))
        );
    }
}
