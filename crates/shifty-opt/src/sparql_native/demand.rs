//! Stage 1 path-demand extraction.
//!
//! Collects the set of property paths referenced in a normalized schema and
//! in SPARQL queries, annotated with their expected access pattern. In stage 1
//! these counts are static approximations; stage 2+ fills in data-derived
//! expected-focus counts.

use shifty_algebra::render::path_to_string;
use shifty_algebra::{Path, Schema, Selector, Shape, ShapeId};
use spargebra::Query;
use spargebra::algebra::{GraphPattern, PropertyPathExpression};
use spargebra::term::TermPattern;
use std::collections::HashMap;

/// Canonical path identifier — the `path_to_string` rendering, which is unique
/// up to the light normalization applied by the `Path` smart constructors.
pub type PathId = String;

/// Static demand estimate for one path (doc §187). The counts are
/// per-occurrence in the AST; stage 2 will multiply by observed focus-node
/// counts once a dataset is available.
#[derive(Debug, Clone, Default)]
pub struct PathDemand {
    pub path: PathId,
    /// Occurrences where start is bound and end is free (or `$this` is subject).
    pub forward_probes: u64,
    /// Occurrences where end is bound and start is free.
    pub reverse_probes: u64,
    /// Occurrences where both endpoints are bound (membership test).
    pub membership_probes: u64,
    /// Occurrences where both endpoints are free (full relation scan).
    pub open_scans: u64,
    /// Rough focus-node multiplier (populated from dataset stats in stage 2).
    pub expected_focuses: u64,
}

fn insert_or_update(
    demand: &mut HashMap<PathId, PathDemand>,
    key: PathId,
    update: impl FnOnce(&mut PathDemand),
) {
    let d = demand.entry(key.clone()).or_insert_with(|| PathDemand {
        path: key,
        ..Default::default()
    });
    update(d);
}

/// Extract path demand from a SPARQL query. The `$this` variable is considered
/// bound at the subject position of a path (SHACL focus-node prebinding).
pub fn extract_sparql_demand(query: &Query) -> HashMap<PathId, PathDemand> {
    let mut demand: HashMap<PathId, PathDemand> = HashMap::new();
    let this = spargebra::term::Variable::new("this").unwrap();
    let pattern = match query {
        Query::Select { pattern, .. } | Query::Ask { pattern, .. } => pattern,
        _ => return demand,
    };
    collect_from_pattern(pattern, &this, &mut demand);
    demand
}

/// Extract path demand from the SHACL algebra (selectors + shape constraints).
pub fn extract_algebra_demand(schema: &Schema) -> HashMap<PathId, PathDemand> {
    let mut demand: HashMap<PathId, PathDemand> = HashMap::new();

    for st in &schema.statements {
        if let Selector::HasPath(path, _) = &st.selector {
            let key = path_to_string(path);
            insert_or_update(&mut demand, key, |d| d.forward_probes += 1);
        }
    }

    for i in 0..schema.arena.len() {
        let id = ShapeId(i as u32);
        let path = match schema.arena.get(id) {
            Shape::Eq(p, _)
            | Shape::Disj(p, _)
            | Shape::Lt(p, _)
            | Shape::Le(p, _)
            | Shape::UniqueLang(p) => Some(p.clone()),
            Shape::Count { path, .. } => Some(path.clone()),
            _ => None,
        };
        if let Some(p) = path {
            let key = path_to_string(&p);
            insert_or_update(&mut demand, key, |d| d.forward_probes += 1);
        }
    }

    demand
}

fn collect_from_pattern(
    pattern: &GraphPattern,
    this: &spargebra::term::Variable,
    demand: &mut HashMap<PathId, PathDemand>,
) {
    match pattern {
        GraphPattern::Path {
            subject,
            path,
            object,
        } => {
            if let Some(alg_path) = property_path_to_algebra(path) {
                let key = path_to_string(&alg_path);
                let s_bound = tp_is_bound(subject, this);
                let o_bound = tp_is_bound(object, this);
                insert_or_update(demand, key, |d| match (s_bound, o_bound) {
                    (true, true) => d.membership_probes += 1,
                    (true, false) => d.forward_probes += 1,
                    (false, true) => d.reverse_probes += 1,
                    (false, false) => d.open_scans += 1,
                });
            }
        }
        GraphPattern::Bgp { .. } | GraphPattern::Values { .. } => {}
        GraphPattern::Join { left, right }
        | GraphPattern::Union { left, right }
        | GraphPattern::Minus { left, right } => {
            collect_from_pattern(left, this, demand);
            collect_from_pattern(right, this, demand);
        }
        GraphPattern::Lateral { left, right } => {
            collect_from_pattern(left, this, demand);
            collect_from_pattern(right, this, demand);
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            collect_from_pattern(left, this, demand);
            collect_from_pattern(right, this, demand);
        }
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Graph { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. }
        | GraphPattern::Service { inner, .. } => {
            collect_from_pattern(inner, this, demand);
        }
    }
}

fn tp_is_bound(tp: &TermPattern, this: &spargebra::term::Variable) -> bool {
    match tp {
        TermPattern::Variable(v) => v == this,
        TermPattern::NamedNode(_) | TermPattern::BlankNode(_) | TermPattern::Literal(_) => true,
        #[allow(unreachable_patterns)]
        _ => false,
    }
}

/// Convert a spargebra `PropertyPathExpression` to the `shifty_algebra::Path`
/// algebra. Returns `None` for `NegatedPropertySet` which has no equivalent.
pub fn property_path_to_algebra(path: &PropertyPathExpression) -> Option<Path> {
    match path {
        PropertyPathExpression::NamedNode(n) => Some(Path::Pred(n.clone())),
        PropertyPathExpression::Reverse(p) => {
            property_path_to_algebra(p).map(|inner| inner.inverse())
        }
        PropertyPathExpression::Sequence(a, b) => {
            let la = property_path_to_algebra(a)?;
            let lb = property_path_to_algebra(b)?;
            Some(Path::seq(vec![la, lb]))
        }
        PropertyPathExpression::Alternative(a, b) => {
            let la = property_path_to_algebra(a)?;
            let lb = property_path_to_algebra(b)?;
            Some(Path::alt(vec![la, lb]))
        }
        PropertyPathExpression::ZeroOrMore(p) => {
            property_path_to_algebra(p).map(|inner| inner.star())
        }
        PropertyPathExpression::OneOrMore(p) => {
            property_path_to_algebra(p).map(|inner| inner.one_or_more())
        }
        PropertyPathExpression::ZeroOrOne(p) => {
            property_path_to_algebra(p).map(|inner| inner.zero_or_one())
        }
        PropertyPathExpression::NegatedPropertySet(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spargebra::SparqlParser;

    fn parse(q: &str) -> Query {
        SparqlParser::new().parse_query(q).unwrap()
    }

    #[test]
    fn path_star_demand() {
        let q = parse(
            "ASK { ?this <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>/<http://www.w3.org/2000/01/rdf-schema#subClassOf>* <http://ex/C> }",
        );
        let demand = extract_sparql_demand(&q);
        // The path appears with ?this (bound) as subject → forward probe
        assert!(!demand.is_empty());
        let totals: u64 = demand
            .values()
            .map(|d| d.forward_probes + d.membership_probes)
            .sum();
        assert!(totals > 0, "expected at least one forward/membership probe");
    }

    #[test]
    fn algebra_demand_picks_up_count_paths() {
        use shifty_algebra::{NamedNode, NodeKindSet, ShapeArena, Statement};
        let nn = |s: &str| NamedNode::new(s).unwrap();
        let mut arena = ShapeArena::new();
        let top = arena.insert(Shape::TestKind(NodeKindSet::IRI));
        let path = Path::Pred(nn("http://ex/p"));
        arena.insert(Shape::Count {
            path,
            min: Some(1),
            max: None,
            qualifier: top,
        });
        let schema = Schema {
            arena,
            statements: vec![Statement {
                selector: Selector::HasOut(nn("http://ex/q")),
                shape: top,
            }],
            rules: Vec::new(),
            names: Default::default(),
        };
        let demand = extract_algebra_demand(&schema);
        // path_to_string wraps unknown IRIs in <>, so the key is "<http://ex/p>"
        assert!(
            demand.contains_key("<http://ex/p>"),
            "should record path <http://ex/p>; got: {demand:?}"
        );
    }
}
