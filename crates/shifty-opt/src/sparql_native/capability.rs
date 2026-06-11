//! Stage 1 capability analysis: classify a parsed SPARQL query as native-capable
//! or fallback-only for the native executor planned in stages 3+.
//!
//! `analyze_capability` walks the Spargebra AST and returns `Native` if every
//! node is in the supported subset defined in `docs/05-sparql-execution.md §128`,
//! or `Fallback { first_unsupported }` recording the first unsupported construct.
//! In stage 1 this is **read-only**: nothing routes on the result yet.

use spargebra::Query;
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::NamedNodePattern;

/// Whether a query can be executed by the planned native engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Capability {
    Native,
    Fallback { first_unsupported: String },
}

impl Capability {
    pub fn is_native(&self) -> bool {
        matches!(self, Capability::Native)
    }

    pub fn fallback_reason(&self) -> Option<&str> {
        match self {
            Capability::Fallback { first_unsupported } => Some(first_unsupported.as_str()),
            Capability::Native => None,
        }
    }

    /// Short-circuit: return `self` if Fallback, else evaluate `f`.
    fn and(self, f: impl FnOnce() -> Self) -> Self {
        match self {
            Capability::Native => f(),
            fallback => fallback,
        }
    }
}

/// Classify `query` as native or fallback. The native capability set matches
/// `docs/05-sparql-execution.md §128-141`:
/// - SELECT and ASK only (no CONSTRUCT, DESCRIBE)
/// - BGPs, fixed named-graph patterns, property paths
/// - JOIN, UNION, PROJECT, DISTINCT
/// - FILTER / EXISTS / NOT EXISTS with supported expressions
/// - Simple BIND (EXTEND)
/// - Inline VALUES
///
/// Fallback triggers on: aggregates, ORDER BY, LIMIT/OFFSET, OPTIONAL, MINUS,
/// LATERAL, SERVICE, variable graph names.
pub fn analyze_capability(query: &Query) -> Capability {
    match query {
        Query::Select { pattern, .. } => check_pattern(pattern),
        Query::Ask { pattern, .. } => check_pattern(pattern),
        Query::Construct { .. } => Capability::Fallback {
            first_unsupported: "CONSTRUCT query".into(),
        },
        Query::Describe { .. } => Capability::Fallback {
            first_unsupported: "DESCRIBE query".into(),
        },
    }
}

fn check_pattern(pattern: &GraphPattern) -> Capability {
    match pattern {
        GraphPattern::Bgp { .. } => Capability::Native,
        // All standard property-path forms (predicate, reverse, seq, alt, */+/?)
        GraphPattern::Path { .. } => Capability::Native,
        GraphPattern::Join { left, right } | GraphPattern::Union { left, right } => {
            check_pattern(left).and(|| check_pattern(right))
        }
        GraphPattern::Filter { expr, inner } => check_expr(expr).and(|| check_pattern(inner)),
        GraphPattern::Graph { name, inner } => match name {
            NamedNodePattern::NamedNode(_) => check_pattern(inner),
            NamedNodePattern::Variable(_) => Capability::Fallback {
                first_unsupported: "variable GRAPH name".into(),
            },
        },
        GraphPattern::Extend {
            inner, expression, ..
        } => check_pattern(inner).and(|| check_expr(expression)),
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner } => check_pattern(inner),
        GraphPattern::Values { .. } => Capability::Native,
        // ---- fallback cases ----
        GraphPattern::OrderBy { .. } => Capability::Fallback {
            first_unsupported: "ORDER BY".into(),
        },
        GraphPattern::Slice { .. } => Capability::Fallback {
            first_unsupported: "LIMIT/OFFSET".into(),
        },
        GraphPattern::Group { .. } => Capability::Fallback {
            first_unsupported: "aggregates (GROUP BY)".into(),
        },
        GraphPattern::Service { .. } => Capability::Fallback {
            first_unsupported: "SERVICE".into(),
        },
        GraphPattern::LeftJoin { .. } => Capability::Fallback {
            first_unsupported: "OPTIONAL".into(),
        },
        GraphPattern::Minus { .. } => Capability::Fallback {
            first_unsupported: "MINUS".into(),
        },
        GraphPattern::Lateral { .. } => Capability::Fallback {
            first_unsupported: "LATERAL".into(),
        },
    }
}

fn check_expr(expr: &Expression) -> Capability {
    match expr {
        Expression::Variable(_) | Expression::NamedNode(_) | Expression::Literal(_) => {
            Capability::Native
        }
        Expression::Bound(_) => Capability::Native,
        Expression::Or(a, b)
        | Expression::And(a, b)
        | Expression::Equal(a, b)
        | Expression::SameTerm(a, b)
        | Expression::Greater(a, b)
        | Expression::GreaterOrEqual(a, b)
        | Expression::Less(a, b)
        | Expression::LessOrEqual(a, b)
        | Expression::Add(a, b)
        | Expression::Subtract(a, b)
        | Expression::Multiply(a, b)
        | Expression::Divide(a, b) => check_expr(a).and(|| check_expr(b)),
        Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => check_expr(a),
        Expression::In(a, list) => check_expr(a).and(|| {
            list.iter()
                .fold(Capability::Native, |c, e| c.and(|| check_expr(e)))
        }),
        // Correlated EXISTS / NOT EXISTS (doc §133)
        Expression::Exists(pattern) => check_pattern(pattern),
        Expression::If(a, b, c) => check_expr(a).and(|| check_expr(b)).and(|| check_expr(c)),
        // Function calls and COALESCE: all arguments must be supported.
        // Individual unsupported functions are identified in stage 3 when we
        // have an actual expression evaluator.
        Expression::Coalesce(list) | Expression::FunctionCall(_, list) => list
            .iter()
            .fold(Capability::Native, |c, e| c.and(|| check_expr(e))),
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
    fn simple_bgp_is_native() {
        let q = parse("SELECT ?s WHERE { ?s <http://ex/p> ?o }");
        assert!(analyze_capability(&q).is_native());
    }

    #[test]
    fn ask_is_native() {
        let q = parse("ASK { ?s <http://ex/p> ?o }");
        assert!(analyze_capability(&q).is_native());
    }

    #[test]
    fn property_path_is_native() {
        let q = parse("SELECT ?s WHERE { ?s <http://ex/p>* ?o }");
        assert!(analyze_capability(&q).is_native());
    }

    #[test]
    fn order_by_is_fallback() {
        let q = parse("SELECT ?s WHERE { ?s <http://ex/p> ?o } ORDER BY ?s");
        assert!(!analyze_capability(&q).is_native());
        assert_eq!(analyze_capability(&q).fallback_reason(), Some("ORDER BY"));
    }

    #[test]
    fn group_by_is_fallback() {
        let q = parse("SELECT (COUNT(?o) AS ?n) WHERE { ?s <http://ex/p> ?o } GROUP BY ?s");
        assert!(!analyze_capability(&q).is_native());
    }

    #[test]
    fn optional_is_fallback() {
        let q = parse("SELECT ?s WHERE { ?s <http://ex/p> ?o OPTIONAL { ?s <http://ex/q> ?r } }");
        assert!(!analyze_capability(&q).is_native());
    }

    #[test]
    fn exists_filter_is_native() {
        let q = parse("ASK { ?this <http://ex/p> ?v FILTER EXISTS { ?v <http://ex/q> ?w } }");
        assert!(analyze_capability(&q).is_native());
    }
}
