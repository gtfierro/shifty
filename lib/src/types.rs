use crate::backend::Binding;
use crate::context::{Context, SourceShape, ValidationContext};
use crate::named_nodes::SHACL;
use oxigraph::model::{NamedNodeRef, NamedOrBlankNodeRef, Term, TermRef, Variable};
use oxigraph::sparql::QueryResults;
pub use shacl_ir::{
    ComponentDescriptor, ComponentID, FeatureToggles, NodeShapeIR, ParameterBindings, Path,
    PropShapeID, PropertyShapeIR, Rule, RuleCondition, RuleID, Severity, ShapeIR, Target,
    TriplePatternTerm, ID,
};
use std::fmt;
use std::hash::Hash;

// ----------- Extension helpers (runtime-specific) -----------

pub(crate) trait TargetEvalExt {
    fn get_target_nodes(
        &self,
        context: &ValidationContext,
        source_shape: SourceShape,
    ) -> Result<Vec<Context>, String>;
}

impl TargetEvalExt for Target {
    fn get_target_nodes(
        &self,
        context: &ValidationContext,
        source_shape: SourceShape,
    ) -> Result<Vec<Context>, String> {
        match self {
            Target::Node(t) => Ok(contexts_from_terms(
                context,
                std::iter::once(t.clone()),
                source_shape,
            )),
            Target::Class(c) => {
                let query_str = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    SELECT DISTINCT ?inst ?target_class WHERE { ?inst rdf:type ?c . ?c rdfs:subClassOf* ?target_class }";
                let target_class_var = Variable::new("target_class").map_err(|e| e.to_string())?;

                let prepared = context.prepare_query(query_str).map_err(|e| {
                    format!(
                        "SPARQL parse error for Target::Class: {} {:?}",
                        query_str, e
                    )
                })?;

                let substitutions: Vec<Binding> = vec![(target_class_var, c.clone())];
                let results = context
                    .execute_prepared(query_str, &prepared, &substitutions, false)
                    .map_err(|e| {
                        format!("SPARQL query error for Target::Class: {} {}", query_str, e)
                    })?;

                match results {
                    QueryResults::Solutions(solutions) => solutions
                        .map(|solution_result| {
                            let solution = solution_result.map_err(|e| e.to_string())?;
                            solution
                                .get("inst")
                                .map(|term_ref| {
                                    build_context(
                                        context,
                                        term_ref.to_owned(),
                                        source_shape.clone(),
                                    )
                                })
                                .ok_or_else(|| {
                                    "Variable 'inst' not found in Target::Class query solution"
                                        .to_string()
                                })
                        })
                        .collect(),
                    _ => Err(format!(
                        "Unexpected result type for Target::Class: {}",
                        query_str
                    )),
                }
            }
            Target::SubjectsOf(p) => {
                if let Term::NamedNode(predicate_node) = p {
                    let query_str = format!(
                        "SELECT DISTINCT ?s WHERE {{ ?s <{}> ?any . }}",
                        predicate_node.as_str()
                    );
                    let prepared = context.prepare_query(&query_str).map_err(|e| {
                        format!(
                            "SPARQL parse error for Target::SubjectsOf: {} {:?}",
                            query_str, e
                        )
                    })?;

                    let results = context
                        .execute_prepared(&query_str, &prepared, &[], false)
                        .map_err(|e| e.to_string())?;

                    match results {
                        QueryResults::Solutions(solutions) => solutions
                            .map(|solution_result| {
                                let solution = solution_result.map_err(|e| e.to_string())?;
                                solution
                                    .get("s")
                                    .map(|term_ref| {
                                        build_context(
                                            context,
                                            term_ref.to_owned(),
                                            source_shape.clone(),
                                        )
                                    })
                                    .ok_or_else(|| {
                                        "Variable 's' not found in Target::SubjectsOf query solution"
                                            .to_string()
                                    })
                            })
                            .collect(),
                        _ => Err(format!(
                            "Unexpected result type for Target::SubjectsOf: {}",
                            query_str
                        )),
                    }
                } else {
                    Err(format!("SubjectsOf target requires NamedNode, got {:?}", p))
                }
            }
            Target::ObjectsOf(p) => {
                if let Term::NamedNode(predicate_node) = p {
                    let query_str = format!(
                        "SELECT DISTINCT ?o WHERE {{ ?any <{}> ?o . }}",
                        predicate_node.as_str()
                    );
                    let prepared = context.prepare_query(&query_str).map_err(|e| {
                        format!(
                            "SPARQL parse error for Target::ObjectsOf: {} {:?}",
                            query_str, e
                        )
                    })?;

                    let results = context
                        .execute_prepared(&query_str, &prepared, &[], false)
                        .map_err(|e| e.to_string())?;

                    match results {
                        QueryResults::Solutions(solutions) => solutions
                            .map(|solution_result| {
                                let solution = solution_result.map_err(|e| e.to_string())?;
                                solution
                                    .get("o")
                                    .map(|term_ref| {
                                        build_context(
                                            context,
                                            term_ref.to_owned(),
                                            source_shape.clone(),
                                        )
                                    })
                                    .ok_or_else(|| {
                                        "Variable 'o' not found in Target::ObjectsOf query solution"
                                            .to_string()
                                    })
                            })
                            .collect(),
                        _ => Err(format!(
                            "Unexpected result type for Target::ObjectsOf: {}",
                            query_str
                        )),
                    }
                } else {
                    Err(format!("ObjectsOf target requires NamedNode, got {:?}", p))
                }
            }
            Target::Advanced(selector) => {
                if let Some(cached) = context.cached_advanced_target(selector) {
                    Ok(contexts_from_terms(
                        context,
                        cached.into_iter(),
                        source_shape,
                    ))
                } else {
                    // SHACL-AF advanced target: sh:select is the common case for AF tests
                    // (filter/ask handling can be added later).
                    let sh = SHACL::new();
                    let selector_ref = selector
                        .try_to_subject_ref()
                        .map_err(|e| format!("Invalid selector term {:?}: {}", selector, e))?;

                    let prefixes = context.prefixes_for_node(selector).map_err(|e| {
                        format!("Failed to resolve prefixes for advanced target: {}", e)
                    })?;

                    // sh:select branch
                    let select_q = context
                        .quads_for_pattern(None, Some(sh.select), None, None)?
                        .into_iter()
                        .find_map(|q| {
                            if q.subject == selector_ref.into() {
                                if let Term::Literal(l) = q.object {
                                    return Some(l.value().to_string());
                                }
                            }
                            None
                        });

                    let select_q = select_q.or_else(|| {
                        context
                            .quads_for_pattern(None, Some(sh.select), None, None)
                            .ok()
                            .and_then(|iter| {
                                iter.into_iter().find_map(|q| match q.object {
                                    Term::Literal(l) => Some(l.value().to_string()),
                                    _ => None,
                                })
                            })
                    });

                    if let Some(select_q) = select_q {
                        let query_str = if prefixes.trim().is_empty() {
                            select_q.clone()
                        } else {
                            format!("{}\n{}", prefixes, select_q)
                        };

                        let prepared = context.prepare_query(&query_str).map_err(|e| {
                            format!(
                                "SPARQL parse error for Target::Advanced select: {:?} {}",
                                e, query_str
                            )
                        })?;

                        let results = context
                            .execute_prepared(&query_str, &prepared, &[], true)
                            .map_err(|e| {
                                format!(
                                    "SPARQL query error for Target::Advanced select: {} {}",
                                    query_str, e
                                )
                            })?;

                        let mut targets = Vec::new();
                        if let QueryResults::Solutions(solutions) = results {
                            for solution_res in solutions {
                                let solution = solution_res.map_err(|e| e.to_string())?;
                                if let Some(t) = solution.get("this") {
                                    targets.push(t.to_owned());
                                } else if let Some(t) = solution.get("target") {
                                    targets.push(t.to_owned());
                                }
                            }
                        }
                        context.store_advanced_target(selector, &targets);
                        return Ok(contexts_from_terms(
                            context,
                            targets.into_iter(),
                            source_shape,
                        ));
                    }

                    Err("Unsupported advanced target (no sh:select found)".to_string())
                }
            }
        }
    }
}

pub fn target_from_predicate_object(predicate: NamedNodeRef, object: TermRef) -> Option<Target> {
    let shacl = SHACL::new();
    if predicate == shacl.target_class {
        Some(Target::Class(object.into_owned()))
    } else if predicate == shacl.target_node {
        Some(Target::Node(object.into_owned()))
    } else if predicate == shacl.target_subjects_of {
        Some(Target::SubjectsOf(object.into_owned()))
    } else if predicate == shacl.target_objects_of {
        Some(Target::ObjectsOf(object.into_owned()))
    } else if predicate == shacl.target || predicate == shacl.target_validator {
        Some(Target::Advanced(object.into_owned()))
    } else {
        None
    }
}

pub trait SeverityExt {
    fn from_term(term: &Term) -> Option<Severity>;
}
impl SeverityExt for Severity {
    fn from_term(term: &Term) -> Option<Severity> {
        let shacl = SHACL::new();
        match term {
            Term::NamedNode(nn) if *nn == shacl.info => Some(Severity::Info),
            Term::NamedNode(nn) if *nn == shacl.warning => Some(Severity::Warning),
            Term::NamedNode(nn) if *nn == shacl.violation => Some(Severity::Violation),
            Term::NamedNode(nn) => Some(Severity::Custom(nn.clone())),
            _ => None,
        }
    }
}

// ----------- existing ToSubjectRef etc. -----------

/// A trait for converting `Term` or `TermRef` into `SubjectRef`.
pub(crate) trait ToSubjectRef {
    fn try_to_subject_ref(&self) -> Result<NamedOrBlankNodeRef<'_>, String>;
}

impl ToSubjectRef for Term {
    fn try_to_subject_ref(&self) -> Result<NamedOrBlankNodeRef<'_>, String> {
        match self {
            Term::NamedNode(n) => Ok(NamedOrBlankNodeRef::NamedNode(n.as_ref())),
            Term::BlankNode(b) => Ok(NamedOrBlankNodeRef::BlankNode(b.as_ref())),
            _ => Err(format!("Invalid subject term {:?}", self)),
        }
    }
}

impl<'a> ToSubjectRef for TermRef<'a> {
    fn try_to_subject_ref(&self) -> Result<NamedOrBlankNodeRef<'a>, String> {
        match self {
            TermRef::NamedNode(n) => Ok(NamedOrBlankNodeRef::NamedNode(*n)),
            TermRef::BlankNode(b) => Ok(NamedOrBlankNodeRef::BlankNode(*b)),
            _ => Err(format!("Invalid subject term {:?}", self)),
        }
    }
}

// ----------- Context helpers copied from old types.rs -----------

fn contexts_from_terms(
    context: &ValidationContext,
    terms: impl Iterator<Item = Term>,
    source_shape: SourceShape,
) -> Vec<Context> {
    terms
        .map(|term| build_context(context, term, source_shape.clone()))
        .collect()
}

fn build_context(context: &ValidationContext, focus: Term, source_shape: SourceShape) -> Context {
    Context::new(focus, None, None, source_shape, context.new_trace())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum TraceItem {
    NodeShape(ID),
    PropertyShape(PropShapeID),
    Component(ComponentID),
}

impl fmt::Display for TraceItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TraceItem::NodeShape(id) => write!(f, "NodeShape({})", id.to_graphviz_id()),
            TraceItem::PropertyShape(id) => write!(f, "PropertyShape({})", id.to_graphviz_id()),
            TraceItem::Component(id) => write!(f, "Component({})", id.to_graphviz_id()),
        }
    }
}
