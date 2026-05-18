use crate::diagnostics::{Diagnostic, DiagnosticSeverity, SourceRef};
use crate::source::{ResolvedShapeSet, ShapeSource, SourceLoadOptions, load_with_ontoenv};
use crate::syntax::{
    AdvancedTargetSyntax, ConstraintComponentSyntax, ConstraintSyntax, ParameterSyntax,
    PredicateObjects, PrefixDeclarationSyntax, RuleSyntax, RuleSyntaxKind, ShapeSyntax,
    ShapeSyntaxDocument, ShapeSyntaxKind, SparqlConstraintSyntax, SparqlValidatorSyntax,
    TargetSyntax,
};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use std::collections::{HashMap, HashSet};
use std::error::Error;

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const SH_NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";
const SH_PROPERTY_SHAPE: &str = "http://www.w3.org/ns/shacl#PropertyShape";
const SH_TRIPLE_RULE: &str = "http://www.w3.org/ns/shacl#TripleRule";
const SH_SPARQL_RULE: &str = "http://www.w3.org/ns/shacl#SPARQLRule";
const SH_TARGET: &str = "http://www.w3.org/ns/shacl#target";
const SH_TARGET_CLASS: &str = "http://www.w3.org/ns/shacl#targetClass";
const SH_TARGET_NODE: &str = "http://www.w3.org/ns/shacl#targetNode";
const SH_TARGET_SUBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetSubjectsOf";
const SH_TARGET_OBJECTS_OF: &str = "http://www.w3.org/ns/shacl#targetObjectsOf";
const SH_PATH: &str = "http://www.w3.org/ns/shacl#path";
const SH_PROPERTY: &str = "http://www.w3.org/ns/shacl#property";
const SH_NODE: &str = "http://www.w3.org/ns/shacl#node";
const SH_NOT: &str = "http://www.w3.org/ns/shacl#not";
const SH_AND: &str = "http://www.w3.org/ns/shacl#and";
const SH_OR: &str = "http://www.w3.org/ns/shacl#or";
const SH_XONE: &str = "http://www.w3.org/ns/shacl#xone";
const SH_QUALIFIED_VALUE_SHAPE: &str = "http://www.w3.org/ns/shacl#qualifiedValueShape";
const SH_CONSTRAINT_COMPONENT: &str = "http://www.w3.org/ns/shacl#ConstraintComponent";
const SH_SPARQL: &str = "http://www.w3.org/ns/shacl#sparql";
const SH_RULE: &str = "http://www.w3.org/ns/shacl#rule";
const SH_CONDITION: &str = "http://www.w3.org/ns/shacl#condition";
const SH_SEVERITY: &str = "http://www.w3.org/ns/shacl#severity";
const SH_DEACTIVATED: &str = "http://www.w3.org/ns/shacl#deactivated";
const SH_ORDER: &str = "http://www.w3.org/ns/shacl#order";
const SH_TARGET_SHAPE: &str = "http://www.w3.org/ns/shacl#targetShape";
const SH_FILTER_SHAPE: &str = "http://www.w3.org/ns/shacl#filterShape";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const RDFS_COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
const SH_PARAMETER: &str = "http://www.w3.org/ns/shacl#parameter";
const SH_MESSAGE: &str = "http://www.w3.org/ns/shacl#message";
const SH_PREFIXES: &str = "http://www.w3.org/ns/shacl#prefixes";
const SH_DECLARE: &str = "http://www.w3.org/ns/shacl#declare";
const SH_VAR_NAME: &str = "http://www.w3.org/ns/shacl#varName";
const SH_NAME: &str = "http://www.w3.org/ns/shacl#name";
const SH_DESCRIPTION: &str = "http://www.w3.org/ns/shacl#description";
const SH_OPTIONAL: &str = "http://www.w3.org/ns/shacl#optional";
const SH_DEFAULT_VALUE: &str = "http://www.w3.org/ns/shacl#defaultValue";
const SH_DATATYPE: &str = "http://www.w3.org/ns/shacl#datatype";
const SH_NODE_VALIDATOR: &str = "http://www.w3.org/ns/shacl#nodeValidator";
const SH_PROPERTY_VALIDATOR: &str = "http://www.w3.org/ns/shacl#propertyValidator";
const SH_VALIDATOR: &str = "http://www.w3.org/ns/shacl#validator";
const SH_SELECT: &str = "http://www.w3.org/ns/shacl#select";
const SH_ASK: &str = "http://www.w3.org/ns/shacl#ask";
const SH_PREFIX: &str = "http://www.w3.org/ns/shacl#prefix";
const SH_NAMESPACE: &str = "http://www.w3.org/ns/shacl#namespace";
const SH_LABEL_TEMPLATE: &str = "http://www.w3.org/ns/shacl#labelTemplate";

pub fn load_and_parse_with_ontoenv(
    sources: &[ShapeSource],
    options: &SourceLoadOptions,
) -> Result<ShapeSyntaxDocument, Box<dyn Error>> {
    let resolved = load_with_ontoenv(sources, options)?;
    Ok(parse_resolved(&resolved))
}

pub fn parse_quads<I>(quads: I) -> ShapeSyntaxDocument
where
    I: IntoIterator<Item = Quad>,
{
    parse_resolved(&ResolvedShapeSet {
        root_graphs: Vec::new(),
        imported_graphs: Vec::new(),
        quads: quads.into_iter().collect(),
    })
}

pub fn parse_resolved(resolved: &ResolvedShapeSet) -> ShapeSyntaxDocument {
    let quads = resolved.quads.clone();
    let provenance_by_graph = provenance_map(resolved);
    let mut by_subject: HashMap<Term, Vec<Quad>> = HashMap::new();
    let mut candidate_shapes = HashSet::new();
    let mut candidate_rules = HashSet::new();
    let mut candidate_components = HashSet::new();
    let mut parameter_nodes = HashSet::new();

    for quad in &quads {
        let subject_term = subject_to_term(&quad.subject);
        by_subject
            .entry(subject_term.clone())
            .or_default()
            .push(quad.clone());

        let pred = quad.predicate.as_str();
        if pred == RDF_TYPE {
            if matches_named(&quad.object, SH_NODE_SHAPE)
                || matches_named(&quad.object, SH_PROPERTY_SHAPE)
            {
                candidate_shapes.insert(subject_term.clone());
            }
            if matches_named(&quad.object, SH_TRIPLE_RULE)
                || matches_named(&quad.object, SH_SPARQL_RULE)
            {
                candidate_rules.insert(subject_term.clone());
            }
            if matches_named(&quad.object, SH_CONSTRAINT_COMPONENT) {
                candidate_components.insert(subject_term.clone());
            }
        }
        if pred == SH_PARAMETER {
            parameter_nodes.insert(quad.object.clone());
        }
        if matches!(
            pred,
            SH_TARGET
                | SH_TARGET_CLASS
                | SH_TARGET_NODE
                | SH_TARGET_SUBJECTS_OF
                | SH_TARGET_OBJECTS_OF
                | SH_PATH
                | SH_PROPERTY
                | SH_SPARQL
                | SH_RULE
        ) {
            candidate_shapes.insert(subject_term);
        }
        if pred == SH_RULE {
            candidate_rules.insert(quad.object.clone());
        }
    }

    discover_inline_shapes_and_rules(&by_subject, &mut candidate_shapes, &mut candidate_rules);
    candidate_shapes.retain(|subject| {
        !parameter_nodes.contains(subject)
            || by_subject.get(subject).into_iter().flatten().any(|quad| {
                quad.predicate.as_str() == RDF_TYPE
                    && (matches_named(&quad.object, SH_NODE_SHAPE)
                        || matches_named(&quad.object, SH_PROPERTY_SHAPE))
            })
    });

    let mut shapes: Vec<ShapeSyntax> = candidate_shapes
        .into_iter()
        .map(|subject| {
            build_shape(
                &subject,
                by_subject.get(&subject),
                &by_subject,
                &provenance_by_graph,
            )
        })
        .collect();
    shapes.sort_by_key(|shape| shape.subject.to_string());

    let mut rules: Vec<RuleSyntax> = candidate_rules
        .into_iter()
        .map(|subject| build_rule(&subject, by_subject.get(&subject), &provenance_by_graph))
        .collect();
    rules.sort_by_key(|rule| rule.subject.to_string());

    let mut constraint_components: Vec<ConstraintComponentSyntax> = candidate_components
        .into_iter()
        .map(|subject| build_constraint_component(&subject, &by_subject, &provenance_by_graph))
        .collect();
    constraint_components.sort_by_key(|component| component.subject.to_string());

    let diagnostics = shapes
        .iter()
        .filter(|shape| {
            matches!(shape.kind, ShapeSyntaxKind::PropertyShape) && shape.path.is_none()
        })
        .map(|shape| Diagnostic {
            severity: DiagnosticSeverity::Warning,
            message: format!("property shape {} has no sh:path", shape.subject),
            source: shape.provenance.first().cloned(),
        })
        .collect();

    ShapeSyntaxDocument {
        shapes,
        rules,
        constraint_components,
        quads,
        sources: resolved
            .all_graphs()
            .into_iter()
            .map(|graph| graph.source.clone())
            .collect(),
        diagnostics,
    }
}

fn build_constraint_component(
    subject: &Term,
    by_subject: &HashMap<Term, Vec<Quad>>,
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> ConstraintComponentSyntax {
    let quads = by_subject.get(subject).cloned().unwrap_or_default();
    let grouped = group_predicates(&quads);
    let mut parameters = Vec::new();
    let mut validators = Vec::new();
    let mut messages = Vec::new();
    let mut prefixes = Vec::new();
    let mut declarations = Vec::new();
    let mut extras = Vec::new();
    let mut label = None;
    let mut label_template = None;
    let mut comment = None;

    for (predicate, objects) in grouped {
        match predicate.as_str() {
            SH_PARAMETER => {
                for object in objects {
                    parameters.push(build_parameter(&object, by_subject));
                }
            }
            SH_NODE_VALIDATOR | SH_PROPERTY_VALIDATOR | SH_VALIDATOR => {
                for object in objects {
                    validators.push(build_validator(&object, by_subject));
                }
            }
            SH_MESSAGE => messages.extend(objects),
            SH_PREFIXES => prefixes.extend(objects),
            SH_DECLARE => {
                for object in objects {
                    declarations.push(build_prefix_declaration(&object, by_subject));
                }
            }
            RDFS_LABEL => label = first_literal_owned(&objects),
            SH_LABEL_TEMPLATE => label_template = first_literal_owned(&objects),
            RDFS_COMMENT => comment = first_literal_owned(&objects),
            RDF_TYPE => {}
            _ => extras.push(PredicateObjects { predicate, objects }),
        }
    }

    ConstraintComponentSyntax {
        subject: subject.clone(),
        parameters,
        validators,
        messages,
        prefixes,
        declarations,
        label,
        label_template,
        comment,
        extras,
        provenance: provenance_for_quads(&quads, provenance_by_graph),
    }
}

fn build_parameter(parameter: &Term, by_subject: &HashMap<Term, Vec<Quad>>) -> ParameterSyntax {
    let quads = by_subject.get(parameter).cloned().unwrap_or_default();
    let grouped = group_predicates(&quads);
    let mut path = None;
    let mut datatype = None;
    let mut var_name = None;
    let mut name = None;
    let mut description = None;
    let mut optional = false;
    let mut default_values = Vec::new();
    let mut extras = Vec::new();
    for (predicate, objects) in grouped {
        match predicate.as_str() {
            SH_PATH => path = objects.first().cloned(),
            SH_DATATYPE => datatype = objects.first().cloned(),
            SH_VAR_NAME => var_name = first_literal_owned(&objects),
            SH_NAME => name = first_literal_owned(&objects),
            SH_DESCRIPTION => description = first_literal_owned(&objects),
            SH_OPTIONAL => optional = objects.iter().any(is_true_literal),
            SH_DEFAULT_VALUE => default_values.extend(objects),
            RDF_TYPE => {}
            _ => extras.push(PredicateObjects { predicate, objects }),
        }
    }
    ParameterSyntax {
        node: parameter.clone(),
        path,
        datatype,
        var_name,
        name,
        description,
        optional,
        default_values,
        extras,
    }
}

fn build_validator(
    validator: &Term,
    by_subject: &HashMap<Term, Vec<Quad>>,
) -> SparqlValidatorSyntax {
    let quads = by_subject.get(validator).cloned().unwrap_or_default();
    let grouped = group_predicates(&quads);
    let mut kind = None;
    let mut select = None;
    let mut ask = None;
    let mut messages = Vec::new();
    let mut prefixes = Vec::new();
    let mut declarations = Vec::new();
    let mut extras = Vec::new();
    for (predicate, objects) in grouped {
        match predicate.as_str() {
            RDF_TYPE => kind = objects.first().cloned(),
            SH_SELECT => select = first_literal_owned(&objects),
            SH_ASK => ask = first_literal_owned(&objects),
            SH_MESSAGE => messages.extend(objects),
            SH_PREFIXES => prefixes.extend(objects),
            SH_DECLARE => {
                for object in objects {
                    declarations.push(build_prefix_declaration(&object, by_subject));
                }
            }
            _ => extras.push(PredicateObjects { predicate, objects }),
        }
    }
    SparqlValidatorSyntax {
        node: validator.clone(),
        kind,
        select,
        ask,
        messages,
        prefixes,
        declarations,
        extras,
    }
}

fn build_sparql_constraint(
    constraint: &Term,
    by_subject: &HashMap<Term, Vec<Quad>>,
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> SparqlConstraintSyntax {
    let quads = by_subject.get(constraint).cloned().unwrap_or_default();
    let grouped = group_predicates(&quads);
    let mut kind = None;
    let mut select = None;
    let mut ask = None;
    let mut messages = Vec::new();
    let mut prefixes = Vec::new();
    let mut declarations = Vec::new();
    let mut extras = Vec::new();
    for (predicate, objects) in grouped {
        match predicate.as_str() {
            RDF_TYPE => kind = objects.first().cloned(),
            SH_SELECT => select = first_literal_owned(&objects),
            SH_ASK => ask = first_literal_owned(&objects),
            SH_MESSAGE => messages.extend(objects),
            SH_PREFIXES => prefixes.extend(objects),
            SH_DECLARE => {
                for object in objects {
                    declarations.push(build_prefix_declaration(&object, by_subject));
                }
            }
            _ => extras.push(PredicateObjects { predicate, objects }),
        }
    }

    SparqlConstraintSyntax {
        node: constraint.clone(),
        kind,
        select,
        ask,
        messages,
        prefixes,
        declarations,
        extras,
        provenance: provenance_for_quads(&quads, provenance_by_graph),
    }
}

fn build_advanced_target(
    target: &Term,
    by_subject: &HashMap<Term, Vec<Quad>>,
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> AdvancedTargetSyntax {
    let quads = by_subject.get(target).cloned().unwrap_or_default();
    let grouped = group_predicates(&quads);
    let mut select = None;
    let mut ask = None;
    let mut target_shape = None;
    let mut filter_shape = None;
    let mut prefixes = Vec::new();
    let mut declarations = Vec::new();
    let mut extras = Vec::new();

    for (predicate, objects) in grouped {
        match predicate.as_str() {
            RDF_TYPE => {}
            SH_SELECT => select = first_literal_owned(&objects),
            SH_ASK => ask = first_literal_owned(&objects),
            SH_TARGET_SHAPE => target_shape = objects.first().cloned(),
            SH_FILTER_SHAPE => filter_shape = objects.first().cloned(),
            SH_PREFIXES => prefixes.extend(objects),
            SH_DECLARE => {
                for object in objects {
                    declarations.push(build_prefix_declaration(&object, by_subject));
                }
            }
            _ => extras.push(PredicateObjects { predicate, objects }),
        }
    }

    AdvancedTargetSyntax {
        node: target.clone(),
        select,
        ask,
        target_shape,
        filter_shape,
        prefixes,
        declarations,
        extras,
        provenance: provenance_for_quads(&quads, provenance_by_graph),
    }
}

fn build_prefix_declaration(
    declaration: &Term,
    by_subject: &HashMap<Term, Vec<Quad>>,
) -> PrefixDeclarationSyntax {
    let quads = by_subject.get(declaration).cloned().unwrap_or_default();
    let grouped = group_predicates(&quads);
    let mut prefix = None;
    let mut namespace = None;
    let mut extras = Vec::new();

    for (predicate, objects) in grouped {
        match predicate.as_str() {
            RDF_TYPE => {}
            SH_PREFIX => prefix = first_literal_owned(&objects),
            SH_NAMESPACE => namespace = objects.first().cloned(),
            _ => extras.push(PredicateObjects { predicate, objects }),
        }
    }

    PrefixDeclarationSyntax {
        node: declaration.clone(),
        prefix,
        namespace,
        extras,
    }
}

fn build_shape(
    subject: &Term,
    quads: Option<&Vec<Quad>>,
    by_subject: &HashMap<Term, Vec<Quad>>,
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> ShapeSyntax {
    let quads = quads.cloned().unwrap_or_default();
    let kind = if quads
        .iter()
        .any(|q| matches_named(&q.object, SH_PROPERTY_SHAPE))
        || quads.iter().any(|q| q.predicate.as_str() == SH_PATH)
    {
        ShapeSyntaxKind::PropertyShape
    } else {
        ShapeSyntaxKind::NodeShape
    };

    let mut targets = Vec::new();
    let mut property_shapes = Vec::new();
    let mut path = None;
    let mut severity = None;
    let mut deactivated = false;
    let mut constraints = Vec::new();
    let mut sparql_constraints = Vec::new();
    let mut extras = Vec::new();
    let mut rule_nodes = Vec::new();

    let grouped = group_predicates(&quads);
    for (predicate, objects) in grouped {
        match predicate.as_str() {
            SH_TARGET_CLASS => targets.extend(objects.into_iter().map(TargetSyntax::Class)),
            SH_TARGET_NODE => targets.extend(objects.into_iter().map(TargetSyntax::Node)),
            SH_TARGET_SUBJECTS_OF => {
                targets.extend(objects.into_iter().map(TargetSyntax::SubjectsOf))
            }
            SH_TARGET_OBJECTS_OF => {
                targets.extend(objects.into_iter().map(TargetSyntax::ObjectsOf))
            }
            SH_TARGET => {
                for object in objects {
                    targets.push(TargetSyntax::Advanced(build_advanced_target(
                        &object,
                        by_subject,
                        provenance_by_graph,
                    )));
                }
            }
            SH_PROPERTY => property_shapes.extend(objects),
            SH_PATH => path = objects.into_iter().next(),
            SH_SEVERITY => severity = objects.into_iter().next(),
            SH_DEACTIVATED => {
                deactivated = objects.iter().any(is_true_literal);
            }
            SH_SPARQL => {
                for object in objects {
                    sparql_constraints.push(build_sparql_constraint(
                        &object,
                        by_subject,
                        provenance_by_graph,
                    ));
                }
            }
            SH_RULE => rule_nodes.extend(objects),
            RDF_TYPE => {}
            _ => {
                if is_constraint_predicate(predicate.as_str())
                    || is_custom_constraint_predicate(predicate.as_str())
                {
                    constraints.push(ConstraintSyntax { predicate, objects });
                } else {
                    extras.push(PredicateObjects { predicate, objects });
                }
            }
        }
    }

    ShapeSyntax {
        subject: subject.clone(),
        kind,
        targets,
        property_shapes,
        path,
        severity,
        deactivated,
        constraints,
        sparql_constraints,
        rule_nodes,
        extras,
        provenance: provenance_for_quads(&quads, provenance_by_graph),
    }
}

fn build_rule(
    subject: &Term,
    quads: Option<&Vec<Quad>>,
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> RuleSyntax {
    let quads = quads.cloned().unwrap_or_default();
    let kind = if quads
        .iter()
        .any(|q| matches_named(&q.object, SH_TRIPLE_RULE))
    {
        RuleSyntaxKind::Triple
    } else if quads
        .iter()
        .any(|q| matches_named(&q.object, SH_SPARQL_RULE))
    {
        RuleSyntaxKind::Sparql
    } else {
        RuleSyntaxKind::Generic
    };

    let grouped = group_predicates(&quads);
    let mut order = None;
    let mut deactivated = false;
    let mut properties = Vec::new();
    for (predicate, objects) in grouped {
        match predicate.as_str() {
            RDF_TYPE => {}
            SH_ORDER => {
                order = objects.first().and_then(parse_numeric_literal);
            }
            SH_DEACTIVATED => {
                deactivated = objects.iter().any(is_true_literal);
            }
            _ => properties.push(PredicateObjects { predicate, objects }),
        }
    }

    RuleSyntax {
        subject: subject.clone(),
        kind,
        order,
        deactivated,
        properties,
        provenance: provenance_for_quads(&quads, provenance_by_graph),
    }
}

fn discover_inline_shapes_and_rules(
    by_subject: &HashMap<Term, Vec<Quad>>,
    candidate_shapes: &mut HashSet<Term>,
    candidate_rules: &mut HashSet<Term>,
) {
    let mut pending_shapes: Vec<Term> = candidate_shapes.iter().cloned().collect();
    let mut pending_rules: Vec<Term> = candidate_rules.iter().cloned().collect();
    let mut visited_shapes = HashSet::new();
    let mut visited_rules = HashSet::new();

    while let Some(shape) = pending_shapes.pop() {
        if !visited_shapes.insert(shape.clone()) {
            continue;
        }
        let Some(quads) = by_subject.get(&shape) else {
            continue;
        };
        for quad in quads {
            match quad.predicate.as_str() {
                SH_TARGET => {
                    discover_advanced_target_shapes(
                        by_subject,
                        candidate_shapes,
                        &mut pending_shapes,
                        &quad.object,
                    );
                }
                SH_PROPERTY | SH_NODE | SH_NOT | SH_QUALIFIED_VALUE_SHAPE => {
                    push_candidate_shape(candidate_shapes, &mut pending_shapes, &quad.object);
                }
                SH_AND | SH_OR | SH_XONE => {
                    for member in list_members(by_subject, &quad.object) {
                        push_candidate_shape(candidate_shapes, &mut pending_shapes, &member);
                    }
                }
                SH_RULE => {
                    if candidate_rules.insert(quad.object.clone()) {
                        pending_rules.push(quad.object.clone());
                    }
                }
                _ => {}
            }
        }
    }

    while let Some(rule) = pending_rules.pop() {
        if !visited_rules.insert(rule.clone()) {
            continue;
        }
        let Some(quads) = by_subject.get(&rule) else {
            continue;
        };
        for quad in quads {
            if quad.predicate.as_str() == SH_CONDITION {
                push_candidate_shape(candidate_shapes, &mut pending_shapes, &quad.object);
            }
        }
    }

    while let Some(shape) = pending_shapes.pop() {
        if !visited_shapes.insert(shape.clone()) {
            continue;
        }
        let Some(quads) = by_subject.get(&shape) else {
            continue;
        };
        for quad in quads {
            match quad.predicate.as_str() {
                SH_TARGET => {
                    discover_advanced_target_shapes(
                        by_subject,
                        candidate_shapes,
                        &mut pending_shapes,
                        &quad.object,
                    );
                }
                SH_PROPERTY | SH_NODE | SH_NOT | SH_QUALIFIED_VALUE_SHAPE => {
                    push_candidate_shape(candidate_shapes, &mut pending_shapes, &quad.object);
                }
                SH_AND | SH_OR | SH_XONE => {
                    for member in list_members(by_subject, &quad.object) {
                        push_candidate_shape(candidate_shapes, &mut pending_shapes, &member);
                    }
                }
                _ => {}
            }
        }
    }
}

fn discover_advanced_target_shapes(
    by_subject: &HashMap<Term, Vec<Quad>>,
    candidate_shapes: &mut HashSet<Term>,
    pending_shapes: &mut Vec<Term>,
    target: &Term,
) {
    let Some(quads) = by_subject.get(target) else {
        return;
    };
    for quad in quads {
        if matches!(quad.predicate.as_str(), SH_TARGET_SHAPE | SH_FILTER_SHAPE) {
            push_candidate_shape(candidate_shapes, pending_shapes, &quad.object);
        }
    }
}

fn push_candidate_shape(
    candidate_shapes: &mut HashSet<Term>,
    pending_shapes: &mut Vec<Term>,
    term: &Term,
) {
    if matches!(term, Term::NamedNode(_) | Term::BlankNode(_))
        && candidate_shapes.insert(term.clone())
    {
        pending_shapes.push(term.clone());
    }
}

fn list_members(by_subject: &HashMap<Term, Vec<Quad>>, head: &Term) -> Vec<Term> {
    let mut members = Vec::new();
    let mut seen = HashSet::new();
    let mut current = head.clone();

    loop {
        if matches!(&current, Term::NamedNode(node) if node.as_str() == RDF_NIL) {
            break;
        }
        if !seen.insert(current.clone()) {
            break;
        }
        let Some(quads) = by_subject.get(&current) else {
            break;
        };
        let first = quads
            .iter()
            .find(|quad| quad.predicate.as_str() == RDF_FIRST)
            .map(|quad| quad.object.clone());
        let rest = quads
            .iter()
            .find(|quad| quad.predicate.as_str() == RDF_REST)
            .map(|quad| quad.object.clone());
        let Some(value) = first else {
            break;
        };
        members.push(value);
        let Some(next) = rest else {
            break;
        };
        current = next;
    }

    members
}

fn provenance_map(resolved: &ResolvedShapeSet) -> HashMap<Option<String>, SourceRef> {
    let mut map = HashMap::new();
    for graph in resolved
        .root_graphs
        .iter()
        .chain(resolved.imported_graphs.iter())
    {
        map.insert(
            Some(graph.graph_iri.as_str().to_string()),
            graph.source.clone(),
        );
    }
    map
}

fn provenance_for_quads(
    quads: &[Quad],
    provenance_by_graph: &HashMap<Option<String>, SourceRef>,
) -> Vec<SourceRef> {
    let mut refs = HashSet::new();
    let mut out = Vec::new();
    for quad in quads {
        let key = match &quad.graph_name {
            GraphName::NamedNode(node) => Some(node.as_str().to_string()),
            _ => None,
        };
        if let Some(source) = provenance_by_graph.get(&key)
            && refs.insert(source.clone())
        {
            out.push(source.clone());
        }
    }
    out.sort_by_key(|source| source.graph_iri.clone());
    out
}

fn group_predicates(quads: &[Quad]) -> Vec<(NamedNode, Vec<Term>)> {
    let mut grouped: HashMap<NamedNode, Vec<Term>> = HashMap::new();
    for quad in quads {
        grouped
            .entry(quad.predicate.clone())
            .or_default()
            .push(quad.object.clone());
    }
    let mut entries: Vec<_> = grouped.into_iter().collect();
    entries.sort_by_key(|(predicate, _)| predicate.as_str().to_string());
    entries
}

fn is_constraint_predicate(predicate: &str) -> bool {
    predicate.starts_with("http://www.w3.org/ns/shacl#")
        && !matches!(
            predicate,
            SH_TARGET
                | SH_TARGET_CLASS
                | SH_TARGET_NODE
                | SH_TARGET_SUBJECTS_OF
                | SH_TARGET_OBJECTS_OF
                | SH_PATH
                | SH_PROPERTY
                | SH_RULE
                | SH_SEVERITY
                | SH_DEACTIVATED
                | SH_ORDER
        )
}

fn is_custom_constraint_predicate(predicate: &str) -> bool {
    !predicate.starts_with("http://www.w3.org/ns/shacl#")
        && !matches!(predicate, RDF_TYPE | RDFS_LABEL | RDFS_COMMENT)
}

fn subject_to_term(subject: &NamedOrBlankNode) -> Term {
    match subject {
        NamedOrBlankNode::NamedNode(node) => Term::NamedNode(node.clone()),
        NamedOrBlankNode::BlankNode(node) => Term::BlankNode(node.clone()),
    }
}

fn matches_named(term: &Term, iri: &str) -> bool {
    matches!(term, Term::NamedNode(node) if node.as_str() == iri)
}

fn parse_numeric_literal(term: &Term) -> Option<f64> {
    match term {
        Term::Literal(lit) => lit.value().parse().ok(),
        _ => None,
    }
}

fn is_true_literal(term: &Term) -> bool {
    matches!(term, Term::Literal(lit) if lit.value().eq_ignore_ascii_case("true") || lit.value() == "1")
}

fn first_literal_owned(values: &[Term]) -> Option<String> {
    values.first().and_then(|term| match term {
        Term::Literal(lit) => Some(lit.value().to_string()),
        _ => None,
    })
}
