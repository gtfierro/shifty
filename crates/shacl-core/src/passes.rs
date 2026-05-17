use crate::algebra::{
    Constraint, ConstraintExpr, ConstraintId, DependencyEdge, FeatureUse, LogicalKind,
    PropertyPath, Rule, RuleExpr, RuleId, Severity, Shape, ShapeId, ShapeKind, ShapeProgram,
    Target, TargetExpr, TargetId, TriplePatternTerm,
};
use crate::diagnostics::{Diagnostic, DiagnosticSeverity, InspectionEdge, InspectionGraph, InspectionNode};
use crate::syntax::{
    ConstraintSyntax, RuleSyntax, RuleSyntaxKind, ShapeSyntax, ShapeSyntaxDocument, ShapeSyntaxKind,
    TargetSyntax,
};
use oxrdf::{NamedNode, NamedOrBlankNode, Quad, Term};
use std::collections::{HashMap, HashSet};

const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const SH_INFO: &str = "http://www.w3.org/ns/shacl#Info";
const SH_WARNING: &str = "http://www.w3.org/ns/shacl#Warning";
const SH_VIOLATION: &str = "http://www.w3.org/ns/shacl#Violation";
const SH_INVERSE_PATH: &str = "http://www.w3.org/ns/shacl#inversePath";
const SH_PATH: &str = "http://www.w3.org/ns/shacl#path";
const SH_ALTERNATIVE_PATH: &str = "http://www.w3.org/ns/shacl#alternativePath";
const SH_ZERO_OR_MORE_PATH: &str = "http://www.w3.org/ns/shacl#zeroOrMorePath";
const SH_ONE_OR_MORE_PATH: &str = "http://www.w3.org/ns/shacl#oneOrMorePath";
const SH_ZERO_OR_ONE_PATH: &str = "http://www.w3.org/ns/shacl#zeroOrOnePath";
const SH_AND: &str = "http://www.w3.org/ns/shacl#and";
const SH_OR: &str = "http://www.w3.org/ns/shacl#or";
const SH_XONE: &str = "http://www.w3.org/ns/shacl#xone";
const SH_NOT: &str = "http://www.w3.org/ns/shacl#not";
const SH_NODE: &str = "http://www.w3.org/ns/shacl#node";
const SH_PROPERTY: &str = "http://www.w3.org/ns/shacl#property";
const SH_QUALIFIED_VALUE_SHAPE: &str = "http://www.w3.org/ns/shacl#qualifiedValueShape";
const SH_QUALIFIED_MIN_COUNT: &str = "http://www.w3.org/ns/shacl#qualifiedMinCount";
const SH_QUALIFIED_MAX_COUNT: &str = "http://www.w3.org/ns/shacl#qualifiedMaxCount";
const SH_QUALIFIED_VALUE_SHAPES_DISJOINT: &str =
    "http://www.w3.org/ns/shacl#qualifiedValueShapesDisjoint";
const SH_CLOSED: &str = "http://www.w3.org/ns/shacl#closed";
const SH_IGNORED_PROPERTIES: &str = "http://www.w3.org/ns/shacl#ignoredProperties";
const SH_HAS_VALUE: &str = "http://www.w3.org/ns/shacl#hasValue";
const SH_IN: &str = "http://www.w3.org/ns/shacl#in";
const SH_SPARQL: &str = "http://www.w3.org/ns/shacl#sparql";
const SH_CLASS: &str = "http://www.w3.org/ns/shacl#class";
const SH_DATATYPE: &str = "http://www.w3.org/ns/shacl#datatype";
const SH_NODE_KIND: &str = "http://www.w3.org/ns/shacl#nodeKind";
const SH_MIN_COUNT: &str = "http://www.w3.org/ns/shacl#minCount";
const SH_MAX_COUNT: &str = "http://www.w3.org/ns/shacl#maxCount";
const SH_MIN_EXCLUSIVE: &str = "http://www.w3.org/ns/shacl#minExclusive";
const SH_MIN_INCLUSIVE: &str = "http://www.w3.org/ns/shacl#minInclusive";
const SH_MAX_EXCLUSIVE: &str = "http://www.w3.org/ns/shacl#maxExclusive";
const SH_MAX_INCLUSIVE: &str = "http://www.w3.org/ns/shacl#maxInclusive";
const SH_MIN_LENGTH: &str = "http://www.w3.org/ns/shacl#minLength";
const SH_MAX_LENGTH: &str = "http://www.w3.org/ns/shacl#maxLength";
const SH_PATTERN: &str = "http://www.w3.org/ns/shacl#pattern";
const SH_LANGUAGE_IN: &str = "http://www.w3.org/ns/shacl#languageIn";
const SH_UNIQUE_LANG: &str = "http://www.w3.org/ns/shacl#uniqueLang";
const SH_EQUALS: &str = "http://www.w3.org/ns/shacl#equals";
const SH_DISJOINT: &str = "http://www.w3.org/ns/shacl#disjoint";
const SH_LESS_THAN: &str = "http://www.w3.org/ns/shacl#lessThan";
const SH_LESS_THAN_OR_EQUALS: &str = "http://www.w3.org/ns/shacl#lessThanOrEquals";
const SH_SELECT: &str = "http://www.w3.org/ns/shacl#select";
const SH_ASK: &str = "http://www.w3.org/ns/shacl#ask";
const SH_TARGET_SHAPE: &str = "http://www.w3.org/ns/shacl#targetShape";
const SH_FILTER_SHAPE: &str = "http://www.w3.org/ns/shacl#filterShape";
const SH_CONSTRUCT: &str = "http://www.w3.org/ns/shacl#construct";
const SH_CONDITION: &str = "http://www.w3.org/ns/shacl#condition";
const SH_RULE_SUBJECT: &str = "http://www.w3.org/ns/shacl#subject";
const SH_RULE_PREDICATE: &str = "http://www.w3.org/ns/shacl#predicate";
const SH_RULE_OBJECT: &str = "http://www.w3.org/ns/shacl#object";

pub fn lower_to_program(document: &ShapeSyntaxDocument) -> ShapeProgram {
    let mut shapes = document.shapes.clone();
    shapes.sort_by_key(|shape| shape.subject.to_string());
    let shape_index: HashMap<String, ShapeId> = shapes
        .iter()
        .enumerate()
        .map(|(idx, shape)| (shape.subject.to_string(), ShapeId((idx + 1) as u64)))
        .collect();

    let mut lowered_shapes = Vec::new();
    let mut lowered_targets = Vec::new();
    let mut lowered_constraints = Vec::new();
    let mut lowered_rules = Vec::new();
    let mut dependencies = Vec::new();
    let mut features = HashSet::new();
    let mut diagnostics = document.diagnostics.clone();
    let mut rule_map: HashMap<String, RuleSyntax> = document
        .rules
        .iter()
        .cloned()
        .map(|rule| (rule.subject.to_string(), rule))
        .collect();
    let quad_index = QuadIndex::new(&document.quads);

    for shape in &shapes {
        let shape_id = shape_index[&shape.subject.to_string()];
        let mut target_ids = Vec::new();
        for target_syntax in &shape.targets {
            let target_id = TargetId((lowered_targets.len() + 1) as u64);
            let expr = lower_target(target_syntax, &quad_index, &shape_index, &mut dependencies, shape_id);
            if matches!(expr, TargetExpr::Advanced { .. }) {
                features.insert(FeatureUse::AdvancedTargets);
                features.insert(FeatureUse::Sparql);
            }
            lowered_targets.push(Target {
                id: target_id,
                owner: shape_id,
                expr,
            });
            target_ids.push(target_id);
        }

        let mut constraint_ids = Vec::new();
        for constraint in &shape.constraints {
            let constraint_id = ConstraintId((lowered_constraints.len() + 1) as u64);
            let expr = lower_constraint(
                shape_id,
                constraint,
                shape,
                &quad_index,
                &shape_index,
                &mut dependencies,
                &mut features,
                &mut diagnostics,
            );
            lowered_constraints.push(Constraint {
                id: constraint_id,
                owner: shape_id,
                expr,
                provenance: shape.provenance.clone(),
            });
            constraint_ids.push(constraint_id);
        }

        let mut property_shapes = Vec::new();
        for property in &shape.property_shapes {
            if let Some(property_id) = shape_index.get(&property.to_string()) {
                property_shapes.push(*property_id);
                dependencies.push(DependencyEdge {
                    from: shape_id,
                    to: *property_id,
                    kind: "property".to_string(),
                });
            } else {
                diagnostics.push(Diagnostic {
                    severity: DiagnosticSeverity::Warning,
                    message: format!(
                        "shape {} references property shape {} that was not discovered",
                        shape.subject, property
                    ),
                    source: shape.provenance.first().cloned(),
                });
            }
        }

        let path = shape
            .path
            .as_ref()
            .map(|term| lower_property_path(term, &quad_index));

        let severity = lower_severity(shape.severity.as_ref());
        let kind = match shape.kind {
            ShapeSyntaxKind::NodeShape => ShapeKind::Node,
            ShapeSyntaxKind::PropertyShape => ShapeKind::Property,
        };

        for rule_term in &shape.rule_nodes {
            if let Some(rule_syntax) = rule_map.remove(&rule_term.to_string()) {
                features.insert(FeatureUse::Rules);
                let rule_id = RuleId((lowered_rules.len() + 1) as u64);
                let expr = lower_rule(
                    &rule_syntax,
                    &quad_index,
                    &shape_index,
                    &mut dependencies,
                    &mut features,
                );
                lowered_rules.push(Rule {
                    id: rule_id,
                    owner: shape_id,
                    expr,
                    deactivated: rule_syntax.deactivated,
                    provenance: rule_syntax.provenance.clone(),
                });
            }
        }

        lowered_shapes.push(Shape {
            id: shape_id,
            source: shape.subject.clone(),
            kind,
            targets: target_ids,
            constraints: constraint_ids,
            property_shapes,
            path,
            severity,
            deactivated: shape.deactivated,
            provenance: shape.provenance.clone(),
        });
    }

    let inspection = build_inspection_graph(
        &lowered_shapes,
        &lowered_constraints,
        &lowered_rules,
        &lowered_targets,
        &dependencies,
        document,
    );

    let mut feature_list: Vec<_> = features.into_iter().collect();
    feature_list.sort_by_key(feature_order);
    dependencies.sort_by_key(|edge| (edge.from, edge.to, edge.kind.clone()));

    ShapeProgram {
        shapes: lowered_shapes,
        constraints: lowered_constraints,
        targets: lowered_targets,
        rules: lowered_rules,
        dependencies,
        source_inventory: document.sources.clone(),
        features: feature_list,
        diagnostics,
        inspection,
        shape_index,
    }
}

fn lower_target(
    target: &TargetSyntax,
    quads: &QuadIndex,
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    owner: ShapeId,
) -> TargetExpr {
    match target {
        TargetSyntax::Class(term) => TargetExpr::Class(term.clone()),
        TargetSyntax::Node(term) => TargetExpr::Node(term.clone()),
        TargetSyntax::SubjectsOf(term) => TargetExpr::SubjectsOf(term.clone()),
        TargetSyntax::ObjectsOf(term) => TargetExpr::ObjectsOf(term.clone()),
        TargetSyntax::Advanced(term) => {
            let select = first_literal(quads.objects_for_subject_predicate(term, SH_SELECT));
            let ask = first_literal(quads.objects_for_subject_predicate(term, SH_ASK));
            let target_shape = quads
                .objects_for_subject_predicate(term, SH_TARGET_SHAPE)
                .first()
                .cloned();
            let filter_shape = quads
                .objects_for_subject_predicate(term, SH_FILTER_SHAPE)
                .first()
                .cloned();
            for dep in [&target_shape, &filter_shape].into_iter().flatten() {
                if let Some(id) = shape_index.get(&dep.to_string()) {
                    dependencies.push(DependencyEdge {
                        from: owner,
                        to: *id,
                        kind: "target".to_string(),
                    });
                }
            }
            TargetExpr::Advanced {
                node: term.clone(),
                select,
                ask,
                target_shape,
                filter_shape,
            }
        }
    }
}

fn lower_constraint(
    owner: ShapeId,
    constraint: &ConstraintSyntax,
    shape: &ShapeSyntax,
    quads: &QuadIndex,
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    features: &mut HashSet<FeatureUse>,
    diagnostics: &mut Vec<Diagnostic>,
) -> ConstraintExpr {
    let predicate = constraint.predicate.as_str();
    match predicate {
        SH_NODE => shape_ref_expr(&constraint.objects, shape_index, dependencies, owner, "node")
            .map(|(shape_id, source)| ConstraintExpr::NodeRef { shape: shape_id, source })
            .unwrap_or_else(|| generic_expr(constraint)),
        SH_PROPERTY => shape_ref_expr(&constraint.objects, shape_index, dependencies, owner, "property")
            .map(|(shape_id, source)| ConstraintExpr::PropertyRef { shape: shape_id, source })
            .unwrap_or_else(|| generic_expr(constraint)),
        SH_QUALIFIED_VALUE_SHAPE => {
            let (shape_id, source) = shape_ref_expr(
                &constraint.objects,
                shape_index,
                dependencies,
                owner,
                "qualified",
            )
            .unwrap_or((None, constraint.objects.first().cloned().unwrap_or_else(|| shape.subject.clone())));
            ConstraintExpr::QualifiedValueShape {
                shape: shape_id,
                source,
                min_count: literal_u64(
                    first_term(shape_property(shape, SH_QUALIFIED_MIN_COUNT))
                        .as_ref(),
                ),
                max_count: literal_u64(
                    first_term(shape_property(shape, SH_QUALIFIED_MAX_COUNT))
                        .as_ref(),
                ),
                disjoint: first_term(shape_property(shape, SH_QUALIFIED_VALUE_SHAPES_DISJOINT))
                    .as_ref()
                    .map(is_true_literal),
            }
        }
        SH_NOT => shape_ref_expr(&constraint.objects, shape_index, dependencies, owner, "not")
            .map(|(shape_id, source)| ConstraintExpr::Not { shape: shape_id, source })
            .unwrap_or_else(|| generic_expr(constraint)),
        SH_AND | SH_OR | SH_XONE => {
            let shapes = constraint
                .objects
                .iter()
                .flat_map(|term| quad_list_terms(quads, term))
                .filter_map(|term| shape_index.get(&term.to_string()).copied())
                .collect::<Vec<_>>();
            for dep in &shapes {
                dependencies.push(DependencyEdge {
                    from: owner,
                    to: *dep,
                    kind: "logical".to_string(),
                });
            }
            ConstraintExpr::Logical {
                kind: match predicate {
                    SH_AND => LogicalKind::And,
                    SH_OR => LogicalKind::Or,
                    _ => LogicalKind::Xone,
                },
                shapes,
            }
        }
        SH_CLASS => single_value_expr(&constraint.objects, ConstraintExpr::Class, constraint),
        SH_DATATYPE => single_value_expr(&constraint.objects, ConstraintExpr::Datatype, constraint),
        SH_NODE_KIND => single_value_expr(&constraint.objects, ConstraintExpr::NodeKind, constraint),
        SH_MIN_COUNT | SH_MAX_COUNT => ConstraintExpr::Cardinality {
            predicate: constraint.predicate.clone(),
            min: if predicate == SH_MIN_COUNT {
                literal_u64(constraint.objects.first())
            } else {
                None
            },
            max: if predicate == SH_MAX_COUNT {
                literal_u64(constraint.objects.first())
            } else {
                None
            },
        },
        SH_MIN_EXCLUSIVE | SH_MIN_INCLUSIVE | SH_MAX_EXCLUSIVE | SH_MAX_INCLUSIVE => {
            ConstraintExpr::NumericRange {
                predicate: constraint.predicate.clone(),
                values: constraint.objects.clone(),
            }
        }
        SH_MIN_LENGTH | SH_MAX_LENGTH | SH_PATTERN | SH_LANGUAGE_IN | SH_UNIQUE_LANG => {
            let values = if predicate == SH_LANGUAGE_IN {
                constraint
                    .objects
                    .iter()
                    .flat_map(|term| quad_list_terms(quads, term))
                    .collect()
            } else {
                constraint.objects.clone()
            };
            ConstraintExpr::StringConstraint {
                predicate: constraint.predicate.clone(),
                values,
            }
        }
        SH_EQUALS | SH_DISJOINT | SH_LESS_THAN | SH_LESS_THAN_OR_EQUALS => {
            ConstraintExpr::PropertyComparison {
                predicate: constraint.predicate.clone(),
                values: constraint.objects.clone(),
            }
        }
        SH_CLOSED => ConstraintExpr::Closed {
            ignored_properties: first_term(shape_property(shape, SH_IGNORED_PROPERTIES))
                .iter()
                .flat_map(|term| quad_list_terms(quads, term))
                .collect(),
        },
        SH_HAS_VALUE => constraint
            .objects
            .first()
            .cloned()
            .map(ConstraintExpr::HasValue)
            .unwrap_or_else(|| generic_expr(constraint)),
        SH_IN => ConstraintExpr::In(
            constraint
                .objects
                .iter()
                .flat_map(|term| quad_list_terms(quads, term))
                .collect(),
        ),
        SH_SPARQL => {
            features.insert(FeatureUse::Sparql);
            constraint
                .objects
                .first()
                .cloned()
                .map(|node| ConstraintExpr::Sparql { node })
                .unwrap_or_else(|| generic_expr(constraint))
        }
        _ => {
            if !constraint.predicate.as_str().starts_with("http://www.w3.org/ns/shacl#") {
                features.insert(FeatureUse::CustomComponents);
            }
            diagnostics.push(Diagnostic {
                severity: DiagnosticSeverity::Info,
                message: format!(
                    "preserving non-specialized constraint {} on shape {}",
                    constraint.predicate, shape.subject
                ),
                source: shape.provenance.first().cloned(),
            });
            generic_expr(constraint)
        }
    }
}

fn lower_rule(
    rule: &RuleSyntax,
    quads: &QuadIndex,
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    features: &mut HashSet<FeatureUse>,
) -> RuleExpr {
    let conditions = rule_shape_conditions(rule, shape_index, dependencies);
    let order = rule.order.map(|value| value.to_string());
    match rule.kind {
        RuleSyntaxKind::Triple => RuleExpr::Triple {
            node: rule.subject.clone(),
            subject: first_term(rule_property(rule, SH_RULE_SUBJECT))
                .map(|term| lower_triple_pattern_term(&term, quads)),
            predicate: first_term(rule_property(rule, SH_RULE_PREDICATE)).and_then(as_named_node),
            object: first_term(rule_property(rule, SH_RULE_OBJECT))
                .map(|term| lower_triple_pattern_term(&term, quads)),
            conditions,
            order,
        },
        RuleSyntaxKind::Sparql => {
            features.insert(FeatureUse::Sparql);
            RuleExpr::Sparql {
                node: rule.subject.clone(),
                query: first_literal(rule_property(rule, SH_CONSTRUCT)),
                conditions,
                order,
            }
        }
        RuleSyntaxKind::Generic => RuleExpr::Generic {
            node: rule.subject.clone(),
            conditions,
            order,
        },
    }
}

fn lower_triple_pattern_term(term: &Term, quads: &QuadIndex) -> TriplePatternTerm {
    if matches!(term, Term::NamedNode(node) if node.as_str() == "http://www.w3.org/ns/shacl#this") {
        TriplePatternTerm::This
    } else if matches!(term, Term::BlankNode(_))
        && quads.has_predicate(term, SH_PATH)
    {
        TriplePatternTerm::Path(lower_property_path(term, quads))
    } else {
        TriplePatternTerm::Constant(term.clone())
    }
}

fn lower_property_path(term: &Term, quads: &QuadIndex) -> PropertyPath {
    match term {
        Term::NamedNode(node) => PropertyPath::Predicate(node.clone()),
        Term::BlankNode(_) => {
            if let Some(inner) = first_term(quads.objects_for_subject_predicate(term, SH_INVERSE_PATH))
            {
                return PropertyPath::Inverse(Box::new(lower_property_path(&inner, quads)));
            }
            if let Some(inner) =
                first_term(quads.objects_for_subject_predicate(term, SH_ALTERNATIVE_PATH))
            {
                return PropertyPath::Alternative(
                    quad_list_terms(quads, &inner)
                        .into_iter()
                        .map(|item| lower_property_path(&item, quads))
                        .collect(),
                );
            }
            if let Some(inner) =
                first_term(quads.objects_for_subject_predicate(term, SH_ZERO_OR_MORE_PATH))
            {
                return PropertyPath::ZeroOrMore(Box::new(lower_property_path(&inner, quads)));
            }
            if let Some(inner) =
                first_term(quads.objects_for_subject_predicate(term, SH_ONE_OR_MORE_PATH))
            {
                return PropertyPath::OneOrMore(Box::new(lower_property_path(&inner, quads)));
            }
            if let Some(inner) =
                first_term(quads.objects_for_subject_predicate(term, SH_ZERO_OR_ONE_PATH))
            {
                return PropertyPath::ZeroOrOne(Box::new(lower_property_path(&inner, quads)));
            }
            let list = quad_list_terms(quads, term);
            if !list.is_empty() {
                return PropertyPath::Sequence(
                    list.into_iter()
                        .map(|item| lower_property_path(&item, quads))
                        .collect(),
                );
            }
            PropertyPath::Unsupported(term.clone())
        }
        _ => PropertyPath::Unsupported(term.clone()),
    }
}

fn build_inspection_graph(
    shapes: &[Shape],
    constraints: &[Constraint],
    rules: &[Rule],
    targets: &[Target],
    dependencies: &[DependencyEdge],
    document: &ShapeSyntaxDocument,
) -> InspectionGraph {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for shape in shapes {
        nodes.push(InspectionNode {
            id: format!("shape:{}", shape.id.0),
            kind: "shape".to_string(),
            label: shape.source.to_string(),
            annotations: HashMap::from([
                ("shape_kind".to_string(), format!("{:?}", shape.kind)),
                ("deactivated".to_string(), shape.deactivated.to_string()),
            ]),
        });
    }

    for constraint in constraints {
        nodes.push(InspectionNode {
            id: format!("constraint:{}", constraint.id.0),
            kind: "constraint".to_string(),
            label: format!("{:?}", constraint.expr),
            annotations: HashMap::new(),
        });
        edges.push(InspectionEdge {
            source: format!("shape:{}", constraint.owner.0),
            target: format!("constraint:{}", constraint.id.0),
            kind: "owns_constraint".to_string(),
            weight: 1,
            annotations: HashMap::new(),
        });
    }

    for rule in rules {
        nodes.push(InspectionNode {
            id: format!("rule:{}", rule.id.0),
            kind: "rule".to_string(),
            label: format!("{:?}", rule.expr),
            annotations: HashMap::from([("deactivated".to_string(), rule.deactivated.to_string())]),
        });
        edges.push(InspectionEdge {
            source: format!("shape:{}", rule.owner.0),
            target: format!("rule:{}", rule.id.0),
            kind: "owns_rule".to_string(),
            weight: 1,
            annotations: HashMap::new(),
        });
    }

    for target in targets {
        nodes.push(InspectionNode {
            id: format!("target:{}", target.id.0),
            kind: "target".to_string(),
            label: format!("{:?}", target.expr),
            annotations: HashMap::new(),
        });
        edges.push(InspectionEdge {
            source: format!("shape:{}", target.owner.0),
            target: format!("target:{}", target.id.0),
            kind: "owns_target".to_string(),
            weight: 1,
            annotations: HashMap::new(),
        });
    }

    for dep in dependencies {
        edges.push(InspectionEdge {
            source: format!("shape:{}", dep.from.0),
            target: format!("shape:{}", dep.to.0),
            kind: dep.kind.clone(),
            weight: 1,
            annotations: HashMap::new(),
        });
    }

    for source in &document.sources {
        nodes.push(InspectionNode {
            id: format!("source:{}", source.graph_iri),
            kind: "source".to_string(),
            label: source.locator.clone().unwrap_or_else(|| source.graph_iri.clone()),
            annotations: HashMap::from([("is_root".to_string(), source.is_root.to_string())]),
        });
    }

    for shape in shapes {
        for source in &shape.provenance {
            edges.push(InspectionEdge {
                source: format!("source:{}", source.graph_iri),
                target: format!("shape:{}", shape.id.0),
                kind: "provides".to_string(),
                weight: 1,
                annotations: HashMap::new(),
            });
        }
    }

    InspectionGraph { nodes, edges }
}

fn shape_ref_expr(
    objects: &[Term],
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    owner: ShapeId,
    kind: &str,
) -> Option<(Option<ShapeId>, Term)> {
    let source = objects.first()?.clone();
    let target = shape_index.get(&source.to_string()).copied();
    if let Some(shape) = target {
        dependencies.push(DependencyEdge {
            from: owner,
            to: shape,
            kind: kind.to_string(),
        });
    }
    Some((target, source))
}

fn lower_severity(term: Option<&Term>) -> Severity {
    match term {
        Some(Term::NamedNode(node)) if node.as_str() == SH_INFO => Severity::Info,
        Some(Term::NamedNode(node)) if node.as_str() == SH_WARNING => Severity::Warning,
        Some(Term::NamedNode(node)) if node.as_str() == SH_VIOLATION => Severity::Violation,
        Some(other) => Severity::Custom(other.clone()),
        None => Severity::Violation,
    }
}

fn generic_expr(constraint: &ConstraintSyntax) -> ConstraintExpr {
    ConstraintExpr::GenericPredicate {
        predicate: constraint.predicate.clone(),
        values: constraint.objects.clone(),
    }
}

fn single_value_expr<F>(objects: &[Term], build: F, fallback: &ConstraintSyntax) -> ConstraintExpr
where
    F: FnOnce(Term) -> ConstraintExpr,
{
    objects
        .first()
        .cloned()
        .map(build)
        .unwrap_or_else(|| generic_expr(fallback))
}

fn rule_shape_conditions(
    rule: &RuleSyntax,
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
) -> Vec<ShapeId> {
    let mut conditions = Vec::new();
    for condition in rule_property(rule, SH_CONDITION) {
        if let Some(id) = shape_index.get(&condition.to_string()).copied() {
            dependencies.push(DependencyEdge {
                from: id,
                to: id,
                kind: "rule_condition".to_string(),
            });
            conditions.push(id);
        }
    }
    conditions
}

fn feature_order(feature: &FeatureUse) -> u8 {
    match feature {
        FeatureUse::Core => 0,
        FeatureUse::AdvancedTargets => 1,
        FeatureUse::Rules => 2,
        FeatureUse::Sparql => 3,
        FeatureUse::Templates => 4,
        FeatureUse::CustomComponents => 5,
    }
}

fn first_term(values: Vec<Term>) -> Option<Term> {
    values.into_iter().next()
}

fn first_literal(values: Vec<Term>) -> Option<String> {
    values.first().and_then(|term| match term {
        Term::Literal(lit) => Some(lit.value().to_string()),
        _ => None,
    })
}

fn literal_u64(term: Option<&Term>) -> Option<u64> {
    match term {
        Some(Term::Literal(lit)) => lit.value().parse().ok(),
        _ => None,
    }
}

fn as_named_node(term: Term) -> Option<NamedNode> {
    match term {
        Term::NamedNode(node) => Some(node),
        _ => None,
    }
}

fn is_true_literal(term: &Term) -> bool {
    matches!(term, Term::Literal(lit) if lit.value().eq_ignore_ascii_case("true") || lit.value() == "1")
}

fn shape_property(shape: &ShapeSyntax, predicate: &str) -> Vec<Term> {
    shape.constraints
        .iter()
        .find(|constraint| constraint.predicate.as_str() == predicate)
        .map(|constraint| constraint.objects.clone())
        .unwrap_or_default()
}

fn rule_property(rule: &RuleSyntax, predicate: &str) -> Vec<Term> {
    rule.properties
        .iter()
        .find(|property| property.predicate.as_str() == predicate)
        .map(|property| property.objects.clone())
        .unwrap_or_default()
}

fn quad_list_terms(quads: &QuadIndex, head: &Term) -> Vec<Term> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut current = head.clone();

    loop {
        if matches!(&current, Term::NamedNode(node) if node.as_str() == RDF_NIL) {
            break;
        }
        if !seen.insert(current.clone()) {
            break;
        }
        let first = quads.objects_for_subject_predicate(&current, RDF_FIRST);
        let rest = quads.objects_for_subject_predicate(&current, RDF_REST);
        let Some(value) = first.first() else {
            break;
        };
        out.push(value.clone());
        let Some(next) = rest.first() else {
            break;
        };
        current = next.clone();
    }

    out
}

struct QuadIndex {
    by_subject_predicate: HashMap<(Term, String), Vec<Term>>,
}

impl QuadIndex {
    fn new(quads: &[Quad]) -> Self {
        let mut by_subject_predicate = HashMap::new();
        for quad in quads {
            let subject = subject_to_term(&quad.subject);
            by_subject_predicate
                .entry((subject, quad.predicate.as_str().to_string()))
                .or_insert_with(Vec::new)
                .push(quad.object.clone());
        }
        Self { by_subject_predicate }
    }

    fn objects_for_subject_predicate(&self, subject: &Term, predicate: &str) -> Vec<Term> {
        self.by_subject_predicate
            .get(&(subject.clone(), predicate.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    fn has_predicate(&self, subject: &Term, predicate: &str) -> bool {
        self.by_subject_predicate
            .contains_key(&(subject.clone(), predicate.to_string()))
    }
}

fn subject_to_term(subject: &NamedOrBlankNode) -> Term {
    match subject {
        NamedOrBlankNode::NamedNode(node) => Term::NamedNode(node.clone()),
        NamedOrBlankNode::BlankNode(node) => Term::BlankNode(node.clone()),
    }
}
