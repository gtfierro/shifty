use crate::algebra::{
    AdvancedTarget, ComponentDefId, Constraint, ConstraintComponent, ConstraintExpr, ConstraintId,
    DependencyEdge, FeatureUse, LogicalKind, ParameterDefinition, PrefixDeclaration, PropertyPath,
    Rule, RuleExpr, RuleId, Severity, Shape, ShapeId, ShapeKind, ShapeProgram, SparqlConstraint,
    SparqlValidator, Target, TargetExpr, TargetId, Template, TemplateBinding, TemplatePart,
    TemplateSlotKind, TriplePatternTerm,
};
use crate::diagnostics::{
    Diagnostic, DiagnosticSeverity, InspectionEdge, InspectionGraph, InspectionNode,
};
use crate::syntax::{
    ConstraintSyntax, RuleSyntax, RuleSyntaxKind, ShapeSyntax, ShapeSyntaxDocument,
    ShapeSyntaxKind, TargetSyntax,
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
const SH_CONSTRUCT: &str = "http://www.w3.org/ns/shacl#construct";
const SH_CONDITION: &str = "http://www.w3.org/ns/shacl#condition";
const SH_RULE_SUBJECT: &str = "http://www.w3.org/ns/shacl#subject";
const SH_RULE_PREDICATE: &str = "http://www.w3.org/ns/shacl#predicate";
const SH_RULE_OBJECT: &str = "http://www.w3.org/ns/shacl#object";
const SH_DECLARE: &str = "http://www.w3.org/ns/shacl#declare";
const SH_PREFIX: &str = "http://www.w3.org/ns/shacl#prefix";
const SH_NAMESPACE: &str = "http://www.w3.org/ns/shacl#namespace";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NormalizeOptions {
    pub prune_deactivated: bool,
}

impl Default for NormalizeOptions {
    fn default() -> Self {
        Self {
            prune_deactivated: false,
        }
    }
}

pub fn lower_to_program(document: &ShapeSyntaxDocument) -> ShapeProgram {
    let quad_index = QuadIndex::new(&document.quads);
    let mut shapes = document.shapes.clone();
    let normalized_keys = normalize_shape_keys(document, &quad_index);
    shapes.sort_by_key(|shape| {
        normalized_keys
            .get(&shape.subject.to_string())
            .cloned()
            .unwrap_or_else(|| shape.subject.to_string())
    });
    let shape_index: HashMap<String, ShapeId> = shapes
        .iter()
        .enumerate()
        .map(|(idx, shape)| (shape.subject.to_string(), ShapeId((idx + 1) as u64)))
        .collect();
    let normalized_shape_index: HashMap<String, ShapeId> = shapes
        .iter()
        .enumerate()
        .map(|(idx, shape)| {
            (
                normalized_keys
                    .get(&shape.subject.to_string())
                    .cloned()
                    .unwrap_or_else(|| shape.subject.to_string()),
                ShapeId((idx + 1) as u64),
            )
        })
        .collect();
    let component_index: HashMap<String, ComponentDefId> = document
        .constraint_components
        .iter()
        .enumerate()
        .map(|(idx, component)| {
            (
                component.subject.to_string(),
                ComponentDefId((idx + 1) as u64),
            )
        })
        .collect();

    let mut lowered_shapes = Vec::new();
    let mut lowered_targets = Vec::new();
    let mut lowered_constraints = Vec::new();
    let mut lowered_rules = Vec::new();
    let lowered_components = lower_constraint_components(document, &component_index);
    let mut dependencies = Vec::new();
    let mut features = HashSet::new();
    features.insert(FeatureUse::Core);
    if lowered_components.iter().any(|component| {
        component.label_template.is_some()
            || component
                .validators
                .iter()
                .any(|validator| validator.select.is_some() || validator.ask.is_some())
    }) {
        features.insert(FeatureUse::Sparql);
    }
    if lowered_components.iter().any(|component| {
        component
            .label_template_expr
            .as_ref()
            .is_some_and(template_has_slots)
            || component.message_templates.iter().any(template_has_slots)
    }) {
        features.insert(FeatureUse::Templates);
    }
    let mut diagnostics = document.diagnostics.clone();
    let mut rule_map: HashMap<String, RuleSyntax> = document
        .rules
        .iter()
        .cloned()
        .map(|rule| (rule.subject.to_string(), rule))
        .collect();

    for shape in &shapes {
        let shape_id = shape_index[&shape.subject.to_string()];
        let mut target_ids = Vec::new();
        for target_syntax in &shape.targets {
            let target_id = TargetId((lowered_targets.len() + 1) as u64);
            let expr = lower_target(
                target_syntax,
                &quad_index,
                &shape_index,
                &mut dependencies,
                shape_id,
                &mut diagnostics,
            );
            if matches!(expr, TargetExpr::Advanced(_)) {
                features.insert(FeatureUse::AdvancedTargets);
            }
            if target_uses_sparql(&expr) {
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
                &document.constraint_components,
                &component_index,
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
        for sparql in &shape.sparql_constraints {
            features.insert(FeatureUse::Sparql);
            let constraint_id = ConstraintId((lowered_constraints.len() + 1) as u64);
            lowered_constraints.push(Constraint {
                id: constraint_id,
                owner: shape_id,
                expr: ConstraintExpr::Sparql(lower_sparql_constraint(sparql, &quad_index)),
                provenance: sparql.provenance.clone(),
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
                    shape_id,
                    &rule_syntax,
                    &quad_index,
                    &shape_index,
                    &mut dependencies,
                    &mut features,
                    &mut diagnostics,
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
            normalized_key: normalized_keys
                .get(&shape.subject.to_string())
                .cloned()
                .unwrap_or_else(|| shape.subject.to_string()),
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

    canonicalize_program(&ShapeProgram {
        shapes: lowered_shapes,
        constraints: lowered_constraints,
        targets: lowered_targets,
        rules: lowered_rules,
        constraint_components: lowered_components,
        dependencies,
        source_inventory: document.sources.clone(),
        features: feature_list,
        diagnostics,
        inspection,
        shape_index,
        normalized_shape_index,
        component_index,
    })
}

pub fn normalize_program(program: &ShapeProgram, options: NormalizeOptions) -> ShapeProgram {
    let canonical = canonicalize_program(program);
    if options.prune_deactivated {
        prune_deactivated_program(&canonical)
    } else {
        canonical
    }
}

pub fn canonicalize_program(program: &ShapeProgram) -> ShapeProgram {
    let mut normalized = program.clone();
    normalized.shapes.sort_by_key(|shape| shape.id);
    normalized
        .constraints
        .sort_by_key(|constraint| constraint.id);
    normalized.targets.sort_by_key(|target| target.id);
    normalized.rules.sort_by_key(|rule| rule.id);
    normalized
        .constraint_components
        .sort_by_key(|component| component.id);
    normalized
        .dependencies
        .sort_by_key(|edge| (edge.from, edge.to, edge.kind.clone()));
    normalized.dependencies.dedup_by(|left, right| {
        left.from == right.from && left.to == right.to && left.kind == right.kind
    });
    normalized.features.sort_by_key(feature_order);
    normalized.features.dedup();
    normalized.source_inventory.sort_by_key(|source| {
        (
            source.is_root,
            source.graph_iri.clone(),
            source.locator.clone(),
        )
    });
    normalized
        .source_inventory
        .dedup_by(|left, right| left.graph_iri == right.graph_iri && left.locator == right.locator);
    normalized.shape_index = normalized
        .shapes
        .iter()
        .map(|shape| (shape.source.to_string(), shape.id))
        .collect();
    normalized.normalized_shape_index = normalized
        .shapes
        .iter()
        .map(|shape| (shape.normalized_key.clone(), shape.id))
        .collect();
    normalized.component_index = normalized
        .constraint_components
        .iter()
        .map(|component| (component.subject.to_string(), component.id))
        .collect();
    normalized.inspection = build_program_inspection_graph(
        &normalized.shapes,
        &normalized.constraints,
        &normalized.rules,
        &normalized.targets,
        &normalized.dependencies,
        &normalized.source_inventory,
    );
    normalized
}

pub fn prune_deactivated_program(program: &ShapeProgram) -> ShapeProgram {
    let kept_shape_ids: HashSet<_> = program
        .shapes
        .iter()
        .filter(|shape| !shape.deactivated)
        .map(|shape| shape.id)
        .collect();
    let kept_constraint_ids: HashSet<_> = program
        .constraints
        .iter()
        .filter(|constraint| kept_shape_ids.contains(&constraint.owner))
        .map(|constraint| constraint.id)
        .collect();
    let kept_target_ids: HashSet<_> = program
        .targets
        .iter()
        .filter(|target| kept_shape_ids.contains(&target.owner))
        .map(|target| target.id)
        .collect();
    let kept_rule_ids: HashSet<_> = program
        .rules
        .iter()
        .filter(|rule| !rule.deactivated && kept_shape_ids.contains(&rule.owner))
        .map(|rule| rule.id)
        .collect();

    let mut pruned = program.clone();
    pruned.shapes = program
        .shapes
        .iter()
        .filter(|shape| kept_shape_ids.contains(&shape.id))
        .cloned()
        .map(|mut shape| {
            shape
                .property_shapes
                .retain(|id| kept_shape_ids.contains(id));
            shape
                .constraints
                .retain(|id| kept_constraint_ids.contains(id));
            shape.targets.retain(|id| kept_target_ids.contains(id));
            shape
        })
        .collect();
    pruned.constraints = program
        .constraints
        .iter()
        .filter(|constraint| kept_constraint_ids.contains(&constraint.id))
        .cloned()
        .collect();
    pruned.targets = program
        .targets
        .iter()
        .filter(|target| kept_target_ids.contains(&target.id))
        .cloned()
        .collect();
    pruned.rules = program
        .rules
        .iter()
        .filter(|rule| kept_rule_ids.contains(&rule.id))
        .cloned()
        .collect();
    pruned.dependencies = program
        .dependencies
        .iter()
        .filter(|edge| kept_shape_ids.contains(&edge.from) && kept_shape_ids.contains(&edge.to))
        .cloned()
        .collect();
    pruned.diagnostics.push(Diagnostic {
        severity: DiagnosticSeverity::Info,
        message: format!(
            "pruned {} deactivated shapes and {} deactivated rules",
            program.shapes.len().saturating_sub(pruned.shapes.len()),
            program.rules.len().saturating_sub(pruned.rules.len()),
        ),
        source: None,
    });
    canonicalize_program(&pruned)
}

fn lower_target(
    target: &TargetSyntax,
    quads: &QuadIndex,
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    owner: ShapeId,
    diagnostics: &mut Vec<Diagnostic>,
) -> TargetExpr {
    match target {
        TargetSyntax::Class(term) => TargetExpr::Class(term.clone()),
        TargetSyntax::Node(term) => TargetExpr::Node(term.clone()),
        TargetSyntax::SubjectsOf(term) => TargetExpr::SubjectsOf(term.clone()),
        TargetSyntax::ObjectsOf(term) => TargetExpr::ObjectsOf(term.clone()),
        TargetSyntax::Advanced(target) => {
            let target_shape_id = target.target_shape.as_ref().and_then(|term| {
                resolve_target_shape_dependency(
                    owner,
                    term,
                    shape_index,
                    dependencies,
                    diagnostics,
                    &target.provenance,
                    "targetShape",
                )
            });
            let filter_shape_id = target.filter_shape.as_ref().and_then(|term| {
                resolve_target_shape_dependency(
                    owner,
                    term,
                    shape_index,
                    dependencies,
                    diagnostics,
                    &target.provenance,
                    "filterShape",
                )
            });
            let declarations = target
                .declarations
                .iter()
                .map(|declaration| PrefixDeclaration {
                    node: declaration.node.clone(),
                    prefix: declaration.prefix.clone(),
                    namespace: declaration.namespace.clone(),
                })
                .collect::<Vec<_>>();
            TargetExpr::Advanced(AdvancedTarget {
                node: target.node.clone(),
                select: target.select.clone(),
                ask: target.ask.clone(),
                target_shape: target.target_shape.clone(),
                target_shape_id,
                filter_shape: target.filter_shape.clone(),
                filter_shape_id,
                prefixes: target.prefixes.clone(),
                declarations: merge_prefix_declarations(
                    declarations,
                    resolve_prefix_reference_declarations(&target.prefixes, quads),
                ),
                provenance: target.provenance.clone(),
            })
        }
    }
}

fn lower_constraint(
    owner: ShapeId,
    constraint: &ConstraintSyntax,
    shape: &ShapeSyntax,
    quads: &QuadIndex,
    shape_index: &HashMap<String, ShapeId>,
    components: &[crate::syntax::ConstraintComponentSyntax],
    component_index: &HashMap<String, ComponentDefId>,
    dependencies: &mut Vec<DependencyEdge>,
    features: &mut HashSet<FeatureUse>,
    diagnostics: &mut Vec<Diagnostic>,
) -> ConstraintExpr {
    let predicate = constraint.predicate.as_str();
    match predicate {
        SH_NODE => shape_ref_expr(
            &constraint.objects,
            shape_index,
            dependencies,
            owner,
            "node",
        )
        .map(|(shape_id, source)| ConstraintExpr::NodeRef {
            shape: shape_id,
            source,
        })
        .unwrap_or_else(|| generic_expr(constraint)),
        SH_PROPERTY => shape_ref_expr(
            &constraint.objects,
            shape_index,
            dependencies,
            owner,
            "property",
        )
        .map(|(shape_id, source)| ConstraintExpr::PropertyRef {
            shape: shape_id,
            source,
        })
        .unwrap_or_else(|| generic_expr(constraint)),
        SH_QUALIFIED_VALUE_SHAPE => {
            let (shape_id, source) = shape_ref_expr(
                &constraint.objects,
                shape_index,
                dependencies,
                owner,
                "qualified",
            )
            .unwrap_or((
                None,
                constraint
                    .objects
                    .first()
                    .cloned()
                    .unwrap_or_else(|| shape.subject.clone()),
            ));
            if shape_id.is_none() {
                diagnostics.push(Diagnostic {
                    severity: DiagnosticSeverity::Warning,
                    message: format!(
                        "qualified value shape {} on {} did not resolve to a discovered shape",
                        source, shape.subject
                    ),
                    source: shape.provenance.first().cloned(),
                });
            }
            ConstraintExpr::QualifiedValueShape {
                shape: shape_id,
                source,
                min_count: literal_u64(
                    first_term(shape_property(shape, SH_QUALIFIED_MIN_COUNT)).as_ref(),
                ),
                max_count: literal_u64(
                    first_term(shape_property(shape, SH_QUALIFIED_MAX_COUNT)).as_ref(),
                ),
                disjoint: first_term(shape_property(shape, SH_QUALIFIED_VALUE_SHAPES_DISJOINT))
                    .as_ref()
                    .map(is_true_literal),
            }
        }
        SH_NOT => shape_ref_expr(&constraint.objects, shape_index, dependencies, owner, "not")
            .map(|(shape_id, source)| ConstraintExpr::Not {
                shape: shape_id,
                source,
            })
            .unwrap_or_else(|| generic_expr(constraint)),
        SH_AND | SH_OR | SH_XONE => {
            let shape_terms: Vec<Term> = constraint
                .objects
                .iter()
                .flat_map(|term| quad_list_terms(quads, term))
                .collect();
            let shapes = resolve_shape_refs(
                &shape_terms,
                shape_index,
                dependencies,
                owner,
                "logical",
                diagnostics,
                shape.provenance.first().cloned(),
                &format!(
                    "logical constraint {} on {}",
                    constraint.predicate, shape.subject
                ),
            );
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
        SH_NODE_KIND => {
            single_value_expr(&constraint.objects, ConstraintExpr::NodeKind, constraint)
        }
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
        _ => {
            if !constraint
                .predicate
                .as_str()
                .starts_with("http://www.w3.org/ns/shacl#")
            {
                features.insert(FeatureUse::CustomComponents);
                let component = resolve_custom_component_for_parameter(
                    &constraint.predicate,
                    components,
                    component_index,
                );
                return ConstraintExpr::CustomComponent {
                    predicate: constraint.predicate.clone(),
                    component,
                    values: constraint.objects.clone(),
                    bindings: lower_component_bindings(shape, components, component),
                    message_templates: lower_component_message_templates(components, component),
                    label_template: lower_component_label_template(components, component),
                };
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

fn lower_constraint_components(
    document: &ShapeSyntaxDocument,
    component_index: &HashMap<String, ComponentDefId>,
) -> Vec<ConstraintComponent> {
    let quad_index = QuadIndex::new(&document.quads);
    document
        .constraint_components
        .iter()
        .map(|component| ConstraintComponent {
            id: component_index[&component.subject.to_string()],
            subject: component.subject.clone(),
            parameters: component
                .parameters
                .iter()
                .map(|parameter| ParameterDefinition {
                    node: parameter.node.clone(),
                    path: parameter.path.clone(),
                    datatype: parameter.datatype.clone(),
                    var_name: parameter.var_name.clone(),
                    name: parameter.name.clone(),
                    description: parameter.description.clone(),
                    optional: parameter.optional,
                    default_values: parameter.default_values.clone(),
                })
                .collect(),
            validators: component
                .validators
                .iter()
                .map(|validator| SparqlValidator {
                    node: validator.node.clone(),
                    kind: validator.kind.clone(),
                    select: validator.select.clone(),
                    ask: validator.ask.clone(),
                    messages: validator.messages.clone(),
                    prefixes: validator.prefixes.clone(),
                    declarations: merge_prefix_declarations(
                        lower_prefix_declarations(&validator.declarations),
                        resolve_prefix_reference_declarations(&validator.prefixes, &quad_index),
                    ),
                })
                .collect(),
            messages: component.messages.clone(),
            message_templates: component
                .messages
                .iter()
                .filter_map(as_literal_value)
                .map(parse_template)
                .collect(),
            prefixes: component.prefixes.clone(),
            declarations: merge_prefix_declarations(
                lower_prefix_declarations(&component.declarations),
                resolve_prefix_reference_declarations(&component.prefixes, &quad_index),
            ),
            label: component.label.clone(),
            label_template: component.label_template.clone(),
            label_template_expr: component
                .label_template
                .as_deref()
                .map(|raw| parse_template(raw.to_string())),
            comment: component.comment.clone(),
            provenance: component.provenance.clone(),
        })
        .collect()
}

fn lower_component_bindings(
    shape: &ShapeSyntax,
    components: &[crate::syntax::ConstraintComponentSyntax],
    component_id: Option<ComponentDefId>,
) -> Vec<TemplateBinding> {
    let Some(component_id) = component_id else {
        return Vec::new();
    };
    let Some(component) = components
        .iter()
        .enumerate()
        .find(|(index, _)| ComponentDefId((index + 1) as u64) == component_id)
        .map(|(_, component)| component)
    else {
        return Vec::new();
    };

    component
        .parameters
        .iter()
        .filter_map(|parameter| {
            let name = parameter
                .var_name
                .clone()
                .or_else(|| parameter.name.clone())?;
            let values = parameter_value_bindings(shape, parameter);
            if values.is_empty() {
                return None;
            }
            Some(TemplateBinding {
                parameter: parameter.node.clone(),
                name,
                from_default: match parameter.path.as_ref() {
                    Some(path) => parameter_value_from_shape(shape, path).is_none(),
                    None => true,
                },
                values,
            })
        })
        .collect()
}

fn parameter_value_bindings(
    shape: &ShapeSyntax,
    parameter: &crate::syntax::ParameterSyntax,
) -> Vec<Term> {
    if let Some(path) = parameter.path.as_ref()
        && let Some(values) = parameter_value_from_shape(shape, path)
    {
        return values;
    }
    parameter.default_values.clone()
}

fn parameter_value_from_shape(shape: &ShapeSyntax, path: &Term) -> Option<Vec<Term>> {
    let Term::NamedNode(path) = path else {
        return None;
    };
    shape
        .constraints
        .iter()
        .find_map(|constraint| (constraint.predicate == *path).then(|| constraint.objects.clone()))
}

fn lower_component_message_templates(
    components: &[crate::syntax::ConstraintComponentSyntax],
    component_id: Option<ComponentDefId>,
) -> Vec<Template> {
    resolve_component_syntax(components, component_id)
        .into_iter()
        .flat_map(|component| component.messages.iter())
        .filter_map(as_literal_value)
        .map(parse_template)
        .collect()
}

fn lower_component_label_template(
    components: &[crate::syntax::ConstraintComponentSyntax],
    component_id: Option<ComponentDefId>,
) -> Option<Template> {
    resolve_component_syntax(components, component_id)
        .and_then(|component| component.label_template.as_deref())
        .map(|raw| parse_template(raw.to_string()))
}

fn resolve_component_syntax<'a>(
    components: &'a [crate::syntax::ConstraintComponentSyntax],
    component_id: Option<ComponentDefId>,
) -> Option<&'a crate::syntax::ConstraintComponentSyntax> {
    let component_id = component_id?;
    components
        .iter()
        .enumerate()
        .find(|(index, _)| ComponentDefId((index + 1) as u64) == component_id)
        .map(|(_, component)| component)
}

fn lower_sparql_constraint(
    constraint: &crate::syntax::SparqlConstraintSyntax,
    quads: &QuadIndex,
) -> SparqlConstraint {
    SparqlConstraint {
        node: constraint.node.clone(),
        kind: constraint.kind.clone(),
        select: constraint.select.clone(),
        ask: constraint.ask.clone(),
        messages: constraint.messages.clone(),
        prefixes: constraint.prefixes.clone(),
        declarations: merge_prefix_declarations(
            lower_prefix_declarations(&constraint.declarations),
            resolve_prefix_reference_declarations(&constraint.prefixes, quads),
        ),
        provenance: constraint.provenance.clone(),
    }
}

fn lower_prefix_declarations(
    declarations: &[crate::syntax::PrefixDeclarationSyntax],
) -> Vec<PrefixDeclaration> {
    declarations
        .iter()
        .map(|declaration| PrefixDeclaration {
            node: declaration.node.clone(),
            prefix: declaration.prefix.clone(),
            namespace: declaration.namespace.clone(),
        })
        .collect()
}

fn resolve_prefix_reference_declarations(
    prefix_refs: &[Term],
    quads: &QuadIndex,
) -> Vec<PrefixDeclaration> {
    prefix_refs
        .iter()
        .flat_map(|prefix_ref| {
            quads
                .objects_for_subject_predicate(prefix_ref, SH_DECLARE)
                .into_iter()
                .map(|declaration| PrefixDeclaration {
                    prefix: first_literal_from_quads(
                        &quads.objects_for_subject_predicate(&declaration, SH_PREFIX),
                    ),
                    namespace: quads
                        .objects_for_subject_predicate(&declaration, SH_NAMESPACE)
                        .into_iter()
                        .next(),
                    node: declaration,
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn merge_prefix_declarations(
    explicit: Vec<PrefixDeclaration>,
    referenced: Vec<PrefixDeclaration>,
) -> Vec<PrefixDeclaration> {
    let mut merged = explicit;
    for declaration in referenced {
        let duplicate = merged.iter().any(|existing| {
            existing.prefix == declaration.prefix
                && existing.namespace == declaration.namespace
                && existing.node == declaration.node
        });
        if !duplicate {
            merged.push(declaration);
        }
    }
    merged
}

fn first_literal_from_quads(values: &[Term]) -> Option<String> {
    values.iter().find_map(as_literal_value)
}

fn resolve_custom_component_for_parameter(
    predicate: &NamedNode,
    components: &[crate::syntax::ConstraintComponentSyntax],
    component_index: &HashMap<String, ComponentDefId>,
) -> Option<ComponentDefId> {
    components.iter().find_map(|component| {
        component
            .parameters
            .iter()
            .any(|parameter| parameter.path.as_ref() == Some(&Term::NamedNode(predicate.clone())))
            .then(|| component_index[&component.subject.to_string()])
    })
}

fn lower_rule(
    owner: ShapeId,
    rule: &RuleSyntax,
    quads: &QuadIndex,
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    features: &mut HashSet<FeatureUse>,
    diagnostics: &mut Vec<Diagnostic>,
) -> RuleExpr {
    let conditions = rule_shape_conditions(
        owner,
        rule,
        shape_index,
        dependencies,
        diagnostics,
        rule.provenance.first().cloned(),
    );
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
    } else if matches!(term, Term::BlankNode(_)) && quads.has_predicate(term, SH_PATH) {
        TriplePatternTerm::Path(lower_property_path(term, quads))
    } else {
        TriplePatternTerm::Constant(term.clone())
    }
}

fn lower_property_path(term: &Term, quads: &QuadIndex) -> PropertyPath {
    match term {
        Term::NamedNode(node) => PropertyPath::Predicate(node.clone()),
        Term::BlankNode(_) => {
            if let Some(inner) =
                first_term(quads.objects_for_subject_predicate(term, SH_INVERSE_PATH))
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
            label: shape.normalized_key.clone(),
            annotations: HashMap::from([
                ("shape_kind".to_string(), format!("{:?}", shape.kind)),
                ("deactivated".to_string(), shape.deactivated.to_string()),
                ("source".to_string(), shape.source.to_string()),
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
            label: source
                .locator
                .clone()
                .unwrap_or_else(|| source.graph_iri.clone()),
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

fn build_program_inspection_graph(
    shapes: &[Shape],
    constraints: &[Constraint],
    rules: &[Rule],
    targets: &[Target],
    dependencies: &[DependencyEdge],
    sources: &[crate::diagnostics::SourceRef],
) -> InspectionGraph {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for shape in shapes {
        nodes.push(InspectionNode {
            id: format!("shape:{}", shape.id.0),
            kind: "shape".to_string(),
            label: shape.normalized_key.clone(),
            annotations: HashMap::from([
                ("shape_kind".to_string(), format!("{:?}", shape.kind)),
                ("deactivated".to_string(), shape.deactivated.to_string()),
                ("source".to_string(), shape.source.to_string()),
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

    for source in sources {
        nodes.push(InspectionNode {
            id: format!("source:{}", source.graph_iri),
            kind: "source".to_string(),
            label: source
                .locator
                .clone()
                .unwrap_or_else(|| source.graph_iri.clone()),
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
    owner: ShapeId,
    rule: &RuleSyntax,
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    diagnostics: &mut Vec<Diagnostic>,
    source: Option<crate::diagnostics::SourceRef>,
) -> Vec<ShapeId> {
    resolve_shape_refs(
        &rule_property(rule, SH_CONDITION),
        shape_index,
        dependencies,
        owner,
        "rule_condition",
        diagnostics,
        source,
        &format!("rule condition(s) on {}", rule.subject),
    )
}

fn target_uses_sparql(target: &TargetExpr) -> bool {
    matches!(
        target,
        TargetExpr::Advanced(AdvancedTarget {
            select: Some(_),
            ..
        }) | TargetExpr::Advanced(AdvancedTarget { ask: Some(_), .. })
    )
}

fn resolve_target_shape_dependency(
    owner: ShapeId,
    term: &Term,
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    diagnostics: &mut Vec<Diagnostic>,
    provenance: &[crate::diagnostics::SourceRef],
    edge_kind: &str,
) -> Option<ShapeId> {
    let resolved = shape_index.get(&term.to_string()).copied();
    if let Some(shape) = resolved {
        dependencies.push(DependencyEdge {
            from: owner,
            to: shape,
            kind: "target".to_string(),
        });
        return Some(shape);
    }
    diagnostics.push(Diagnostic {
        severity: DiagnosticSeverity::Warning,
        message: format!("unresolved {edge_kind} reference {}", term),
        source: provenance.first().cloned(),
    });
    None
}

fn normalize_shape_keys(
    document: &ShapeSyntaxDocument,
    quads: &QuadIndex,
) -> HashMap<String, String> {
    let rule_index: HashMap<String, &RuleSyntax> = document
        .rules
        .iter()
        .map(|rule| (rule.subject.to_string(), rule))
        .collect();
    let shapes_by_subject: HashMap<String, &ShapeSyntax> = document
        .shapes
        .iter()
        .map(|shape| (shape.subject.to_string(), shape))
        .collect();
    let mut keys: HashMap<String, String> = document
        .shapes
        .iter()
        .filter_map(|shape| match &shape.subject {
            Term::NamedNode(_) => Some((shape.subject.to_string(), shape.subject.to_string())),
            _ => None,
        })
        .collect();

    loop {
        let mut changed = false;
        for shape in &document.shapes {
            let Some(owner_key) = keys.get(&shape.subject.to_string()).cloned() else {
                continue;
            };
            for (role, term) in shape_reference_slots(shape, &rule_index, quads) {
                if !matches!(term, Term::BlankNode(_))
                    || !shapes_by_subject.contains_key(&term.to_string())
                {
                    continue;
                }
                let candidate = format!("{owner_key}/{role}");
                match keys.get(&term.to_string()) {
                    Some(existing) if existing <= &candidate => {}
                    _ => {
                        keys.insert(term.to_string(), candidate);
                        changed = true;
                    }
                }
            }
        }
        if !changed {
            break;
        }
    }

    let mut fallback_terms: Vec<_> = document
        .shapes
        .iter()
        .filter(|shape| !keys.contains_key(&shape.subject.to_string()))
        .map(|shape| shape.subject.to_string())
        .collect();
    fallback_terms.sort();
    for (index, term) in fallback_terms.into_iter().enumerate() {
        keys.insert(term, format!("inline:shape[{}]", index));
    }
    keys
}

fn shape_reference_slots(
    shape: &ShapeSyntax,
    rule_index: &HashMap<String, &RuleSyntax>,
    quads: &QuadIndex,
) -> Vec<(String, Term)> {
    let mut refs = Vec::new();
    for (index, term) in shape.property_shapes.iter().enumerate() {
        refs.push((format!("property[{index}]"), term.clone()));
    }
    for constraint in &shape.constraints {
        match constraint.predicate.as_str() {
            SH_NODE | SH_NOT | SH_QUALIFIED_VALUE_SHAPE => {
                for (index, term) in constraint.objects.iter().enumerate() {
                    refs.push((
                        format!(
                            "{}[{index}]",
                            compact_shacl_local_name(constraint.predicate.as_str())
                        ),
                        term.clone(),
                    ));
                }
            }
            SH_AND | SH_OR | SH_XONE => {
                for (group_index, term) in constraint.objects.iter().enumerate() {
                    let members = quad_list_terms(quads, term);
                    for (member_index, member) in members.iter().enumerate() {
                        refs.push((
                            format!(
                                "{}[{group_index}][{member_index}]",
                                compact_shacl_local_name(constraint.predicate.as_str())
                            ),
                            member.clone(),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    for (index, target) in shape.targets.iter().enumerate() {
        if let TargetSyntax::Advanced(target) = target {
            if let Some(term) = target.target_shape.as_ref() {
                refs.push((format!("target[{index}]/targetShape"), term.clone()));
            }
            if let Some(term) = target.filter_shape.as_ref() {
                refs.push((format!("target[{index}]/filterShape"), term.clone()));
            }
        }
    }
    for (rule_index_in_shape, rule_term) in shape.rule_nodes.iter().enumerate() {
        if let Some(rule) = rule_index.get(&rule_term.to_string()) {
            let conditions = rule_property(rule, SH_CONDITION);
            for (condition_index, condition) in conditions.iter().enumerate() {
                refs.push((
                    format!("rule[{rule_index_in_shape}]/condition[{condition_index}]"),
                    condition.clone(),
                ));
            }
        }
    }
    refs
}

fn compact_shacl_local_name(iri: &str) -> &str {
    iri.rsplit('#').next().unwrap_or(iri)
}

fn parse_template(raw: String) -> Template {
    let mut parts = Vec::new();
    let mut cursor = 0usize;
    while let Some(start_rel) = raw[cursor..].find('{') {
        let start = cursor + start_rel;
        if start > cursor {
            parts.push(TemplatePart::Text(raw[cursor..start].to_string()));
        }
        let Some(end_rel) = raw[start + 1..].find('}') else {
            parts.push(TemplatePart::Text(raw[start..].to_string()));
            cursor = raw.len();
            break;
        };
        let end = start + 1 + end_rel;
        let token = &raw[start + 1..end];
        let parsed = token
            .strip_prefix('?')
            .map(|name| (TemplateSlotKind::Variable, name))
            .or_else(|| {
                token
                    .strip_prefix('$')
                    .map(|name| (TemplateSlotKind::Parameter, name))
            });
        match parsed {
            Some((kind, name)) if !name.is_empty() => parts.push(TemplatePart::Slot {
                kind,
                name: name.to_string(),
            }),
            _ => parts.push(TemplatePart::Text(raw[start..=end].to_string())),
        }
        cursor = end + 1;
    }
    if cursor < raw.len() {
        parts.push(TemplatePart::Text(raw[cursor..].to_string()));
    }
    Template { raw, parts }
}

fn template_has_slots(template: &Template) -> bool {
    template
        .parts
        .iter()
        .any(|part| matches!(part, TemplatePart::Slot { .. }))
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

fn as_literal_value(term: &Term) -> Option<String> {
    match term {
        Term::Literal(lit) => Some(lit.value().to_string()),
        _ => None,
    }
}

fn is_true_literal(term: &Term) -> bool {
    matches!(term, Term::Literal(lit) if lit.value().eq_ignore_ascii_case("true") || lit.value() == "1")
}

fn shape_property(shape: &ShapeSyntax, predicate: &str) -> Vec<Term> {
    shape
        .constraints
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

fn resolve_shape_refs(
    terms: &[Term],
    shape_index: &HashMap<String, ShapeId>,
    dependencies: &mut Vec<DependencyEdge>,
    owner: ShapeId,
    edge_kind: &str,
    diagnostics: &mut Vec<Diagnostic>,
    source: Option<crate::diagnostics::SourceRef>,
    context: &str,
) -> Vec<ShapeId> {
    let mut resolved = Vec::new();
    for term in terms {
        if let Some(id) = shape_index.get(&term.to_string()).copied() {
            dependencies.push(DependencyEdge {
                from: owner,
                to: id,
                kind: edge_kind.to_string(),
            });
            resolved.push(id);
        } else {
            diagnostics.push(Diagnostic {
                severity: DiagnosticSeverity::Warning,
                message: format!("unresolved shape reference {} in {}", term, context),
                source: source.clone(),
            });
        }
    }
    resolved
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
        Self {
            by_subject_predicate,
        }
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
