use oxrdf::{NamedNode, Term};
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use shifty_shacl_core::algebra::{
    ConstraintExpr, ConstraintId, LogicalKind, PropertyPath, RuleExpr, RuleId, ShapeId,
    ShapeProgram, SparqlConstraint, TargetExpr, TargetId, Template, TemplateBinding,
    TriplePatternTerm,
};
use shifty_shacl_core::backend_views::BackendBucket;
use shifty_shacl_core::plan::{ValidationPlan, ValidationPlanNode};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct CompiledValidationProgram {
    pub shape_positions: HashMap<ShapeId, usize>,
    pub constraint_positions: HashMap<ConstraintId, usize>,
    pub target_positions: HashMap<TargetId, usize>,
    pub targets_by_owner: HashMap<ShapeId, Vec<TargetId>>,
    pub property_shapes_by_owner: HashMap<ShapeId, Vec<ShapeId>>,
    pub constraint_order_by_shape: HashMap<ShapeId, Vec<ConstraintId>>,
    pub target_scans: Vec<CompiledTargetScan>,
    pub constraint_batches: Vec<CompiledConstraintBatch>,
    pub constraint_ops: HashMap<ConstraintId, CompiledConstraintOp>,
    pub rule_plans: HashMap<RuleId, CompiledRulePlan>,
    pub has_sparql_work: bool,
    pub has_advanced_targets: bool,
    pub has_sparql_rules: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledTargetScan {
    pub shape: ShapeId,
    pub targets: Vec<TargetId>,
    pub empty_scan: bool,
    pub cacheable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledConstraintBatch {
    pub shape: ShapeId,
    pub bucket: BackendBucket,
    pub constraints: Vec<ConstraintId>,
    pub dead_constraints: Vec<ConstraintId>,
    pub vacuous_constraints: Vec<ConstraintId>,
}

#[derive(Debug, Clone)]
pub struct CompiledRulePlan {
    pub rule_id: RuleId,
    pub owner_shape: ShapeId,
    pub mode: RuleExecutionMode,
    pub uses_conditions: bool,
    pub kind: String,
    pub focus_stable: bool,
    pub condition_dependencies: Vec<String>,
    pub condition_dependencies_global: bool,
}

#[derive(Debug, Clone)]
pub enum RuleExecutionMode {
    PredicateDriven(Vec<String>),
    Global,
}

#[derive(Debug, Clone)]
pub enum CompiledConstraintOp {
    Cardinality {
        predicate: NamedNode,
        min: Option<u64>,
        max: Option<u64>,
    },
    Class(Term),
    Datatype(Term),
    NodeKind(Term),
    HasValue(Term),
    In(Vec<Term>),
    NumericRange {
        predicate: NamedNode,
        values: Vec<Term>,
    },
    StringConstraint {
        predicate: NamedNode,
        values: Vec<Term>,
        compiled_pattern: Option<CompiledPattern>,
    },
    PropertyComparison {
        predicate: NamedNode,
        values: Vec<Term>,
    },
    NodeRef {
        shape: Option<ShapeId>,
        source: Term,
    },
    PropertyRef {
        shape: Option<ShapeId>,
        source: Term,
    },
    QualifiedValueShape {
        shape: Option<ShapeId>,
        source: Term,
        min_count: Option<u64>,
        max_count: Option<u64>,
        disjoint: Option<bool>,
    },
    Logical {
        kind: LogicalKind,
        shapes: Vec<ShapeId>,
    },
    Not {
        shape: Option<ShapeId>,
        source: Term,
    },
    Closed {
        ignored_properties: Vec<Term>,
    },
    Sparql(SparqlConstraint),
    CustomComponent {
        predicate: NamedNode,
        component: Option<shifty_shacl_core::algebra::ComponentDefId>,
        values: Vec<Term>,
        bindings: Vec<TemplateBinding>,
        message_templates: Vec<Template>,
        label_template: Option<Template>,
    },
    GenericPredicate {
        predicate: NamedNode,
        values: Vec<Term>,
    },
}

#[derive(Debug, Clone)]
pub struct CompiledPattern {
    pub pattern: String,
    pub case_insensitive: bool,
    pub regex: Regex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InspectablePhysicalPlan {
    pub shape_count: usize,
    pub constraint_count: usize,
    pub target_scans: Vec<CompiledTargetScan>,
    pub constraint_batches: Vec<CompiledConstraintBatch>,
    pub constraint_ops: Vec<InspectableConstraintOp>,
    pub rule_plans: Vec<InspectableRulePlan>,
    pub has_sparql_work: bool,
    pub has_advanced_targets: bool,
    pub has_sparql_rules: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InspectableConstraintOp {
    pub constraint: ConstraintId,
    pub kind: String,
    pub compiled_kind: String,
    pub precompiled_regex: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InspectableRulePlan {
    pub rule_id: RuleId,
    pub owner_shape: ShapeId,
    pub kind: String,
    pub mode: String,
    pub dependency_predicates: Vec<String>,
    pub uses_conditions: bool,
    pub focus_stable: bool,
    pub condition_dependencies: Vec<String>,
    pub condition_dependencies_global: bool,
}

pub fn compile_validation_plan(plan: &ValidationPlan) -> CompiledValidationProgram {
    let program = &plan.view.program;
    let shape_positions = program
        .shapes
        .iter()
        .enumerate()
        .map(|(index, shape)| (shape.id, index))
        .collect::<HashMap<_, _>>();
    let constraint_positions = program
        .constraints
        .iter()
        .enumerate()
        .map(|(index, constraint)| (constraint.id, index))
        .collect::<HashMap<_, _>>();
    let target_positions = program
        .targets
        .iter()
        .enumerate()
        .map(|(index, target)| (target.id, index))
        .collect::<HashMap<_, _>>();
    let targets_by_owner = program
        .shapes
        .iter()
        .map(|shape| (shape.id, shape.targets.clone()))
        .collect::<HashMap<_, _>>();
    let property_shapes_by_owner = program
        .shapes
        .iter()
        .map(|shape| (shape.id, shape.property_shapes.clone()))
        .collect::<HashMap<_, _>>();

    let target_scans = plan
        .nodes
        .iter()
        .filter_map(|node| match node {
            ValidationPlanNode::TargetScan { shape, targets } => {
                let empty_scan = plan
                    .annotations
                    .target_scans
                    .get(&shape.0)
                    .and_then(|annotation| annotation.empty_scan)
                    .unwrap_or(false);
                let cacheable = targets.iter().all(|target_id| {
                    target_positions
                        .get(target_id)
                        .and_then(|index| program.targets.get(*index))
                        .is_some_and(|target| target_expr_cacheable(&target.expr))
                });
                Some(CompiledTargetScan {
                    shape: *shape,
                    targets: targets.clone(),
                    empty_scan,
                    cacheable,
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    let constraint_batches = plan
        .nodes
        .iter()
        .filter_map(|node| match node {
            ValidationPlanNode::ConstraintBatch {
                shape,
                bucket,
                constraints,
            } => {
                let dead_constraints = plan
                    .annotations
                    .constraint_batches
                    .get(&shape.0)
                    .map(|annotation| annotation.dead_constraint_candidates.clone())
                    .unwrap_or_default();
                let vacuous_constraints = plan
                    .annotations
                    .constraint_batches
                    .get(&shape.0)
                    .map(|annotation| annotation.vacuous_constraint_candidates.clone())
                    .unwrap_or_default();
                Some(CompiledConstraintBatch {
                    shape: *shape,
                    bucket: bucket.clone(),
                    constraints: constraints.clone(),
                    dead_constraints,
                    vacuous_constraints,
                })
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    let constraint_order_by_shape = constraint_batches
        .iter()
        .map(|batch| (batch.shape, batch.constraints.clone()))
        .collect::<HashMap<_, _>>();
    let constraint_ops = program
        .constraints
        .iter()
        .map(|constraint| (constraint.id, compile_constraint_op(&constraint.expr)))
        .collect::<HashMap<_, _>>();
    let rule_plans = plan
        .executable_rules
        .iter()
        .map(|rule| (rule.id, compile_rule_plan(program, rule)))
        .collect::<HashMap<_, _>>();
    let has_advanced_targets = program
        .targets
        .iter()
        .any(|target| matches!(target.expr, TargetExpr::Advanced(_)));
    let has_sparql_constraints = program.constraints.iter().any(|constraint| {
        matches!(
            constraint.expr,
            ConstraintExpr::Sparql(_) | ConstraintExpr::CustomComponent { .. }
        )
    });
    let has_sparql_rules = plan
        .executable_rules
        .iter()
        .any(|rule| matches!(rule.expr, RuleExpr::Sparql { .. }));

    CompiledValidationProgram {
        shape_positions,
        constraint_positions,
        target_positions,
        targets_by_owner,
        property_shapes_by_owner,
        constraint_order_by_shape,
        target_scans,
        constraint_batches,
        constraint_ops,
        rule_plans,
        has_sparql_work: has_sparql_constraints || has_advanced_targets || has_sparql_rules,
        has_advanced_targets,
        has_sparql_rules,
    }
}

impl CompiledValidationProgram {
    pub fn inspect(&self) -> InspectablePhysicalPlan {
        let mut constraint_ops = self
            .constraint_ops
            .iter()
            .map(|(constraint, op)| InspectableConstraintOp {
                constraint: *constraint,
                kind: constraint_op_kind(op).to_string(),
                compiled_kind: compiled_op_name(op).to_string(),
                precompiled_regex: matches!(
                    op,
                    CompiledConstraintOp::StringConstraint {
                        compiled_pattern: Some(_),
                        ..
                    }
                ),
            })
            .collect::<Vec<_>>();
        constraint_ops.sort_by_key(|entry| entry.constraint.0);
        let mut rule_plans = self
            .rule_plans
            .values()
            .map(|rule| InspectableRulePlan {
                rule_id: rule.rule_id,
                owner_shape: rule.owner_shape,
                kind: rule.kind.clone(),
                mode: rule_mode_name(&rule.mode).to_string(),
                dependency_predicates: match &rule.mode {
                    RuleExecutionMode::PredicateDriven(predicates) => predicates.clone(),
                    RuleExecutionMode::Global => Vec::new(),
                },
                uses_conditions: rule.uses_conditions,
                focus_stable: rule.focus_stable,
                condition_dependencies: rule.condition_dependencies.clone(),
                condition_dependencies_global: rule.condition_dependencies_global,
            })
            .collect::<Vec<_>>();
        rule_plans.sort_by_key(|entry| entry.rule_id.0);
        InspectablePhysicalPlan {
            shape_count: self.shape_positions.len(),
            constraint_count: self.constraint_positions.len(),
            target_scans: self.target_scans.clone(),
            constraint_batches: self.constraint_batches.clone(),
            constraint_ops,
            rule_plans,
            has_sparql_work: self.has_sparql_work,
            has_advanced_targets: self.has_advanced_targets,
            has_sparql_rules: self.has_sparql_rules,
        }
    }
}

fn compile_rule_plan(
    program: &ShapeProgram,
    rule: &shifty_shacl_core::algebra::Rule,
) -> CompiledRulePlan {
    let uses_conditions = match &rule.expr {
        RuleExpr::Triple { conditions, .. }
        | RuleExpr::Sparql { conditions, .. }
        | RuleExpr::Generic { conditions, .. } => !conditions.is_empty(),
    };
    let kind = match &rule.expr {
        RuleExpr::Triple { .. } => "triple",
        RuleExpr::Sparql { .. } => "sparql",
        RuleExpr::Generic { .. } => "generic",
    }
    .to_string();
    let mode = rule_execution_mode(program, rule);
    let (condition_dependencies, condition_dependencies_global) =
        compile_condition_dependencies(program, rule);
    let focus_stable = rule_focus_stable(rule);
    CompiledRulePlan {
        rule_id: rule.id,
        owner_shape: rule.owner,
        mode,
        uses_conditions,
        kind,
        focus_stable,
        condition_dependencies,
        condition_dependencies_global,
    }
}

fn compile_condition_dependencies(
    program: &ShapeProgram,
    rule: &shifty_shacl_core::algebra::Rule,
) -> (Vec<String>, bool) {
    let conditions = match &rule.expr {
        RuleExpr::Triple { conditions, .. }
        | RuleExpr::Sparql { conditions, .. }
        | RuleExpr::Generic { conditions, .. } => conditions,
    };
    let mut deps = Vec::new();
    let mut global = false;
    for condition in conditions {
        collect_shape_dependencies(program, *condition, &mut deps, &mut global, &mut Vec::new());
    }
    deps.sort();
    deps.dedup();
    (deps, global)
}

fn rule_focus_stable(rule: &shifty_shacl_core::algebra::Rule) -> bool {
    match &rule.expr {
        RuleExpr::Triple {
            subject, object, ..
        } => {
            triple_term_focus_stable(subject.as_ref()) && triple_term_focus_stable(object.as_ref())
        }
        RuleExpr::Sparql { .. } | RuleExpr::Generic { .. } => false,
    }
}

fn triple_term_focus_stable(term: Option<&TriplePatternTerm>) -> bool {
    matches!(
        term,
        Some(TriplePatternTerm::This | TriplePatternTerm::Constant(_))
    )
}

fn rule_execution_mode(
    program: &ShapeProgram,
    rule: &shifty_shacl_core::algebra::Rule,
) -> RuleExecutionMode {
    match &rule.expr {
        RuleExpr::Sparql { .. } | RuleExpr::Generic { .. } => RuleExecutionMode::Global,
        RuleExpr::Triple {
            subject,
            object,
            conditions,
            ..
        } => {
            let mut deps = Vec::new();
            let mut global = false;
            collect_owner_target_dependencies(program, rule.owner, &mut deps, &mut global);
            collect_term_dependencies(subject.as_ref(), &mut deps, &mut global);
            collect_term_dependencies(object.as_ref(), &mut deps, &mut global);
            for condition in conditions {
                collect_shape_dependencies(
                    program,
                    *condition,
                    &mut deps,
                    &mut global,
                    &mut Vec::new(),
                );
            }
            deps.sort();
            deps.dedup();
            if global {
                RuleExecutionMode::Global
            } else if deps.is_empty() {
                RuleExecutionMode::Global
            } else {
                RuleExecutionMode::PredicateDriven(deps)
            }
        }
    }
}

fn collect_owner_target_dependencies(
    program: &ShapeProgram,
    owner: ShapeId,
    deps: &mut Vec<String>,
    global: &mut bool,
) {
    let Some(shape) = program.shapes.iter().find(|shape| shape.id == owner) else {
        return;
    };
    for target_id in &shape.targets {
        let Some(target) = program
            .targets
            .iter()
            .find(|target| target.id == *target_id)
        else {
            continue;
        };
        match &target.expr {
            TargetExpr::Node(_) => {}
            TargetExpr::Class(_) => {
                deps.push("http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string());
                deps.push("http://www.w3.org/2000/01/rdf-schema#subClassOf".to_string());
            }
            TargetExpr::SubjectsOf(term) | TargetExpr::ObjectsOf(term) => {
                if let Term::NamedNode(node) = term {
                    deps.push(node.as_str().to_string());
                } else {
                    *global = true;
                }
            }
            TargetExpr::Advanced(_) => *global = true,
        }
    }
}

fn collect_term_dependencies(
    term: Option<&TriplePatternTerm>,
    deps: &mut Vec<String>,
    global: &mut bool,
) {
    match term {
        Some(TriplePatternTerm::Path(path)) => collect_path_dependencies(path, deps, global),
        Some(TriplePatternTerm::This | TriplePatternTerm::Constant(_)) | None => {}
    }
}

fn collect_path_dependencies(path: &PropertyPath, deps: &mut Vec<String>, global: &mut bool) {
    match path {
        PropertyPath::Predicate(predicate) => deps.push(predicate.as_str().to_string()),
        PropertyPath::Inverse(inner)
        | PropertyPath::ZeroOrMore(inner)
        | PropertyPath::OneOrMore(inner)
        | PropertyPath::ZeroOrOne(inner) => collect_path_dependencies(inner, deps, global),
        PropertyPath::Sequence(parts) | PropertyPath::Alternative(parts) => {
            for part in parts {
                collect_path_dependencies(part, deps, global);
            }
        }
        PropertyPath::Unsupported(_) => *global = true,
    }
}

fn collect_shape_dependencies(
    program: &ShapeProgram,
    shape_id: ShapeId,
    deps: &mut Vec<String>,
    global: &mut bool,
    active: &mut Vec<ShapeId>,
) {
    if active.contains(&shape_id) {
        return;
    }
    active.push(shape_id);
    let Some(shape) = program.shapes.iter().find(|shape| shape.id == shape_id) else {
        active.pop();
        return;
    };
    if let Some(path) = &shape.path {
        collect_path_dependencies(path, deps, global);
    }
    for property_shape_id in &shape.property_shapes {
        collect_shape_dependencies(program, *property_shape_id, deps, global, active);
    }
    for constraint_id in &shape.constraints {
        let Some(constraint) = program
            .constraints
            .iter()
            .find(|constraint| constraint.id == *constraint_id)
        else {
            continue;
        };
        match &constraint.expr {
            ConstraintExpr::PropertyComparison { values, .. } => {
                for value in values {
                    if let Term::NamedNode(node) = value {
                        deps.push(node.as_str().to_string());
                    }
                }
            }
            ConstraintExpr::NodeRef {
                shape: Some(inner), ..
            }
            | ConstraintExpr::PropertyRef {
                shape: Some(inner), ..
            }
            | ConstraintExpr::QualifiedValueShape {
                shape: Some(inner), ..
            }
            | ConstraintExpr::Not {
                shape: Some(inner), ..
            } => {
                collect_shape_dependencies(program, *inner, deps, global, active);
            }
            ConstraintExpr::Logical { shapes, .. } => {
                for inner in shapes {
                    collect_shape_dependencies(program, *inner, deps, global, active);
                }
            }
            ConstraintExpr::Sparql(_)
            | ConstraintExpr::CustomComponent { .. }
            | ConstraintExpr::Closed { .. }
            | ConstraintExpr::GenericPredicate { .. } => *global = true,
            ConstraintExpr::NodeRef { shape: None, .. }
            | ConstraintExpr::PropertyRef { shape: None, .. }
            | ConstraintExpr::QualifiedValueShape { shape: None, .. }
            | ConstraintExpr::Not { shape: None, .. }
            | ConstraintExpr::Class(_)
            | ConstraintExpr::Datatype(_)
            | ConstraintExpr::NodeKind(_)
            | ConstraintExpr::Cardinality { .. }
            | ConstraintExpr::NumericRange { .. }
            | ConstraintExpr::StringConstraint { .. }
            | ConstraintExpr::HasValue(_)
            | ConstraintExpr::In(_) => {}
        }
    }
    active.pop();
}

fn rule_mode_name(mode: &RuleExecutionMode) -> &'static str {
    match mode {
        RuleExecutionMode::PredicateDriven(_) => "predicate_driven",
        RuleExecutionMode::Global => "global",
    }
}

fn compile_constraint_op(expr: &ConstraintExpr) -> CompiledConstraintOp {
    match expr {
        ConstraintExpr::Cardinality {
            predicate,
            min,
            max,
        } => CompiledConstraintOp::Cardinality {
            predicate: predicate.clone(),
            min: *min,
            max: *max,
        },
        ConstraintExpr::Class(expected) => CompiledConstraintOp::Class(expected.clone()),
        ConstraintExpr::Datatype(expected) => CompiledConstraintOp::Datatype(expected.clone()),
        ConstraintExpr::NodeKind(expected) => CompiledConstraintOp::NodeKind(expected.clone()),
        ConstraintExpr::HasValue(expected) => CompiledConstraintOp::HasValue(expected.clone()),
        ConstraintExpr::In(allowed) => CompiledConstraintOp::In(allowed.clone()),
        ConstraintExpr::NumericRange { predicate, values } => CompiledConstraintOp::NumericRange {
            predicate: predicate.clone(),
            values: values.clone(),
        },
        ConstraintExpr::StringConstraint { predicate, values } => {
            CompiledConstraintOp::StringConstraint {
                predicate: predicate.clone(),
                values: values.clone(),
                compiled_pattern: compile_pattern(predicate, values),
            }
        }
        ConstraintExpr::PropertyComparison { predicate, values } => {
            CompiledConstraintOp::PropertyComparison {
                predicate: predicate.clone(),
                values: values.clone(),
            }
        }
        ConstraintExpr::NodeRef { shape, source } => CompiledConstraintOp::NodeRef {
            shape: *shape,
            source: source.clone(),
        },
        ConstraintExpr::PropertyRef { shape, source } => CompiledConstraintOp::PropertyRef {
            shape: *shape,
            source: source.clone(),
        },
        ConstraintExpr::QualifiedValueShape {
            shape,
            source,
            min_count,
            max_count,
            disjoint,
        } => CompiledConstraintOp::QualifiedValueShape {
            shape: *shape,
            source: source.clone(),
            min_count: *min_count,
            max_count: *max_count,
            disjoint: *disjoint,
        },
        ConstraintExpr::Logical { kind, shapes } => CompiledConstraintOp::Logical {
            kind: kind.clone(),
            shapes: shapes.clone(),
        },
        ConstraintExpr::Not { shape, source } => CompiledConstraintOp::Not {
            shape: *shape,
            source: source.clone(),
        },
        ConstraintExpr::Closed { ignored_properties } => CompiledConstraintOp::Closed {
            ignored_properties: ignored_properties.clone(),
        },
        ConstraintExpr::Sparql(sparql) => CompiledConstraintOp::Sparql(sparql.clone()),
        ConstraintExpr::CustomComponent {
            predicate,
            component,
            values,
            bindings,
            message_templates,
            label_template,
        } => CompiledConstraintOp::CustomComponent {
            predicate: predicate.clone(),
            component: *component,
            values: values.clone(),
            bindings: bindings.clone(),
            message_templates: message_templates.clone(),
            label_template: label_template.clone(),
        },
        ConstraintExpr::GenericPredicate { predicate, values } => {
            CompiledConstraintOp::GenericPredicate {
                predicate: predicate.clone(),
                values: values.clone(),
            }
        }
    }
}

fn compile_pattern(predicate: &NamedNode, values: &[Term]) -> Option<CompiledPattern> {
    if predicate.as_str() != "http://www.w3.org/ns/shacl#pattern" {
        return None;
    }
    let pattern = values.first().and_then(term_string)?;
    let case_insensitive = values
        .iter()
        .skip(1)
        .filter_map(term_string)
        .any(|flags| flags.contains('i'));
    let mut builder = RegexBuilder::new(&pattern);
    builder.case_insensitive(case_insensitive);
    let regex = builder.build().ok()?;
    Some(CompiledPattern {
        pattern,
        case_insensitive,
        regex,
    })
}

pub fn target_expr_cacheable(target: &TargetExpr) -> bool {
    !matches!(target, TargetExpr::Advanced(_))
}

fn constraint_op_kind(op: &CompiledConstraintOp) -> &'static str {
    match op {
        CompiledConstraintOp::Cardinality { .. } => "cardinality",
        CompiledConstraintOp::Class(_) => "class",
        CompiledConstraintOp::Datatype(_) => "datatype",
        CompiledConstraintOp::NodeKind(_) => "node_kind",
        CompiledConstraintOp::HasValue(_) => "has_value",
        CompiledConstraintOp::In(_) => "in",
        CompiledConstraintOp::NumericRange { .. } => "numeric_range",
        CompiledConstraintOp::StringConstraint { .. } => "string_constraint",
        CompiledConstraintOp::PropertyComparison { .. } => "property_comparison",
        CompiledConstraintOp::NodeRef { .. } => "node_ref",
        CompiledConstraintOp::PropertyRef { .. } => "property_ref",
        CompiledConstraintOp::QualifiedValueShape { .. } => "qualified_value_shape",
        CompiledConstraintOp::Logical { .. } => "logical",
        CompiledConstraintOp::Not { .. } => "not",
        CompiledConstraintOp::Closed { .. } => "closed",
        CompiledConstraintOp::Sparql(_) => "sparql",
        CompiledConstraintOp::CustomComponent { .. } => "custom_component",
        CompiledConstraintOp::GenericPredicate { .. } => "generic_predicate",
    }
}

fn compiled_op_name(op: &CompiledConstraintOp) -> &'static str {
    match op {
        CompiledConstraintOp::StringConstraint {
            compiled_pattern: Some(_),
            ..
        } => "precompiled_pattern",
        CompiledConstraintOp::Cardinality { .. }
        | CompiledConstraintOp::Class(_)
        | CompiledConstraintOp::Datatype(_)
        | CompiledConstraintOp::NodeKind(_)
        | CompiledConstraintOp::HasValue(_)
        | CompiledConstraintOp::In(_)
        | CompiledConstraintOp::NumericRange { .. }
        | CompiledConstraintOp::StringConstraint { .. }
        | CompiledConstraintOp::PropertyComparison { .. }
        | CompiledConstraintOp::NodeRef { .. }
        | CompiledConstraintOp::PropertyRef { .. }
        | CompiledConstraintOp::QualifiedValueShape { .. }
        | CompiledConstraintOp::Logical { .. }
        | CompiledConstraintOp::Not { .. }
        | CompiledConstraintOp::Closed { .. } => "direct",
        CompiledConstraintOp::Sparql(_)
        | CompiledConstraintOp::CustomComponent { .. }
        | CompiledConstraintOp::GenericPredicate { .. } => "fallback",
    }
}

fn term_string(term: &Term) -> Option<String> {
    match term {
        Term::Literal(literal) => Some(literal.value().to_string()),
        Term::NamedNode(node) => Some(node.as_str().to_string()),
        _ => None,
    }
}
