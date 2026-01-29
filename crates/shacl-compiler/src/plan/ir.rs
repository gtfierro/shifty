use oxigraph::model::{Quad, Term};
use serde::{Deserialize, Serialize};
use shacl_ir::{
    ComponentDescriptor, ComponentID, Path, PropShapeID, Rule, RuleCondition, RuleID, Severity,
    ShapeIR, SparqlRule, Target, TriplePatternTerm, TripleRule, ID,
};
use std::collections::HashMap;

pub type TermId = u64;
pub type PathId = u64;
pub type ShapeId = u64;
pub type PropId = u64;
pub type CompId = u64;
pub type RuleId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PlanShapeKind {
    Node,
    Property,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanShape {
    pub id: ShapeId,
    pub kind: PlanShapeKind,
    pub targets: Vec<PlanTarget>,
    pub constraints: Vec<CompId>,
    pub path: Option<PathId>,
    pub severity: Severity,
    pub deactivated: bool,
    pub term: TermId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanComponent {
    pub id: CompId,
    pub kind: ComponentKind,
    pub params: ComponentParams,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ComponentKind {
    Node,
    Property,
    QualifiedValueShape,
    Class,
    Datatype,
    NodeKind,
    MinCount,
    MaxCount,
    MinExclusive,
    MinInclusive,
    MaxExclusive,
    MaxInclusive,
    MinLength,
    MaxLength,
    Pattern,
    LanguageIn,
    UniqueLang,
    Equals,
    Disjoint,
    LessThan,
    LessThanOrEquals,
    Not,
    And,
    Or,
    Xone,
    Closed,
    HasValue,
    In,
    Sparql,
    Custom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCustomBinding {
    pub var_name: String,
    pub value: TermId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanCustomValidator {
    pub query: String,
    pub is_ask: bool,
    pub prefixes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComponentParams {
    Node {
        shape: ShapeId,
    },
    Property {
        shape: PropId,
    },
    QualifiedValueShape {
        shape: ShapeId,
        min_count: Option<u64>,
        max_count: Option<u64>,
        disjoint: Option<bool>,
    },
    Class {
        class: TermId,
    },
    Datatype {
        datatype: TermId,
    },
    NodeKind {
        node_kind: TermId,
    },
    MinCount {
        min_count: u64,
    },
    MaxCount {
        max_count: u64,
    },
    MinExclusive {
        value: TermId,
    },
    MinInclusive {
        value: TermId,
    },
    MaxExclusive {
        value: TermId,
    },
    MaxInclusive {
        value: TermId,
    },
    MinLength {
        length: u64,
    },
    MaxLength {
        length: u64,
    },
    Pattern {
        pattern: String,
        flags: Option<String>,
    },
    LanguageIn {
        languages: Vec<String>,
    },
    UniqueLang {
        enabled: bool,
    },
    Equals {
        property: TermId,
    },
    Disjoint {
        property: TermId,
    },
    LessThan {
        property: TermId,
    },
    LessThanOrEquals {
        property: TermId,
    },
    Not {
        shape: ShapeId,
    },
    And {
        shapes: Vec<ShapeId>,
    },
    Or {
        shapes: Vec<ShapeId>,
    },
    Xone {
        shapes: Vec<ShapeId>,
    },
    Closed {
        closed: bool,
        ignored_properties: Vec<TermId>,
    },
    HasValue {
        value: TermId,
    },
    In {
        values: Vec<TermId>,
    },
    Sparql {
        query: String,
        constraint_node: TermId,
    },
    Custom {
        iri: TermId,
        bindings: Vec<PlanCustomBinding>,
        validator: Option<PlanCustomValidator>,
        node_validator: Option<PlanCustomValidator>,
        property_validator: Option<PlanCustomValidator>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanTarget {
    Class(TermId),
    Node(TermId),
    SubjectsOf(TermId),
    ObjectsOf(TermId),
    Advanced(TermId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanPath {
    Simple(TermId),
    Inverse(PathId),
    Sequence(Vec<PathId>),
    Alternative(Vec<PathId>),
    ZeroOrMore(PathId),
    OneOrMore(PathId),
    ZeroOrOne(PathId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanOrder {
    pub node_shapes: Vec<ShapeId>,
    pub property_shapes: Vec<PropId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanTriple {
    pub subject: TermId,
    pub predicate: TermId,
    pub object: TermId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRule {
    pub id: RuleId,
    pub kind: PlanRuleKind,
    pub conditions: Vec<ShapeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanRuleKind {
    Sparql {
        query: String,
    },
    Triple {
        subject: PlanRuleTerm,
        predicate: TermId,
        object: PlanRuleTerm,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlanRuleTerm {
    This,
    Constant(TermId),
    Path(PathId),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanIR {
    pub terms: Vec<Term>,
    pub paths: Vec<PlanPath>,
    pub components: Vec<PlanComponent>,
    pub shapes: Vec<PlanShape>,
    pub shape_triples: Vec<PlanTriple>,
    pub shape_graph: TermId,
    pub order: PlanOrder,
    pub rules: Vec<PlanRule>,
    pub node_shape_rules: HashMap<ShapeId, Vec<RuleId>>,
    pub property_shape_rules: HashMap<PropId, Vec<RuleId>>,
}

impl PlanIR {
    pub fn from_shape_ir(ir: &ShapeIR) -> Result<Self, String> {
        PlanBuilder::default().build(ir)
    }

    pub fn from_shape_ir_with_quads(ir: &ShapeIR, shape_quads: &[Quad]) -> Result<Self, String> {
        PlanBuilder::default().build_with_quads(ir, shape_quads)
    }

    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Default)]
struct PlanBuilder {
    terms: Vec<Term>,
    term_index: HashMap<Term, TermId>,
    paths: Vec<PlanPath>,
    path_index: HashMap<Path, PathId>,
    prop_shape_id_map: HashMap<PropShapeID, PropId>,
}

impl PlanBuilder {
    fn build(mut self, ir: &ShapeIR) -> Result<PlanIR, String> {
        self.build_with_quads(ir, &ir.shape_quads)
    }

    fn build_with_quads(mut self, ir: &ShapeIR, shape_quads: &[Quad]) -> Result<PlanIR, String> {
        let property_offset = ir
            .node_shapes
            .iter()
            .map(|shape| shape.id.0)
            .max()
            .map(|max_id| max_id + 1)
            .unwrap_or(0);
        self.prop_shape_id_map = ir
            .property_shapes
            .iter()
            .map(|shape| (shape.id, property_offset + shape.id.0))
            .collect();

        let mut components: Vec<PlanComponent> = Vec::new();
        let mut component_ids: Vec<ComponentID> = ir.components.keys().cloned().collect();
        component_ids.sort_by_key(|id| id.0);

        for component_id in component_ids {
            let descriptor = ir
                .components
                .get(&component_id)
                .ok_or_else(|| format!("Missing component descriptor {}", component_id.0))?;
            let component =
                self.component_from_descriptor(component_id, descriptor, &ir.shape_quads)?;
            components.push(component);
        }

        let mut shapes: Vec<PlanShape> = Vec::new();

        let mut node_shapes = ir.node_shapes.clone();
        node_shapes.sort_by_key(|shape| shape.id.0);
        for shape in node_shapes {
            let targets = self.targets_from(&shape.targets)?;
            let shape_term = self.shape_term_for_node(ir, shape.id)?;
            shapes.push(PlanShape {
                id: shape.id.0,
                kind: PlanShapeKind::Node,
                targets,
                constraints: shape.constraints.iter().map(|id| id.0).collect(),
                path: None,
                severity: shape.severity.clone(),
                deactivated: shape.deactivated,
                term: shape_term,
            });
        }

        let mut property_shapes = ir.property_shapes.clone();
        property_shapes.sort_by_key(|shape| shape.id.0);
        for shape in property_shapes {
            let targets = self.targets_from(&shape.targets)?;
            let path_id = self.intern_path(&shape.path)?;
            let shape_term = self.shape_term_for_property(ir, shape.id)?;
            let shape_id = self
                .map_property_shape_id(shape.id)
                .ok_or_else(|| format!("Missing property shape mapping {}", shape.id.0))?;
            shapes.push(PlanShape {
                id: shape_id,
                kind: PlanShapeKind::Property,
                targets,
                constraints: shape.constraints.iter().map(|id| id.0).collect(),
                path: Some(path_id),
                severity: shape.severity.clone(),
                deactivated: shape.deactivated,
                term: shape_term,
            });
        }

        let mut shape_triples = Vec::new();
        for quad in shape_quads {
            let subject: Term = quad.subject.clone().into();
            let predicate: Term = quad.predicate.clone().into();
            let object: Term = quad.object.clone();
            let subject_id = self.intern_term(&subject);
            let predicate_id = self.intern_term(&predicate);
            let object_id = self.intern_term(&object);
            shape_triples.push(PlanTriple {
                subject: subject_id,
                predicate: predicate_id,
                object: object_id,
            });
        }

        let mut node_order: Vec<ShapeId> = ir.node_shapes.iter().map(|s| s.id.0).collect();
        node_order.sort();
        let mut prop_order: Vec<PropId> = Vec::new();
        for shape in &ir.property_shapes {
            let mapped = self
                .map_property_shape_id(shape.id)
                .ok_or_else(|| format!("Missing property shape mapping {}", shape.id.0))?;
            prop_order.push(mapped);
        }
        prop_order.sort();

        let shape_graph = self.intern_term(&Term::NamedNode(ir.shape_graph.clone()));
        let (rules, node_shape_rules, property_shape_rules) = self.build_rules(ir)?;

        Ok(PlanIR {
            terms: self.terms,
            paths: self.paths,
            components,
            shapes,
            shape_triples,
            shape_graph,
            order: PlanOrder {
                node_shapes: node_order,
                property_shapes: prop_order,
            },
            rules,
            node_shape_rules,
            property_shape_rules,
        })
    }

    fn targets_from(&mut self, targets: &[Target]) -> Result<Vec<PlanTarget>, String> {
        let mut out = Vec::with_capacity(targets.len());
        for target in targets {
            let mapped = match target {
                Target::Class(term) => PlanTarget::Class(self.intern_term(term)),
                Target::Node(term) => PlanTarget::Node(self.intern_term(term)),
                Target::SubjectsOf(term) => PlanTarget::SubjectsOf(self.intern_term(term)),
                Target::ObjectsOf(term) => PlanTarget::ObjectsOf(self.intern_term(term)),
                Target::Advanced(term) => PlanTarget::Advanced(self.intern_term(term)),
            };
            out.push(mapped);
        }
        Ok(out)
    }

    fn shape_term_for_node(&mut self, ir: &ShapeIR, id: ID) -> Result<TermId, String> {
        let term = ir
            .node_shape_terms
            .get(&id)
            .ok_or_else(|| format!("Missing term for node shape {}", id.0))?;
        Ok(self.intern_term(term))
    }

    fn shape_term_for_property(&mut self, ir: &ShapeIR, id: PropShapeID) -> Result<TermId, String> {
        let term = ir
            .property_shape_terms
            .get(&id)
            .ok_or_else(|| format!("Missing term for property shape {}", id.0))?;
        Ok(self.intern_term(term))
    }

    fn map_property_shape_id(&self, id: PropShapeID) -> Option<PropId> {
        self.prop_shape_id_map.get(&id).copied()
    }

    fn component_from_descriptor(
        &mut self,
        component_id: ComponentID,
        descriptor: &ComponentDescriptor,
        shape_quads: &[oxigraph::model::Quad],
    ) -> Result<PlanComponent, String> {
        let (kind, params) = match descriptor {
            ComponentDescriptor::Node { shape } => (
                ComponentKind::Node,
                ComponentParams::Node { shape: shape.0 },
            ),
            ComponentDescriptor::Property { shape } => (
                ComponentKind::Property,
                ComponentParams::Property {
                    shape: self
                        .map_property_shape_id(*shape)
                        .ok_or_else(|| {
                            format!("Missing property shape mapping {}", shape.0)
                        })?,
                },
            ),
            ComponentDescriptor::QualifiedValueShape {
                shape,
                min_count,
                max_count,
                disjoint,
            } => (
                ComponentKind::QualifiedValueShape,
                ComponentParams::QualifiedValueShape {
                    shape: shape.0,
                    min_count: *min_count,
                    max_count: *max_count,
                    disjoint: *disjoint,
                },
            ),
            ComponentDescriptor::Class { class } => (
                ComponentKind::Class,
                ComponentParams::Class {
                    class: self.intern_term(class),
                },
            ),
            ComponentDescriptor::Datatype { datatype } => (
                ComponentKind::Datatype,
                ComponentParams::Datatype {
                    datatype: self.intern_term(datatype),
                },
            ),
            ComponentDescriptor::NodeKind { node_kind } => (
                ComponentKind::NodeKind,
                ComponentParams::NodeKind {
                    node_kind: self.intern_term(node_kind),
                },
            ),
            ComponentDescriptor::MinCount { min_count } => (
                ComponentKind::MinCount,
                ComponentParams::MinCount {
                    min_count: *min_count,
                },
            ),
            ComponentDescriptor::MaxCount { max_count } => (
                ComponentKind::MaxCount,
                ComponentParams::MaxCount {
                    max_count: *max_count,
                },
            ),
            ComponentDescriptor::MinExclusive { value } => (
                ComponentKind::MinExclusive,
                ComponentParams::MinExclusive {
                    value: self.intern_term(value),
                },
            ),
            ComponentDescriptor::MinInclusive { value } => (
                ComponentKind::MinInclusive,
                ComponentParams::MinInclusive {
                    value: self.intern_term(value),
                },
            ),
            ComponentDescriptor::MaxExclusive { value } => (
                ComponentKind::MaxExclusive,
                ComponentParams::MaxExclusive {
                    value: self.intern_term(value),
                },
            ),
            ComponentDescriptor::MaxInclusive { value } => (
                ComponentKind::MaxInclusive,
                ComponentParams::MaxInclusive {
                    value: self.intern_term(value),
                },
            ),
            ComponentDescriptor::MinLength { length } => (
                ComponentKind::MinLength,
                ComponentParams::MinLength { length: *length },
            ),
            ComponentDescriptor::MaxLength { length } => (
                ComponentKind::MaxLength,
                ComponentParams::MaxLength { length: *length },
            ),
            ComponentDescriptor::Pattern { pattern, flags } => (
                ComponentKind::Pattern,
                ComponentParams::Pattern {
                    pattern: pattern.clone(),
                    flags: flags.clone(),
                },
            ),
            ComponentDescriptor::LanguageIn { languages } => (
                ComponentKind::LanguageIn,
                ComponentParams::LanguageIn {
                    languages: languages.clone(),
                },
            ),
            ComponentDescriptor::UniqueLang { enabled } => (
                ComponentKind::UniqueLang,
                ComponentParams::UniqueLang { enabled: *enabled },
            ),
            ComponentDescriptor::Equals { property } => (
                ComponentKind::Equals,
                ComponentParams::Equals {
                    property: self.intern_term(property),
                },
            ),
            ComponentDescriptor::Disjoint { property } => (
                ComponentKind::Disjoint,
                ComponentParams::Disjoint {
                    property: self.intern_term(property),
                },
            ),
            ComponentDescriptor::LessThan { property } => (
                ComponentKind::LessThan,
                ComponentParams::LessThan {
                    property: self.intern_term(property),
                },
            ),
            ComponentDescriptor::LessThanOrEquals { property } => (
                ComponentKind::LessThanOrEquals,
                ComponentParams::LessThanOrEquals {
                    property: self.intern_term(property),
                },
            ),
            ComponentDescriptor::Not { shape } => {
                (ComponentKind::Not, ComponentParams::Not { shape: shape.0 })
            }
            ComponentDescriptor::And { shapes } => (
                ComponentKind::And,
                ComponentParams::And {
                    shapes: shapes.iter().map(|id| id.0).collect(),
                },
            ),
            ComponentDescriptor::Or { shapes } => (
                ComponentKind::Or,
                ComponentParams::Or {
                    shapes: shapes.iter().map(|id| id.0).collect(),
                },
            ),
            ComponentDescriptor::Xone { shapes } => (
                ComponentKind::Xone,
                ComponentParams::Xone {
                    shapes: shapes.iter().map(|id| id.0).collect(),
                },
            ),
            ComponentDescriptor::Closed {
                closed,
                ignored_properties,
            } => (
                ComponentKind::Closed,
                ComponentParams::Closed {
                    closed: *closed,
                    ignored_properties: ignored_properties
                        .iter()
                        .map(|term| self.intern_term(term))
                        .collect(),
                },
            ),
            ComponentDescriptor::HasValue { value } => (
                ComponentKind::HasValue,
                ComponentParams::HasValue {
                    value: self.intern_term(value),
                },
            ),
            ComponentDescriptor::In { values } => (
                ComponentKind::In,
                ComponentParams::In {
                    values: values.iter().map(|term| self.intern_term(term)).collect(),
                },
            ),
            ComponentDescriptor::Sparql { constraint_node } => {
                let query = sparql_query_for(shape_quads, constraint_node)?;
                let constraint_term = self.intern_term(constraint_node);
                (
                    ComponentKind::Sparql,
                    ComponentParams::Sparql {
                        query,
                        constraint_node: constraint_term,
                    },
                )
            }
            ComponentDescriptor::Custom {
                definition,
                parameter_values,
            } => {
                let iri = self.intern_term(&Term::NamedNode(definition.iri.clone()));
                let mut bindings = Vec::new();
                for param in &definition.parameters {
                    if let Some(values) = parameter_values.get(&param.path) {
                        if let Some(value) = values.first() {
                            let var_name = param
                                .var_name
                                .clone()
                                .unwrap_or_else(|| local_name(param.path.as_str()));
                            let value_id = self.intern_term(value);
                            bindings.push(PlanCustomBinding { var_name, value: value_id });
                        }
                    }
                }
                let validator = definition.validator.as_ref().map(plan_custom_validator);
                let node_validator = definition.node_validator.as_ref().map(plan_custom_validator);
                let property_validator =
                    definition.property_validator.as_ref().map(plan_custom_validator);
                (
                    ComponentKind::Custom,
                    ComponentParams::Custom {
                        iri,
                        bindings,
                        validator,
                        node_validator,
                        property_validator,
                    },
                )
            }
        };

        Ok(PlanComponent {
            id: component_id.0,
            kind,
            params,
        })
    }

    fn intern_term(&mut self, term: &Term) -> TermId {
        if let Some(id) = self.term_index.get(term) {
            return *id;
        }
        let id = self.terms.len() as TermId;
        self.terms.push(term.clone());
        self.term_index.insert(term.clone(), id);
        id
    }

    fn intern_path(&mut self, path: &Path) -> Result<PathId, String> {
        if let Some(id) = self.path_index.get(path) {
            return Ok(*id);
        }
        let plan_path = match path {
            Path::Simple(term) => PlanPath::Simple(self.intern_term(term)),
            Path::Inverse(inner) => PlanPath::Inverse(self.intern_path(inner)?),
            Path::Sequence(paths) => {
                let mut ids = Vec::with_capacity(paths.len());
                for p in paths {
                    ids.push(self.intern_path(p)?);
                }
                PlanPath::Sequence(ids)
            }
            Path::Alternative(paths) => {
                let mut ids = Vec::with_capacity(paths.len());
                for p in paths {
                    ids.push(self.intern_path(p)?);
                }
                PlanPath::Alternative(ids)
            }
            Path::ZeroOrMore(inner) => PlanPath::ZeroOrMore(self.intern_path(inner)?),
            Path::OneOrMore(inner) => PlanPath::OneOrMore(self.intern_path(inner)?),
            Path::ZeroOrOne(inner) => PlanPath::ZeroOrOne(self.intern_path(inner)?),
        };
        let id = self.paths.len() as PathId;
        self.paths.push(plan_path);
        self.path_index.insert(path.clone(), id);
        Ok(id)
    }

    fn build_rules(
        &mut self,
        ir: &ShapeIR,
    ) -> Result<
        (
            Vec<PlanRule>,
            HashMap<ShapeId, Vec<RuleId>>,
            HashMap<PropId, Vec<RuleId>>,
        ),
        String,
    > {
        let mut rule_ids: Vec<RuleID> = ir.rules.keys().cloned().collect();
        rule_ids.sort_by_key(|id| id.0);
        let mut rules: Vec<PlanRule> = Vec::new();
        for rule_id in rule_ids {
            let rule = ir
                .rules
                .get(&rule_id)
                .ok_or_else(|| format!("Missing rule {}", rule_id.0))?;
            rules.push(self.plan_rule_from(rule)?);
        }

        let node_shape_rules = ir
            .node_shape_rules
            .iter()
            .map(|(shape_id, rule_ids)| {
                let mut ids: Vec<_> = rule_ids.iter().map(|id| id.0).collect();
                ids.sort();
                (shape_id.0, ids)
            })
            .collect();

        let mut property_shape_rules: HashMap<PropId, Vec<RuleId>> = HashMap::new();
        for (shape_id, rule_ids) in &ir.prop_shape_rules {
            let mapped = self
                .map_property_shape_id(*shape_id)
                .ok_or_else(|| format!("Missing property shape mapping {}", shape_id.0))?;
            let mut ids: Vec<_> = rule_ids.iter().map(|id| id.0).collect();
            ids.sort();
            property_shape_rules.insert(mapped, ids);
        }

        Ok((rules, node_shape_rules, property_shape_rules))
    }

    fn plan_rule_from(&mut self, rule: &Rule) -> Result<PlanRule, String> {
        let (kind, conditions) = match rule {
            Rule::Sparql(sparql_rule) => (
                PlanRuleKind::Sparql {
                    query: sparql_rule.query.clone(),
                },
                self.rule_conditions(&sparql_rule.condition_shapes),
            ),
            Rule::Triple(triple_rule) => (
                PlanRuleKind::Triple {
                    subject: self.rule_term_from_template(&triple_rule.subject)?,
                    predicate: self.intern_term(&Term::NamedNode(triple_rule.predicate.clone())),
                    object: self.rule_term_from_template(&triple_rule.object)?,
                },
                self.rule_conditions(&triple_rule.condition_shapes),
            ),
        };
        Ok(PlanRule {
            id: rule.id().0,
            kind,
            conditions,
        })
    }

    fn rule_term_from_template(
        &mut self,
        template: &TriplePatternTerm,
    ) -> Result<PlanRuleTerm, String> {
        match template {
            TriplePatternTerm::This => Ok(PlanRuleTerm::This),
            TriplePatternTerm::Constant(term) => Ok(PlanRuleTerm::Constant(self.intern_term(term))),
            TriplePatternTerm::Path(path) => Ok(PlanRuleTerm::Path(self.intern_path(path)?)),
        }
    }

    fn rule_conditions(&self, conditions: &[RuleCondition]) -> Vec<ShapeId> {
        conditions
            .iter()
            .filter_map(|cond| match cond {
                RuleCondition::NodeShape(shape_id) => Some(shape_id.0),
            })
            .collect()
    }
}

fn sparql_query_for(
    shape_quads: &[oxigraph::model::Quad],
    constraint_node: &Term,
) -> Result<String, String> {
    let select_pred = "http://www.w3.org/ns/shacl#select";
    for quad in shape_quads {
        if quad.predicate.as_str() != select_pred {
            continue;
        }
        let subject: Term = quad.subject.clone().into();
        if &subject != constraint_node {
            continue;
        }
        match &quad.object {
            Term::Literal(lit) => return Ok(lit.value().to_string()),
            _ => return Err("sh:select value must be a literal string".to_string()),
        }
    }
    Err("SPARQL constraint is missing sh:select".to_string())
}

fn plan_custom_validator(validator: &shacl_ir::SPARQLValidator) -> PlanCustomValidator {
    PlanCustomValidator {
        query: validator.query.clone(),
        is_ask: validator.is_ask,
        prefixes: validator.prefixes.clone(),
    }
}

fn local_name(iri: &str) -> String {
    iri.rsplit(|ch| ch == '#' || ch == '/')
        .next()
        .unwrap_or(iri)
        .to_string()
}
