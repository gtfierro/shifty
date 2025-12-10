use oxigraph::model::{NamedNode, Quad, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ---------------- IDs ----------------
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ID(pub u64);
impl From<u64> for ID {
    fn from(item: u64) -> Self {
        ID(item)
    }
}
impl std::fmt::Display for ID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl ID {
    pub fn to_graphviz_id(&self) -> String {
        format!("n{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ComponentID(pub u64);
impl From<u64> for ComponentID {
    fn from(item: u64) -> Self {
        ComponentID(item)
    }
}
impl std::fmt::Display for ComponentID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl ComponentID {
    pub fn to_graphviz_id(&self) -> String {
        format!("c{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuleID(pub u64);
impl From<u64> for RuleID {
    fn from(item: u64) -> Self {
        RuleID(item)
    }
}
impl std::fmt::Display for RuleID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl RuleID {
    pub fn to_graphviz_id(&self) -> String {
        format!("r{}", self.0)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PropShapeID(pub u64);
impl From<u64> for PropShapeID {
    fn from(item: u64) -> Self {
        PropShapeID(item)
    }
}
impl std::fmt::Display for PropShapeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl PropShapeID {
    pub fn to_graphviz_id(&self) -> String {
        format!("p{}", self.0)
    }
}

// ---------------- Paths ----------------
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Path {
    Simple(Term),
    Inverse(Box<Path>),
    Sequence(Vec<Path>),
    Alternative(Vec<Path>),
    ZeroOrMore(Box<Path>),
    OneOrMore(Box<Path>),
    ZeroOrOne(Box<Path>),
}

impl Path {
    pub fn to_sparql_path(&self) -> Result<String, String> {
        match self {
            Path::Simple(term) => match term {
                Term::NamedNode(nn) => Ok(format!("<{}>", nn.as_str())),
                _ => Err(format!("Simple path must be an IRI {:?}", self)),
            },
            Path::Inverse(inner) => {
                let inner_sparql = inner.to_sparql_path()?;
                match &**inner {
                    Path::Simple(_) => Ok(format!("^{}", inner_sparql)),
                    _ => Ok(format!("^({})", inner_sparql)),
                }
            }
            Path::Sequence(paths) => {
                if paths.is_empty() {
                    return Err("Sequence path must have at least one element".to_string());
                }
                if paths.len() == 1 {
                    return paths[0].to_sparql_path();
                }
                let mut sparql_paths = Vec::new();
                for p in paths {
                    sparql_paths.push(p.to_sparql_path()?);
                }
                Ok(format!("({})", sparql_paths.join(" / ")))
            }
            Path::Alternative(paths) => {
                if paths.is_empty() {
                    return Err("Alternative path must have at least one element".to_string());
                }
                if paths.len() == 1 {
                    return paths[0].to_sparql_path();
                }
                let mut sparql_paths = Vec::new();
                for p in paths {
                    sparql_paths.push(p.to_sparql_path()?);
                }
                Ok(format!("({})", sparql_paths.join(" | ")))
            }
            Path::ZeroOrMore(inner) => {
                let inner_sparql = inner.to_sparql_path()?;
                match &**inner {
                    Path::Simple(_) => Ok(format!("{}*", inner_sparql)),
                    _ => Ok(format!("({})*", inner_sparql)),
                }
            }
            Path::OneOrMore(inner) => {
                let inner_sparql = inner.to_sparql_path()?;
                match &**inner {
                    Path::Simple(_) => Ok(format!("{}+", inner_sparql)),
                    _ => Ok(format!("({})+", inner_sparql)),
                }
            }
            Path::ZeroOrOne(inner) => {
                let inner_sparql = inner.to_sparql_path()?;
                match &**inner {
                    Path::Simple(_) => Ok(format!("{}?", inner_sparql)),
                    _ => Ok(format!("({})?", inner_sparql)),
                }
            }
        }
    }

    pub fn is_simple_predicate(&self) -> bool {
        matches!(self, Path::Simple(Term::NamedNode(_)))
    }
}

// ---------------- Targets ----------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Target {
    Class(Term),
    Node(Term),
    SubjectsOf(Term),
    ObjectsOf(Term),
    Advanced(Term),
}

// ---------------- Severity ----------------
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Severity {
    Info,
    Warning,
    #[default]
    Violation,
    Custom(NamedNode),
}

// ---------------- Feature toggles ----------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureToggles {
    pub enable_af: bool,
    pub enable_rules: bool,
    pub skip_invalid_rules: bool,
}
impl Default for FeatureToggles {
    fn default() -> Self {
        Self {
            enable_af: true,
            enable_rules: true,
            skip_invalid_rules: false,
        }
    }
}

// ---------------- Templates ----------------
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateParameter {
    pub subject: Term,
    pub path: NamedNode,
    pub name: Option<String>,
    pub description: Option<String>,
    pub optional: bool,
    pub default_values: Vec<Term>,
    pub var_name: Option<String>,
    pub extra: BTreeMap<NamedNode, Vec<Term>>,
}
impl TemplateParameter {
    pub fn has_default(&self) -> bool {
        !self.default_values.is_empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TemplateValidators {
    pub validator: Option<SPARQLValidator>,
    pub node_validator: Option<SPARQLValidator>,
    pub property_validator: Option<SPARQLValidator>,
}
impl TemplateValidators {
    pub fn is_empty(&self) -> bool {
        self.validator.is_none()
            && self.node_validator.is_none()
            && self.property_validator.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentTemplateDefinition {
    pub iri: NamedNode,
    pub label: Option<String>,
    pub comment: Option<String>,
    pub parameters: Vec<TemplateParameter>,
    pub validators: TemplateValidators,
    pub messages: Vec<Term>,
    pub severity: Option<Severity>,
    pub prefix_declarations: Vec<PrefixDeclaration>,
    pub extra: BTreeMap<NamedNode, Vec<Term>>,
}
impl ComponentTemplateDefinition {
    pub fn parameter_by_path(&self, path: &NamedNode) -> Option<&TemplateParameter> {
        self.parameters.iter().find(|param| &param.path == path)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeTemplateDefinition {
    pub iri: NamedNode,
    pub label: Option<String>,
    pub comment: Option<String>,
    pub parameters: Vec<TemplateParameter>,
    pub body: Term,
    pub prefix_declarations: Vec<PrefixDeclaration>,
    pub extra: BTreeMap<NamedNode, Vec<Term>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrefixDeclaration {
    pub prefix: String,
    pub namespace: String,
}

// ---------------- Components ----------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComponentDescriptor {
    Node {
        shape: ID,
    },
    Property {
        shape: PropShapeID,
    },
    QualifiedValueShape {
        shape: ID,
        min_count: Option<u64>,
        max_count: Option<u64>,
        disjoint: Option<bool>,
    },
    Class {
        class: Term,
    },
    Datatype {
        datatype: Term,
    },
    NodeKind {
        node_kind: Term,
    },
    MinCount {
        min_count: u64,
    },
    MaxCount {
        max_count: u64,
    },
    MinExclusive {
        value: Term,
    },
    MinInclusive {
        value: Term,
    },
    MaxExclusive {
        value: Term,
    },
    MaxInclusive {
        value: Term,
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
        property: Term,
    },
    Disjoint {
        property: Term,
    },
    LessThan {
        property: Term,
    },
    LessThanOrEquals {
        property: Term,
    },
    Not {
        shape: ID,
    },
    And {
        shapes: Vec<ID>,
    },
    Or {
        shapes: Vec<ID>,
    },
    Xone {
        shapes: Vec<ID>,
    },
    Closed {
        closed: bool,
        ignored_properties: Vec<Term>,
    },
    HasValue {
        value: Term,
    },
    In {
        values: Vec<Term>,
    },
    Sparql {
        constraint_node: Term,
    },
    Custom {
        definition: Box<CustomConstraintComponentDefinition>,
        parameter_values: HashMap<NamedNode, Vec<Term>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Parameter {
    pub subject: Term,
    pub path: NamedNode,
    pub optional: bool,
    pub var_name: Option<String>,
    pub default_values: Vec<Term>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub extra: BTreeMap<NamedNode, Vec<Term>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SPARQLValidator {
    pub query: String,
    pub is_ask: bool,
    pub messages: Vec<Term>,
    pub prefixes: String,
    pub severity: Option<Severity>,
    pub require_this: bool,
    pub require_path: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomConstraintComponentDefinition {
    pub iri: NamedNode,
    pub parameters: Vec<Parameter>,
    pub validator: Option<SPARQLValidator>,
    pub node_validator: Option<SPARQLValidator>,
    pub property_validator: Option<SPARQLValidator>,
    pub messages: Vec<Term>,
    pub severity: Option<Severity>,
    pub template: Option<ComponentTemplateDefinition>,
}

pub type ParameterBindings = HashMap<NamedNode, Vec<Term>>;

// ---------------- Rules ----------------
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RuleOrder(pub f64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Rule {
    Sparql(SparqlRule),
    Triple(TripleRule),
}
impl Rule {
    pub fn id(&self) -> RuleID {
        match self {
            Rule::Sparql(rule) => rule.id,
            Rule::Triple(rule) => rule.id,
        }
    }
    pub fn order(&self) -> Option<RuleOrder> {
        match self {
            Rule::Sparql(rule) => rule.order,
            Rule::Triple(rule) => rule.order,
        }
    }
    pub fn is_deactivated(&self) -> bool {
        match self {
            Rule::Sparql(rule) => rule.deactivated,
            Rule::Triple(rule) => rule.deactivated,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparqlRule {
    pub id: RuleID,
    pub query: String,
    pub source_term: Term,
    pub condition_shapes: Vec<RuleCondition>,
    pub deactivated: bool,
    pub order: Option<RuleOrder>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TripleRule {
    pub id: RuleID,
    pub subject: TriplePatternTerm,
    pub predicate: NamedNode,
    pub object: TriplePatternTerm,
    pub condition_shapes: Vec<RuleCondition>,
    pub deactivated: bool,
    pub order: Option<RuleOrder>,
    pub source_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleCondition {
    NodeShape(ID),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TriplePatternTerm {
    This,
    Constant(Term),
    Path(Path),
}
impl TriplePatternTerm {
    pub fn is_this(&self) -> bool {
        matches!(self, TriplePatternTerm::This)
    }
}

// ---------------- Shape IR ----------------
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeShapeIR {
    pub id: ID,
    pub targets: Vec<Target>,
    pub constraints: Vec<ComponentID>,
    pub severity: Severity,
    pub deactivated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PropertyShapeIR {
    pub id: PropShapeID,
    pub targets: Vec<Target>,
    pub path: Path,
    pub path_term: Term,
    pub constraints: Vec<ComponentID>,
    pub severity: Severity,
    pub deactivated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeIR {
    pub shape_graph: NamedNode,
    pub data_graph: Option<NamedNode>,
    pub node_shapes: Vec<NodeShapeIR>,
    pub property_shapes: Vec<PropertyShapeIR>,
    pub components: HashMap<ComponentID, ComponentDescriptor>,
    pub component_templates: HashMap<NamedNode, ComponentTemplateDefinition>,
    pub shape_templates: HashMap<NamedNode, ShapeTemplateDefinition>,
    pub shape_template_cache: HashMap<String, ID>,
    pub node_shape_terms: HashMap<ID, Term>,
    pub property_shape_terms: HashMap<PropShapeID, Term>,
    pub shape_quads: Vec<Quad>,
    pub rules: HashMap<RuleID, Rule>,
    pub node_shape_rules: HashMap<ID, Vec<RuleID>>,
    pub prop_shape_rules: HashMap<PropShapeID, Vec<RuleID>>,
    pub features: FeatureToggles,
}
