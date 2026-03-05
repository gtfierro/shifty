use oxigraph::model::Term;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenIR {
    pub meta: SrcGenMeta,
    pub node_shapes: Vec<SrcGenNodeShape>,
    pub property_shapes: Vec<SrcGenPropertyShape>,
    pub components: Vec<SrcGenComponent>,
    #[serde(default)]
    pub rules: Vec<SrcGenRule>,
    pub fallback_annotations: Vec<FallbackAnnotation>,
    #[serde(default)]
    pub rule_fallback_annotations: Vec<RuleFallbackAnnotation>,
    #[serde(default, skip_serializing)]
    pub embedded_shape_ir_bincode: Vec<u8>,
}

impl SrcGenIR {
    pub fn to_json_pretty(&self) -> Result<String, String> {
        serde_json::to_string_pretty(self)
            .map_err(|err| format!("failed to serialize SrcGenIR: {err}"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenMeta {
    pub compiler_track: String,
    pub schema_version: u32,
    pub shape_graph_iri: String,
    pub data_graph_iri: String,
    pub rule_count: usize,
    pub specialization_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenNodeShape {
    pub id: u64,
    pub iri: String,
    pub target_classes: Vec<String>,
    #[serde(default)]
    pub target_nodes: Vec<Term>,
    #[serde(default)]
    pub target_subjects_of: Vec<String>,
    #[serde(default)]
    pub target_objects_of: Vec<String>,
    pub constraints: Vec<u64>,
    #[serde(default)]
    pub supported_constraints: Vec<u64>,
    #[serde(default)]
    pub fallback_constraints: Vec<u64>,
    pub property_shapes: Vec<u64>,
    pub supported: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenPropertyShape {
    pub id: u64,
    pub iri: String,
    pub path_predicate: Option<String>,
    pub constraints: Vec<u64>,
    #[serde(default)]
    pub supported_constraints: Vec<u64>,
    #[serde(default)]
    pub fallback_constraints: Vec<u64>,
    pub supported: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenComponent {
    pub id: u64,
    pub iri: String,
    pub kind: SrcGenComponentKind,
    pub fallback_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SrcGenComponentKind {
    PropertyLink,
    Class {
        class_iri: String,
    },
    Datatype {
        datatype_iri: String,
    },
    NodeKind {
        node_kind_iri: String,
    },
    MinCount {
        min_count: u64,
    },
    MaxCount {
        max_count: u64,
    },
    MinLength {
        min_length: u64,
    },
    MaxLength {
        max_length: u64,
    },
    MinExclusive {
        value_sparql: String,
    },
    MinInclusive {
        value_sparql: String,
    },
    MaxExclusive {
        value_sparql: String,
    },
    MaxInclusive {
        value_sparql: String,
    },
    Pattern {
        pattern: String,
        flags: Option<String>,
    },
    HasValue {
        value_sparql: String,
    },
    In {
        values_sparql: Vec<String>,
    },
    Sparql {
        query: String,
        prefixes: String,
        requires_path: bool,
        constraint_term: String,
    },
    Node {
        shape_iri: String,
    },
    Not {
        shape_iri: String,
    },
    And {
        shape_iris: Vec<String>,
    },
    Or {
        shape_iris: Vec<String>,
    },
    Xone {
        shape_iris: Vec<String>,
    },
    QualifiedValueShape {
        shape_iri: String,
        min_count: Option<u64>,
        max_count: Option<u64>,
        disjoint: bool,
    },
    Closed {
        closed: bool,
        ignored_property_iris: Vec<String>,
    },
    LanguageIn {
        languages: Vec<String>,
    },
    UniqueLang {
        enabled: bool,
    },
    Equals {
        property_iri: String,
    },
    Disjoint {
        property_iri: String,
    },
    LessThan {
        property_iri: String,
    },
    LessThanOrEquals {
        property_iri: String,
    },
    Unsupported {
        kind: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FallbackAnnotation {
    pub component_id: u64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenRule {
    pub id: u64,
    #[serde(default)]
    pub target_classes: Vec<String>,
    #[serde(default)]
    pub target_nodes: Vec<Term>,
    #[serde(default)]
    pub target_subjects_of: Vec<String>,
    #[serde(default)]
    pub target_objects_of: Vec<String>,
    pub kind: SrcGenRuleKind,
    pub fallback_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SrcGenRuleKind {
    Triple {
        subject: SrcGenRuleSubject,
        predicate_iri: String,
        object: SrcGenRuleObject,
        #[serde(default)]
        condition_shape_iris: Vec<String>,
    },
    Sparql {
        query: String,
        #[serde(default)]
        condition_shape_iris: Vec<String>,
    },
    Unsupported {
        kind: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SrcGenRuleObject {
    Constant(Term),
    This,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SrcGenRuleSubject {
    This,
    Constant(Term),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleFallbackAnnotation {
    pub rule_id: u64,
    pub reason: String,
}
