use oxigraph::model::Term;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const PROGRAM_SCHEMA_VERSION: u32 = 1;
pub const KERNEL_VERSION: u32 = 1;

pub type TermPool = Vec<Term>;
pub type ExtensionMap = BTreeMap<String, serde_json::Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledProgram {
    pub header: ProgramHeader,
    pub terms: TermPool,
    pub shapes: Vec<ShapeRow>,
    pub components: Vec<ComponentRow>,
    pub paths: Vec<PathRow>,
    pub targets: Vec<TargetRow>,
    pub rules: Vec<RuleRow>,
    pub shape_graph_triples: Vec<TripleRow>,
    pub static_hints: StaticHints,
    pub ext: ExtensionMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramHeader {
    pub schema_version: u32,
    pub min_kernel_version: u32,
    pub program_hash: [u8; 32],
    pub feature_bits: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeRow {
    pub id: u64,
    pub kind: ShapeKind,
    pub term: u64,
    pub target_ids: Vec<u64>,
    pub component_ids: Vec<u64>,
    pub path_id: Option<u64>,
    pub deactivated: bool,
    pub severity: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShapeKind {
    Node,
    Property,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentRow {
    pub id: u64,
    pub kind: String,
    pub params: serde_json::Value,
    pub source_constraint_component_term: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathRow {
    pub id: u64,
    pub spec: serde_json::Value,
    pub term: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetRow {
    pub id: u64,
    pub spec: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleRow {
    pub id: u64,
    pub kind: serde_json::Value,
    pub conditions: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TripleRow {
    pub subject: u64,
    pub predicate: u64,
    pub object: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StaticHints {
    pub shape_graph_iri_term: u64,
    pub default_data_graph_iri_term: Option<u64>,
    pub shape_id_to_term: Vec<IdTermRow>,
    pub component_id_to_iri_term: Vec<IdTermRow>,
    pub path_id_to_term: Vec<IdTermRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdTermRow {
    pub id: u64,
    pub term: u64,
}

impl CompiledProgram {
    pub fn term(&self, term_id: u64) -> Option<&Term> {
        self.terms.get(term_id as usize)
    }
}
