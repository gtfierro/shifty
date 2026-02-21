use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenIR {
    pub meta: SrcGenMeta,
    pub node_shapes: Vec<SrcGenNodeShape>,
    pub property_shapes: Vec<SrcGenPropertyShape>,
    pub components: Vec<SrcGenComponent>,
    pub fallback_annotations: Vec<FallbackAnnotation>,
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
    pub specialization_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenNodeShape {
    pub id: u64,
    pub iri: String,
    pub target_classes: Vec<String>,
    pub constraints: Vec<u64>,
    pub property_shapes: Vec<u64>,
    pub supported: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenPropertyShape {
    pub id: u64,
    pub iri: String,
    pub path_predicate: Option<String>,
    pub constraints: Vec<u64>,
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
    Class { class_iri: String },
    Datatype { datatype_iri: String },
    MinCount { min_count: u64 },
    MaxCount { max_count: u64 },
    Unsupported { kind: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FallbackAnnotation {
    pub component_id: u64,
    pub reason: String,
}
