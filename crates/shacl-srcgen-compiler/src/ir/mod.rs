use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenIR {
    pub meta: SrcGenMeta,
    pub shapes: Vec<SrcGenShape>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenShape {
    pub id: u64,
    pub iri: String,
    pub kind: ShapeKind,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ShapeKind {
    Node,
    Property,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrcGenComponent {
    pub id: u64,
    pub iri: String,
    pub fallback_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FallbackAnnotation {
    pub component_id: u64,
    pub reason: String,
}
