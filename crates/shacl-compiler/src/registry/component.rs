use crate::plan::{ComponentKind, ComponentParams};

#[derive(Default, Debug)]
pub struct PropertyEmission {
    pub needs_count: bool,
    pub needs_values: bool,
    pub pre_loop_lines: Vec<String>,
    pub per_value_lines: Vec<String>,
    pub post_loop_lines: Vec<String>,
}

impl PropertyEmission {
    pub fn merge(&mut self, other: PropertyEmission) {
        self.needs_count |= other.needs_count;
        self.needs_values |= other.needs_values;
        self.pre_loop_lines.extend(other.pre_loop_lines);
        self.per_value_lines.extend(other.per_value_lines);
        self.post_loop_lines.extend(other.post_loop_lines);
    }
}

#[derive(Default, Debug)]
pub struct NodeEmission {
    pub lines: Vec<String>,
}

pub struct EmitContext<'a> {
    pub shape_id: u64,
    pub component_id: u64,
    pub path_id: Option<u64>,
    pub path_sparql: Option<&'a str>,
    pub term_iri: &'a dyn Fn(u64) -> Result<String, String>,
    pub term_expr: &'a dyn Fn(u64) -> Result<String, String>,
    pub term_sparql: &'a dyn Fn(u64) -> Result<String, String>,
    pub qualified_siblings: Option<&'a [u64]>,
}

pub trait ComponentCodegen {
    fn kind(&self) -> ComponentKind;

    fn emit_property(
        &self,
        _ctx: EmitContext<'_>,
        _params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        Err(format!(
            "component {:?} not supported on property shapes",
            self.kind()
        ))
    }

    fn emit_node(
        &self,
        _ctx: EmitContext<'_>,
        _params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        Err(format!(
            "component {:?} not supported on node shapes",
            self.kind()
        ))
    }
}
