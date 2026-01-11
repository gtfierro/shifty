use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct NodeKindHandler;

impl ComponentCodegen for NodeKindHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::NodeKind
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let node_kind = match params {
            ComponentParams::NodeKind { node_kind } => *node_kind,
            _ => return Err("nodeKind params mismatch".to_string()),
        };
        let node_kind_iri = (ctx.term_iri)(node_kind)?;
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !matches_node_kind(&value, \"{}\") {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            node_kind_iri,
            ctx.shape_id,
            ctx.component_id,
            match ctx.path_iri {
                Some(path) => format!("Some(\"{}\")", path),
                None => "None".to_string(),
            }
        ));
        Ok(emission)
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let node_kind = match params {
            ComponentParams::NodeKind { node_kind } => *node_kind,
            _ => return Err("nodeKind params mismatch".to_string()),
        };
        let node_kind_iri = (ctx.term_iri)(node_kind)?;
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !matches_node_kind(&focus, \"{}\") {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            node_kind_iri,
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}
