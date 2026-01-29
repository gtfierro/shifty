use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct NodeHandler;

impl ComponentCodegen for NodeHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Node
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let shape = match params {
            ComponentParams::Node { shape } => *shape,
            _ => return Err("node params mismatch".to_string()),
        };
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !node_shape_conforms_{}(store, graph, &value) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            shape,
            ctx.shape_id,
            ctx.component_id,
            match ctx.path_id {
                Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
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
        let shape = match params {
            ComponentParams::Node { shape } => *shape,
            _ => return Err("node params mismatch".to_string()),
        };
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !node_shape_conforms_{}(store, graph, &focus) {{\n            report.record({}, {}, focus, Some(focus), None);\n        }}",
            shape,
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}
