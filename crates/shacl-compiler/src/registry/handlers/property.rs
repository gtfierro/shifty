use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission};

pub struct PropertyHandler;

impl ComponentCodegen for PropertyHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Property
    }

    fn emit_node(
        &self,
        _ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let shape = match params {
            ComponentParams::Property { shape } => *shape,
            _ => return Err("property params mismatch".to_string()),
        };
        let line = format!(
            "        validate_property_shape_{}_for_focus(store, graph, &focus, report);",
            shape
        );
        let mut emission = NodeEmission::default();
        emission.lines.push(line);
        Ok(emission)
    }
}
