use crate::plan::ComponentKind;
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct UnsupportedHandler;

impl ComponentCodegen for UnsupportedHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Class
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        _params: &crate::plan::ComponentParams,
    ) -> Result<PropertyEmission, String> {
        Err(format!(
            "component {:?} not implemented in compiler",
            ctx.kind
        ))
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        _params: &crate::plan::ComponentParams,
    ) -> Result<NodeEmission, String> {
        Err(format!(
            "component {:?} not implemented in compiler",
            ctx.kind
        ))
    }
}
