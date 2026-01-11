use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct HasValueHandler;

impl ComponentCodegen for HasValueHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::HasValue
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let value = match params {
            ComponentParams::HasValue { value } => *value,
            _ => return Err("hasValue params mismatch".to_string()),
        };
        let value_expr = (ctx.term_expr)(value)?;
        let mut emission = PropertyEmission::default();
        emission.needs_values = true;
        emission.post_loop_lines.push(format!(
            "    if !values.iter().any(|v| v == &{}) {{\n        report.record({}, {}, focus, None, Some(\"{}\"));\n    }}",
            value_expr,
            ctx.shape_id,
            ctx.component_id,
            ctx.path_iri.unwrap_or("")
        ));
        Ok(emission)
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let value = match params {
            ComponentParams::HasValue { value } => *value,
            _ => return Err("hasValue params mismatch".to_string()),
        };
        let value_expr = (ctx.term_expr)(value)?;
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if focus != &{} {{\n            report.record({}, {}, focus, None, None);\n        }}",
            value_expr,
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}
