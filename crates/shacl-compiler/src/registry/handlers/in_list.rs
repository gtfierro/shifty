use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct InHandler;

impl ComponentCodegen for InHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::In
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let values = match params {
            ComponentParams::In { values } => values,
            _ => return Err("in params mismatch".to_string()),
        };
        let mut emission = PropertyEmission::default();
        let mut items = Vec::new();
        for value in values {
            items.push((ctx.term_expr)(*value)?);
        }
        emission.pre_loop_lines.push(format!(
            "    let allowed_{}: Vec<Term> = vec![{}];",
            ctx.component_id,
            items.join(", ")
        ));
        emission.per_value_lines.push(format!(
            "        if !allowed_{}.iter().any(|v| v == &value) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            ctx.component_id,
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
        let values = match params {
            ComponentParams::In { values } => values,
            _ => return Err("in params mismatch".to_string()),
        };
        let mut items = Vec::new();
        for value in values {
            items.push((ctx.term_expr)(*value)?);
        }
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        let allowed_{}: Vec<Term> = vec![{}];",
            ctx.component_id,
            items.join(", ")
        ));
        emission.lines.push(format!(
            "        if !allowed_{}.iter().any(|v| v == focus) {{\n            report.record({}, {}, focus, Some(focus), None);\n        }}",
            ctx.component_id,
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}
