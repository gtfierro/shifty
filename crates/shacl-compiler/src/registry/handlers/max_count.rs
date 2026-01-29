use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, PropertyEmission};

pub struct MaxCountHandler;

impl ComponentCodegen for MaxCountHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::MaxCount
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let max_count = match params {
            ComponentParams::MaxCount { max_count } => *max_count,
            _ => return Err("maxCount params mismatch".to_string()),
        };
        let path_id = ctx
            .path_id
            .ok_or_else(|| "maxCount component requires a path".to_string())?;
        let mut emission = PropertyEmission::default();
        emission.needs_count = true;
        emission.post_loop_lines.push(format!(
            "    if count > {} {{\n        report.record({}, {}, focus, None, Some(ResultPath::PathId({})));\n    }}",
            max_count, ctx.shape_id, ctx.component_id, path_id
        ));
        Ok(emission)
    }
}
