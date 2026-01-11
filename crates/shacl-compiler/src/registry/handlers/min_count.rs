use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, PropertyEmission};

pub struct MinCountHandler;

impl ComponentCodegen for MinCountHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::MinCount
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let min_count = match params {
            ComponentParams::MinCount { min_count } => *min_count,
            _ => return Err("minCount params mismatch".to_string()),
        };
        let path_iri = ctx
            .path_iri
            .ok_or_else(|| "minCount component requires a path".to_string())?;
        let mut emission = PropertyEmission::default();
        emission.needs_count = true;
        emission.post_loop_lines.push(format!(
            "    if count < {} {{\n        report.record({}, {}, focus, None, Some(\"{}\"));\n    }}",
            min_count, ctx.shape_id, ctx.component_id, path_iri
        ));
        Ok(emission)
    }
}
