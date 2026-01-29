use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct MinLengthHandler;
pub struct MaxLengthHandler;

impl ComponentCodegen for MinLengthHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::MinLength
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let min = match params {
            ComponentParams::MinLength { length } => *length,
            _ => return Err("minLength params mismatch".to_string()),
        };
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !literal_length_at_least(&value, {}) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            min,
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
        let min = match params {
            ComponentParams::MinLength { length } => *length,
            _ => return Err("minLength params mismatch".to_string()),
        };
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !literal_length_at_least(&focus, {}) {{\n            report.record({}, {}, focus, Some(focus), None);\n        }}",
            min,
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for MaxLengthHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::MaxLength
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let max = match params {
            ComponentParams::MaxLength { length } => *length,
            _ => return Err("maxLength params mismatch".to_string()),
        };
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !literal_length_at_most(&value, {}) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            max,
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
        let max = match params {
            ComponentParams::MaxLength { length } => *length,
            _ => return Err("maxLength params mismatch".to_string()),
        };
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !literal_length_at_most(&focus, {}) {{\n            report.record({}, {}, focus, Some(focus), None);\n        }}",
            max,
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}
