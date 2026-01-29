use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, PropertyEmission};

pub struct QualifiedValueShapeHandler;

impl ComponentCodegen for QualifiedValueShapeHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::QualifiedValueShape
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let (shape, min, max, disjoint) = match params {
            ComponentParams::QualifiedValueShape {
                shape,
                min_count,
                max_count,
                disjoint,
            } => (*shape, *min_count, *max_count, *disjoint),
            _ => return Err("qualifiedValueShape params mismatch".to_string()),
        };

        let mut emission = PropertyEmission::default();
        emission.pre_loop_lines.push(format!(
            "    let mut qualified_count_{}: u64 = 0;",
            ctx.component_id
        ));
        emission.per_value_lines.push(format!(
            "        let conforms_target = node_shape_conforms_{}(store, graph, &value);",
            shape
        ));
        if disjoint.unwrap_or(false) {
            let siblings = ctx
                .qualified_siblings
                .ok_or_else(|| "qualifiedValueShape disjoint requires sibling list".to_string())?;
            let mut sibling_checks = Vec::new();
            for sibling in siblings {
                sibling_checks.push(format!(
                    "node_shape_conforms_{}(store, graph, &value)",
                    sibling
                ));
            }
            let sibling_expr = if sibling_checks.is_empty() {
                "false".to_string()
            } else {
                sibling_checks.join(" || ")
            };
            emission
                .per_value_lines
                .push(format!("        let conforms_sibling = {};", sibling_expr));
            emission.per_value_lines.push(format!(
                "        if conforms_target && !conforms_sibling {{ qualified_count_{} += 1; }}",
                ctx.component_id
            ));
        } else {
            emission.per_value_lines.push(format!(
                "        if conforms_target {{ qualified_count_{} += 1; }}",
                ctx.component_id
            ));
        }

        if let Some(min) = min {
            emission.post_loop_lines.push(format!(
                "    if qualified_count_{} < {} {{\n        report.record({}, {}, focus, None, {});\n    }}",
                ctx.component_id,
                min,
                ctx.shape_id,
                ctx.component_id,
                match ctx.path_id {
                    Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
                    None => "None".to_string(),
                }
            ));
        }
        if let Some(max) = max {
            emission.post_loop_lines.push(format!(
                "    if qualified_count_{} > {} {{\n        report.record({}, {}, focus, None, {});\n    }}",
                ctx.component_id,
                max,
                ctx.shape_id,
                ctx.component_id,
                match ctx.path_id {
                    Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
                    None => "None".to_string(),
                }
            ));
        }
        Ok(emission)
    }
}
