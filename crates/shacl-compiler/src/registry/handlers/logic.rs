use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct NotHandler;
pub struct AndHandler;
pub struct OrHandler;
pub struct XoneHandler;

impl ComponentCodegen for NotHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Not
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let shape = match params {
            ComponentParams::Not { shape } => *shape,
            _ => return Err("not params mismatch".to_string()),
        };
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if node_shape_conforms_{}(store, graph, &value) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            shape,
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
        let shape = match params {
            ComponentParams::Not { shape } => *shape,
            _ => return Err("not params mismatch".to_string()),
        };
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if node_shape_conforms_{}(store, graph, &focus) {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            shape, ctx.shape_id, ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for AndHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::And
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let shapes = match params {
            ComponentParams::And { shapes } => shapes,
            _ => return Err("and params mismatch".to_string()),
        };
        let checks = shapes
            .iter()
            .map(|shape| format!("node_shape_conforms_{}(store, graph, &value)", shape))
            .collect::<Vec<_>>()
            .join(" && ");
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !({}) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            checks,
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
        let shapes = match params {
            ComponentParams::And { shapes } => shapes,
            _ => return Err("and params mismatch".to_string()),
        };
        let checks = shapes
            .iter()
            .map(|shape| format!("node_shape_conforms_{}(store, graph, &focus)", shape))
            .collect::<Vec<_>>()
            .join(" && ");
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !({}) {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            checks, ctx.shape_id, ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for OrHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Or
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let shapes = match params {
            ComponentParams::Or { shapes } => shapes,
            _ => return Err("or params mismatch".to_string()),
        };
        let checks = shapes
            .iter()
            .map(|shape| format!("node_shape_conforms_{}(store, graph, &value)", shape))
            .collect::<Vec<_>>()
            .join(" || ");
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !({}) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            checks,
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
        let shapes = match params {
            ComponentParams::Or { shapes } => shapes,
            _ => return Err("or params mismatch".to_string()),
        };
        let checks = shapes
            .iter()
            .map(|shape| format!("node_shape_conforms_{}(store, graph, &focus)", shape))
            .collect::<Vec<_>>()
            .join(" || ");
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !({}) {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            checks, ctx.shape_id, ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for XoneHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Xone
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let shapes = match params {
            ComponentParams::Xone { shapes } => shapes,
            _ => return Err("xone params mismatch".to_string()),
        };
        let sum = shapes
            .iter()
            .map(|shape| {
                format!(
                    "(node_shape_conforms_{}(store, graph, &value) as i32)",
                    shape
                )
            })
            .collect::<Vec<_>>()
            .join(" + ");
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if ({}) != 1 {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            sum,
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
        let shapes = match params {
            ComponentParams::Xone { shapes } => shapes,
            _ => return Err("xone params mismatch".to_string()),
        };
        let sum = shapes
            .iter()
            .map(|shape| {
                format!(
                    "(node_shape_conforms_{}(store, graph, &focus) as i32)",
                    shape
                )
            })
            .collect::<Vec<_>>()
            .join(" + ");
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if ({}) != 1 {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            sum, ctx.shape_id, ctx.component_id
        ));
        Ok(emission)
    }
}
