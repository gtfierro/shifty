use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct MinExclusiveHandler;
pub struct MinInclusiveHandler;
pub struct MaxExclusiveHandler;
pub struct MaxInclusiveHandler;

fn emit_range_check(
    ctx: &EmitContext<'_>,
    params: &ComponentParams,
    op: &str,
) -> Result<(String, String), String> {
    let term_id = match params {
        ComponentParams::MinExclusive { value }
        | ComponentParams::MinInclusive { value }
        | ComponentParams::MaxExclusive { value }
        | ComponentParams::MaxInclusive { value } => *value,
        _ => return Err("range params mismatch".to_string()),
    };
    let value_sparql = (ctx.term_sparql)(term_id)?;
    let query = format!("ASK {{ FILTER(?value {} {}) }}", op, value_sparql);
    Ok((query, value_sparql))
}

impl ComponentCodegen for MinExclusiveHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::MinExclusive
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let (query, _) = emit_range_check(&ctx, params, ">")?;
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !sparql_any_solution_lenient(\"{}\", store, None, None, Some(&value)) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            escape_rust_string(&query),
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
        let (query, _) = emit_range_check(&ctx, params, ">")?;
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !sparql_any_solution_lenient(\"{}\", store, None, None, Some(&focus)) {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            escape_rust_string(&query),
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for MinInclusiveHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::MinInclusive
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let (query, _) = emit_range_check(&ctx, params, ">=")?;
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !sparql_any_solution_lenient(\"{}\", store, None, None, Some(&value)) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            escape_rust_string(&query),
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
        let (query, _) = emit_range_check(&ctx, params, ">=")?;
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !sparql_any_solution_lenient(\"{}\", store, None, None, Some(&focus)) {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            escape_rust_string(&query),
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for MaxExclusiveHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::MaxExclusive
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let (query, _) = emit_range_check(&ctx, params, "<")?;
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !sparql_any_solution_lenient(\"{}\", store, None, None, Some(&value)) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            escape_rust_string(&query),
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
        let (query, _) = emit_range_check(&ctx, params, "<")?;
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !sparql_any_solution_lenient(\"{}\", store, None, None, Some(&focus)) {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            escape_rust_string(&query),
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for MaxInclusiveHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::MaxInclusive
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let (query, _) = emit_range_check(&ctx, params, "<=")?;
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !sparql_any_solution_lenient(\"{}\", store, None, None, Some(&value)) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            escape_rust_string(&query),
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
        let (query, _) = emit_range_check(&ctx, params, "<=")?;
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !sparql_any_solution_lenient(\"{}\", store, None, None, Some(&focus)) {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            escape_rust_string(&query),
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}

fn escape_rust_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
