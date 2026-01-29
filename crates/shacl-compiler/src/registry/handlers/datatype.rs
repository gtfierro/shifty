use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct DatatypeHandler;

impl ComponentCodegen for DatatypeHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Datatype
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let datatype = match params {
            ComponentParams::Datatype { datatype } => *datatype,
            _ => return Err("datatype params mismatch".to_string()),
        };
        let datatype_iri = (ctx.term_iri)(datatype)?;
        let path_id = ctx
            .path_id
            .ok_or_else(|| "datatype component requires a path".to_string())?;
        let line = format!(
            "        if !is_literal_with_datatype(&value, \"{}\") {{\n            report.record({}, {}, focus, Some(&value), Some(ResultPath::PathId({})));\n        }}",
            datatype_iri,
            ctx.shape_id,
            ctx.component_id,
            path_id
        );
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(line);
        Ok(emission)
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let datatype = match params {
            ComponentParams::Datatype { datatype } => *datatype,
            _ => return Err("datatype params mismatch".to_string()),
        };
        let datatype_iri = (ctx.term_iri)(datatype)?;
        let line = format!(
            "        if !is_literal_with_datatype(&focus, \"{}\") {{\n            report.record({}, {}, focus, Some(focus), None);\n        }}",
            datatype_iri,
            ctx.shape_id,
            ctx.component_id
        );
        let mut emission = NodeEmission::default();
        emission.lines.push(line);
        Ok(emission)
    }
}
