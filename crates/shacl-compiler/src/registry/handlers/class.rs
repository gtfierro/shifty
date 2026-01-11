use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct ClassHandler;

impl ComponentCodegen for ClassHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Class
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let class = match params {
            ComponentParams::Class { class } => *class,
            _ => return Err("class params mismatch".to_string()),
        };
        let class_iri = (ctx.term_iri)(class)?;
        let mut emission = PropertyEmission::default();
        emission.per_value_lines.push(format!(
            "        if !has_rdf_type(store, graph, &value, \"{}\") {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            class_iri,
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
        let class = match params {
            ComponentParams::Class { class } => *class,
            _ => return Err("class params mismatch".to_string()),
        };
        let class_iri = (ctx.term_iri)(class)?;
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        if !has_rdf_type(store, graph, &focus, \"{}\") {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            class_iri,
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}
