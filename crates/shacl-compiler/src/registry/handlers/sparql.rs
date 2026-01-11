use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct SparqlHandler;

impl ComponentCodegen for SparqlHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Sparql
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let query = match params {
            ComponentParams::Sparql { query } => query,
            _ => return Err("sparql params mismatch".to_string()),
        };
        let path_iri = ctx.path_iri;
        let mut emission = PropertyEmission::default();
        let mut lines = Vec::new();
        lines.push(format!(
            "        let mut query = String::from(\"{}\");",
            escape_rust_string(query)
        ));
        if let Some(path) = path_iri {
            lines.push(format!(
                "        if query.contains(\"$PATH\") {{ query = query.replace(\"$PATH\", \"{}\"); }}",
                path
            ));
        }
        lines.push(
            "        let matches = sparql_any_solution(&query, store, Some(focus), Some(&value));"
                .to_string(),
        );
        lines.push(format!(
            "        if matches {{ report.record({}, {}, focus, Some(&value), {}); }}",
            ctx.shape_id,
            ctx.component_id,
            match path_iri {
                Some(path) => format!("Some(\"{}\")", path),
                None => "None".to_string(),
            }
        ));
        emission.per_value_lines.extend(lines);
        Ok(emission)
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let query = match params {
            ComponentParams::Sparql { query } => query,
            _ => return Err("sparql params mismatch".to_string()),
        };
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        let mut query = String::from(\"{}\");",
            escape_rust_string(query)
        ));
        emission.lines.push(
            "        let matches = sparql_any_solution(&query, store, Some(&focus), None);"
                .to_string(),
        );
        emission.lines.push(format!(
            "        if matches {{ report.record({}, {}, &focus, None, None); }}",
            ctx.shape_id, ctx.component_id
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
