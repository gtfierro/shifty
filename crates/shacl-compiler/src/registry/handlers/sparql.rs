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
        let (query, constraint_node) = match params {
            ComponentParams::Sparql {
                query,
                constraint_node,
            } => (query, constraint_node),
            _ => return Err("sparql params mismatch".to_string()),
        };
        let path_iri = ctx.path_sparql;
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
        let constraint_expr = (ctx.term_expr)(*constraint_node)?;
        lines.push(format!("        let constraint = {};", constraint_expr));
        lines.push("        let prefixes = prefixes_for_selector(store, &constraint);".to_string());
        lines.push("        let solutions = sparql_select_solutions_with_bindings(&query, &prefixes, store, graph, Some(focus), None, &[]);".to_string());
        lines.push(format!(
            "        let default_path: Option<ResultPath> = {};",
            match ctx.path_id {
                Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
                None => "None".to_string(),
            }
        ));
        lines.push(
            "        let mut seen: HashSet<(Option<Term>, Option<Term>)> = HashSet::new();"
                .to_string(),
        );
        lines.push("        for row in solutions {".to_string());
        lines.push("            let value = row.get(\"value\").cloned();".to_string());
        lines.push("            let path_term = row.get(\"path\").cloned();".to_string());
        lines.push("            let key = (value.clone(), path_term.clone());".to_string());
        lines.push("            if !seen.insert(key) { continue; }".to_string());
        lines.push("            let path = if let Some(path_term) = path_term { Some(ResultPath::Term(path_term)) } else { default_path.clone() };".to_string());
        lines.push(format!(
            "            report.record({}, {}, focus, value.as_ref(), path);",
            ctx.shape_id, ctx.component_id
        ));
        lines.push("        }".to_string());
        emission.per_value_lines.extend(lines);
        Ok(emission)
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let (query, constraint_node) = match params {
            ComponentParams::Sparql {
                query,
                constraint_node,
            } => (query, constraint_node),
            _ => return Err("sparql params mismatch".to_string()),
        };
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        let mut query = String::from(\"{}\");",
            escape_rust_string(query)
        ));
        let constraint_expr = (ctx.term_expr)(*constraint_node)?;
        emission
            .lines
            .push(format!("        let constraint = {};", constraint_expr));
        emission
            .lines
            .push("        let prefixes = prefixes_for_selector(store, &constraint);".to_string());
        emission.lines.push(
            "        let solutions = sparql_select_solutions_with_bindings(&query, &prefixes, store, graph, Some(&focus), None, &[]);"
                .to_string(),
        );
        emission
            .lines
            .push("        let default_path: Option<ResultPath> = None;".to_string());
        emission.lines.push(
            "        let mut seen: HashSet<(Option<Term>, Option<Term>)> = HashSet::new();"
                .to_string(),
        );
        emission
            .lines
            .push("        for row in solutions {".to_string());
        emission.lines.push(
            "            let value = row.get(\"value\").cloned().unwrap_or_else(|| focus.clone());"
                .to_string(),
        );
        emission
            .lines
            .push("            let path_term = row.get(\"path\").cloned();".to_string());
        emission
            .lines
            .push("            let key = (Some(value.clone()), path_term.clone());".to_string());
        emission
            .lines
            .push("            if !seen.insert(key) { continue; }".to_string());
        emission
            .lines
            .push("            let path = if let Some(path_term) = path_term { Some(ResultPath::Term(path_term)) } else { default_path.clone() };".to_string());
        emission.lines.push(format!(
            "            report.record({}, {}, &focus, Some(&value), path);",
            ctx.shape_id, ctx.component_id
        ));
        emission.lines.push("        }".to_string());
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
