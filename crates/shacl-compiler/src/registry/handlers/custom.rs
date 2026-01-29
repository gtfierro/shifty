use crate::plan::{ComponentKind, ComponentParams, PlanCustomBinding};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct CustomHandler;

impl ComponentCodegen for CustomHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Custom
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let (bindings, validator, property_validator) = match params {
            ComponentParams::Custom {
                bindings,
                validator,
                property_validator,
                ..
            } => (bindings, validator, property_validator),
            _ => return Err("custom params mismatch".to_string()),
        };
        let chosen = property_validator.as_ref().or(validator.as_ref());
        let Some(chosen) = chosen else {
            return Ok(PropertyEmission::default());
        };

        let mut emission = PropertyEmission::default();
        let bindings_var = format!("param_bindings_{}", ctx.component_id);
        let query_var = format!("query_{}", ctx.component_id);
        let prefixes = escape_rust_string(&chosen.prefixes);

        emit_binding_lines(
            &mut emission.pre_loop_lines,
            &bindings_var,
            bindings,
            &ctx,
            "    ",
        )?;
        emission.pre_loop_lines.push(format!(
            "    let mut {} = String::from(\"{}\");",
            query_var,
            escape_rust_string(&chosen.query)
        ));
        if let Some(path) = ctx.path_sparql {
            emission.pre_loop_lines.push(format!(
                "    if {}.contains(\"$PATH\") {{ {} = {}.replace(\"$PATH\", \"{}\"); }}",
                query_var, query_var, query_var, path
            ));
        }

        let path_expr = match ctx.path_id {
            Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
            None => "None".to_string(),
        };

        if chosen.is_ask {
            emission.per_value_lines.push(format!(
                "        let conforms = sparql_any_solution_with_bindings(&{}, \"{}\", store, graph, Some(focus), Some(&value), &{});",
                query_var, prefixes, bindings_var
            ));
            emission.per_value_lines.push(format!(
                "        if !conforms {{ report.record({}, {}, focus, Some(&value), {}); }}",
                ctx.shape_id, ctx.component_id, path_expr
            ));
        } else {
            emission.post_loop_lines.push(format!(
                "    let solutions = sparql_select_solutions_with_bindings(&{}, \"{}\", store, graph, Some(focus), None, &{});",
                query_var, prefixes, bindings_var
            ));
            emission
                .post_loop_lines
                .push("    let mut seen: HashSet<Term> = HashSet::new();".to_string());
            emission
                .post_loop_lines
                .push("    let mut recorded_without_value = false;".to_string());
            emission
                .post_loop_lines
                .push("    for row in solutions {".to_string());
            emission
                .post_loop_lines
                .push("        if let Some(value) = row.get(\"value\").cloned() {".to_string());
            emission
                .post_loop_lines
                .push("            if !seen.insert(value.clone()) { continue; }".to_string());
            emission.post_loop_lines.push(format!(
                "            report.record({}, {}, focus, Some(&value), {});",
                ctx.shape_id, ctx.component_id, path_expr
            ));
            emission.post_loop_lines.push("        } else if !recorded_without_value {".to_string());
            emission
                .post_loop_lines
                .push("            recorded_without_value = true;".to_string());
            emission.post_loop_lines.push(format!(
                "            report.record({}, {}, focus, None, {});",
                ctx.shape_id, ctx.component_id, path_expr
            ));
            emission.post_loop_lines.push("        }".to_string());
            emission.post_loop_lines.push("    }".to_string());
        }

        Ok(emission)
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let (bindings, validator, node_validator) = match params {
            ComponentParams::Custom {
                bindings,
                validator,
                node_validator,
                ..
            } => (bindings, validator, node_validator),
            _ => return Err("custom params mismatch".to_string()),
        };
        let chosen = node_validator.as_ref().or(validator.as_ref());
        let Some(chosen) = chosen else {
            return Ok(NodeEmission::default());
        };

        let mut emission = NodeEmission::default();
        let bindings_var = format!("param_bindings_{}", ctx.component_id);
        let query_var = format!("query_{}", ctx.component_id);
        let prefixes = escape_rust_string(&chosen.prefixes);

        emit_binding_lines(&mut emission.lines, &bindings_var, bindings, &ctx, "        ")?;
        emission.lines.push(format!(
            "        let mut {} = String::from(\"{}\");",
            query_var,
            escape_rust_string(&chosen.query)
        ));

        if chosen.is_ask {
            emission.lines.push(format!(
                "        let conforms = sparql_any_solution_with_bindings(&{}, \"{}\", store, graph, Some(focus), Some(focus), &{});",
                query_var, prefixes, bindings_var
            ));
            emission.lines.push(format!(
                "        if !conforms {{ report.record({}, {}, focus, Some(focus), None); }}",
                ctx.shape_id, ctx.component_id
            ));
        } else {
            emission.lines.push(format!(
                "        let solutions = sparql_select_solutions_with_bindings(&{}, \"{}\", store, graph, Some(focus), None, &{});",
                query_var, prefixes, bindings_var
            ));
            emission
                .lines
                .push("        let mut seen: HashSet<Term> = HashSet::new();".to_string());
            emission.lines.push("        for row in solutions {".to_string());
            emission.lines.push(
                "            let value = row.get(\"value\").cloned().unwrap_or_else(|| focus.clone());"
                    .to_string(),
            );
            emission
                .lines
                .push("            if !seen.insert(value.clone()) { continue; }".to_string());
            emission.lines.push(format!(
                "            report.record({}, {}, focus, Some(&value), None);",
                ctx.shape_id, ctx.component_id
            ));
            emission.lines.push("        }".to_string());
        }

        Ok(emission)
    }
}

fn emit_binding_lines(
    lines: &mut Vec<String>,
    bindings_var: &str,
    bindings: &[PlanCustomBinding],
    ctx: &EmitContext<'_>,
    indent: &str,
) -> Result<(), String> {
    lines.push(format!(
        "{}let mut {}: Vec<(&str, Term)> = Vec::new();",
        indent, bindings_var
    ));
    for binding in bindings {
        let value_expr = (ctx.term_expr)(binding.value)?;
        lines.push(format!(
            "{}{}.push((\"{}\", {}));",
            indent, bindings_var, binding.var_name, value_expr
        ));
    }
    Ok(())
}

fn escape_rust_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
