use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct EqualsHandler;
pub struct DisjointHandler;
pub struct LessThanHandler;
pub struct LessThanOrEqualsHandler;

impl ComponentCodegen for EqualsHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Equals
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let prop = match params {
            ComponentParams::Equals { property } => *property,
            _ => return Err("equals params mismatch".to_string()),
        };
        let prop_iri = (ctx.term_iri)(prop)?;
        let mut emission = PropertyEmission::default();
        emission.needs_values = true;
        emission.post_loop_lines.push(format!(
            "    let other_values = values_for_predicate(store, graph, focus, \"{}\");",
            prop_iri
        ));
        emission.post_loop_lines.push(
            "    let values_set: std::collections::HashSet<Term> = values.into_iter().collect();"
                .to_string(),
        );
        emission.post_loop_lines.push("    let other_set: std::collections::HashSet<Term> = other_values.into_iter().collect();".to_string());
        emission.post_loop_lines.push(format!(
            "    for value in values_set.difference(&other_set) {{\n        report.record({}, {}, focus, Some(value), {});\n    }}",
            ctx.shape_id,
            ctx.component_id,
            match ctx.path_id {
                Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
                None => "None".to_string(),
            }
        ));
        emission.post_loop_lines.push(format!(
            "    for value in other_set.difference(&values_set) {{\n        report.record({}, {}, focus, Some(value), {});\n    }}",
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
        let prop = match params {
            ComponentParams::Equals { property } => *property,
            _ => return Err("equals params mismatch".to_string()),
        };
        let prop_iri = (ctx.term_iri)(prop)?;
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        let values: Vec<Term> = vec![focus.clone()];"
        ));
        emission
            .lines
            .push(format!(
                "        let other_values = values_for_predicate(store, graph, focus, \"{}\");",
                prop_iri
            ));
        emission.lines.push(
            "        let values_set: std::collections::HashSet<Term> = values.into_iter().collect();"
                .to_string(),
        );
        emission.lines.push(
            "        let other_set: std::collections::HashSet<Term> = other_values.into_iter().collect();"
                .to_string(),
        );
        emission.lines.push(format!(
            "        for value in values_set.difference(&other_set) {{\n            report.record({}, {}, focus, Some(value), None);\n        }}",
            ctx.shape_id, ctx.component_id
        ));
        emission.lines.push(format!(
            "        for value in other_set.difference(&values_set) {{\n            report.record({}, {}, focus, Some(value), None);\n        }}",
            ctx.shape_id, ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for DisjointHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Disjoint
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let prop = match params {
            ComponentParams::Disjoint { property } => *property,
            _ => return Err("disjoint params mismatch".to_string()),
        };
        let prop_iri = (ctx.term_iri)(prop)?;
        let mut emission = PropertyEmission::default();
        emission.needs_values = true;
        emission.post_loop_lines.push(format!(
            "    let other_values = values_for_predicate(store, graph, focus, \"{}\");",
            prop_iri
        ));
        emission.post_loop_lines.push("    let other_set: std::collections::HashSet<Term> = other_values.into_iter().collect();".to_string());
        emission.post_loop_lines.push(format!(
            "    for value in &values {{\n        if other_set.contains(value) {{\n            report.record({}, {}, focus, Some(value), {});\n        }}\n    }}",
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
        let prop = match params {
            ComponentParams::Disjoint { property } => *property,
            _ => return Err("disjoint params mismatch".to_string()),
        };
        let prop_iri = (ctx.term_iri)(prop)?;
        let mut emission = NodeEmission::default();
        emission
            .lines
            .push(format!(
                "        let other_values = values_for_predicate(store, graph, focus, \"{}\");",
                prop_iri
            ));
        emission.lines.push(
            "        let other_set: std::collections::HashSet<Term> = other_values.into_iter().collect();"
                .to_string(),
        );
        emission.lines.push(format!(
            "        if other_set.contains(focus) {{\n            report.record({}, {}, focus, Some(focus), None);\n        }}",
            ctx.shape_id, ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for LessThanHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::LessThan
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let prop = match params {
            ComponentParams::LessThan { property } => *property,
            _ => return Err("lessThan params mismatch".to_string()),
        };
        let prop_iri = (ctx.term_iri)(prop)?;
        let mut emission = PropertyEmission::default();
        emission.needs_values = true;
        emission.post_loop_lines.push(format!(
            "    let other_values = values_for_predicate(store, graph, focus, \"{}\");",
            prop_iri
        ));
        emission
            .post_loop_lines
            .push("    for value in &values {".to_string());
        emission
            .post_loop_lines
            .push("        for other in &other_values {".to_string());
        emission.post_loop_lines.push(
            "            let query = format!(\"ASK {{ FILTER(?value < {}) }}\", term_to_sparql(other));"
                .to_string(),
        );
        emission.post_loop_lines.push(
            "            if !sparql_any_solution(&query, store, graph, None, None, Some(value)) {"
                .to_string(),
        );
        emission.post_loop_lines.push(format!(
            "                report.record({}, {}, focus, Some(value), {});",
            ctx.shape_id,
            ctx.component_id,
            match ctx.path_id {
                Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
                None => "None".to_string(),
            }
        ));
        emission.post_loop_lines.push("            }".to_string());
        emission.post_loop_lines.push("        }".to_string());
        emission.post_loop_lines.push("    }".to_string());
        Ok(emission)
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let prop = match params {
            ComponentParams::LessThan { property } => *property,
            _ => return Err("lessThan params mismatch".to_string()),
        };
        let prop_iri = (ctx.term_iri)(prop)?;
        let mut emission = NodeEmission::default();
        emission
            .lines
            .push(format!(
                "        let other_values = values_for_predicate(store, graph, focus, \"{}\");",
                prop_iri
            ));
        emission.lines.push("        for other in &other_values {".to_string());
        emission.lines.push(
            "            let query = format!(\"ASK {{ FILTER(?value < {}) }}\", term_to_sparql(other));"
                .to_string(),
        );
        emission.lines.push(
            "            if !sparql_any_solution(&query, store, graph, None, None, Some(focus)) {"
                .to_string(),
        );
        emission.lines.push(format!(
            "                report.record({}, {}, focus, Some(focus), None);",
            ctx.shape_id, ctx.component_id
        ));
        emission.lines.push("            }".to_string());
        emission.lines.push("        }".to_string());
        Ok(emission)
    }
}

impl ComponentCodegen for LessThanOrEqualsHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::LessThanOrEquals
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let prop = match params {
            ComponentParams::LessThanOrEquals { property } => *property,
            _ => return Err("lessThanOrEquals params mismatch".to_string()),
        };
        let prop_iri = (ctx.term_iri)(prop)?;
        let mut emission = PropertyEmission::default();
        emission.needs_values = true;
        emission.post_loop_lines.push(format!(
            "    let other_values = values_for_predicate(store, graph, focus, \"{}\");",
            prop_iri
        ));
        emission
            .post_loop_lines
            .push("    for value in &values {".to_string());
        emission
            .post_loop_lines
            .push("        for other in &other_values {".to_string());
        emission.post_loop_lines.push(
            "            let query = format!(\"ASK {{ FILTER(?value <= {}) }}\", term_to_sparql(other));"
                .to_string(),
        );
        emission.post_loop_lines.push(
            "            if !sparql_any_solution(&query, store, graph, None, None, Some(value)) {"
                .to_string(),
        );
        emission.post_loop_lines.push(format!(
            "                report.record({}, {}, focus, Some(value), {});",
            ctx.shape_id,
            ctx.component_id,
            match ctx.path_id {
                Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
                None => "None".to_string(),
            }
        ));
        emission.post_loop_lines.push("            }".to_string());
        emission.post_loop_lines.push("        }".to_string());
        emission.post_loop_lines.push("    }".to_string());
        Ok(emission)
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let prop = match params {
            ComponentParams::LessThanOrEquals { property } => *property,
            _ => return Err("lessThanOrEquals params mismatch".to_string()),
        };
        let prop_iri = (ctx.term_iri)(prop)?;
        let mut emission = NodeEmission::default();
        emission
            .lines
            .push(format!(
                "        let other_values = values_for_predicate(store, graph, focus, \"{}\");",
                prop_iri
            ));
        emission.lines.push("        for other in &other_values {".to_string());
        emission.lines.push(
            "            let query = format!(\"ASK {{ FILTER(?value <= {}) }}\", term_to_sparql(other));"
                .to_string(),
        );
        emission.lines.push(
            "            if !sparql_any_solution(&query, store, graph, None, None, Some(focus)) {"
                .to_string(),
        );
        emission.lines.push(format!(
            "                report.record({}, {}, focus, Some(focus), None);",
            ctx.shape_id, ctx.component_id
        ));
        emission.lines.push("            }".to_string());
        emission.lines.push("        }".to_string());
        Ok(emission)
    }
}
