use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission};

pub struct ClosedHandler;

impl ComponentCodegen for ClosedHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Closed
    }

    fn emit_node(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<NodeEmission, String> {
        let (closed, ignored) = match params {
            ComponentParams::Closed {
                closed,
                ignored_properties,
            } => (*closed, ignored_properties),
            _ => return Err("closed params mismatch".to_string()),
        };
        if !closed {
            return Ok(NodeEmission::default());
        }
        let mut emission = NodeEmission::default();
        let mut ignored_exprs = Vec::new();
        for term_id in ignored {
            ignored_exprs.push((ctx.term_iri)(*term_id)?);
        }
        emission.lines.push(format!(
            "        if let Some(subject) = subject_ref(&focus) {{\n            let mut allowed: std::collections::HashSet<String> = allowed_predicates_{}();\n            let ignored: Vec<&str> = vec![{}];\n            for ig in ignored {{ allowed.insert(ig.to_string()); }}\n            for quad in store.quads_for_pattern(Some(subject), None, None, graph) {{\n                let quad = match quad {{ Ok(q) => q, Err(_) => continue }};\n                let predicate = quad.predicate;\n                let pred = predicate.as_str().to_string();\n                if !allowed.contains(&pred) {{\n                    let value = quad.object;\n                    report.record({}, {}, &focus, Some(&value), Some(ResultPath::Term(Term::NamedNode(predicate.clone()))));\n                }}\n            }}\n        }}",
            ctx.shape_id,
            ignored_exprs
                .into_iter()
                .map(|s| format!("\"{}\"", s))
                .collect::<Vec<_>>()
                .join(", "),
            ctx.shape_id,
            ctx.component_id
        ));
        Ok(emission)
    }
}
