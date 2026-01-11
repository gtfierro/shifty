use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct PatternHandler;

impl ComponentCodegen for PatternHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::Pattern
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let (pattern, flags) = match params {
            ComponentParams::Pattern { pattern, flags } => (pattern, flags),
            _ => return Err("pattern params mismatch".to_string()),
        };
        let regex_var = format!("regex_{}", ctx.component_id);
        let regex_pattern = pattern_with_flags(pattern, flags.as_deref());
        let mut emission = PropertyEmission::default();
        emission.pre_loop_lines.push(format!(
            "    let {} = regex::Regex::new(\"{}\").unwrap();",
            regex_var,
            escape_rust_string(&regex_pattern)
        ));
        emission.per_value_lines.push(format!(
            "        if !literal_matches_regex(&value, &{}) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            regex_var,
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
        let (pattern, flags) = match params {
            ComponentParams::Pattern { pattern, flags } => (pattern, flags),
            _ => return Err("pattern params mismatch".to_string()),
        };
        let regex_var = format!("regex_{}", ctx.component_id);
        let regex_pattern = pattern_with_flags(pattern, flags.as_deref());
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        let {} = regex::Regex::new(\"{}\").unwrap();",
            regex_var,
            escape_rust_string(&regex_pattern)
        ));
        emission.lines.push(format!(
            "        if !literal_matches_regex(&focus, &{}) {{\n            report.record({}, {}, &focus, None, None);\n        }}",
            regex_var, ctx.shape_id, ctx.component_id
        ));
        Ok(emission)
    }
}

fn pattern_with_flags(pattern: &str, flags: Option<&str>) -> String {
    let mut flag_prefix = String::new();
    if let Some(flags) = flags {
        let mut opts = String::new();
        for ch in flags.chars() {
            match ch {
                'i' | 'm' | 's' | 'x' | 'U' => opts.push(ch),
                _ => {}
            }
        }
        if !opts.is_empty() {
            flag_prefix = format!("(?{})", opts);
        }
    }
    format!("{}{}", flag_prefix, pattern)
}

fn escape_rust_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
