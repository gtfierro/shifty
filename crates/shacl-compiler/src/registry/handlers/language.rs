use crate::plan::{ComponentKind, ComponentParams};
use crate::registry::component::{ComponentCodegen, EmitContext, NodeEmission, PropertyEmission};

pub struct LanguageInHandler;
pub struct UniqueLangHandler;

impl ComponentCodegen for LanguageInHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::LanguageIn
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let languages = match params {
            ComponentParams::LanguageIn { languages } => languages,
            _ => return Err("languageIn params mismatch".to_string()),
        };
        let list = languages
            .iter()
            .map(|lang| format!("\"{}\"", escape_rust_string(lang)))
            .collect::<Vec<_>>()
            .join(", ");
        let mut emission = PropertyEmission::default();
        emission.pre_loop_lines.push(format!(
            "    let allowed_langs_{} = [{}];",
            ctx.component_id, list
        ));
        emission.per_value_lines.push(format!(
            "        if !language_in_allowed(&value, &allowed_langs_{}) {{\n            report.record({}, {}, focus, Some(&value), {});\n        }}",
            ctx.component_id,
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
        let languages = match params {
            ComponentParams::LanguageIn { languages } => languages,
            _ => return Err("languageIn params mismatch".to_string()),
        };
        let list = languages
            .iter()
            .map(|lang| format!("\"{}\"", escape_rust_string(lang)))
            .collect::<Vec<_>>()
            .join(", ");
        let mut emission = NodeEmission::default();
        emission.lines.push(format!(
            "        let allowed_langs_{} = [{}];",
            ctx.component_id, list
        ));
        emission.lines.push(format!(
            "        if !language_in_allowed(&focus, &allowed_langs_{}) {{\n            report.record({}, {}, focus, Some(focus), None);\n        }}",
            ctx.component_id, ctx.shape_id, ctx.component_id
        ));
        Ok(emission)
    }
}

impl ComponentCodegen for UniqueLangHandler {
    fn kind(&self) -> ComponentKind {
        ComponentKind::UniqueLang
    }

    fn emit_property(
        &self,
        ctx: EmitContext<'_>,
        params: &ComponentParams,
    ) -> Result<PropertyEmission, String> {
        let enabled = match params {
            ComponentParams::UniqueLang { enabled } => *enabled,
            _ => return Err("uniqueLang params mismatch".to_string()),
        };
        if !enabled {
            return Ok(PropertyEmission::default());
        }
        let mut emission = PropertyEmission::default();
        emission.pre_loop_lines.push(format!(
            "    let mut langs_seen_{}: std::collections::HashSet<String> = std::collections::HashSet::new();",
            ctx.component_id
        ));
        emission.pre_loop_lines.push(format!(
            "    let mut langs_dup_{}: std::collections::HashSet<String> = std::collections::HashSet::new();",
            ctx.component_id
        ));
        emission.per_value_lines.push(format!(
            "        if let Term::Literal(lit) = &value {{\n            if let Some(lang) = lit.language() {{\n                if !lang.is_empty() {{\n                    let lower = lang.to_lowercase();\n                    if !langs_seen_{}.insert(lower.clone()) {{\n                        langs_dup_{}.insert(lower);\n                    }}\n                }}\n            }}\n        }}",
            ctx.component_id,
            ctx.component_id
        ));
        emission.post_loop_lines.push(format!(
            "    for dup in &langs_dup_{} {{\n        report.record({}, {}, focus, None, {});\n    }}",
            ctx.component_id,
            ctx.shape_id,
            ctx.component_id,
            match ctx.path_id {
                Some(path_id) => format!("Some(ResultPath::PathId({}))", path_id),
                None => "None".to_string(),
            }
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
