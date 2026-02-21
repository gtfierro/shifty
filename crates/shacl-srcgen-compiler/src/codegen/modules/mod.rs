use crate::ir::SrcGenIR;
use crate::SrcGenBackend;

pub mod inference;
pub mod paths;
pub mod prelude;
pub mod report;
pub mod run;
pub mod targets;
pub mod validators_node;
pub mod validators_property;

pub fn generate_module_files(
    ir: &SrcGenIR,
    backend: SrcGenBackend,
) -> Result<Vec<(String, String)>, String> {
    Ok(vec![
        ("prelude.rs".to_string(), prelude::generate(ir, backend)?),
        ("paths.rs".to_string(), paths::generate(ir)?),
        ("targets.rs".to_string(), targets::generate(ir)?),
        (
            "validators_property.rs".to_string(),
            validators_property::generate(ir)?,
        ),
        (
            "validators_node.rs".to_string(),
            validators_node::generate(ir)?,
        ),
        ("inference.rs".to_string(), inference::generate(ir)?),
        ("report.rs".to_string(), report::generate(ir)?),
        ("run.rs".to_string(), run::generate(ir)?),
    ])
}
