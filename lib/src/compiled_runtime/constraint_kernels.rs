use crate::compiled_runtime::program::CompiledProgram;
use std::collections::BTreeSet;

pub fn supported_component_kinds(program: &CompiledProgram) -> BTreeSet<String> {
    program
        .components
        .iter()
        .map(|component| component.kind.clone())
        .collect()
}
