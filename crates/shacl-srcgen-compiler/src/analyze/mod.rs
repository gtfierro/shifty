use crate::ir::SrcGenIR;

#[derive(Debug, Clone, Copy)]
pub struct AnalysisSummary {
    pub shape_count: usize,
    pub component_count: usize,
    pub fallback_component_count: usize,
}

pub fn analyze(ir: &SrcGenIR) -> AnalysisSummary {
    AnalysisSummary {
        shape_count: ir.node_shapes.len() + ir.property_shapes.len(),
        component_count: ir.components.len(),
        fallback_component_count: ir.components.iter().filter(|c| c.fallback_only).count(),
    }
}
