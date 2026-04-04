use crate::compiled_runtime::analysis::{AnalysisContext, AnalysisPhase, AnalysisState, Analyzer};
use std::collections::HashSet;

/// Analyzer that detects target sharing opportunities across shapes.
///
/// Multiple shapes often use identical target expressions (e.g., `sh:targetClass s223:Equipment`).
/// This analyzer counts total targets vs unique target expressions to measure sharing potential.
pub struct TargetSharingAnalyzer;

impl Analyzer for TargetSharingAnalyzer {
    fn id(&self) -> &'static str {
        "target-sharing"
    }

    fn phase(&self) -> AnalysisPhase {
        AnalysisPhase::Static
    }

    fn run(&self, ctx: &AnalysisContext<'_>, state: &mut AnalysisState) -> Result<(), String> {
        let mut total_target_refs = 0u64;
        let mut unique_target_ids = HashSet::new();

        // Analyze all shapes in the program
        // Each shape has target_ids that reference entries in the targets table
        for shape in &ctx.program.shapes {
            // Count target references for this shape
            let shape_target_count = shape.target_ids.len();
            total_target_refs += shape_target_count as u64;

            // Collect unique target IDs
            for target_id in &shape.target_ids {
                unique_target_ids.insert(*target_id);
            }
        }

        let unique_count = unique_target_ids.len() as u64;
        let shared_refs = if total_target_refs > unique_count {
            total_target_refs - unique_count
        } else {
            0
        };

        // Store statistics
        state
            .counters
            .insert("total_target_refs".to_string(), total_target_refs);
        state
            .counters
            .insert("unique_target_ids".to_string(), unique_count);
        state
            .counters
            .insert("shared_target_refs".to_string(), shared_refs);

        // Calculate sharing percentage
        if total_target_refs > 0 {
            let sharing_pct = (shared_refs as f64 / total_target_refs as f64) * 100.0;
            state.notes.insert(
                format!("{}_summary", self.id()),
                format!(
                    "{}/{} target references are shared ({:.1}% deduplication opportunity)",
                    shared_refs, total_target_refs, sharing_pct
                ),
            );
        } else {
            state.notes.insert(
                format!("{}_summary", self.id()),
                "No target references found in program".to_string(),
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compiled_runtime::analysis::AnalysisState;
    use crate::compiled_runtime::program::{
        CompiledProgram, ProgramHeader, ShapeKind, ShapeRow, StaticHints,
    };
    use oxigraph::store::Store;

    #[test]
    fn test_no_sharing() {
        let program = CompiledProgram {
            header: ProgramHeader {
                schema_version: 1,
                min_kernel_version: 1,
                program_hash: [0; 32],
                feature_bits: 0,
            },
            terms: vec![],
            shapes: vec![
                ShapeRow {
                    id: 1,
                    kind: ShapeKind::Node,
                    term: 0,
                    target_ids: vec![1], // Target ID 1
                    component_ids: vec![],
                    path_id: None,
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
                ShapeRow {
                    id: 2,
                    kind: ShapeKind::Node,
                    term: 0,
                    target_ids: vec![2], // Target ID 2 (different)
                    component_ids: vec![],
                    path_id: None,
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
            ],
            components: vec![],
            paths: vec![],
            targets: vec![],
            rules: vec![],
            shape_graph_triples: vec![],
            static_hints: StaticHints::default(),
            ext: Default::default(),
        };

        let store = Store::new().unwrap();
        let ctx = AnalysisContext {
            program: &program,
            store: &store,
            data_graph: None,
        };

        let analyzer = TargetSharingAnalyzer;
        let mut state = AnalysisState::default();

        analyzer.run(&ctx, &mut state).unwrap();

        assert_eq!(state.counters.get("total_target_refs"), Some(&2));
        assert_eq!(state.counters.get("unique_target_ids"), Some(&2));
        assert_eq!(state.counters.get("shared_target_refs"), Some(&0));
    }

    #[test]
    fn test_with_sharing() {
        let program = CompiledProgram {
            header: ProgramHeader {
                schema_version: 1,
                min_kernel_version: 1,
                program_hash: [0; 32],
                feature_bits: 0,
            },
            terms: vec![],
            shapes: vec![
                ShapeRow {
                    id: 1,
                    kind: ShapeKind::Node,
                    term: 0,
                    target_ids: vec![1], // Target ID 1
                    component_ids: vec![],
                    path_id: None,
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
                ShapeRow {
                    id: 2,
                    kind: ShapeKind::Node,
                    term: 0,
                    target_ids: vec![1], // Target ID 1 (same as shape 1)
                    component_ids: vec![],
                    path_id: None,
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
            ],
            components: vec![],
            paths: vec![],
            targets: vec![],
            rules: vec![],
            shape_graph_triples: vec![],
            static_hints: StaticHints::default(),
            ext: Default::default(),
        };

        let store = Store::new().unwrap();
        let ctx = AnalysisContext {
            program: &program,
            store: &store,
            data_graph: None,
        };

        let analyzer = TargetSharingAnalyzer;
        let mut state = AnalysisState::default();

        analyzer.run(&ctx, &mut state).unwrap();

        assert_eq!(state.counters.get("total_target_refs"), Some(&2));
        assert_eq!(state.counters.get("unique_target_ids"), Some(&1));
        assert_eq!(state.counters.get("shared_target_refs"), Some(&1));
    }
}
