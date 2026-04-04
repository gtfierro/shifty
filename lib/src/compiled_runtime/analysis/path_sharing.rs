use crate::compiled_runtime::analysis::{AnalysisContext, AnalysisPhase, AnalysisState, Analyzer};
use std::collections::HashMap;

/// Analyzer that detects path sharing opportunities across property shapes.
///
/// Many property shapes use identical path expressions (e.g., `s223:hasProperty`).
/// This analyzer counts total paths vs unique path expressions to measure sharing potential.
pub struct PathSharingAnalyzer;

impl Analyzer for PathSharingAnalyzer {
    fn id(&self) -> &'static str {
        "path-sharing"
    }

    fn phase(&self) -> AnalysisPhase {
        AnalysisPhase::Static
    }

    fn run(&self, ctx: &AnalysisContext<'_>, state: &mut AnalysisState) -> Result<(), String> {
        let mut total_path_refs = 0u64;
        let mut path_id_usage: HashMap<u64, u64> = HashMap::new();

        // Analyze all property shapes in the program
        // Property shapes have path_id that references entries in the paths table
        for shape in &ctx.program.shapes {
            if let Some(path_id) = shape.path_id {
                total_path_refs += 1;
                *path_id_usage.entry(path_id).or_insert(0) += 1;
            }
        }

        let unique_paths = path_id_usage.len() as u64;
        let shared_refs = if total_path_refs > unique_paths {
            total_path_refs - unique_paths
        } else {
            0
        };

        // Count how many paths are used by multiple shapes
        let highly_shared_paths = path_id_usage
            .values()
            .filter(|&&usage_count| usage_count > 1)
            .count() as u64;

        // Store statistics
        state
            .counters
            .insert("total_path_refs".to_string(), total_path_refs);
        state
            .counters
            .insert("unique_path_ids".to_string(), unique_paths);
        state
            .counters
            .insert("shared_path_refs".to_string(), shared_refs);
        state
            .counters
            .insert("highly_shared_paths".to_string(), highly_shared_paths);

        // Calculate sharing percentage
        if total_path_refs > 0 {
            let sharing_pct = (shared_refs as f64 / total_path_refs as f64) * 100.0;
            state.notes.insert(
                format!("{}_summary", self.id()),
                format!(
                    "{}/{} path references are shared ({:.1}% batching opportunity), {} paths used by multiple shapes",
                    shared_refs, total_path_refs, sharing_pct, highly_shared_paths
                ),
            );
        } else {
            state.notes.insert(
                format!("{}_summary", self.id()),
                "No path references found in program".to_string(),
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
                    kind: ShapeKind::Property,
                    term: 0,
                    target_ids: vec![],
                    component_ids: vec![],
                    path_id: Some(1), // Path ID 1
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
                ShapeRow {
                    id: 2,
                    kind: ShapeKind::Property,
                    term: 0,
                    target_ids: vec![],
                    component_ids: vec![],
                    path_id: Some(2), // Path ID 2 (different)
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

        let analyzer = PathSharingAnalyzer;
        let mut state = AnalysisState::default();

        analyzer.run(&ctx, &mut state).unwrap();

        assert_eq!(state.counters.get("total_path_refs"), Some(&2));
        assert_eq!(state.counters.get("unique_path_ids"), Some(&2));
        assert_eq!(state.counters.get("shared_path_refs"), Some(&0));
        assert_eq!(state.counters.get("highly_shared_paths"), Some(&0));
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
                    kind: ShapeKind::Property,
                    term: 0,
                    target_ids: vec![],
                    component_ids: vec![],
                    path_id: Some(1), // Path ID 1
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
                ShapeRow {
                    id: 2,
                    kind: ShapeKind::Property,
                    term: 0,
                    target_ids: vec![],
                    component_ids: vec![],
                    path_id: Some(1), // Path ID 1 (same as shape 1)
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
                ShapeRow {
                    id: 3,
                    kind: ShapeKind::Property,
                    term: 0,
                    target_ids: vec![],
                    component_ids: vec![],
                    path_id: Some(1), // Path ID 1 (same again)
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

        let analyzer = PathSharingAnalyzer;
        let mut state = AnalysisState::default();

        analyzer.run(&ctx, &mut state).unwrap();

        assert_eq!(state.counters.get("total_path_refs"), Some(&3));
        assert_eq!(state.counters.get("unique_path_ids"), Some(&1));
        assert_eq!(state.counters.get("shared_path_refs"), Some(&2));
        assert_eq!(state.counters.get("highly_shared_paths"), Some(&1));
    }

    #[test]
    fn test_mixed_shapes() {
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
                    target_ids: vec![],
                    component_ids: vec![],
                    path_id: None, // Node shape has no path
                    deactivated: false,
                    severity: "Violation".to_string(),
                },
                ShapeRow {
                    id: 2,
                    kind: ShapeKind::Property,
                    term: 0,
                    target_ids: vec![],
                    component_ids: vec![],
                    path_id: Some(1), // Property shape with path
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

        let analyzer = PathSharingAnalyzer;
        let mut state = AnalysisState::default();

        analyzer.run(&ctx, &mut state).unwrap();

        assert_eq!(state.counters.get("total_path_refs"), Some(&1));
        assert_eq!(state.counters.get("unique_path_ids"), Some(&1));
        assert_eq!(state.counters.get("shared_path_refs"), Some(&0));
    }
}
