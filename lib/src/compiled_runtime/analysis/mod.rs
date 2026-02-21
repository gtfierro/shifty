pub mod data;
pub mod shape;

use crate::compiled_runtime::program::CompiledProgram;
use oxigraph::model::GraphNameRef;
use oxigraph::store::Store;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub struct AnalysisBudget {
    pub max_static_analyzers: usize,
    pub max_runtime_analyzers: usize,
}

impl Default for AnalysisBudget {
    fn default() -> Self {
        Self {
            max_static_analyzers: usize::MAX,
            max_runtime_analyzers: usize::MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisPhase {
    Static,
    Runtime,
}

pub struct AnalysisContext<'a> {
    pub program: &'a CompiledProgram,
    pub store: &'a Store,
    pub data_graph: Option<GraphNameRef<'a>>,
}

#[derive(Debug, Default, Clone)]
pub struct AnalysisState {
    pub counters: BTreeMap<String, u64>,
    pub notes: BTreeMap<String, String>,
}

pub trait Analyzer: Send + Sync {
    fn id(&self) -> &'static str;
    fn phase(&self) -> AnalysisPhase;
    fn run(&self, ctx: &AnalysisContext<'_>, state: &mut AnalysisState) -> Result<(), String>;
}

#[derive(Default)]
pub struct AnalyzerRegistry {
    analyzers: Vec<Box<dyn Analyzer>>,
}

impl AnalyzerRegistry {
    pub fn with_defaults() -> Self {
        let mut registry = Self::default();
        registry.register(shape::ShapeStructureAnalyzer);
        registry.register(data::DataGraphProfileAnalyzer);
        registry
    }

    pub fn register<A>(&mut self, analyzer: A)
    where
        A: Analyzer + 'static,
    {
        self.analyzers.push(Box::new(analyzer));
    }

    pub fn run_phase(
        &self,
        phase: AnalysisPhase,
        limit: usize,
        ctx: &AnalysisContext<'_>,
        state: &mut AnalysisState,
    ) -> Result<(), String> {
        let mut run_count = 0usize;
        for analyzer in &self.analyzers {
            if analyzer.phase() != phase {
                continue;
            }
            if run_count >= limit {
                break;
            }
            analyzer.run(ctx, state)?;
            run_count += 1;
        }
        Ok(())
    }
}
