use crate::compiled_runtime::analysis::AnalysisState;
use crate::compiled_runtime::program::{CompiledProgram, ShapeKind};

pub type ShapeId = u64;

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub active_node_shapes: Vec<ShapeId>,
    pub active_property_shapes: Vec<ShapeId>,
    pub component_strategies: Vec<ComponentStrategy>,
    pub partitions: Vec<PlanPartition>,
}

#[derive(Debug, Clone)]
pub struct ComponentStrategy {
    pub component_id: u64,
    pub strategy: StrategyKind,
}

#[derive(Debug, Clone, Copy)]
pub enum StrategyKind {
    Default,
}

#[derive(Debug, Clone)]
pub struct PlanPartition {
    pub shape_ids: Vec<ShapeId>,
}

pub trait PlanRewriter: Send + Sync {
    fn id(&self) -> &'static str;
    fn rewrite(&self, plan: &mut ExecutionPlan, state: &AnalysisState) -> Result<(), String>;
}

#[derive(Default)]
pub struct PlanRewriterRegistry {
    rewriters: Vec<Box<dyn PlanRewriter>>,
}

impl PlanRewriterRegistry {
    pub fn register<R>(&mut self, rewriter: R)
    where
        R: PlanRewriter + 'static,
    {
        self.rewriters.push(Box::new(rewriter));
    }

    pub fn rewrite(
        &self,
        plan: &mut ExecutionPlan,
        state: &AnalysisState,
    ) -> Result<(), String> {
        for rewriter in &self.rewriters {
            rewriter.rewrite(plan, state)?;
        }
        Ok(())
    }
}

pub fn build_default_plan(program: &CompiledProgram) -> ExecutionPlan {
    let mut active_node_shapes = Vec::new();
    let mut active_property_shapes = Vec::new();

    for shape in &program.shapes {
        if shape.deactivated {
            continue;
        }
        match shape.kind {
            ShapeKind::Node => active_node_shapes.push(shape.id),
            ShapeKind::Property => active_property_shapes.push(shape.id),
        }
    }

    let component_strategies = program
        .components
        .iter()
        .map(|component| ComponentStrategy {
            component_id: component.id,
            strategy: StrategyKind::Default,
        })
        .collect();

    let mut partition_shapes = active_node_shapes.clone();
    partition_shapes.extend(active_property_shapes.iter().copied());

    let partitions = if partition_shapes.is_empty() {
        Vec::new()
    } else {
        vec![PlanPartition {
            shape_ids: partition_shapes,
        }]
    };

    ExecutionPlan {
        active_node_shapes,
        active_property_shapes,
        component_strategies,
        partitions,
    }
}
