//! One graph-role table shared by session operations.

use crate::frozen::FrozenIndexedDataset;
use crate::validate::{ValidationGraphMode, graph_union};
use oxrdf::Graph;
use std::sync::Arc;

pub(crate) fn focus_graph(
    data: &Arc<Graph>,
    shapes: &Graph,
    separate: bool,
    mode: ValidationGraphMode,
) -> Arc<Graph> {
    if separate && mode == ValidationGraphMode::UnionAll {
        Arc::new(graph_union(data, shapes))
    } else {
        Arc::clone(data)
    }
}

pub(crate) fn frozen(
    data: &Graph,
    shapes: &Graph,
    separate: bool,
    mode: ValidationGraphMode,
) -> FrozenIndexedDataset {
    if !separate || mode == ValidationGraphMode::Data {
        FrozenIndexedDataset::from_graphs(data, shapes)
    } else {
        FrozenIndexedDataset::from_graph_union_with_shapes(data, shapes)
    }
}
