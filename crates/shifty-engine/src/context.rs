//! One graph-role table shared by session operations.

use crate::frozen::FrozenIndexedDataset;
use crate::validate::{ValidationGraphMode, graph_union};
use oxrdf::Graph;

pub(crate) fn focus_graph(
    data: &Graph,
    shapes: &Graph,
    separate: bool,
    mode: ValidationGraphMode,
) -> Graph {
    if separate && mode == ValidationGraphMode::UnionAll {
        graph_union(data, shapes)
    } else {
        data.clone()
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
