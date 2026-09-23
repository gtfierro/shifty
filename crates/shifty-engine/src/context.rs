//! One graph-role table shared by session operations.

use crate::frozen::{FrozenIndexedDataset, SourceStorage};
use crate::validate::ValidationGraphMode;
use oxrdf::Graph;

pub(crate) fn frozen(
    data: &Graph,
    source: std::sync::Arc<SourceStorage>,
    separate: bool,
    mode: ValidationGraphMode,
) -> FrozenIndexedDataset {
    FrozenIndexedDataset::from_data_with_source(
        data,
        source,
        separate,
        separate && mode != ValidationGraphMode::Data,
    )
}
