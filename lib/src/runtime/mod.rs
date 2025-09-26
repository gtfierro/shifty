//! Runtime layer bridging structural descriptors to executable validators.

pub(crate) mod component;
pub(crate) mod engine;
pub(crate) mod shapes;
pub(crate) mod validators;

pub(crate) use component::*;
pub(crate) use engine::*;
pub(crate) use shapes::*;
pub(crate) use validators::*;
