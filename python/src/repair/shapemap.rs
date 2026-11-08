//! Shapes-side metadata for the Python shape-map projection.

use oxrdf::dataset::CanonicalizationAlgorithm;
use oxrdf::{Dataset, GraphNameRef, Term};
use shifty_algebra::Schema;
use shifty_parse::{Loaded, vocab};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

type LayoutEntries = HashMap<u32, (Option<String>, String)>;

/// Shared by a session and its runs. Computing anonymous source identities is
/// deferred until a shape map actually needs them, never ordinary validation.
pub(super) struct BindingLayout {
    schema: Arc<Schema>,
    shapes: Arc<Loaded>,
    entries: OnceLock<LayoutEntries>,
}

impl BindingLayout {
    pub(super) fn new(schema: Arc<Schema>, shapes: Arc<Loaded>) -> Self {
        Self {
            schema,
            shapes,
            entries: OnceLock::new(),
        }
    }

    pub(super) fn entries(&self) -> &LayoutEntries {
        self.entries.get_or_init(|| {
            // Canonicalization supplies identities independent of parser-created
            // blank-node labels and graph interning order. OxRDF's algorithm is
            // deterministic within a library version; these are display-order
            // identities, not persistent public identifiers.
            let dataset: Dataset = self
                .shapes
                .graph
                .iter()
                .map(|triple| triple.in_graph(GraphNameRef::DefaultGraph))
                .collect();
            let blanks = dataset.canonicalize_blank_nodes(CanonicalizationAlgorithm::Unstable);
            self.schema
                .sources
                .iter()
                .map(|(id, source)| {
                    let identity = match source {
                        Term::BlankNode(node) => blanks[&node.as_ref()].to_string(),
                        _ => source.to_string(),
                    };
                    let order = shifty_parse::graph::term_to_node(source)
                        .and_then(|node| self.shapes.object(&node, vocab::SH_ORDER))
                        .and_then(|value| match value {
                            Term::Literal(value) => Some(value.value().to_owned()),
                            _ => None,
                        });
                    (id.0, (order, identity))
                })
                .collect()
        })
    }
}
