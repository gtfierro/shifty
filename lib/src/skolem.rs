use oxigraph::model::NamedNode;

pub(crate) const SKOLEM_MARKER: &str = "/.sk/";

pub(crate) fn skolem_base(iri: &NamedNode) -> String {
    format!("{}{}", iri.as_str().trim_end_matches('/'), SKOLEM_MARKER)
}
