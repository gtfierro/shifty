//! Core SHACL formalism IR (Layer 1).
//!
//! This crate is the formal heart of the engine: the path algebra `π`, the
//! shape grammar `φ`, value types `T`, selectors, schemas, and the reference
//! denotational semantics. It is the SHACL fragment of arXiv:2502.01295
//! specialized to RDF (see `docs/00-formalism.md`), plus the extensions
//! catalogued in `docs/01-gap-analysis.md`.
//!
//! Nothing here depends on a graph store or a parser; downstream crates
//! (`shacl-parse`, `shacl-opt`, `shacl-engine`) build on these types.
//!
//! Status: Layer 0 scaffold — IR types land in Layer 1.

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold_builds() {}
}
