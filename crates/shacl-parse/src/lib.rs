//! RDF shapes graph -> formalism IR lowering (Layer 2).
//!
//! Discovers shapes in an RDF graph and lowers all SHACL Core + SHACL-AF
//! vocabulary into the small core IR from `shacl-algebra`, applying every sugar
//! rule (`class -> path`, `xone -> ∧∨¬`, `minCount/maxCount -> Count`, etc.; see
//! `docs/01-gap-analysis.md`). SPARQL/JS constructs become opaque leaves with
//! explicit diagnostics — never silent wrong answers.
//!
//! Status: Layer 0 scaffold.

#[cfg(test)]
mod tests {
    #[test]
    fn scaffold_builds() {}
}
