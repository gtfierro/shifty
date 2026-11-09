//! Helpers for locally adapted report fixtures. The upstream manifest runner
//! in python/conformance deliberately uses stricter report isomorphism.

use oxrdf::Term;
use shifty_algebra::render::path_to_string;
use shifty_engine::ValidationResult;
use shifty_parse::vocab;

pub type Key = (String, Option<String>, Option<String>, String, String);

/// Render complex paths structurally; their expected and actual roots can
/// have different blank-node labels in the same fixture.
fn path_term_key(loaded: &shifty_parse::Loaded, t: &Term) -> String {
    if matches!(t, Term::BlankNode(_))
        && let Ok(p) = shifty_parse::path::parse_path(loaded, t)
    {
        return path_to_string(&p);
    }
    t.to_string()
}

/// Adapted fixtures use anonymous source shapes as placeholders.
fn shape_term_key(t: &Term) -> String {
    match t {
        Term::BlankNode(_) => "_:BLANK".to_string(),
        other => other.to_string(),
    }
}

pub fn result_key(loaded: &shifty_parse::Loaded, r: &ValidationResult) -> Key {
    (
        r.focus.to_string(),
        r.path.as_ref().map(|p| path_term_key(loaded, p)),
        r.value.as_ref().map(|v| v.to_string()),
        format!("<{}>", r.component.as_str()),
        shape_term_key(&r.source_shape),
    )
}

pub fn expected_report(loaded: &shifty_parse::Loaded) -> Option<(bool, Vec<Key>)> {
    use oxrdf::NamedOrBlankNode;
    let report: NamedOrBlankNode = loaded
        .graph
        .subjects_for_predicate_object(vocab::RDF_TYPE, vocab::SH_VALIDATION_REPORT)
        .next()?
        .into_owned();

    let conforms = matches!(
        loaded.object(&report, vocab::SH_CONFORMS),
        Some(Term::Literal(ref l)) if l.value() == "true"
    );

    let mut keys = Vec::new();
    for res in loaded.objects(&report, vocab::SH_RESULT) {
        let Some(rn) = shifty_parse::graph::term_to_node(&res) else {
            continue;
        };
        let focus = loaded.object(&rn, vocab::SH_FOCUS_NODE)?;
        let component = loaded.object(&rn, vocab::SH_SOURCE_CONSTRAINT_COMPONENT)?;
        let source = loaded.object(&rn, vocab::SH_SOURCE_SHAPE)?;
        keys.push((
            focus.to_string(),
            loaded
                .object(&rn, vocab::SH_RESULT_PATH)
                .as_ref()
                .map(|p| path_term_key(loaded, p)),
            loaded
                .object(&rn, vocab::SH_VALUE)
                .as_ref()
                .map(|v| v.to_string()),
            component.to_string(),
            shape_term_key(&source),
        ));
    }
    Some((conforms, keys))
}
