//! Human-readable rendering of the IR for debugging (`shacl inspect`).
//!
//! Shapes form a graph, so we render a **flat arena dump**: one line per arena
//! slot, `@i = <φ>`, with child shapes referenced as `@j`. This is unambiguous,
//! cycle-proof, and shows sharing — exactly what you want when inspecting how a
//! lowering produced the IR. Paths and value types (which are trees) render
//! inline in the formalism's notation.

use crate::path::Path;
use crate::schema::Schema;
use crate::selector::Selector;
use crate::shape::{Shape, ShapeArena, ShapeId};
use crate::term::{NodeKindSet, Term};
use crate::value_type::{Bound, ValueType};
use std::collections::BTreeSet;

/// Render a whole schema as a flat, cycle-safe text dump. Only shapes reachable
/// from the statements/rules are shown (intermediate arena slots are elided);
/// the header reports `reachable/total`.
pub fn schema_to_text(schema: &Schema) -> String {
    let reachable = reachable_shapes(schema);
    let mut out = String::new();
    out.push_str(&format!(
        "schema: {} statement(s), {} rule(s), {}/{} shape(s)\n",
        schema.statements.len(),
        schema.rules.len(),
        reachable.len(),
        schema.arena.len()
    ));

    out.push_str("shapes:\n");
    for id in &reachable {
        out.push_str(&format!("  @{} = {}\n", id.0, shape_def(&schema.arena, *id)));
    }

    if !schema.statements.is_empty() {
        out.push_str("statements:\n");
        for st in &schema.statements {
            out.push_str(&format!(
                "  {}  ⇒  {}\n",
                selector_to_string(&st.selector),
                child(&schema.arena, st.shape)
            ));
        }
    }

    if !schema.rules.is_empty() {
        out.push_str("rules:\n");
        for r in &schema.rules {
            let conds: Vec<String> = r
                .conditions
                .iter()
                .map(|c| child(&schema.arena, *c))
                .collect();
            out.push_str(&format!(
                "  on {} [if {}] order={} {}\n",
                selector_to_string(&r.selector),
                if conds.is_empty() { "·".into() } else { conds.join(", ") },
                r.order.unwrap_or(0),
                if r.deactivated { "(deactivated)" } else { "" },
            ));
        }
    }

    out
}

/// A reference to a child shape: `⊤` is inlined (it carries no information),
/// everything else prints as its slot label.
fn child(arena: &ShapeArena, id: ShapeId) -> String {
    match arena.get(id) {
        Shape::Top => "⊤".to_string(),
        _ => format!("@{}", id.0),
    }
}

/// Shapes reachable from the schema's statements and rules, following shape
/// references through selectors and shape children.
fn reachable_shapes(schema: &Schema) -> BTreeSet<ShapeId> {
    let mut stack: Vec<ShapeId> = Vec::new();
    for st in &schema.statements {
        stack.push(st.shape);
        stack.extend(selector_shapes(&st.selector));
    }
    for r in &schema.rules {
        stack.extend(r.conditions.iter().copied());
        stack.extend(selector_shapes(&r.selector));
    }
    let mut seen = BTreeSet::new();
    while let Some(id) = stack.pop() {
        if seen.insert(id) {
            stack.extend(shape_children(schema.arena.get(id)));
        }
    }
    seen
}

fn shape_children(shape: &Shape) -> Vec<ShapeId> {
    match shape {
        Shape::Not(c) => vec![*c],
        Shape::And(cs) | Shape::Or(cs) => cs.clone(),
        Shape::Count { qualifier, .. } => vec![*qualifier],
        _ => Vec::new(),
    }
}

fn selector_shapes(sel: &Selector) -> Vec<ShapeId> {
    match sel {
        Selector::HasPath(_, id) => vec![*id],
        _ => Vec::new(),
    }
}

fn shape_def(arena: &ShapeArena, id: ShapeId) -> String {
    match arena.get(id) {
        Shape::Top => "⊤".to_string(),
        Shape::Pending => "⟨pending⟩".to_string(),
        Shape::TestConst(t) => format!("test({})", term_to_string(t)),
        Shape::TestType(vt) => format!("test({})", value_type_to_string(vt)),
        Shape::TestKind(k) => format!("nodeKind({})", node_kinds_to_string(k)),
        Shape::Closed(q) => {
            let preds: Vec<String> = q.iter().map(|n| compact(n.as_str())).collect();
            format!("closed{{{}}}", preds.join(", "))
        }
        Shape::Eq(p, pred) => format!("eq({}, {})", path_to_string(p), compact(pred.as_str())),
        Shape::Disj(p, pred) => format!("disj({}, {})", path_to_string(p), compact(pred.as_str())),
        Shape::Lt(p, pred) => format!("lt({}, {})", path_to_string(p), compact(pred.as_str())),
        Shape::Le(p, pred) => format!("le({}, {})", path_to_string(p), compact(pred.as_str())),
        Shape::UniqueLang(p) => format!("uniqueLang({})", path_to_string(p)),
        Shape::Not(c) => format!("¬{}", child(arena, *c)),
        Shape::And(cs) => join_children(arena, cs, " ∧ "),
        Shape::Or(cs) => join_children(arena, cs, " ∨ "),
        Shape::Count { path, min, max, qualifier } => {
            let lo = min.map(|n| n.to_string()).unwrap_or_default();
            let hi = max.map(|n| n.to_string()).unwrap_or_default();
            format!(
                "∃[{lo}..{hi}] {} . {}",
                path_to_string(path),
                child(arena, *qualifier)
            )
        }
        Shape::Sparql(c) => format!("sparql({:?}){{…}}", c.kind),
    }
}

fn join_children(arena: &ShapeArena, cs: &[ShapeId], sep: &str) -> String {
    if cs.is_empty() {
        return "()".to_string();
    }
    cs.iter()
        .map(|c| child(arena, *c))
        .collect::<Vec<_>>()
        .join(sep)
}

fn selector_to_string(sel: &Selector) -> String {
    match sel {
        Selector::HasOut(q) => format!("∃ {} .⊤", compact(q.as_str())),
        Selector::HasIn(q) => format!("∃ {}⁻ .⊤", compact(q.as_str())),
        Selector::IsConst(t) => format!("node({})", term_to_string(t)),
        Selector::HasPath(p, _) => format!("∃≥1 {} . φ", path_to_string(p)),
        Selector::Sparql(_) => "sparql{…}".to_string(),
    }
}

// ---- paths (precedence: atom > * > ^ > / > |) ----

pub fn path_to_string(p: &Path) -> String {
    render_alt(p)
}

fn render_alt(p: &Path) -> String {
    match p {
        Path::Alt(parts) => parts.iter().map(render_seq).collect::<Vec<_>>().join(" | "),
        _ => render_seq(p),
    }
}

fn render_seq(p: &Path) -> String {
    match p {
        Path::Seq(parts) => parts.iter().map(render_unary).collect::<Vec<_>>().join("/"),
        _ => render_unary(p),
    }
}

fn render_unary(p: &Path) -> String {
    match p {
        Path::Inverse(inner) => format!("^{}", render_postfix(inner)),
        _ => render_postfix(p),
    }
}

fn render_postfix(p: &Path) -> String {
    match p {
        Path::Star(inner) => format!("{}*", render_atom(inner)),
        _ => render_atom(p),
    }
}

fn render_atom(p: &Path) -> String {
    match p {
        Path::Id => "id".to_string(),
        Path::Pred(nn) => compact(nn.as_str()),
        // compound paths in atom position need grouping
        _ => format!("({})", render_alt(p)),
    }
}

// ---- value types ----

fn value_type_to_string(vt: &ValueType) -> String {
    match vt {
        ValueType::Any => "any".to_string(),
        ValueType::Datatype(nn) => format!("datatype({})", compact(nn.as_str())),
        ValueType::NumericRange { lo, hi } => {
            let mut parts = Vec::new();
            if let Some(Bound { value, inclusive }) = lo {
                parts.push(format!("{}{}", if *inclusive { "≥" } else { ">" }, value));
            }
            if let Some(Bound { value, inclusive }) = hi {
                parts.push(format!("{}{}", if *inclusive { "≤" } else { "<" }, value));
            }
            format!("range({})", parts.join(", "))
        }
        ValueType::Length { min, max } => {
            let lo = min.map(|n| n.to_string()).unwrap_or_default();
            let hi = max.map(|n| n.to_string()).unwrap_or_default();
            format!("length[{lo}..{hi}]")
        }
        ValueType::Pattern { regex, flags } => format!("pattern(/{regex}/{flags})"),
        ValueType::LangIn(langs) => format!("langIn({})", langs.join(", ")),
        ValueType::And(parts) => parts
            .iter()
            .map(value_type_to_string)
            .collect::<Vec<_>>()
            .join(" & "),
    }
}

fn node_kinds_to_string(k: &NodeKindSet) -> String {
    let mut parts = Vec::new();
    if k.iri {
        parts.push("IRI");
    }
    if k.blank {
        parts.push("BlankNode");
    }
    if k.literal {
        parts.push("Literal");
    }
    parts.join("|")
}

fn term_to_string(t: &Term) -> String {
    match t {
        Term::NamedNode(nn) => compact(nn.as_str()),
        other => other.to_string(),
    }
}

// ---- IRI compaction against well-known namespaces ----

const WELL_KNOWN: &[(&str, &str)] = &[
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("sh", "http://www.w3.org/ns/shacl#"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
];

/// Compact an IRI using well-known prefixes, else `<iri>`.
fn compact(iri: &str) -> String {
    for (prefix, ns) in WELL_KNOWN {
        if let Some(local) = iri.strip_prefix(ns) {
            return format!("{prefix}:{local}");
        }
    }
    format!("<{iri}>")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Statement;
    use crate::term::NamedNode;

    fn nn(s: &str) -> NamedNode {
        NamedNode::new(s).unwrap()
    }

    #[test]
    fn path_precedence_and_compaction() {
        // (ex:a/^ex:b)|ex:c*
        let p = Path::alt(vec![
            Path::seq(vec![
                Path::Pred(nn("http://ex/a")),
                Path::Inverse(Box::new(Path::Pred(nn("http://ex/b")))),
            ]),
            Path::star(Path::Pred(nn("http://www.w3.org/ns/shacl#c"))),
        ]);
        assert_eq!(path_to_string(&p), "<http://ex/a>/^<http://ex/b> | sh:c*");
    }

    #[test]
    fn schema_dump_renders_cycle() {
        // S := nodeKind(IRI) ∧ ∃[1..] ex:knows . S
        let mut schema = Schema::new();
        let knows = nn("http://ex/knows");
        let s = schema.arena.reserve();
        let kind = schema.arena.insert(Shape::TestKind(NodeKindSet::IRI));
        let reaches = schema.arena.insert(Shape::Count {
            path: Path::Pred(knows.clone()),
            min: Some(1),
            max: None,
            qualifier: s,
        });
        schema.arena.set(s, Shape::And(vec![kind, reaches]));
        schema.statements.push(Statement {
            selector: Selector::HasOut(knows),
            shape: s,
        });

        let text = schema_to_text(&schema);
        assert!(text.contains("@0 = @1 ∧ @2"));
        assert!(text.contains("@1 = nodeKind(IRI)"));
        assert!(text.contains("@2 = ∃[1..] <http://ex/knows> . @0"));
        assert!(text.contains("∃ <http://ex/knows> .⊤  ⇒  @0"));
    }
}
