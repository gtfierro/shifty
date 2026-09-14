//! Human-readable rendering of the IR for debugging (`shacl inspect`).
//!
//! Shapes form a graph, so we render a **flat arena dump**: one line per arena
//! slot, `@i = <φ>`, with child shapes referenced as `@j`. This is unambiguous,
//! cycle-proof, and shows sharing — exactly what you want when inspecting how a
//! lowering produced the IR. Paths and value types (which are trees) render
//! inline in the formalism's notation.

use crate::path::Path;
use crate::prefix::Prefixes;
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
    let px = &schema.prefixes;
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
        // Every name, not just one: a shape carrying two means CSE collapsed
        // them, which a dump of the IR should show rather than hide.
        let name_suffix = schema
            .names
            .get(id)
            .map(|iris| format!("  # {}", compact_all(iris, px)))
            .unwrap_or_default();
        out.push_str(&format!(
            "  @{} = {}{}\n",
            id.0,
            shape_def(&schema.arena, *id, px),
            name_suffix
        ));
    }

    if !schema.statements.is_empty() {
        out.push_str("statements:\n");
        for st in &schema.statements {
            out.push_str(&format!(
                "  {}  ⇒  {}\n",
                selector_to_string_px(&st.selector, px),
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
                "  on {} [if {}] order={} {} ⟹ {}\n",
                selector_to_string_px(&r.selector, px),
                if conds.is_empty() {
                    "·".into()
                } else {
                    conds.join(", ")
                },
                r.order.unwrap_or(0),
                if r.deactivated { "(deactivated)" } else { "" },
                rule_head_to_string(&r.head, px),
            ));
        }
    }

    out
}

fn rule_head_to_string(head: &crate::rule::RuleHead, px: &Prefixes) -> String {
    use crate::rule::RuleHead;
    match head {
        RuleHead::Triple {
            subject,
            predicate,
            object,
        } => format!(
            "+({}, {}, {})",
            node_expr_to_string(subject, px),
            node_expr_to_string(predicate, px),
            node_expr_to_string(object, px),
        ),
        RuleHead::Sparql(_) => "construct{…}".to_string(),
    }
}

fn node_expr_to_string(e: &crate::expr::NodeExpr, px: &Prefixes) -> String {
    use crate::expr::NodeExpr;
    match e {
        NodeExpr::This => "this".to_string(),
        NodeExpr::Constant(t) => term_to_string(t, px),
        NodeExpr::Path(p) => path_to_string_in(p, px),
        NodeExpr::Filter { input, shape } => {
            format!("filter({}, @{})", node_expr_to_string(input, px), shape.0)
        }
        NodeExpr::Intersection(es) => es
            .iter()
            .map(|e| node_expr_to_string(e, px))
            .collect::<Vec<_>>()
            .join(" ∩ "),
        NodeExpr::Union(es) => es
            .iter()
            .map(|e| node_expr_to_string(e, px))
            .collect::<Vec<_>>()
            .join(" ∪ "),
        NodeExpr::Function { iri, args } => format!(
            "{}({})",
            px.compact(iri.as_str()),
            args.iter()
                .map(|e| node_expr_to_string(e, px))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
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
            stack.extend(schema.arena.get(id).child_shapes());
        }
    }
    seen
}

fn selector_shapes(sel: &Selector) -> Vec<ShapeId> {
    match sel {
        Selector::HasPath(_, id) => vec![*id],
        _ => Vec::new(),
    }
}

fn shape_def(arena: &ShapeArena, id: ShapeId, px: &Prefixes) -> String {
    match arena.get(id) {
        Shape::Annotated {
            severity, shape, ..
        } => {
            format!("severity({}, {})", severity, child(arena, *shape))
        }
        Shape::Top => "⊤".to_string(),
        Shape::Pending => "⟨pending⟩".to_string(),
        Shape::TestConst(t) => format!("test({})", term_to_string(t, px)),
        Shape::TestType(vt) => format!("test({})", value_type_to_string_in(vt, px)),
        Shape::TestKind(k) => format!("nodeKind({})", node_kinds_to_string(k)),
        Shape::Closed(q) => {
            let preds: Vec<String> = q.iter().map(|n| px.compact(n.as_str())).collect();
            format!("closed{{{}}}", preds.join(", "))
        }
        Shape::Eq(p, pred) => format!(
            "eq({}, {})",
            path_to_string_in(p, px),
            px.compact(pred.as_str())
        ),
        Shape::Disj(p, pred) => format!(
            "disj({}, {})",
            path_to_string_in(p, px),
            px.compact(pred.as_str())
        ),
        Shape::Lt(p, pred) => format!(
            "lt({}, {})",
            path_to_string_in(p, px),
            px.compact(pred.as_str())
        ),
        Shape::Le(p, pred) => format!(
            "le({}, {})",
            path_to_string_in(p, px),
            px.compact(pred.as_str())
        ),
        Shape::UniqueLang(p) => format!("uniqueLang({})", path_to_string_in(p, px)),
        Shape::Not(c) => format!("¬{}", child(arena, *c)),
        Shape::And(cs) => join_children(arena, cs, " ∧ "),
        Shape::Or(cs) => join_children(arena, cs, " ∨ "),
        Shape::Count {
            path,
            min,
            max,
            qualifier,
        } => {
            let lo = min.map(|n| n.to_string()).unwrap_or_default();
            let hi = max.map(|n| n.to_string()).unwrap_or_default();
            format!(
                "∃[{lo}..{hi}] {} . {}",
                path_to_string_in(path, px),
                child(arena, *qualifier)
            )
        }
        Shape::Sparql(c) => format!("sparql({:?}){{…}}", c.kind),
        Shape::Expression(e) => format!("expr({}) = true", node_expr_to_string(e, px)),
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

/// Render a single shape (its top-level form; children as `@id`), with only the
/// well-known prefixes compacted.
pub fn shape_to_string(arena: &ShapeArena, id: ShapeId) -> String {
    shape_to_string_in(arena, id, &Prefixes::default())
}

/// Render a single shape (its top-level form; children as `@id`), compacting
/// IRIs against `px`. Useful for constraint messages in validation reports.
pub fn shape_to_string_in(arena: &ShapeArena, id: ShapeId, px: &Prefixes) -> String {
    shape_def(arena, id, px)
}

/// Cap on the terminal text one description may emit. Shapes form a DAG, so
/// inlining every child can in principle blow up on a heavily shared schema;
/// past the cap the remainder is elided with `…`. This is a size guard only —
/// it never falls back to an arena slot label, which tells the reader of a
/// validation report nothing.
const DESCRIBE_BUDGET: usize = 4096;

/// Stand-in for a shape that encloses itself. Recursive shapes have no finite
/// inlining, so the one structural place a description must stop names the
/// recursion rather than an arena slot. The negation side needs its own wording:
/// the guard fires on the shape's identity, which says nothing about the polarity
/// the reader is standing in.
const RECURSIVE: &str = "this same shape (recursive)";
const NOT_RECURSIVE: &str = "not this same shape (recursive)";

/// A fully-expanded, human-readable description of a shape, for validation
/// messages and repair-hole display: every child shape is inlined recursively,
/// the `sh:class` encoding is named `instance of C`, a `∃[..0]` count is stated
/// as the universal it is, and leaves render in the formalism's notation. The
/// result never contains an `@id` slot reference — an arena index is an internal
/// identifier and is meaningless outside a debugging dump. Expansion stops only
/// where it must: a shape that encloses itself renders as [`RECURSIVE`], and
/// text past [`DESCRIBE_BUDGET`] is elided with `…`. Prefer this over
/// [`shape_to_string`] when the reader wants the *whole* constraint, not a
/// one-level form with pointers.
pub fn describe_shape(arena: &ShapeArena, id: ShapeId) -> String {
    describe_shape_in(arena, id, &Prefixes::default())
}

/// [`describe_shape`], compacting IRIs against `px`.
pub fn describe_shape_in(arena: &ShapeArena, id: ShapeId, px: &Prefixes) -> String {
    Describer::new(arena, px).describe(id).flat()
}

/// [`describe_shape_in`], laid out over several lines and indented by nesting
/// depth, breaking only where a subtree does not fit in `width`.
///
/// A description that already fits comes back byte-identical to the one-line
/// form, so this is safe to use unconditionally. Prefer the one-line form for
/// anything a consumer embeds mid-line or serializes as an RDF literal; this is
/// for display to a person.
pub fn describe_shape_pretty(
    arena: &ShapeArena,
    id: ShapeId,
    px: &Prefixes,
    width: usize,
) -> String {
    Describer::new(arena, px).describe(id).pretty(width, 0)
}

/// Join the descriptions of several shapes a value must *all* satisfy with “and”
/// — the rendering of a conjunction held as separate shapes (e.g. a `ConformsToAll`
/// hole). Each member is itself fully expanded via [`describe_shape`].
pub fn describe_shapes(arena: &ShapeArena, ids: &[ShapeId]) -> String {
    describe_shapes_in(arena, ids, &Prefixes::default())
}

/// [`describe_shapes`], compacting IRIs against `px`.
pub fn describe_shapes_in(arena: &ShapeArena, ids: &[ShapeId], px: &Prefixes) -> String {
    if ids.is_empty() {
        return "any node".to_string();
    }
    ids.iter()
        .map(|id| Describer::new(arena, px).describe(*id).flat_nested())
        .collect::<Vec<_>>()
        .join(" and ")
}

/// Describe the *positive* requirement `¬ψ` for an NNF shape `ψ` — the
/// reporting-side inverse of the optimizer's NNF negation ([`normalize`]'s
/// `mk_not`). A universal `∀π.φ` is stored as `∃≤0 π.¬φ`; when it fails, each
/// offending value satisfies `ψ = ¬φ`, so what it *should* satisfy is `φ = ¬ψ`.
/// This renders that `φ` in [`describe_shape`]'s vocabulary rather than echoing
/// the machine's double-negated `ψ` (e.g. `∃≤0 rdf:type/… . test(C)` ⇒
/// `instance of C`, a complemented `nodeKind` ⇒ the original kinds), under the
/// same no-slot-references guarantee.
///
/// [`normalize`]: fn@crate::normalize
pub fn describe_negation(arena: &ShapeArena, id: ShapeId) -> String {
    describe_negation_in(arena, id, &Prefixes::default())
}

/// [`describe_negation`], compacting IRIs against `px`.
pub fn describe_negation_in(arena: &ShapeArena, id: ShapeId, px: &Prefixes) -> String {
    Describer::new(arena, px).negate(id).flat()
}

/// [`describe_negation_in`], laid out over several lines — see
/// [`describe_shape_pretty`].
pub fn describe_negation_pretty(
    arena: &ShapeArena,
    id: ShapeId,
    px: &Prefixes,
    width: usize,
) -> String {
    Describer::new(arena, px).negate(id).pretty(width, 0)
}

/// Columns one nesting level adds in the pretty layout.
const PRETTY_INDENT: usize = 2;

/// A sensible line width for the pretty layout when a caller has no better
/// number — wide enough that only genuinely nested constraints break.
pub const PRETTY_WIDTH: usize = 100;

/// The rendered *structure* of a description, before it is committed to a
/// layout. One traversal of the shape builds it and [`Doc::flat`] /
/// [`Doc::pretty`] are two ways of printing it, so the semantic rules that
/// shape a description — naming the `sh:class` encoding, stating `∃[..0]` as a
/// universal, De Morgan on the negation side, the cycle and size guards — are
/// written once and cannot drift between the one-line and indented forms.
enum Doc {
    /// A leaf. Printed verbatim; there is nothing inside it to break at.
    Atom(String),
    /// `head` immediately followed by `body` — `∀ π . `, `∃[m..n] π . `. The
    /// body is parenthesized only when it is itself a join.
    Prefixed { head: String, body: Box<Doc> },
    /// `head` followed by an always-parenthesized body — `not (`…`)`.
    Bracketed { head: &'static str, body: Box<Doc> },
    /// Two or more children joined by `sep`. A child that is itself a join is
    /// parenthesized, so the result parses without knowing any precedence.
    Join { sep: &'static str, parts: Vec<Doc> },
}

impl Doc {
    /// Does this need parentheses to sit unambiguously inside a larger form?
    fn is_join(&self) -> bool {
        matches!(self, Doc::Join { .. })
    }

    /// The whole description on one line. This is the canonical form: it is what
    /// goes into a report message, which downstream consumers embed mid-line and
    /// serialize as an RDF literal.
    fn flat(&self) -> String {
        match self {
            Doc::Atom(text) => text.clone(),
            Doc::Prefixed { head, body } => format!("{head}{}", body.flat_nested()),
            Doc::Bracketed { head, body } => format!("{head}({})", body.flat()),
            Doc::Join { sep, parts } => parts
                .iter()
                .map(Doc::flat_nested)
                .collect::<Vec<_>>()
                .join(sep),
        }
    }

    fn flat_nested(&self) -> String {
        if self.is_join() {
            format!("({})", self.flat())
        } else {
            self.flat()
        }
    }

    /// The description laid out over several lines, indented by nesting depth,
    /// breaking only where a subtree does not fit in `width`. Short descriptions
    /// — the overwhelming majority — come back byte-identical to [`Doc::flat`],
    /// so this never costs legibility to buy it.
    ///
    /// `indent` is the column this term's continuation and closing lines sit at.
    fn pretty(&self, width: usize, indent: usize) -> String {
        let flat = self.flat();
        if indent + flat.chars().count() <= width {
            return flat;
        }
        self.pretty_broken(width, indent)
    }

    /// Lay this term out with its own top level broken, whether or not it would
    /// have fit. Sibling conjuncts of a broken join are laid out this way so they
    /// read alike, rather than one folding back onto a single line because it
    /// happened to land a few columns under the limit.
    fn pretty_broken(&self, width: usize, indent: usize) -> String {
        let pad = " ".repeat(indent);
        let inner_pad = " ".repeat(indent + PRETTY_INDENT);
        // A subtree that breaks gets its own indented block, closing back at this
        // term's column.
        let block = |body: &Doc, force: bool| {
            let inner = indent + PRETTY_INDENT;
            let laid_out = if force {
                body.pretty_broken(width, inner)
            } else {
                body.pretty(width, inner)
            };
            format!("(\n{inner_pad}{laid_out}\n{pad})")
        };
        match self {
            // Nothing to break at: an over-long leaf stays over-long rather than
            // being cut somewhere that would change what it says.
            Doc::Atom(text) => text.clone(),
            Doc::Prefixed { head, body } if body.is_join() => {
                format!("{head}{}", block(body, false))
            }
            Doc::Prefixed { head, body } => format!("{head}{}", body.pretty(width, indent)),
            Doc::Bracketed { head, body } => format!("{head}{}", block(body, false)),
            // One child per line, the separator leading each continuation so the
            // connective is the first thing read on the line it applies to.
            Doc::Join { sep, parts } => parts
                .iter()
                .enumerate()
                .map(|(i, part)| {
                    let text = if part.is_join() {
                        block(part, true)
                    } else {
                        part.pretty(width, indent)
                    };
                    if i == 0 {
                        text
                    } else {
                        format!("\n{pad}{}{text}", sep.trim_start())
                    }
                })
                .collect::<Vec<_>>()
                .join(""),
        }
    }
}

/// State shared by one description: the shapes currently being expanded (the
/// cycle guard) and how much terminal text has been emitted (the size guard).
/// Both exist so that expansion can be unbounded in *depth* — the reader of a
/// report needs the whole constraint — without risking non-termination or an
/// unbounded message.
struct Describer<'a> {
    arena: &'a ShapeArena,
    prefixes: &'a Prefixes,
    /// Shapes on the current expansion path; re-entering one is a cycle.
    open: Vec<ShapeId>,
    /// Characters of terminal text emitted so far. Joiners and brackets are not
    /// charged: they are proportional to the terminals they connect, so bounding
    /// the terminals bounds the whole rendering.
    spent: usize,
}

impl<'a> Describer<'a> {
    fn new(arena: &'a ShapeArena, prefixes: &'a Prefixes) -> Self {
        Self {
            arena,
            prefixes,
            open: Vec::new(),
            spent: 0,
        }
    }

    /// Account for terminal text against the size budget.
    fn emit(&mut self, s: String) -> String {
        self.spent += s.chars().count();
        s
    }

    /// The two reasons a description stops early, checked before every descent.
    /// `recursive` is the caller's wording for a cycle, which differs by polarity.
    fn stop(&self, id: ShapeId, recursive: &'static str) -> Option<&'static str> {
        if self.open.contains(&id) {
            Some(recursive)
        } else if self.spent >= DESCRIBE_BUDGET {
            Some("…")
        } else {
            None
        }
    }

    /// Expand `body` with `id` marked open, so a reference back to `id` from
    /// inside it is recognized as a cycle instead of recursing forever.
    fn within(&mut self, id: ShapeId, body: impl FnOnce(&mut Self) -> Doc) -> Doc {
        self.open.push(id);
        let out = body(self);
        self.open.pop();
        out
    }

    fn describe(&mut self, id: ShapeId) -> Doc {
        if let Some(stop) = self.stop(id, RECURSIVE) {
            return Doc::Atom(self.emit(stop.to_string()));
        }
        // ∃≥1 (rdf:type/rdfs:subClassOf*).test(C) — the encoding of sh:class C.
        if let Some(class) = class_target_shape(id, self.arena) {
            return Doc::Atom(self.emit(format!(
                "instance of {}",
                term_to_string(&class, self.prefixes)
            )));
        }
        // ∃≤0 (rdf:type/rdfs:subClassOf*).test(C) — its NNF negation. Naming it
        // here too keeps the count rule below from unfolding it into a universal
        // over a bare `test(C)`, which says the same thing far less directly.
        if let Some(class) = negated_class_target_shape(id, self.arena) {
            return Doc::Atom(self.emit(format!(
                "not an instance of {}",
                term_to_string(&class, self.prefixes)
            )));
        }
        // `sh:xone`, before the disjunction it was rewritten into.
        if let Some(alternatives) = xone_alternatives(id, self.arena) {
            return self.within(id, |me| Doc::Bracketed {
                head: "exactly one of ",
                body: Box::new(Doc::Join {
                    sep: ", ",
                    parts: alternatives.iter().map(|a| me.describe(*a)).collect(),
                }),
            });
        }
        let arena = self.arena;
        match arena.get(id) {
            Shape::Top | Shape::Pending => Doc::Atom(self.emit("any node".to_string())),
            // sh:severity is transparent — describe the wrapped shape.
            Shape::Annotated { shape, .. } => self.within(id, |me| me.describe(*shape)),
            // `not (…)` around something that itself renders as a negation leaves
            // the reader unwinding two of them to learn what to do: `not (∄ p)`
            // is `∃[1..] p`. Invert through `negate` instead — except for a
            // boolean combination, where De Morgan trades one clear `not (a and
            // b)` for a longer disjunction.
            Shape::Not(c) if !is_boolean_shape(arena, *c) => self.within(id, |me| me.negate(*c)),
            Shape::Not(c) => self.within(id, |me| Doc::Bracketed {
                head: "not ",
                body: Box::new(me.describe(*c)),
            }),
            Shape::And(cs) => self.within(id, |me| me.join(cs, " and ", Self::describe)),
            Shape::Or(cs) => self.within(id, |me| me.join(cs, " or ", Self::describe)),
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => self.within(id, |me| me.count(path, *min, *max, *qualifier)),
            // Everything else carries no child shape in the shape grammar.
            _ => self.leaf(id),
        }
    }

    fn negate(&mut self, id: ShapeId) -> Doc {
        if let Some(stop) = self.stop(id, NOT_RECURSIVE) {
            return Doc::Atom(self.emit(stop.to_string()));
        }
        // ψ = ∃≤0 (rdf:type/…).test(C)  ⇒  ¬ψ = "instance of C".
        if let Some(class) = negated_class_target_shape(id, self.arena) {
            return Doc::Atom(self.emit(format!(
                "instance of {}",
                term_to_string(&class, self.prefixes)
            )));
        }
        // ψ = ∃≥1 (rdf:type/…).test(C)  ⇒  ¬ψ = "not an instance of C" (e.g. the
        // qualifier of an `sh:qualifiedMaxCount 0` over `sh:class C`).
        if let Some(class) = class_target_shape(id, self.arena) {
            return Doc::Atom(self.emit(format!(
                "not an instance of {}",
                term_to_string(&class, self.prefixes)
            )));
        }
        let arena = self.arena;
        match arena.get(id) {
            // ¬⊤ = ⊥: unsatisfiable. Shouldn't reach reporting, but render honestly.
            Shape::Top | Shape::Pending => Doc::Atom(self.emit("no value".to_string())),
            Shape::Annotated { shape, .. } => self.within(id, |me| me.negate(*shape)),
            // ¬¬φ = φ
            Shape::Not(c) => self.within(id, |me| me.describe(*c)),
            // De Morgan: ¬(a ∧ b) = ¬a ∨ ¬b, ¬(a ∨ b) = ¬a ∧ ¬b.
            Shape::And(cs) => self.within(id, |me| me.join(cs, " or ", Self::negate)),
            Shape::Or(cs) => self.within(id, |me| me.join(cs, " and ", Self::negate)),
            // ¬(∃[min..max] π.q) = ∃[..min-1] π.q ∪ ∃[max+1..] π.q (qualifier stays).
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => self.within(id, |me| {
                let mut alts = Vec::new();
                if let Some(lo) = min
                    && *lo > 0
                {
                    alts.push(me.count(path, None, Some(lo - 1), *qualifier));
                }
                if let Some(hi) = max {
                    alts.push(me.count(path, Some(hi + 1), None, *qualifier));
                }
                match alts.len() {
                    0 => Doc::Atom("no value".to_string()), // ¬∃[0..] = ⊥
                    1 => alts.remove(0),
                    _ => Doc::Join {
                        sep: " or ",
                        parts: alts,
                    },
                }
            }),
            // ¬nodeKind(K) = nodeKind(K̄).
            Shape::TestKind(k) => {
                let comp = k.complement();
                let text = if comp.is_empty() {
                    "no value".to_string()
                } else {
                    format!("nodeKind({})", node_kinds_to_string(&comp))
                };
                Doc::Atom(self.emit(text))
            }
            // Any other leaf: its plain negation reads fine.
            _ => Doc::Bracketed {
                head: "not ",
                body: Box::new(self.leaf(id)),
            },
        }
    }

    /// Render a count `∃[lo..hi] π . q`.
    ///
    /// When `hi = 0` the count is a **universal** — `∃[..0] π . q` holds exactly
    /// when *every* value along `π` satisfies `¬q` — and it is rendered that way,
    /// with the qualifier inverted through [`Describer::negate`]. Echoing the
    /// machine's form instead hands the reader a double negative, because the
    /// qualifier of a lowered universal is itself almost always a negation:
    /// `∃[..0] hasMedium . ∃[..0] rdf:type/rdfs:subClassOf* . test(C)` says
    /// "every `hasMedium` is an instance of C", which no reader recovers from two
    /// stacked `∃[..0]`s.
    fn count(
        &mut self,
        path: &Path,
        min: Option<u64>,
        max: Option<u64>,
        qualifier: ShapeId,
    ) -> Doc {
        let path = path_to_string_in(path, self.prefixes);
        if max == Some(0) && matches!(min, None | Some(0)) {
            // ∀ π . ¬⊤ = ∀ π . ⊥: no values along π at all.
            if matches!(self.arena.get(qualifier), Shape::Top | Shape::Pending) {
                return Doc::Atom(self.emit(format!("∄ {path}")));
            }
            let head = self.emit(format!("∀ {path} . "));
            return Doc::Prefixed {
                head,
                body: Box::new(self.negate(qualifier)),
            };
        }
        let lo = min.map(|n| n.to_string()).unwrap_or_default();
        let hi = max.map(|n| n.to_string()).unwrap_or_default();
        // A `⊤` qualifier counts everything along the path — `. any node` adds a
        // clause that says nothing, the same way `∄ p` drops it.
        if matches!(self.arena.get(qualifier), Shape::Top | Shape::Pending) {
            return Doc::Atom(self.emit(format!("∃[{lo}..{hi}] {path}")));
        }
        let head = self.emit(format!("∃[{lo}..{hi}] {path} . "));
        Doc::Prefixed {
            head,
            body: Box::new(self.describe(qualifier)),
        }
    }

    /// A shape with no children in the shape grammar: its one-level formal
    /// rendering is already fully expanded — except `Shape::Expression`, whose
    /// node expression can carry a `sh:filterShape` reference that must be
    /// inlined too rather than printed as a slot.
    fn leaf(&mut self, id: ShapeId) -> Doc {
        let arena = self.arena;
        match arena.get(id) {
            Shape::Expression(e) => self.within(id, |me| {
                let rendered = me.node_expr(e);
                Doc::Atom(format!("expr({rendered}) = true"))
            }),
            _ => Doc::Atom(self.emit(shape_def(arena, id, self.prefixes))),
        }
    }

    /// [`node_expr_to_string`], but with `sh:filterShape` references expanded in
    /// place instead of rendered as `@id`.
    fn node_expr(&mut self, e: &crate::expr::NodeExpr) -> String {
        use crate::expr::NodeExpr;
        match e {
            NodeExpr::Filter { input, shape } => {
                let input = self.node_expr(input);
                let shape = self.describe(*shape).flat_nested();
                format!("filter({input}, {shape})")
            }
            NodeExpr::Intersection(es) => self.join_node_exprs(es, " ∩ "),
            NodeExpr::Union(es) => self.join_node_exprs(es, " ∪ "),
            NodeExpr::Function { iri, args } => {
                let args = self.join_node_exprs(args, ", ");
                {
                    let name = self.prefixes.compact(iri.as_str());
                    self.emit(format!("{name}({args})"))
                }
            }
            // No shape references below here.
            _ => {
                let text = node_expr_to_string(e, self.prefixes);
                self.emit(text)
            }
        }
    }

    fn join_node_exprs(&mut self, es: &[crate::expr::NodeExpr], sep: &str) -> String {
        es.iter()
            .map(|e| self.node_expr(e))
            .collect::<Vec<_>>()
            .join(sep)
    }

    /// Join several children with `sep`, rendering each through `render`
    /// (describe, or negate for a De Morgan expansion) and bracketing any member
    /// that came out as a join of its own.
    fn join(
        &mut self,
        cs: &[ShapeId],
        sep: &'static str,
        render: fn(&mut Self, ShapeId) -> Doc,
    ) -> Doc {
        match cs {
            // For a conjunction this is ⊤ and for a disjunction ⊥; neither is
            // informative, and the negation side inherits the same shrug.
            [] => Doc::Atom(self.emit("()".to_string())),
            [only] => render(self, *only),
            _ => Doc::Join {
                sep,
                parts: cs.iter().map(|c| render(self, *c)).collect(),
            },
        }
    }
}

/// Render a selector with only the well-known prefixes compacted.
pub fn selector_to_string(sel: &Selector) -> String {
    selector_to_string_px(sel, &Prefixes::default())
}

/// Render a selector, compacting IRIs against `px`.
pub fn selector_to_string_px(sel: &Selector, px: &Prefixes) -> String {
    match sel {
        Selector::HasOut(q) => format!("∃ {} .⊤", px.compact(q.as_str())),
        Selector::HasIn(q) => format!("∃ {}⁻ .⊤", px.compact(q.as_str())),
        Selector::IsConst(t) => format!("node({})", term_to_string(t, px)),
        Selector::HasPath(p, _) => format!("∃≥1 {} . φ", path_to_string_in(p, px)),
        Selector::Sparql(_) => "sparql{…}".to_string(),
    }
}

/// Like [`selector_to_string`], but resolves a path selector's qualifier against
/// `arena`: class targets render as `class(C)`, and any other path target shows
/// its actual qualifier shape instead of a bare `φ`. Prefer this whenever the
/// arena is in hand — the resolved form is far more useful for debugging.
pub fn selector_to_string_in(sel: &Selector, arena: &ShapeArena) -> String {
    selector_to_string_in_px(sel, arena, &Prefixes::default())
}

/// [`selector_to_string_in`], compacting IRIs against `px`.
pub fn selector_to_string_in_px(sel: &Selector, arena: &ShapeArena, px: &Prefixes) -> String {
    if let Some(class) = class_target(sel, arena) {
        return format!("class({})", term_to_string(class, px));
    }
    match sel {
        Selector::HasPath(p, q) => {
            format!(
                "∃≥1 {} . {}",
                path_to_string_in(p, px),
                shape_def(arena, *q, px)
            )
        }
        other => selector_to_string_px(other, px),
    }
}

/// If `sel` targets a class — the `∃≥1 rdf:type/rdfs:subClassOf* . test(C)` form
/// that `sh:targetClass` and implicit class targets lower to — the class term
/// `C`. `None` for every other selector.
pub fn class_target<'a>(sel: &'a Selector, arena: &'a ShapeArena) -> Option<&'a Term> {
    let Selector::HasPath(path, qualifier) = sel else {
        return None;
    };
    if !is_class_path(path) {
        return None;
    }
    match arena.get(*qualifier) {
        Shape::TestConst(class) => Some(class),
        _ => None,
    }
}

/// If `id` is a `∃≥1 (rdf:type/rdfs:subClassOf*).test(C)` shape (the encoding
/// of `sh:class C`), return `C`. `None` for all other shapes.
pub fn class_target_shape(id: ShapeId, arena: &ShapeArena) -> Option<Term> {
    let Shape::Count {
        ref path,
        min: Some(1),
        max: None,
        qualifier,
    } = arena.get(id).clone()
    else {
        return None;
    };
    if !is_class_path(path) {
        return None;
    }
    if let Shape::TestConst(c) = arena.get(qualifier).clone() {
        Some(c)
    } else {
        None
    }
}

/// If `id` is the NNF negation of a `sh:class C` value constraint —
/// `∃≤0 (rdf:type/rdfs:subClassOf*).test(C)`, i.e. "*not* an instance of `C`" —
/// return `C`. This is the form the optimizer produces for the `¬φ` inside a
/// universal `∀π.(instance of C) ≡ ∃≤0 π.¬(instance of C)`, since negating the
/// `min: Some(1)` class count rewrites it to `max: Some(0)`. `None` otherwise.
pub fn negated_class_target_shape(id: ShapeId, arena: &ShapeArena) -> Option<Term> {
    let Shape::Count {
        ref path,
        min: None,
        max: Some(0),
        qualifier,
    } = arena.get(id).clone()
    else {
        return None;
    };
    if !is_class_path(path) {
        return None;
    }
    if let Shape::TestConst(c) = arena.get(qualifier).clone() {
        Some(c)
    } else {
        None
    }
}

/// Whether a shape is a boolean combination of more than one member, ignoring
/// transparent `sh:severity` wrappers.
fn is_boolean_shape(arena: &ShapeArena, id: ShapeId) -> bool {
    match arena.get(id) {
        Shape::Annotated { shape, .. } => is_boolean_shape(arena, *shape),
        Shape::And(cs) | Shape::Or(cs) => cs.len() > 1,
        _ => false,
    }
}

/// If `id` is the `⋁ᵢ (φᵢ ∧ ⋀_{j≠i} ¬φⱼ)` rewrite of `sh:xone`, the author's
/// alternatives `φᵢ` in branch order. `None` for any other disjunction.
///
/// The rewrite is what the engine evaluates, but it is not what the author
/// wrote: reported as a plain disjunction it says "none of 2 alternatives
/// satisfied" about a node that in fact satisfied *both*, which is the opposite
/// of the finding. Recovering the `φᵢ` lets the report talk about the shape the
/// author wrote. Interning makes this exact rather than approximate — the `¬φⱼ`
/// in one branch and the `φⱼ` heading another are the same arena slot.
pub fn xone_alternatives(id: ShapeId, arena: &ShapeArena) -> Option<Vec<ShapeId>> {
    let Shape::Or(branches) = arena.get(id) else {
        return None;
    };
    if branches.len() < 2 {
        return None;
    }
    // Each branch asserts one alternative and denies every other, so a branch
    // has exactly as many members as there are branches.
    let mut positives = Vec::with_capacity(branches.len());
    for branch in branches {
        let Shape::And(members) = arena.get(*branch) else {
            return None;
        };
        if members.len() != branches.len() {
            return None;
        }
        let mut positive = None;
        for member in members {
            if !matches!(arena.get(*member), Shape::Not(_)) && positive.replace(*member).is_some() {
                return None;
            }
        }
        positives.push(positive?);
    }
    for (i, branch) in branches.iter().enumerate() {
        let Shape::And(members) = arena.get(*branch) else {
            return None;
        };
        let denied: BTreeSet<ShapeId> = members
            .iter()
            .filter_map(|m| match arena.get(*m) {
                Shape::Not(inner) => Some(*inner),
                _ => None,
            })
            .collect();
        let others: BTreeSet<ShapeId> = positives
            .iter()
            .enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, p)| *p)
            .collect();
        if denied != others {
            return None;
        }
    }
    Some(positives)
}

/// Is `p` the `rdf:type/rdfs:subClassOf*` path used to encode class targeting?
fn is_class_path(p: &Path) -> bool {
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    let Path::Seq(parts) = p else { return false };
    matches!(
        parts.as_slice(),
        [Path::Pred(ty), Path::Star(sub)]
            if ty.as_str() == RDF_TYPE
                && matches!(sub.as_ref(), Path::Pred(s) if s.as_str() == RDFS_SUBCLASS_OF)
    )
}

// ---- paths (precedence: atom > * > ^ > / > |) ----

/// Render a path with only the well-known prefixes compacted. Prefer
/// [`path_to_string_in`] wherever the document's declarations are in hand.
pub fn path_to_string(p: &Path) -> String {
    path_to_string_in(p, &Prefixes::default())
}

/// Render a path, compacting IRIs against `px`.
pub fn path_to_string_in(p: &Path, px: &Prefixes) -> String {
    render_alt(p, px)
}

fn render_alt(p: &Path, px: &Prefixes) -> String {
    match p {
        Path::Alt(parts) => parts
            .iter()
            .map(|p| render_seq(p, px))
            .collect::<Vec<_>>()
            .join(" | "),
        _ => render_seq(p, px),
    }
}

fn render_seq(p: &Path, px: &Prefixes) -> String {
    match p {
        Path::Seq(parts) => parts
            .iter()
            .map(|p| render_unary(p, px))
            .collect::<Vec<_>>()
            .join("/"),
        _ => render_unary(p, px),
    }
}

fn render_unary(p: &Path, px: &Prefixes) -> String {
    match p {
        Path::Inverse(inner) => format!("^{}", render_postfix(inner, px)),
        _ => render_postfix(p, px),
    }
}

fn render_postfix(p: &Path, px: &Prefixes) -> String {
    match p {
        Path::Star(inner) => format!("{}*", render_atom(inner, px)),
        _ => render_atom(p, px),
    }
}

fn render_atom(p: &Path, px: &Prefixes) -> String {
    match p {
        Path::Id => "id".to_string(),
        Path::Pred(nn) => px.compact(nn.as_str()),
        // compound paths in atom position need grouping
        _ => format!("({})", render_alt(p, px)),
    }
}

// ---- value types ----

/// Render a value type with only the well-known prefixes compacted.
pub fn value_type_to_string(vt: &ValueType) -> String {
    value_type_to_string_in(vt, &Prefixes::default())
}

/// Render a value type, compacting IRIs against `px`.
pub fn value_type_to_string_in(vt: &ValueType, px: &Prefixes) -> String {
    match vt {
        ValueType::Any => "any".to_string(),
        ValueType::Datatype(nn) => format!("datatype({})", px.compact(nn.as_str())),
        ValueType::NumericRange { lo, hi } => {
            let mut parts = Vec::new();
            if let Some(Bound { value, inclusive }) = lo {
                let value = literal_to_string(value, px);
                parts.push(format!("{}{value}", if *inclusive { "≥" } else { ">" }));
            }
            if let Some(Bound { value, inclusive }) = hi {
                let value = literal_to_string(value, px);
                parts.push(format!("{}{value}", if *inclusive { "≤" } else { "<" }));
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
            .map(|vt| value_type_to_string_in(vt, px))
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

/// Render a term for display, compacting an IRI against `px`. Literals and
/// blank nodes keep their canonical spelling — only an IRI has a vocabulary.
pub fn term_to_string_in(t: &Term, px: &Prefixes) -> String {
    term_to_string(t, px)
}

fn term_to_string(t: &Term, px: &Prefixes) -> String {
    match t {
        Term::NamedNode(nn) => px.compact(nn.as_str()),
        Term::Literal(literal) => literal_to_string(literal, px),
        other => other.to_string(),
    }
}

/// A typed literal's datatype is an IRI like any other, and left absolute it is
/// most of the length of the term.
fn literal_to_string(literal: &crate::term::Literal, px: &Prefixes) -> String {
    match literal.language() {
        Some(language) => format!("{:?}@{language}", literal.value()),
        None if literal.datatype().as_str() == XSD_STRING => format!("{:?}", literal.value()),
        None => format!(
            "{:?}^^{}",
            literal.value(),
            px.compact(literal.datatype().as_str())
        ),
    }
}

/// A plain string literal carries this datatype implicitly, so spelling it out
/// would add noise to every quoted value in a report.
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

// ---- IRI compaction ----

/// Every name a shape answers to, compacted and comma-joined.
fn compact_all(iris: &[String], px: &Prefixes) -> String {
    iris.iter()
        .map(|iri| px.compact(iri))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Render the algebra AST as a Graphviz DOT digraph.
///
/// Each reachable arena slot becomes a node labeled `@id\n<φ-form>`. Structural
/// edges (Not→child, And/Or→children, Count→qualifier) are drawn as solid arcs.
/// Statements appear as diamond entry nodes; rules as hexagon entry nodes with
/// dashed condition edges.
pub fn schema_to_dot(schema: &Schema) -> String {
    let px = &schema.prefixes;
    let reachable = reachable_shapes(schema);
    let mut out = String::from("digraph shifty_algebra_ast {\n");
    out.push_str("  rankdir=TB;\n");
    out.push_str("  node [shape=box, style=rounded, fontname=monospace];\n\n");

    // Shape nodes
    for id in &reachable {
        let def = shape_def_dot(&schema.arena, *id, px);
        let name_line = schema
            .names
            .get(id)
            .map(|iris| format!("\n{}", compact_all(iris, px)))
            .unwrap_or_default();
        let label = dot_escape(&format!("@{}{}\n{}", id.0, name_line, def));
        let node_attrs = match schema.arena.get(*id) {
            Shape::Top => format!(
                "shape=ellipse, style=\"rounded,filled\", fillcolor=lightgray, label=\"{label}\""
            ),
            Shape::Not(_) => format!(
                "shape=ellipse, style=\"rounded,filled\", fillcolor=lightsalmon, label=\"{label}\""
            ),
            Shape::And(_) => format!(
                "shape=box, style=\"rounded,filled\", fillcolor=lightblue, label=\"{label}\""
            ),
            Shape::Or(_) => format!(
                "shape=box, style=\"rounded,filled\", fillcolor=lightyellow, label=\"{label}\""
            ),
            Shape::Count { .. } => format!(
                "shape=box, style=\"rounded,filled\", fillcolor=lightgreen, label=\"{label}\""
            ),
            _ => format!("label=\"{label}\""),
        };
        out.push_str(&format!("  shape_{} [{}];\n", id.0, node_attrs));
    }
    out.push('\n');

    // Structural edges between shapes
    for id in &reachable {
        match schema.arena.get(*id) {
            Shape::Not(c) => {
                out.push_str(&format!(
                    "  shape_{} -> shape_{} [label=\"¬\"];\n",
                    id.0, c.0
                ));
            }
            Shape::And(cs) => {
                for (i, c) in cs.iter().enumerate() {
                    out.push_str(&format!(
                        "  shape_{} -> shape_{} [label=\"{}\"];\n",
                        id.0, c.0, i
                    ));
                }
            }
            Shape::Or(cs) => {
                for (i, c) in cs.iter().enumerate() {
                    out.push_str(&format!(
                        "  shape_{} -> shape_{} [label=\"{}\"];\n",
                        id.0, c.0, i
                    ));
                }
            }
            Shape::Count { qualifier, .. } => {
                out.push_str(&format!(
                    "  shape_{} -> shape_{} [label=\"qualifier\", style=dashed, color=darkgreen];\n",
                    id.0, qualifier.0
                ));
            }
            _ => {}
        }
    }
    out.push('\n');

    // Statement entry nodes
    for (i, st) in schema.statements.iter().enumerate() {
        let sel_label = dot_escape(&selector_to_string_px(&st.selector, px));
        out.push_str(&format!(
            "  stmt_{i} [shape=diamond, style=filled, fillcolor=lightyellow, label=\"stmt:{i}\\n{sel_label}\"];\n"
        ));
        out.push_str(&format!("  stmt_{i} -> shape_{};\n", st.shape.0));
        if let Selector::HasPath(_, shape_id) = &st.selector {
            out.push_str(&format!(
                "  stmt_{i} -> shape_{} [style=dashed, color=gray50, label=\"path-shape\"];\n",
                shape_id.0
            ));
        }
    }
    out.push('\n');

    // Rule entry nodes
    for (i, r) in schema.rules.iter().enumerate() {
        let sel_label = dot_escape(&selector_to_string_px(&r.selector, px));
        let order_label = r.order.map(|o| format!(" ord={o}")).unwrap_or_default();
        let deact = if r.deactivated { " (off)" } else { "" };
        out.push_str(&format!(
            "  rule_{i} [shape=hexagon, style=filled, fillcolor=plum, label=\"rule:{i}\\n{sel_label}{}{}\"];\n",
            dot_escape(&order_label),
            dot_escape(deact)
        ));
        for (j, c) in r.conditions.iter().enumerate() {
            out.push_str(&format!(
                "  rule_{i} -> shape_{} [style=dashed, color=purple4, label=\"cond:{j}\"];\n",
                c.0
            ));
        }
    }

    out.push_str("}\n");
    out
}

/// Shape label for the DOT rendering: leaf shapes show their full definition,
/// composite shapes show only their combinator (children are shown via edges).
fn shape_def_dot(arena: &ShapeArena, id: ShapeId, px: &Prefixes) -> String {
    match arena.get(id) {
        Shape::Annotated { severity, .. } => format!("severity({severity})"),
        Shape::Top => "⊤".to_string(),
        Shape::Pending => "⟨pending⟩".to_string(),
        Shape::TestConst(t) => format!("test({})", term_to_string(t, px)),
        Shape::TestType(vt) => format!("test({})", value_type_to_string_in(vt, px)),
        Shape::TestKind(k) => format!("nodeKind({})", node_kinds_to_string(k)),
        Shape::Closed(q) => {
            let preds: Vec<String> = q.iter().map(|n| px.compact(n.as_str())).collect();
            format!("closed{{{}}}", preds.join(", "))
        }
        Shape::Eq(p, pred) => format!(
            "eq({}, {})",
            path_to_string_in(p, px),
            px.compact(pred.as_str())
        ),
        Shape::Disj(p, pred) => format!(
            "disj({}, {})",
            path_to_string_in(p, px),
            px.compact(pred.as_str())
        ),
        Shape::Lt(p, pred) => format!(
            "lt({}, {})",
            path_to_string_in(p, px),
            px.compact(pred.as_str())
        ),
        Shape::Le(p, pred) => format!(
            "le({}, {})",
            path_to_string_in(p, px),
            px.compact(pred.as_str())
        ),
        Shape::UniqueLang(p) => format!("uniqueLang({})", path_to_string_in(p, px)),
        Shape::Not(_) => "¬".to_string(),
        Shape::And(cs) => format!("∧ ({})", cs.len()),
        Shape::Or(cs) => format!("∨ ({})", cs.len()),
        Shape::Count { path, min, max, .. } => {
            let lo = min.map(|n| n.to_string()).unwrap_or_default();
            let hi = max.map(|n| n.to_string()).unwrap_or_default();
            format!("∃[{lo}..{hi}] {}", path_to_string_in(path, px))
        }
        Shape::Sparql(c) => format!("sparql({:?})", c.kind),
        Shape::Expression(_) => "expr = true".to_string(),
    }
}

fn dot_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Statement;
    use crate::term::NamedNode;
    use std::sync::Arc;

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

    fn class_path() -> Path {
        Path::seq(vec![
            Path::Pred(nn("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")),
            Path::star(Path::Pred(nn(
                "http://www.w3.org/2000/01/rdf-schema#subClassOf",
            ))),
        ])
    }

    #[test]
    fn class_target_is_detected_and_rendered_with_the_class() {
        let mut arena = ShapeArena::new();
        let qualifier = arena.insert(Shape::TestConst(Term::NamedNode(nn("http://ex/Person"))));
        let sel = Selector::HasPath(class_path(), qualifier);

        // the structured accessor recovers the class term…
        assert_eq!(
            class_target(&sel, &arena),
            Some(&Term::NamedNode(nn("http://ex/Person")))
        );
        // …and the arena-aware renderer names it instead of printing a bare φ.
        assert_eq!(
            selector_to_string_in(&sel, &arena),
            "class(<http://ex/Person>)"
        );
        // the arena-free renderer still falls back to the φ form.
        assert_eq!(
            selector_to_string(&sel),
            "∃≥1 rdf:type/rdfs:subClassOf* . φ"
        );
    }

    #[test]
    fn non_class_path_target_resolves_its_qualifier_inline() {
        let mut arena = ShapeArena::new();
        let qualifier = arena.insert(Shape::TestKind(NodeKindSet::IRI));
        let sel = Selector::HasPath(Path::Pred(nn("http://ex/p")), qualifier);

        assert_eq!(class_target(&sel, &arena), None);
        // the qualifier is shown rather than dropped to φ.
        assert_eq!(
            selector_to_string_in(&sel, &arena),
            "∃≥1 <http://ex/p> . nodeKind(IRI)"
        );
    }

    /// A `∃≥1 (rdf:type/rdfs:subClassOf*).test(C)` shape — the lowering of `sh:class C`.
    fn class_shape(arena: &mut ShapeArena, iri: &str) -> ShapeId {
        let test = arena.insert(Shape::TestConst(Term::NamedNode(nn(iri))));
        arena.insert(Shape::Count {
            path: class_path(),
            min: Some(1),
            max: None,
            qualifier: test,
        })
    }

    #[test]
    fn describe_shape_inlines_every_child() {
        let mut arena = ShapeArena::new();
        let a = class_shape(&mut arena, "http://ex/A");
        let b = class_shape(&mut arena, "http://ex/B");
        let or = arena.insert(Shape::Or(vec![a, b]));

        // A disjunction of class shapes expands fully — no bare `@id` slot refs,
        // unlike the one-level `shape_to_string`.
        assert_eq!(
            describe_shape(&arena, or),
            "instance of <http://ex/A> or instance of <http://ex/B>"
        );
        assert_eq!(shape_to_string(&arena, or), format!("@{} ∨ @{}", a.0, b.0));

        // `describe_shapes` (a ConformsToAll-style conjunction of separate shapes)
        // joins each member's full description with “and”.
        let kind = arena.insert(Shape::TestKind(NodeKindSet::IRI));
        assert_eq!(
            describe_shapes(&arena, &[a, kind]),
            "instance of <http://ex/A> and nodeKind(IRI)"
        );
    }

    #[test]
    fn describe_shape_guards_recursive_shapes() {
        // S := ⊤ ∧ ∃≥1 ex:knows . S  — a cyclic shape must terminate, and the one
        // place it stops names the recursion rather than an arena slot.
        let mut arena = ShapeArena::new();
        let s = arena.reserve();
        let top = arena.insert(Shape::Top);
        let reaches = arena.insert(Shape::Count {
            path: Path::Pred(nn("http://ex/knows")),
            min: Some(1),
            max: None,
            qualifier: s,
        });
        arena.set(s, Shape::And(vec![top, reaches]));

        assert_eq!(
            describe_shape(&arena, s),
            format!("any node and ∃[1..] <http://ex/knows> . {RECURSIVE}")
        );
        // ¬S = ¬⊤ ∨ ∃≤0 ex:knows . S — the cycle guard covers the negation side too.
        assert_eq!(
            describe_negation(&arena, s),
            format!("no value or ∀ <http://ex/knows> . {NOT_RECURSIVE}")
        );
    }

    #[test]
    fn describe_shape_never_emits_an_arena_slot_label() {
        // Deep nesting well past the old fixed depth budget: sh:severity wrappers
        // are transparent, so a chain of them must not cost the reader the tail of
        // the description. (This is the shape of the s223 report that surfaced the
        // bug: the elided tail was the one part that distinguished the conjuncts.)
        let mut arena = ShapeArena::new();
        let mut id = class_shape(&mut arena, "http://ex/Leaf");
        for _ in 0..32 {
            id = arena.insert(Shape::Annotated {
                severity: crate::severity::Severity::Violation,
                messages: Arc::from(Vec::new()),
                shape: id,
            });
            id = arena.insert(Shape::Count {
                path: Path::Pred(nn("http://ex/p")),
                min: Some(1),
                max: None,
                qualifier: id,
            });
        }
        let rendered = describe_shape(&arena, id);
        assert!(!rendered.contains('@'), "{rendered}");
        assert!(
            rendered.ends_with("instance of <http://ex/Leaf>"),
            "{rendered}"
        );
        assert_eq!(rendered.matches("∃[1..]").count(), 32, "{rendered}");
    }

    #[test]
    fn describe_shape_elides_past_the_size_budget() {
        // A wide conjunction of long IRIs blows the budget; expansion stops with an
        // ellipsis, never with a slot label.
        let mut arena = ShapeArena::new();
        let long = "http://ex/".to_string() + &"x".repeat(200);
        let cs: Vec<ShapeId> = (0..64).map(|_| class_shape(&mut arena, &long)).collect();
        let and = arena.insert(Shape::And(cs));

        let rendered = describe_shape(&arena, and);
        assert!(!rendered.contains('@'), "{rendered}");
        assert!(rendered.contains('…'), "{rendered}");
    }

    #[test]
    fn describe_shape_inlines_a_node_expression_filter_shape() {
        // `sh:filterShape` is a shape reference inside a node expression — the one
        // place outside the shape grammar that could still print a slot label.
        let mut arena = ShapeArena::new();
        let filter = class_shape(&mut arena, "http://ex/A");
        let expr = arena.insert(Shape::Expression(crate::expr::NodeExpr::Filter {
            input: Box::new(crate::expr::NodeExpr::This),
            shape: filter,
        }));

        assert_eq!(
            describe_shape(&arena, expr),
            "expr(filter(this, instance of <http://ex/A>)) = true"
        );
    }

    #[test]
    fn a_description_that_fits_is_not_broken() {
        // The overwhelming majority of report messages are one short clause;
        // the pretty layout must leave those exactly as they are.
        let mut arena = ShapeArena::new();
        let a = class_shape(&mut arena, "http://ex/A");
        let px = Prefixes::default();

        let flat = describe_shape_in(&arena, a, &px);
        assert_eq!(flat, "instance of <http://ex/A>");
        assert_eq!(describe_shape_pretty(&arena, a, &px, PRETTY_WIDTH), flat);
    }

    #[test]
    fn a_long_description_breaks_at_its_nesting() {
        // The s223 shape that motivated this: a conjunction whose second member
        // negates another conjunction. Both layouts come from one traversal, so
        // the broken form says exactly what the one-line form says.
        let mut arena = ShapeArena::new();
        let px = Prefixes::new([("ex".to_string(), "http://ex/".to_string())]);
        // `∀ hasMedium . instance of M` as the optimizer stores it: `∃[..0]`
        // over the NNF negation of the class test.
        let inlet = |arena: &mut ShapeArena, medium: &str| {
            let point = class_shape(arena, "http://ex/InletConnectionPoint");
            let not_medium = negated_class_shape(arena, medium);
            let all = arena.insert(Shape::Count {
                path: Path::Pred(nn("http://ex/hasMedium")),
                min: None,
                max: Some(0),
                qualifier: not_medium,
            });
            arena.insert(Shape::And(vec![point, all]))
        };
        let signal = inlet(&mut arena, "http://ex/Electricity-Signal");
        let power = inlet(&mut arena, "http://ex/Constituent-Electricity");
        let negated = arena.insert(Shape::Not(power));
        let both = arena.insert(Shape::And(vec![signal, negated]));

        assert_eq!(
            describe_shape_pretty(&arena, both, &px, 60),
            "\
(
  instance of ex:InletConnectionPoint
  and ∀ ex:hasMedium . instance of ex:Electricity-Signal
)
and not (
  instance of ex:InletConnectionPoint
  and ∀ ex:hasMedium . instance of ex:Constituent-Electricity
)"
        );
    }

    #[test]
    fn sibling_groups_of_a_broken_join_break_alike() {
        // One conjunct fitting under the limit by a few columns while its sibling
        // does not would lay the two out differently for no reason the reader can
        // see. A group nested in a broken join always breaks.
        let mut arena = ShapeArena::new();
        let px = Prefixes::default();
        let group = |arena: &mut ShapeArena, a: &str, b: &str| {
            let x = class_shape(arena, a);
            let y = class_shape(arena, b);
            arena.insert(Shape::And(vec![x, y]))
        };
        let short = group(&mut arena, "http://ex/A", "http://ex/B");
        let long = group(
            &mut arena,
            "http://ex/LongerClassName",
            "http://ex/AnotherLongName",
        );
        let both = arena.insert(Shape::And(vec![short, long]));

        let pretty = describe_shape_pretty(&arena, both, &px, 60);
        let opens = pretty.matches("(\n").count();
        assert_eq!(opens, 2, "both groups should be broken:\n{pretty}");
    }

    #[test]
    fn a_negated_negative_is_stated_positively() {
        // `sh:not [ sh:maxCount 0 ]` — `not (∄ p)` makes a reader unwind two
        // negations to learn that the path needs a value.
        let mut arena = ShapeArena::new();
        let px = Prefixes::default();
        let top = arena.insert(Shape::Top);
        let empty = arena.insert(Shape::Count {
            path: Path::Pred(nn("http://ex/legs")),
            min: None,
            max: Some(0),
            qualifier: top,
        });
        let not_empty = arena.insert(Shape::Not(empty));
        assert_eq!(
            describe_shape_in(&arena, not_empty, &px),
            "∃[1..] <http://ex/legs>"
        );

        // A boolean combination keeps the plain form: De Morgan would trade one
        // clear `not (a and b)` for a longer disjunction.
        let a = class_shape(&mut arena, "http://ex/A");
        let b = class_shape(&mut arena, "http://ex/B");
        let both = arena.insert(Shape::And(vec![a, b]));
        let neither = arena.insert(Shape::Not(both));
        assert_eq!(
            describe_shape_in(&arena, neither, &px),
            "not (instance of <http://ex/A> and instance of <http://ex/B>)"
        );
    }

    #[test]
    fn a_xone_is_named_rather_than_shown_as_its_rewrite() {
        // `sh:xone` lowers to `⋁ᵢ (φᵢ ∧ ⋀_{j≠i} ¬φⱼ)`, which no reader recognizes
        // as the shape they wrote.
        let mut arena = ShapeArena::new();
        let px = Prefixes::default();
        let a = class_shape(&mut arena, "http://ex/A");
        let b = class_shape(&mut arena, "http://ex/B");
        let xone = arena.xone(vec![a, b]);

        assert_eq!(
            xone_alternatives(xone, &arena),
            Some(vec![a, b]),
            "the author's alternatives should be recoverable"
        );
        assert_eq!(
            describe_shape_in(&arena, xone, &px),
            "exactly one of (instance of <http://ex/A>, instance of <http://ex/B>)"
        );

        // A plain disjunction must not be mistaken for one.
        let or = arena.insert(Shape::Or(vec![a, b]));
        assert_eq!(xone_alternatives(or, &arena), None);
    }

    #[test]
    fn a_typed_literal_compacts_its_datatype() {
        let mut arena = ShapeArena::new();
        let px = Prefixes::default();
        let typed = arena.insert(Shape::TestConst(Term::Literal(
            crate::term::Literal::new_typed_literal(
                "10",
                nn("http://www.w3.org/2001/XMLSchema#integer"),
            ),
        )));
        assert_eq!(
            describe_shape_in(&arena, typed, &px),
            "test(\"10\"^^xsd:integer)"
        );
        // A plain string carries xsd:string implicitly; spelling it out is noise.
        let plain = arena.insert(Shape::TestConst(Term::Literal(
            crate::term::Literal::new_simple_literal("hi"),
        )));
        assert_eq!(describe_shape_in(&arena, plain, &px), "test(\"hi\")");
    }

    #[test]
    fn a_plain_count_drops_the_vacuous_qualifier() {
        // `∃[1..] p . any node` — the clause says nothing, the way `∄ p` omits it.
        let mut arena = ShapeArena::new();
        let px = Prefixes::default();
        let top = arena.insert(Shape::Top);
        let some = arena.insert(Shape::Count {
            path: Path::Pred(nn("http://ex/p")),
            min: Some(1),
            max: None,
            qualifier: top,
        });
        assert_eq!(describe_shape_in(&arena, some, &px), "∃[1..] <http://ex/p>");
    }

    /// The NNF of `¬(sh:class C)`: `∃≤0 (rdf:type/rdfs:subClassOf*).test(C)`.
    fn negated_class_shape(arena: &mut ShapeArena, iri: &str) -> ShapeId {
        let test = arena.insert(Shape::TestConst(Term::NamedNode(nn(iri))));
        arena.insert(Shape::Count {
            path: class_path(),
            min: None,
            max: Some(0),
            qualifier: test,
        })
    }

    #[test]
    fn negated_class_target_shape_recovers_the_class() {
        let mut arena = ShapeArena::new();
        let neg = negated_class_shape(&mut arena, "http://ex/QuantityKind");
        assert_eq!(
            negated_class_target_shape(neg, &arena),
            Some(Term::NamedNode(nn("http://ex/QuantityKind")))
        );
        // The *positive* class encoding must not be mistaken for its negation.
        let pos = class_shape(&mut arena, "http://ex/QuantityKind");
        assert_eq!(negated_class_target_shape(pos, &arena), None);
    }

    #[test]
    fn describe_negation_inverts_the_common_nnf_forms() {
        let mut arena = ShapeArena::new();

        // ¬(∃≤0 classpath.test(C)) = "instance of C"  (the sh:class universal)
        let neg_class = negated_class_shape(&mut arena, "http://ex/C");
        assert_eq!(
            describe_negation(&arena, neg_class),
            "instance of <http://ex/C>"
        );

        // ¬(∃≥1 classpath.test(C)) = "not an instance of C"  (qualifiedMaxCount 0)
        let pos_class = class_shape(&mut arena, "http://ex/C");
        assert_eq!(
            describe_negation(&arena, pos_class),
            "not an instance of <http://ex/C>"
        );

        // ¬nodeKind(Blank|Literal) = nodeKind(IRI)  (a complemented sh:nodeKind)
        let kind = arena.insert(Shape::TestKind(NodeKindSet::IRI.complement()));
        assert_eq!(describe_negation(&arena, kind), "nodeKind(IRI)");

        // De Morgan: `sh:class C` + `sh:nodeKind IRI` on one path is the universal
        // `∀p.(instance of C ∧ nodeKind IRI)`, whose `¬φ` normalizes to the `Or` of
        // the two negated members. Its negation reads back as the conjunction.
        let or = arena.insert(Shape::Or(vec![neg_class, kind]));
        assert_eq!(
            describe_negation(&arena, or),
            "instance of <http://ex/C> and nodeKind(IRI)"
        );
    }
}
