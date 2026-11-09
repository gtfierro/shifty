//! The kernel builder: Shapeshift AST to a shifty [`Schema`].
//!
//! Every surface construct compiles to one arena term through shifty's own
//! smart constructors, so evaluation, evidence, and repair need no knowledge of
//! Shapeshift. Construction is two-phase: every named shape (and named field)
//! is reserved as a `Pending` slot before any body is expanded, so forward and
//! mutual references resolve without ordering rules. Bodies are then expanded
//! on demand, templates are expanded once per distinct argument tuple, and the
//! admission check runs on the finished, un-normalized arena so a refused
//! recursion is reported in the author's names.
//!
//! Named slots are wrapped in `Annotated` (as shifty's SHACL lowering does for
//! every shape node). Besides carrying severity and messages, the wrapper keeps
//! a named shape a distinct slot: the `and` smart constructor never flattens
//! it into an including shape, so provenance and raising can still see it.

use crate::ast::{self, *};
use crate::diagnostic::{Diagnostic, nearest};
use crate::display;
use crate::vocab::{TermKind, Vocabulary, WELL_KNOWN_NAMESPACES, split_iri};
use oxrdf::{Literal, NamedNode, Term};
use serde::{Deserialize, Serialize};
use shifty_algebra::{
    Bound, Import, ImportSource, NodeKindSet, Origin, OriginKind, Path, Prefixes, Schema, Selector,
    Severity, Shape, ShapeId, SourceLocation, SparqlTarget, Statement, ValueType,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

pub const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
pub const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
pub const OWL: &str = "http://www.w3.org/2002/07/owl#";
pub const SH: &str = "http://www.w3.org/ns/shacl#";

/// Prefixes every file may use without declaring them.
pub const DEFAULT_PREFIXES: &[(&str, &str)] = &[
    ("rdf", RDF),
    ("rdfs", RDFS),
    ("xsd", XSD),
    ("owl", OWL),
    ("sh", SH),
];

/// Built-in datatype keywords and the IRIs they stand for.
pub const DATATYPE_KEYWORDS: &[(&str, &str)] = &[
    ("string", "http://www.w3.org/2001/XMLSchema#string"),
    ("integer", "http://www.w3.org/2001/XMLSchema#integer"),
    ("decimal", "http://www.w3.org/2001/XMLSchema#decimal"),
    ("double", "http://www.w3.org/2001/XMLSchema#double"),
    ("float", "http://www.w3.org/2001/XMLSchema#float"),
    ("boolean", "http://www.w3.org/2001/XMLSchema#boolean"),
    ("date", "http://www.w3.org/2001/XMLSchema#date"),
    ("dateTime", "http://www.w3.org/2001/XMLSchema#dateTime"),
    ("time", "http://www.w3.org/2001/XMLSchema#time"),
    ("anyURI", "http://www.w3.org/2001/XMLSchema#anyURI"),
    (
        "langString",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
    ),
];

/// `rdf:type/rdfs:subClassOf*`, the path shifty's lowering uses for
/// `sh:class` and `sh:targetClass`.
pub fn class_path() -> Path {
    Path::seq(vec![
        Path::Pred(NamedNode::new_unchecked(format!("{RDF}type"))),
        Path::star(Path::Pred(NamedNode::new_unchecked(format!(
            "{RDFS}subClassOf"
        )))),
    ])
}

/// The sort of a template parameter, inferred from how the body uses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Sort {
    /// Unused in the body; accepts anything.
    Any,
    Node,
    Path,
    Shape,
    Int,
}

impl Sort {
    pub fn word(&self) -> &'static str {
        match self {
            Sort::Any => "anything",
            Sort::Node => "node",
            Sort::Path => "path",
            Sort::Shape => "shape",
            Sort::Int => "int",
        }
    }
}

/// How one module refers to another unit through an `import ... as alias`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Binding {
    Module(usize),
    Rdf(usize),
}

/// One parsed `.shm` file and how its imports bind.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleUnit {
    /// Label for locations (a path, or `<repl>`).
    pub label: String,
    pub module: Module,
    /// `alias -> unit` for each aliased import.
    #[serde(default)]
    pub bindings: BTreeMap<String, Binding>,
    /// Units imported without an alias: their names are in scope unqualified.
    #[serde(default)]
    pub open: Vec<Binding>,
}

/// One imported RDF graph, already lowered by shifty's SHACL front end.
#[derive(Debug, Clone)]
pub struct RdfUnit {
    pub label: String,
    /// Canonical IRI of the import (relayed as `owl:imports` on export).
    pub iri: Option<String>,
    pub schema: Schema,
    pub vocabulary: Vocabulary,
}

/// Everything the builder needs: modules (index 0 is the root) and RDF
/// imports. Produced by the import resolver, or by hand for a single file.
#[derive(Debug, Clone, Default)]
pub struct Program {
    pub modules: Vec<ModuleUnit>,
    pub rdf: Vec<RdfUnit>,
}

impl Program {
    /// A program with one module and no resolved imports.
    pub fn single(label: impl Into<String>, module: Module) -> Self {
        Program {
            modules: vec![ModuleUnit {
                label: label.into(),
                module,
                bindings: BTreeMap::new(),
                open: Vec::new(),
            }],
            rdf: Vec::new(),
        }
    }
}

/// Portability facts about the built schema, recorded on export.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Portability {
    pub recursive: bool,
    pub shape_targets: bool,
    pub sparql_targets: bool,
}

/// Information about one template expansion, for export annotations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expansion {
    pub template: String,
    pub args: Vec<String>,
    pub slot: ShapeId,
}

#[derive(Debug, Clone)]
pub struct BuildOutput {
    pub schema: Schema,
    pub diagnostics: Vec<Diagnostic>,
    /// Root-module shape names to their slots (named shapes only).
    pub shapes: BTreeMap<String, ShapeId>,
    /// Every named slot's short display name (`VAV`, `sensors.Sensor`,
    /// `VAV.fedBy`, `HasPointOf(brick:AHU)`).
    pub display: HashMap<ShapeId, String>,
    /// Slots defined by an imported `.shm` module: slot -> that module's base.
    pub defined_in: HashMap<ShapeId, String>,
    /// Slots that belong to imported RDF graphs (not raised on export).
    pub imported_rdf: HashSet<ShapeId>,
    /// Statement indices that came from imported RDF graphs.
    pub imported_statements: HashSet<usize>,
    pub expansions: Vec<Expansion>,
    pub portability: Portability,
    pub vocabulary: Vocabulary,
    /// IRIs of RDF imports, for `owl:imports`.
    pub rdf_imports: Vec<String>,
    pub base: Option<String>,
}

impl BuildOutput {
    pub fn has_errors(&self) -> bool {
        self.diagnostics.iter().any(Diagnostic::is_error)
    }

    pub fn errors(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics.iter().filter(|d| d.is_error())
    }
}

/// Build a program into a schema.
pub fn build(program: &Program) -> BuildOutput {
    let mut b = Builder::new(program);
    b.run();
    b.finish()
}

/// Build one module with no imports.
pub fn build_module(label: &str, module: &Module) -> BuildOutput {
    build(&Program::single(label, module.clone()))
}

#[derive(Debug, Clone)]
enum Bound_ {
    Node(Term),
    Path(Path),
    Shape(ShapeId),
    Int(u64),
}

#[derive(Clone)]
struct Ctx {
    module: usize,
    /// Display name of the shape whose body is being expanded.
    shape: String,
    /// Name minted for the shape's slot; field names hang off it.
    shape_name: Option<String>,
    env: Arc<HashMap<String, Bound_>>,
    template: bool,
}

enum Resolved {
    Named(usize, usize),
    Template(usize, usize),
    Slot(ShapeId),
}

struct Builder<'p> {
    p: &'p Program,
    schema: Schema,
    diags: Vec<Diagnostic>,
    decls: Vec<HashMap<String, usize>>,
    slots: HashMap<(usize, usize), ShapeId>,
    fields: HashMap<(usize, usize, String), ShapeId>,
    expanded: HashSet<(usize, usize)>,
    in_progress: HashSet<(usize, usize)>,
    expansions: HashMap<(usize, usize, Vec<String>), ShapeId>,
    expansion_list: Vec<Expansion>,
    sorts: HashMap<(usize, usize), Vec<Sort>>,
    display: HashMap<ShapeId, String>,
    defined_in: HashMap<ShapeId, String>,
    iri_slots: HashMap<String, ShapeId>,
    rdf_locals: Vec<HashMap<String, Vec<ShapeId>>>,
    imported_rdf: HashSet<ShapeId>,
    imported_statements: HashSet<usize>,
    vocab: Vocabulary,
    warned_namespaces: HashSet<String>,
    depth: usize,
    portability: Portability,
}

const MAX_EXPANSION_DEPTH: usize = 64;

impl<'p> Builder<'p> {
    fn new(p: &'p Program) -> Self {
        let mut vocab = Vocabulary::default();
        for r in &p.rdf {
            vocab.extend(&r.vocabulary);
        }
        Builder {
            p,
            schema: Schema::new(),
            diags: Vec::new(),
            decls: Vec::new(),
            slots: HashMap::new(),
            fields: HashMap::new(),
            expanded: HashSet::new(),
            in_progress: HashSet::new(),
            expansions: HashMap::new(),
            expansion_list: Vec::new(),
            sorts: HashMap::new(),
            display: HashMap::new(),
            defined_in: HashMap::new(),
            iri_slots: HashMap::new(),
            rdf_locals: Vec::new(),
            imported_rdf: HashSet::new(),
            imported_statements: HashSet::new(),
            vocab,
            warned_namespaces: HashSet::new(),
            depth: 0,
            portability: Portability::default(),
        }
    }

    fn loc(&self, module: usize, span: Span) -> Option<SourceLocation> {
        Some(SourceLocation {
            file: self.p.modules[module].label.clone(),
            line: span.line,
            column: span.column,
        })
    }

    fn error(&mut self, module: usize, span: Span, msg: impl Into<String>) -> &mut Diagnostic {
        let loc = self.loc(module, span);
        self.diags.push(Diagnostic::error(msg, loc));
        self.diags.last_mut().unwrap()
    }

    fn warning(&mut self, module: usize, span: Span, msg: impl Into<String>) -> &mut Diagnostic {
        let loc = self.loc(module, span);
        self.diags.push(Diagnostic::warning(msg, loc));
        self.diags.last_mut().unwrap()
    }

    fn module(&self, m: usize) -> &'p Module {
        &self.p.modules[m].module
    }

    fn decl(&self, m: usize, d: usize) -> &'p ShapeDecl {
        &self.p.modules[m].module.shapes[d]
    }

    /// The display prefix for names from module `m` (`alias.` for imports).
    fn qualifier(&self, m: usize) -> String {
        if m == 0 {
            return String::new();
        }
        for unit in &self.p.modules[0..] {
            for (alias, b) in &unit.bindings {
                if *b == Binding::Module(m) {
                    return format!("{alias}.");
                }
            }
        }
        String::new()
    }

    fn mint(&self, m: usize, local: &str) -> String {
        match &self.module(m).base {
            Some(base) if base.ends_with('#') || base.ends_with('/') => format!("{base}{local}"),
            Some(base) => format!("{base}#{local}"),
            None => {
                let label: String = self.p.modules[m]
                    .label
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
                    .collect();
                format!("urn:shapeshift:{label}#{local}")
            }
        }
    }

    fn run(&mut self) {
        // RDF imports first: their slots and statements join the arena as-is.
        for (i, unit) in self.p.rdf.iter().enumerate() {
            let before = self.schema.statements.len();
            let start = self.schema.arena.len();
            let remap = self.schema.merge(unit.schema.clone());
            let _ = remap;
            for idx in before..self.schema.statements.len() {
                self.imported_statements.insert(idx);
            }
            for id in start..self.schema.arena.len() {
                self.imported_rdf.insert(ShapeId(id as u32));
            }
            let mut locals: HashMap<String, Vec<ShapeId>> = HashMap::new();
            for id in start..self.schema.arena.len() {
                let id = ShapeId(id as u32);
                for name in self.schema.names_of(id).to_vec() {
                    self.iri_slots.entry(name.clone()).or_insert(id);
                    locals
                        .entry(split_iri(&name).1.to_string())
                        .or_default()
                        .push(id);
                }
            }
            let _ = i;
            self.rdf_locals.push(locals);
        }

        // Phase 1: index declarations, reserve named slots and named fields.
        for m in 0..self.p.modules.len() {
            let mut index = HashMap::new();
            for (d, decl) in self.module(m).shapes.iter().enumerate() {
                if let Some(prev) = index.insert(decl.name.clone(), d) {
                    let prev_line = self.decl(m, prev).span.line;
                    self.error(
                        m,
                        decl.span,
                        format!("shape `{}` is defined twice", decl.name),
                    )
                    .notes
                    .push(format!("first defined on line {prev_line}"));
                }
            }
            self.decls.push(index);
        }
        for m in 0..self.p.modules.len() {
            let q = self.qualifier(m);
            for (d, decl) in self.module(m).shapes.iter().enumerate() {
                if decl.is_template() {
                    if !decl.targets.is_empty() {
                        self.error(
                            m,
                            decl.span,
                            format!(
                                "template `{}` cannot have `for` clauses; give each use its own shape",
                                decl.name
                            ),
                        );
                    }
                    continue;
                }
                let slot = self.schema.arena.reserve();
                let iri = self.mint(m, &decl.name);
                if let Some(other) = self.iri_slots.insert(iri.clone(), slot)
                    && other != slot
                {
                    self.error(
                        m,
                        decl.span,
                        format!("two definitions mint the same shape IRI <{iri}>"),
                    );
                }
                self.schema.add_name(slot, iri);
                self.display.insert(slot, format!("{q}{}", decl.name));
                if m != 0
                    && let Some(base) = &self.module(m).base
                {
                    self.defined_in.insert(slot, base.clone());
                }
                self.slots.insert((m, d), slot);
                let mut names = Vec::new();
                collect_field_names(&decl.body, &mut names);
                let mut seen = HashSet::new();
                for (name, span) in names {
                    if !seen.insert(name.clone()) {
                        self.error(
                            m,
                            span,
                            format!("field `{name}` is defined twice in shape `{}`", decl.name),
                        );
                        continue;
                    }
                    let fslot = self.schema.arena.reserve();
                    let fname = format!("{}.{}", decl.name, name);
                    self.schema.add_name(fslot, self.mint(m, &fname));
                    self.display.insert(fslot, format!("{q}{fname}"));
                    if m != 0
                        && let Some(base) = &self.module(m).base
                    {
                        self.defined_in.insert(fslot, base.clone());
                    }
                    self.fields.insert((m, d, name), fslot);
                }
            }
        }

        // Template parameter sorts, two passes so pass-through arguments can
        // take the sort of the callee's parameter.
        for _ in 0..2 {
            for m in 0..self.p.modules.len() {
                for (d, decl) in self.module(m).shapes.iter().enumerate() {
                    if decl.is_template() {
                        let sorts = self.infer_sorts(m, d, false);
                        self.sorts.insert((m, d), sorts);
                    }
                }
            }
        }
        for m in 0..self.p.modules.len() {
            for (d, decl) in self.module(m).shapes.iter().enumerate() {
                if decl.is_template() {
                    let sorts = self.infer_sorts(m, d, true);
                    self.sorts.insert((m, d), sorts);
                }
            }
        }

        // Phase 2: expand every named shape (bodies and targets).
        for m in 0..self.p.modules.len() {
            for d in 0..self.module(m).shapes.len() {
                if !self.decl(m, d).is_template() {
                    self.ensure_expanded(m, d);
                }
            }
        }

        // Header metadata.
        let mut prefixes: Vec<(String, String)> = Vec::new();
        for (p, ns) in DEFAULT_PREFIXES {
            prefixes.push((p.to_string(), ns.to_string()));
        }
        for unit in &self.p.modules {
            for p in &unit.module.prefixes {
                prefixes.retain(|(q, _)| q != &p.name);
                prefixes.push((p.name.clone(), p.iri.clone()));
            }
        }
        for (p, ns) in self.schema.prefixes.declarations() {
            if !prefixes.iter().any(|(q, _)| q == p) {
                prefixes.push((p.clone(), ns.clone()));
            }
        }
        self.schema.prefixes = Prefixes::new(prefixes);
        for imp in &self.module(0).imports {
            let source = match &imp.source {
                ImportTarget::Iri(i) => match NamedNode::new(i) {
                    Ok(n) => ImportSource::Iri(n),
                    Err(_) => {
                        self.error(0, imp.span, format!("invalid import IRI <{i}>"));
                        continue;
                    }
                },
                ImportTarget::Path(p) => ImportSource::Path(p.clone()),
            };
            self.schema
                .imports
                .push(Import::new(source, imp.alias.clone()));
        }

        // Fill any reserved slot that was never reached (e.g. a field inside a
        // shape whose body failed to build) so the arena is finalized.
        for i in 0..self.schema.arena.len() {
            let id = ShapeId(i as u32);
            if matches!(self.schema.arena.get(id), Shape::Pending) {
                self.schema.arena.set(id, Shape::Top);
            }
        }

        self.admission();
    }

    fn finish(self) -> BuildOutput {
        let mut shapes = BTreeMap::new();
        for ((m, d), slot) in &self.slots {
            if *m == 0 {
                shapes.insert(self.decl(0, *d).name.clone(), *slot);
            }
        }
        let rdf_imports = self.p.rdf.iter().filter_map(|r| r.iri.clone()).collect();
        let base = self.p.modules.first().and_then(|m| m.module.base.clone());
        BuildOutput {
            schema: self.schema,
            diagnostics: self.diags,
            shapes,
            display: self.display,
            defined_in: self.defined_in,
            imported_rdf: self.imported_rdf,
            imported_statements: self.imported_statements,
            expansions: self.expansion_list,
            portability: self.portability,
            vocabulary: self.vocab,
            rdf_imports,
            base,
        }
    }

    // ---------------------------------------------------------------- names

    fn prefix_iri(&self, m: usize, prefix: &str) -> Option<String> {
        if let Some(p) = self.module(m).prefixes.iter().find(|p| p.name == prefix) {
            return Some(p.iri.clone());
        }
        DEFAULT_PREFIXES
            .iter()
            .find(|(p, _)| *p == prefix)
            .map(|(_, ns)| ns.to_string())
    }

    fn expand_prefixed(
        &mut self,
        m: usize,
        span: Span,
        prefix: &str,
        local: &str,
    ) -> Option<String> {
        match self.prefix_iri(m, prefix) {
            Some(ns) => Some(format!("{ns}{local}")),
            None => {
                let known: Vec<String> = self
                    .module(m)
                    .prefixes
                    .iter()
                    .map(|p| p.name.clone())
                    .chain(DEFAULT_PREFIXES.iter().map(|(p, _)| p.to_string()))
                    .collect();
                let hint = nearest(prefix, known.iter().map(String::as_str))
                    .map(|n| format!("did you mean `{n}:`?"));
                let d = self.error(m, span, format!("unknown prefix `{prefix}:`"));
                d.hint = Some(hint.unwrap_or_else(|| {
                    format!("declare it with `prefix {prefix}: <...>` at the top of the file")
                }));
                None
            }
        }
    }

    fn iri(&mut self, m: usize, span: Span, s: &str) -> Option<NamedNode> {
        match NamedNode::new(s) {
            Ok(n) => Some(n),
            Err(e) => {
                self.error(m, span, format!("invalid IRI <{s}>: {e}"));
                None
            }
        }
    }

    /// Check a class or property IRI against loaded vocabularies.
    fn check_vocab(&mut self, m: usize, span: Span, iri: &str, display: &str, kind: TermKind) {
        if self.vocab.is_empty() {
            return;
        }
        let (ns, local) = split_iri(iri);
        if WELL_KNOWN_NAMESPACES.contains(&ns) {
            return;
        }
        if !self.vocab.covers(ns) {
            if self.warned_namespaces.insert(ns.to_string()) {
                self.warning(
                    m,
                    span,
                    format!("names in <{ns}> are not checked: no imported vocabulary defines that namespace"),
                );
            }
            return;
        }
        if self.vocab.contains(iri, kind) {
            return;
        }
        let other = match kind {
            TermKind::Class => TermKind::Property,
            TermKind::Property => TermKind::Class,
        };
        let (what, other_what) = match kind {
            TermKind::Class => ("class", "property"),
            TermKind::Property => ("property", "class"),
        };
        if self.vocab.contains(iri, other) {
            self.warning(
                m,
                span,
                format!("`{display}` is a {other_what} in the imported vocabulary, used here as a {what}"),
            );
            return;
        }
        let locals = self.vocab.locals_in(ns, kind);
        let suggestion = nearest(local, locals.iter().copied()).map(str::to_string);
        let d = self.error(
            m,
            span,
            format!("unknown {what} `{display}`: the imported vocabulary does not define it"),
        );
        if let Some(s) = suggestion {
            let pfx = display.split(':').next().unwrap_or("");
            d.hint = Some(if display.contains(':') && !display.starts_with('<') {
                format!("did you mean `{pfx}:{s}`?")
            } else {
                format!("did you mean <{ns}{s}>?")
            });
        }
    }

    fn resolve_node(
        &mut self,
        ctx: &Ctx,
        span: Span,
        n: &NodeRef,
        kind: Option<TermKind>,
    ) -> Option<Term> {
        match n {
            NodeRef::Iri(s) => {
                let iri = self.iri(ctx.module, span, s)?;
                if let Some(k) = kind {
                    self.check_vocab(ctx.module, span, iri.as_str(), &format!("<{s}>"), k);
                }
                Some(Term::NamedNode(iri))
            }
            NodeRef::Prefixed { prefix, local } => {
                let full = self.expand_prefixed(ctx.module, span, prefix, local)?;
                let iri = self.iri(ctx.module, span, &full)?;
                if let Some(k) = kind {
                    self.check_vocab(ctx.module, span, &full, &format!("{prefix}:{local}"), k);
                }
                Some(Term::NamedNode(iri))
            }
            NodeRef::Name(name) => match ctx.env.get(name) {
                Some(Bound_::Node(t)) => Some(t.clone()),
                Some(Bound_::Path(Path::Pred(p))) => Some(Term::NamedNode(p.clone())),
                Some(other) => {
                    let what = match other {
                        Bound_::Path(_) => "a path",
                        Bound_::Shape(_) => "a shape",
                        Bound_::Int(_) => "a number",
                        Bound_::Node(_) => unreachable!(),
                    };
                    self.error(
                        ctx.module,
                        span,
                        format!("parameter `{name}` is {what} here, but a node is needed"),
                    );
                    None
                }
                None => {
                    let is_shape = self.lookup_shape(ctx, name).is_some();
                    let d = self.error(ctx.module, span, format!("`{name}` is not a node"));
                    if is_shape {
                        d.hint = Some(match kind {
                            Some(TermKind::Class) => format!(
                                "`{name}` is a shape; to require that values conform to it, write `matches {name}`"
                            ),
                            _ => format!("`{name}` is a shape, not an RDF node"),
                        });
                    } else {
                        d.hint = Some(
                            "write a prefixed name like `ex:thing` or a full IRI like `<https://...>`"
                                .into(),
                        );
                    }
                    None
                }
            },
        }
    }

    fn named_node(
        &mut self,
        ctx: &Ctx,
        span: Span,
        n: &NodeRef,
        kind: Option<TermKind>,
    ) -> Option<NamedNode> {
        match self.resolve_node(ctx, span, n, kind)? {
            Term::NamedNode(nn) => Some(nn),
            other => {
                self.error(ctx.module, span, format!("expected an IRI, found {other}"));
                None
            }
        }
    }

    fn value_term(&mut self, ctx: &Ctx, span: Span, v: &Value) -> Option<Term> {
        match v {
            Value::Node(n) => self.resolve_node(ctx, span, n, None),
            Value::String { value, lang } => Some(Term::Literal(match lang {
                Some(l) => match Literal::new_language_tagged_literal(value.clone(), l.clone()) {
                    Ok(lit) => lit,
                    Err(e) => {
                        self.error(ctx.module, span, format!("invalid language tag `{l}`: {e}"));
                        return None;
                    }
                },
                None => Literal::new_simple_literal(value.clone()),
            })),
            Value::Number(n) => Some(Term::Literal(number_literal(n))),
            Value::Boolean(b) => Some(Term::Literal(Literal::new_typed_literal(
                b.to_string(),
                NamedNode::new_unchecked(format!("{XSD}boolean")),
            ))),
        }
    }

    /// Find a shape by (possibly dotted) name from the perspective of `ctx`.
    fn lookup_shape(&self, ctx: &Ctx, name: &str) -> Option<Resolved> {
        if let Some(Bound_::Shape(s)) = ctx.env.get(name) {
            return Some(Resolved::Slot(*s));
        }
        self.lookup_in_module(ctx.module, name, true)
    }

    fn lookup_in_module(&self, m: usize, name: &str, follow_open: bool) -> Option<Resolved> {
        if let Some(d) = self.decls[m].get(name) {
            return Some(if self.decl(m, *d).is_template() {
                Resolved::Template(m, *d)
            } else {
                Resolved::Named(m, *d)
            });
        }
        if let Some((head, rest)) = name.split_once('.') {
            // Shape.field
            if let Some(d) = self.decls[m].get(head)
                && let Some(slot) = self.fields.get(&(m, *d, rest.to_string()))
            {
                return Some(Resolved::Slot(*slot));
            }
            // alias.Name / alias.Shape.field
            match self.p.modules[m].bindings.get(head) {
                Some(Binding::Module(other)) => return self.lookup_in_module(*other, rest, false),
                Some(Binding::Rdf(r)) => {
                    return self.rdf_locals[*r]
                        .get(rest)
                        .and_then(|v| v.first())
                        .map(|s| Resolved::Slot(*s));
                }
                None => {}
            }
        }
        if follow_open {
            for b in &self.p.modules[m].open {
                if let Binding::Module(other) = b
                    && let Some(r) = self.lookup_in_module(*other, name, false)
                {
                    return Some(r);
                }
            }
        }
        None
    }

    /// Resolve a call target written as a prefixed name or IRI (a shape from
    /// an imported SHACL graph).
    fn lookup_rdf_shape(&mut self, ctx: &Ctx, span: Span, name: &str) -> Option<ShapeId> {
        let (iri, alias_local) = if let Some(inner) = name.strip_prefix('<') {
            (Some(inner.trim_end_matches('>').to_string()), None)
        } else if let Some((prefix, local)) = name.split_once(':') {
            let iri = self
                .prefix_iri(ctx.module, prefix)
                .map(|ns| format!("{ns}{local}"));
            let alias = match self.p.modules[ctx.module].bindings.get(prefix) {
                Some(Binding::Rdf(r)) => Some((*r, local.to_string())),
                _ => None,
            };
            (iri, alias)
        } else {
            (None, None)
        };
        if let Some(iri) = &iri
            && let Some(s) = self.iri_slots.get(iri)
        {
            return Some(*s);
        }
        if let Some((r, local)) = alias_local
            && let Some(s) = self.rdf_locals[r].get(&local).and_then(|v| v.first())
        {
            return Some(*s);
        }
        let is_class = iri.as_ref().is_some_and(|i| self.vocab.has_class(i));
        let d = self.error(ctx.module, span, format!("no shape named `{name}`"));
        if is_class {
            d.hint = Some(format!(
                "`{name}` is a class, not a shape; to require class membership, write `a {name}`"
            ));
        } else {
            d.hint = Some("`matches` takes the name of a shape defined in this file, an imported file, or an imported SHACL graph".into());
        }
        None
    }

    // ---------------------------------------------------------------- sorts

    fn infer_sorts(&mut self, m: usize, d: usize, report: bool) -> Vec<Sort> {
        let decl = self.decl(m, d);
        let mut uses: HashMap<String, Vec<(Sort, Span)>> = HashMap::new();
        let params: HashSet<&str> = decl.params.iter().map(String::as_str).collect();
        {
            let mut v = SortVisitor {
                b: self,
                m,
                params: &params,
                uses: &mut uses,
            };
            for l in &decl.body {
                v.line(l);
            }
        }
        let mut out = Vec::new();
        for p in &decl.params {
            let mut sort = Sort::Any;
            let mut conflict = None;
            for (s, span) in uses.get(p).map(Vec::as_slice).unwrap_or(&[]) {
                sort = match (sort, *s) {
                    (Sort::Any, x) | (x, Sort::Any) => x,
                    (a, b) if a == b => a,
                    (Sort::Node, Sort::Path) | (Sort::Path, Sort::Node) => Sort::Node,
                    (a, b) => {
                        conflict = Some((a, b, *span));
                        a
                    }
                };
            }
            if report && let Some((a, b, span)) = conflict {
                let d = self.error(
                    m,
                    span,
                    format!(
                        "parameter `{p}` of `{}` is used both as a {} and as a {}",
                        decl.name,
                        a.word(),
                        b.word()
                    ),
                );
                d.hint = Some(match (a, b) {
                    (Sort::Shape, Sort::Node) | (Sort::Node, Sort::Shape) => {
                        "`a X` needs a class; `matches X` needs a shape. Use two parameters if you need both.".into()
                    }
                    _ => "use a separate parameter for each role".into(),
                });
            }
            out.push(sort);
        }
        out
    }

    // ------------------------------------------------------------ expansion

    fn ensure_expanded(&mut self, m: usize, d: usize) {
        if self.expanded.contains(&(m, d)) || self.in_progress.contains(&(m, d)) {
            return;
        }
        self.in_progress.insert((m, d));
        let decl = self.decl(m, d);
        let slot = self.slots[&(m, d)];
        let display = self.display[&slot].clone();
        let ctx = Ctx {
            module: m,
            shape: display.clone(),
            shape_name: Some(decl.name.clone()),
            env: Arc::new(HashMap::new()),
            template: false,
        };
        let body = self.block(&ctx, &decl.body, Some((m, d)));
        let (severity, messages) = self.annotation_values(&decl.annotations);
        self.schema.arena.set(
            slot,
            Shape::Annotated {
                severity,
                messages,
                shape: body,
            },
        );
        self.schema.add_origin(
            slot,
            Origin::new(OriginKind::Shape { name: display }, self.loc(m, decl.span)),
        );
        if let Some(doc) = &decl.annotations.doc {
            self.schema.docs.insert(slot, doc.clone());
        }
        for t in &decl.targets {
            if let Some(sel) = self.target(&ctx, t) {
                self.schema.statements.push(Statement {
                    selector: sel,
                    shape: slot,
                });
            }
        }
        self.in_progress.remove(&(m, d));
        self.expanded.insert((m, d));
    }

    fn annotation_values(&self, a: &Annotations) -> (Severity, Arc<[Term]>) {
        let severity = match a.severity {
            Some(SeverityLevel::Info) => Severity::Info,
            Some(SeverityLevel::Warning) => Severity::Warning,
            _ => Severity::Violation,
        };
        let messages: Vec<Term> = a
            .messages
            .iter()
            .map(|m| Term::Literal(Literal::new_simple_literal(m.clone())))
            .collect();
        (severity, messages.into())
    }

    fn target(&mut self, ctx: &Ctx, t: &TargetDecl) -> Option<Selector> {
        let span = t.span;
        Some(match &t.kind {
            Target::Class(n) => {
                if let NodeRef::Name(name) = n
                    && !ctx.env.contains_key(name)
                {
                    let shape = self.lookup_shape(ctx, name).is_some();
                    let d = self.error(
                        ctx.module,
                        span,
                        format!("`for every {name}` needs a class"),
                    );
                    d.hint = Some(if shape {
                        format!(
                            "`{name}` is a shape; to target the nodes that conform to it, write `for every node matching {name}`"
                        )
                    } else {
                        "write the class as a prefixed name, e.g. `for every brick:VAV`".into()
                    });
                    return None;
                }
                let c = self.resolve_node(ctx, span, n, Some(TermKind::Class))?;
                let tc = self.schema.arena.insert(Shape::TestConst(c));
                Selector::HasPath(class_path(), tc)
            }
            Target::SubjectsOf(p) => {
                Selector::HasOut(self.named_node(ctx, span, p, Some(TermKind::Property))?)
            }
            Target::ObjectsOf(p) => {
                Selector::HasIn(self.named_node(ctx, span, p, Some(TermKind::Property))?)
            }
            Target::Node(v) => Selector::IsConst(self.value_term(ctx, span, v)?),
            Target::Matching(call) => {
                let s = self.call(ctx, call, false)?;
                self.portability.shape_targets = true;
                Selector::HasPath(Path::Id, s)
            }
            Target::Sparql(q) => {
                let query = self.sparql_target(ctx, span, q)?;
                self.portability.sparql_targets = true;
                Selector::Sparql(SparqlTarget { query })
            }
        })
    }

    fn sparql_target(&mut self, ctx: &Ctx, span: Span, q: &str) -> Option<String> {
        let mut text = String::new();
        let mut declared: Vec<(String, String)> = DEFAULT_PREFIXES
            .iter()
            .map(|(p, n)| (p.to_string(), n.to_string()))
            .collect();
        for p in &self.module(ctx.module).prefixes {
            declared.retain(|(q, _)| q != &p.name);
            declared.push((p.name.clone(), p.iri.clone()));
        }
        for (p, ns) in &declared {
            text.push_str(&format!("PREFIX {p}: <{ns}>\n"));
        }
        text.push_str(q);
        match spargebra::SparqlParser::new().parse_query(&text) {
            Ok(parsed) => {
                if !matches!(parsed, spargebra::Query::Select { .. }) {
                    self.error(ctx.module, span, "a SPARQL target must be a SELECT query");
                    return None;
                }
                Some(parsed.to_string())
            }
            Err(e) => {
                self.error(ctx.module, span, format!("invalid SPARQL target: {e}"));
                None
            }
        }
    }

    /// Compile a block of lines into one conjunction.
    fn block(&mut self, ctx: &Ctx, lines: &[Line], owner: Option<(usize, usize)>) -> ShapeId {
        let mut parts = Vec::new();
        let mut closed: Vec<&Line> = Vec::new();
        for l in lines {
            if matches!(l.kind, LineKind::Closed) {
                closed.push(l);
                continue;
            }
            if let Some(s) = self.line(ctx, l, owner) {
                parts.push(s);
            }
        }
        if let Some(l) = closed.first() {
            let mut q = BTreeSet::new();
            let mut seen = HashSet::new();
            for p in &parts {
                self.collect_preds(*p, &mut q, &mut seen);
            }
            let c = self.schema.arena.insert(Shape::Closed(q));
            let c = self.annotate(ctx, l, c, self.schema.arena.len() as u32 - 1);
            parts.push(c);
        }
        self.schema.arena.and(parts)
    }

    /// Predicates a block names at its top level, for `only these properties`.
    fn collect_preds(&self, id: ShapeId, q: &mut BTreeSet<NamedNode>, seen: &mut HashSet<ShapeId>) {
        if !seen.insert(id) {
            return;
        }
        match self.schema.arena.get(id) {
            Shape::Annotated { shape, .. } => self.collect_preds(*shape, q, seen),
            Shape::And(cs) | Shape::Or(cs) => {
                for c in cs.clone() {
                    self.collect_preds(c, q, seen);
                }
            }
            Shape::Not(c) => self.collect_preds(*c, q, seen),
            Shape::Count { path, .. }
            | Shape::Eq(path, _)
            | Shape::Disj(path, _)
            | Shape::Lt(path, _)
            | Shape::Le(path, _)
            | Shape::UniqueLang(path) => {
                if let Path::Pred(p) = path {
                    q.insert(p.clone());
                }
            }
            _ => {}
        }
    }

    /// Wrap a line's slot in `Annotated` when it carries severity or
    /// messages, and attach its docstring.
    fn annotate(&mut self, _ctx: &Ctx, l: &Line, slot: ShapeId, mark: u32) -> ShapeId {
        let a = &l.annotations;
        let out = if a.severity.is_some() || !a.messages.is_empty() {
            let (severity, messages) = self.annotation_values(a);
            self.schema.arena.insert(Shape::Annotated {
                severity,
                messages,
                shape: slot,
            })
        } else {
            slot
        };
        if let Some(doc) = &a.doc
            && out.0 >= mark
        {
            self.schema.docs.insert(out, doc.clone());
        }
        out
    }

    fn line(&mut self, ctx: &Ctx, l: &Line, owner: Option<(usize, usize)>) -> Option<ShapeId> {
        let mark = self.schema.arena.len() as u32;
        let slot = match &l.kind {
            LineKind::Field(f) => return self.field(ctx, l, f, owner, mark),
            LineKind::Include(call) => {
                let s = self.call(ctx, call, true)?;
                let into = ctx.shape.clone();
                let from = display::call(call);
                self.schema.add_origin(
                    s,
                    Origin::new(
                        OriginKind::Include { from, into },
                        self.loc(ctx.module, l.span),
                    ),
                );
                s
            }
            LineKind::Boolean(b) => self.boolean(ctx, l, b, owner)?,
            LineKind::Closed => unreachable!("handled by block"),
        };
        Some(self.annotate(ctx, l, slot, mark))
    }

    fn int(&mut self, ctx: &Ctx, span: Span, i: &IntArg) -> Option<u64> {
        match i {
            IntArg::Lit(n) => Some(*n),
            IntArg::Param(p) => match ctx.env.get(p) {
                Some(Bound_::Int(n)) => Some(*n),
                _ => {
                    self.error(ctx.module, span, format!("`{p}` is not a number"));
                    None
                }
            },
        }
    }

    fn field(
        &mut self,
        ctx: &Ctx,
        l: &Line,
        f: &Field,
        owner: Option<(usize, usize)>,
        mark: u32,
    ) -> Option<ShapeId> {
        let span = l.span;
        let path = self.path(ctx, span, &f.path)?;
        let mut parts = Vec::new();
        let mut each = Vec::new();
        for q in &f.qualifiers {
            if let Qualifier::Each(t) = q
                && let Some(s) = self.test(ctx, span, t)
            {
                each.push(s);
            }
        }
        if !each.is_empty() {
            let inner = self.schema.arena.and(each);
            let neg = self.schema.arena.not(inner);
            parts.push(self.schema.arena.count(path.clone(), None, Some(0), neg));
        }
        if let Some(c) = &f.count {
            let (min, max) = match c {
                Count::Required => (Some(1), None),
                Count::Optional => (None, None),
                Count::Exactly(n) => {
                    let n = self.int(ctx, span, n)?;
                    (Some(n), Some(n))
                }
                Count::AtLeast(n) => (Some(self.int(ctx, span, n)?), None),
                Count::AtMost(n) => (None, Some(self.int(ctx, span, n)?)),
                Count::Between(a, b) => {
                    let a = self.int(ctx, span, a)?;
                    let b = self.int(ctx, span, b)?;
                    if a > b {
                        self.error(
                            ctx.module,
                            span,
                            format!("`between {a} and {b}` can never hold: the minimum is larger than the maximum"),
                        );
                        return None;
                    }
                    (Some(a), Some(b))
                }
            };
            let top = self.schema.arena.top();
            parts.push(self.schema.arena.count(path.clone(), min, max, top));
        }
        for q in &f.qualifiers {
            match q {
                Qualifier::Each(_) => {}
                Qualifier::Qualified { bound, n, test } => {
                    let n = self.int(ctx, span, n)?;
                    let t = self.test(ctx, span, test)?;
                    let (min, max) = match bound {
                        QualifiedBound::AtLeast => (Some(n), None),
                        QualifiedBound::AtMost => (None, Some(n)),
                        QualifiedBound::Exactly => (Some(n), Some(n)),
                    };
                    parts.push(self.schema.arena.count(path.clone(), min, max, t));
                }
                Qualifier::SameAs(p)
                | Qualifier::DifferentFrom(p)
                | Qualifier::LessThan(p)
                | Qualifier::AtMostProperty(p) => {
                    let other = self.named_node(ctx, span, p, Some(TermKind::Property))?;
                    let shape = match q {
                        Qualifier::SameAs(_) => Shape::Eq(path.clone(), other),
                        Qualifier::DifferentFrom(_) => Shape::Disj(path.clone(), other),
                        Qualifier::LessThan(_) => Shape::Lt(path.clone(), other),
                        _ => Shape::Le(path.clone(), other),
                    };
                    parts.push(self.schema.arena.insert(shape));
                }
                Qualifier::UniqueLanguage => {
                    parts.push(self.schema.arena.insert(Shape::UniqueLang(path.clone())));
                }
            }
        }
        let body = self.schema.arena.and(parts);
        let origin = Origin::new(
            OriginKind::Field {
                shape: ctx.shape.clone(),
                name: f.name.clone(),
                path: display::path(&f.path),
                text: display::field_text(f),
            },
            self.loc(ctx.module, span),
        );
        // A named field of a named shape has a reserved slot of its own.
        let reserved = match (&f.name, owner) {
            (Some(name), Some((m, d))) if !ctx.template => {
                self.fields.get(&(m, d, name.clone())).copied()
            }
            _ => None,
        };
        let slot = if let Some(r) = reserved {
            let (severity, messages) = self.annotation_values(&l.annotations);
            self.schema.arena.set(
                r,
                Shape::Annotated {
                    severity,
                    messages,
                    shape: body,
                },
            );
            if let Some(doc) = &l.annotations.doc {
                self.schema.docs.insert(r, doc.clone());
            }
            r
        } else if f.name.is_some() && ctx.template {
            // Named field inside a template expansion: name it after the
            // expansion so it still exports as a named property shape.
            let (severity, messages) = self.annotation_values(&l.annotations);
            let s = self.schema.arena.insert(Shape::Annotated {
                severity,
                messages,
                shape: body,
            });
            if let Some(base) = &ctx.shape_name {
                let fname = format!("{base}.{}", f.name.as_deref().unwrap());
                self.schema.add_name(s, self.mint(ctx.module, &fname));
                self.display
                    .insert(s, format!("{}.{}", ctx.shape, f.name.as_deref().unwrap()));
            }
            if let Some(doc) = &l.annotations.doc {
                self.schema.docs.insert(s, doc.clone());
            }
            s
        } else {
            self.annotate(ctx, l, body, mark)
        };
        if slot.0 >= mark || reserved.is_some() {
            self.schema.add_origin(slot, origin);
        }
        Some(slot)
    }

    fn path(&mut self, ctx: &Ctx, span: Span, p: &PathExpr) -> Option<Path> {
        Some(match p {
            PathExpr::Node(NodeRef::Name(name)) => match ctx.env.get(name) {
                Some(Bound_::Path(p)) => p.clone(),
                Some(Bound_::Node(Term::NamedNode(n))) => Path::Pred(n.clone()),
                Some(_) => {
                    self.error(
                        ctx.module,
                        span,
                        format!("parameter `{name}` is not a path"),
                    );
                    return None;
                }
                None => {
                    let shapeish = name.contains('.') || self.lookup_shape(ctx, name).is_some();
                    let d = self.error(
                        ctx.module,
                        span,
                        format!("`{name}` is not a property or path"),
                    );
                    d.hint = Some(if shapeish {
                        format!("to add the constraints of `{name}` here, write `include {name}`")
                    } else {
                        "write the property as a prefixed name, e.g. `brick:hasPoint`".into()
                    });
                    return None;
                }
            },
            PathExpr::Node(n) => {
                Path::Pred(self.named_node(ctx, span, n, Some(TermKind::Property))?)
            }
            PathExpr::SelfPath => Path::Id,
            PathExpr::Inverse(i) => self.path(ctx, span, i)?.inverse(),
            PathExpr::Seq(ps) => {
                let mut out = Vec::new();
                for q in ps {
                    out.push(self.path(ctx, span, q)?);
                }
                Path::seq(out)
            }
            PathExpr::Alt(ps) => {
                let mut out = Vec::new();
                for q in ps {
                    out.push(self.path(ctx, span, q)?);
                }
                Path::alt(out)
            }
            PathExpr::Star(i) => self.path(ctx, span, i)?.star(),
            PathExpr::Plus(i) => self.path(ctx, span, i)?.one_or_more(),
            PathExpr::Optional(i) => self.path(ctx, span, i)?.zero_or_one(),
        })
    }

    fn test(&mut self, ctx: &Ctx, span: Span, t: &Test) -> Option<ShapeId> {
        let a = &mut self.schema.arena;
        let _ = a;
        Some(match t {
            Test::Class(n) => {
                let c = self.resolve_node(ctx, span, n, Some(TermKind::Class))?;
                let tc = self.schema.arena.insert(Shape::TestConst(c));
                self.schema.arena.count(class_path(), Some(1), None, tc)
            }
            Test::Matches(call) => self.call(ctx, call, false)?,
            Test::Is(v) => {
                let t = self.value_term(ctx, span, v)?;
                self.schema.arena.insert(Shape::TestConst(t))
            }
            Test::OneOf(vs) => {
                let mut alts = Vec::new();
                for v in vs {
                    let t = self.value_term(ctx, span, v)?;
                    alts.push(self.schema.arena.insert(Shape::TestConst(t)));
                }
                self.schema.arena.or(alts)
            }
            Test::Datatype(DatatypeRef::Builtin(w)) => {
                let iri = DATATYPE_KEYWORDS
                    .iter()
                    .find(|(k, _)| k == w)
                    .map(|(_, i)| *i)
                    .unwrap_or(XSD);
                self.schema
                    .arena
                    .insert(Shape::TestType(ValueType::Datatype(
                        NamedNode::new_unchecked(iri),
                    )))
            }
            Test::Datatype(DatatypeRef::Node(n)) => {
                let d = self.named_node(ctx, span, n, None)?;
                self.schema
                    .arena
                    .insert(Shape::TestType(ValueType::Datatype(d)))
            }
            Test::Kind(k) => self.schema.arena.insert(Shape::TestKind(match k {
                NodeKind::Iri => NodeKindSet::IRI,
                NodeKind::Literal => NodeKindSet::LITERAL,
                NodeKind::Blank => NodeKindSet::BLANK_NODE,
            })),
            Test::Length { min, max } => {
                if let (Some(a), Some(b)) = (min, max)
                    && a > b
                {
                    self.error(
                        ctx.module,
                        span,
                        format!("`length {a} to {b}` can never hold: the minimum is larger than the maximum"),
                    );
                    return None;
                }
                self.schema.arena.insert(Shape::TestType(ValueType::Length {
                    min: *min,
                    max: *max,
                }))
            }
            Test::Range { from, to } => {
                let lo = from.as_ref().map(|n| Bound {
                    value: number_literal(n),
                    inclusive: true,
                });
                let hi = to.as_ref().map(|n| Bound {
                    value: number_literal(n),
                    inclusive: true,
                });
                if let (Some(f), Some(t)) = (from, to)
                    && let (Ok(a), Ok(b)) = (f.lexical.parse::<f64>(), t.lexical.parse::<f64>())
                    && a > b
                {
                    self.error(
                        ctx.module,
                        span,
                        format!(
                            "`from {} to {}` can never hold: the lower end is above the upper end",
                            f.lexical, t.lexical
                        ),
                    );
                    return None;
                }
                self.schema
                    .arena
                    .insert(Shape::TestType(ValueType::NumericRange { lo, hi }))
            }
            Test::Pattern { regex, flags } => {
                if let Err(e) = regex::Regex::new(regex) {
                    self.error(ctx.module, span, format!("invalid pattern: {e}"));
                    return None;
                }
                self.schema
                    .arena
                    .insert(Shape::TestType(ValueType::Pattern {
                        regex: regex.clone(),
                        flags: flags.clone(),
                    }))
            }
            Test::Language(ls) => self
                .schema
                .arena
                .insert(Shape::TestType(ValueType::LangIn(ls.clone()))),
        })
    }

    /// Resolve and (for templates) expand a call to a shape slot.
    fn call(&mut self, ctx: &Ctx, call: &Call, include: bool) -> Option<ShapeId> {
        let span = call.span;
        if call.name.contains(':') || call.name.starts_with('<') {
            if !call.args.is_empty() {
                self.error(
                    ctx.module,
                    span,
                    "shapes from SHACL graphs take no arguments",
                );
                return None;
            }
            return self.lookup_rdf_shape(ctx, span, &call.name);
        }
        let Some(resolved) = self.lookup_shape(ctx, &call.name) else {
            let mut names: Vec<String> = self.decls[ctx.module].keys().cloned().collect();
            names.extend(ctx.env.keys().cloned());
            let hint = nearest(&call.name, names.iter().map(String::as_str)).map(str::to_string);
            let d = self.error(ctx.module, span, format!("no shape named `{}`", call.name));
            d.hint = Some(match hint {
                Some(h) => format!("did you mean `{h}`?"),
                None if !include => "to require class membership instead, write `a <class>`".into(),
                None => "define it with `shape Name { ... }`".into(),
            });
            return None;
        };
        match resolved {
            Resolved::Slot(s) => {
                if !call.args.is_empty() {
                    self.error(
                        ctx.module,
                        span,
                        format!("`{}` takes no arguments", call.name),
                    );
                }
                Some(s)
            }
            Resolved::Named(m, d) => {
                if !call.args.is_empty() {
                    self.error(
                        ctx.module,
                        span,
                        format!(
                            "`{}` has no parameters, but was given {}",
                            call.name,
                            call.args.len()
                        ),
                    );
                }
                // Expand on demand so included constraints are visible to
                // `only these properties`; a cycle just uses the reserved slot.
                self.ensure_expanded(m, d);
                Some(self.slots[&(m, d)])
            }
            Resolved::Template(m, d) => self.expand_template(ctx, call, m, d),
        }
    }

    fn arg_key(&self, b: &Bound_) -> String {
        match b {
            Bound_::Node(t) => t.to_string(),
            Bound_::Path(p) => format!("{p:?}"),
            Bound_::Shape(s) => self
                .schema
                .name_of(*s)
                .map(str::to_string)
                .unwrap_or_else(|| format!("@{}", s.0)),
            Bound_::Int(n) => n.to_string(),
        }
    }

    fn arg_display(&self, b: &Bound_) -> String {
        match b {
            Bound_::Node(t) => match t {
                Term::NamedNode(n) => self.schema_prefixes_compact(n.as_str()),
                other => other.to_string(),
            },
            Bound_::Path(p) => shifty_algebra::render::path_to_string(p),
            Bound_::Shape(s) => self
                .display
                .get(s)
                .cloned()
                .unwrap_or_else(|| format!("@{}", s.0)),
            Bound_::Int(n) => n.to_string(),
        }
    }

    fn schema_prefixes_compact(&self, iri: &str) -> String {
        for unit in &self.p.modules {
            for p in &unit.module.prefixes {
                if let Some(local) = iri.strip_prefix(&p.iri) {
                    return format!("{}:{local}", p.name);
                }
            }
        }
        for (p, ns) in DEFAULT_PREFIXES {
            if let Some(local) = iri.strip_prefix(ns) {
                return format!("{p}:{local}");
            }
        }
        format!("<{iri}>")
    }

    fn eval_arg(&mut self, ctx: &Ctx, span: Span, arg: &Arg, sort: Sort) -> Option<Bound_> {
        match (sort, arg) {
            (_, Arg::Node(NodeRef::Name(n))) if ctx.env.contains_key(n) => {
                let b = ctx.env[n].clone();
                let ok = matches!(
                    (sort, &b),
                    (Sort::Any, _)
                        | (Sort::Node, Bound_::Node(_))
                        | (Sort::Node, Bound_::Path(Path::Pred(_)))
                        | (Sort::Path, Bound_::Path(_))
                        | (Sort::Path, Bound_::Node(Term::NamedNode(_)))
                        | (Sort::Shape, Bound_::Shape(_))
                        | (Sort::Int, Bound_::Int(_))
                );
                if !ok {
                    self.error(
                        ctx.module,
                        span,
                        format!("`{n}` cannot be passed where a {} is expected", sort.word()),
                    );
                    return None;
                }
                Some(b)
            }
            (Sort::Int, Arg::Int(n)) => Some(Bound_::Int(*n)),
            (Sort::Int, _) => {
                self.error(ctx.module, span, "expected a whole number");
                None
            }
            (Sort::Shape, Arg::Node(NodeRef::Name(n))) => {
                let c = Call {
                    name: n.clone(),
                    args: Vec::new(),
                    span,
                };
                Some(Bound_::Shape(self.call(ctx, &c, false)?))
            }
            (Sort::Shape, Arg::Node(NodeRef::Prefixed { prefix, local })) => Some(Bound_::Shape(
                self.lookup_rdf_shape(ctx, span, &format!("{prefix}:{local}"))?,
            )),
            (Sort::Shape, Arg::Node(NodeRef::Iri(i))) => Some(Bound_::Shape(
                self.lookup_rdf_shape(ctx, span, &format!("<{i}>"))?,
            )),
            (Sort::Shape, Arg::Call(c)) | (Sort::Any, Arg::Call(c)) => {
                Some(Bound_::Shape(self.call(ctx, c, false)?))
            }
            (Sort::Shape, _) => {
                self.error(ctx.module, span, "expected a shape name");
                None
            }
            (Sort::Path, Arg::Path(p)) | (Sort::Any, Arg::Path(p)) => {
                Some(Bound_::Path(self.path(ctx, span, p)?))
            }
            (Sort::Path, Arg::Node(n)) => Some(Bound_::Path(Path::Pred(self.named_node(
                ctx,
                span,
                n,
                Some(TermKind::Property),
            )?))),
            (Sort::Node, Arg::Path(_)) => {
                self.error(ctx.module, span, "expected a node, found a path");
                None
            }
            (Sort::Node | Sort::Any, Arg::Node(n)) => {
                if let NodeRef::Name(name) = n
                    && sort == Sort::Any
                    && self.lookup_shape(ctx, name).is_some()
                {
                    let c = Call {
                        name: name.clone(),
                        args: Vec::new(),
                        span,
                    };
                    return Some(Bound_::Shape(self.call(ctx, &c, false)?));
                }
                Some(Bound_::Node(self.resolve_node(ctx, span, n, None)?))
            }
            (Sort::Node | Sort::Any | Sort::Path, Arg::Literal(v)) => {
                if sort == Sort::Path {
                    self.error(ctx.module, span, "expected a path, found a literal");
                    return None;
                }
                Some(Bound_::Node(self.value_term(ctx, span, v)?))
            }
            (Sort::Node, Arg::Int(n)) | (Sort::Any, Arg::Int(n)) => {
                if sort == Sort::Any {
                    return Some(Bound_::Int(*n));
                }
                Some(Bound_::Node(Term::Literal(Literal::new_typed_literal(
                    n.to_string(),
                    NamedNode::new_unchecked(format!("{XSD}integer")),
                ))))
            }
            (Sort::Node, Arg::Call(_)) | (Sort::Path, Arg::Call(_)) => {
                self.error(
                    ctx.module,
                    span,
                    format!("expected a {}, found a shape", sort.word()),
                );
                None
            }
            (Sort::Path, Arg::Int(_)) => {
                self.error(ctx.module, span, "expected a path, found a number");
                None
            }
        }
    }

    fn expand_template(&mut self, ctx: &Ctx, call: &Call, m: usize, d: usize) -> Option<ShapeId> {
        let decl = self.decl(m, d);
        let span = call.span;
        if call.args.len() != decl.params.len() {
            self.error(
                ctx.module,
                span,
                format!(
                    "`{}` takes {} argument{} ({}), but was given {}",
                    decl.name,
                    decl.params.len(),
                    if decl.params.len() == 1 { "" } else { "s" },
                    decl.params.join(", "),
                    call.args.len()
                ),
            );
            return None;
        }
        let sorts = self.sorts.get(&(m, d)).cloned().unwrap_or_default();
        let mut env = HashMap::new();
        let mut keys = Vec::new();
        let mut shown = Vec::new();
        for (i, (p, a)) in decl.params.iter().zip(&call.args).enumerate() {
            let sort = sorts.get(i).copied().unwrap_or(Sort::Any);
            let v = self.eval_arg(ctx, span, a, sort)?;
            keys.push(self.arg_key(&v));
            shown.push(self.arg_display(&v));
            env.insert(p.clone(), v);
        }
        let key = (m, d, keys.clone());
        if let Some(s) = self.expansions.get(&key) {
            return Some(*s);
        }
        if self.depth >= MAX_EXPANSION_DEPTH {
            self.error(
                ctx.module,
                span,
                format!("expanding `{}` does not terminate: each expansion calls it again with new arguments", decl.name),
            );
            return None;
        }
        let slot = self.schema.arena.reserve();
        self.expansions.insert(key, slot);
        let q = self.qualifier(m);
        let display = format!("{q}{}({})", decl.name, shown.join(", "));
        let local = format!("{}-{}", decl.name, stable_hash(&keys.join("\u{1f}")));
        self.schema.add_name(slot, self.mint(m, &local));
        self.display.insert(slot, display.clone());
        if m != 0
            && let Some(base) = &self.module(m).base
        {
            self.defined_in.insert(slot, base.clone());
        }
        self.expansion_list.push(Expansion {
            template: format!("{q}{}", decl.name),
            args: shown.clone(),
            slot,
        });
        let inner = Ctx {
            module: m,
            shape: display.clone(),
            shape_name: Some(local),
            env: Arc::new(env),
            template: true,
        };
        self.depth += 1;
        let body = self.block(&inner, &decl.body, None);
        self.depth -= 1;
        let (severity, messages) = self.annotation_values(&decl.annotations);
        self.schema.arena.set(
            slot,
            Shape::Annotated {
                severity,
                messages,
                shape: body,
            },
        );
        self.schema.add_origin(
            slot,
            Origin::new(
                OriginKind::MacroCall {
                    name: format!("{q}{}", decl.name),
                    args: shown.clone(),
                },
                self.loc(ctx.module, span),
            ),
        );
        if let Some(doc) = &decl.annotations.doc {
            self.schema
                .docs
                .insert(slot, format!("{doc} ({})", display));
        }
        Some(slot)
    }

    fn boolean(
        &mut self,
        ctx: &Ctx,
        _l: &Line,
        b: &ast::Boolean,
        owner: Option<(usize, usize)>,
    ) -> Option<ShapeId> {
        let mark = self.schema.arena.len() as u32;
        let branches: Vec<ShapeId> = b
            .blocks
            .iter()
            .map(|blk| self.block(ctx, &blk.lines, owner))
            .collect();
        let a = &mut self.schema.arena;
        let (frame, slot) = match b.frame {
            Frame::AllOf => (shifty_algebra::BooleanFrame::AllOf, a.and(branches.clone())),
            Frame::AnyOf => (shifty_algebra::BooleanFrame::AnyOf, a.or(branches.clone())),
            Frame::ExactlyOneOf => (
                shifty_algebra::BooleanFrame::ExactlyOneOf,
                a.xone(branches.clone()),
            ),
            Frame::NoneOf => {
                let o = a.or(branches.clone());
                (shifty_algebra::BooleanFrame::NoneOf, a.not(o))
            }
            Frame::When => {
                let nc = a.not(branches[0]);
                (
                    shifty_algebra::BooleanFrame::When,
                    a.or(vec![nc, branches[1]]),
                )
            }
            Frame::AtLeast(n) => (
                shifty_algebra::BooleanFrame::AtLeast(n),
                threshold(a, &branches, n as usize),
            ),
        };
        if slot.0 >= mark && !branches.contains(&slot) && self.schema.names_of(slot).is_empty() {
            self.schema.add_origin(
                slot,
                Origin::new(
                    OriginKind::Frame {
                        frame,
                        branches: branches.clone(),
                    },
                    self.loc(
                        ctx.module,
                        b.blocks.first().map_or(Span::default(), |b| b.span),
                    ),
                ),
            );
        }
        Some(slot)
    }

    // ------------------------------------------------------------ admission

    fn label(&self, id: ShapeId) -> Option<String> {
        if let Some(d) = self.display.get(&id) {
            return Some(d.clone());
        }
        for o in self.schema.origins_of(id) {
            match &o.kind {
                OriginKind::Field { text, .. } => return Some(format!("`{text}`")),
                OriginKind::Frame { frame, .. } => return Some(frame.phrase()),
                _ => {}
            }
        }
        if let Some(n) = self.schema.name_of(id) {
            return Some(self.schema.prefixes.compact(n));
        }
        None
    }

    fn admission(&mut self) {
        let refused = shifty_opt::refused_components(&self.schema.arena);
        for comp in refused {
            // Start the loop at a named shape so it reads in author terms.
            let mut cycle = comp.cycle.clone();
            if let Some(pos) = cycle
                .iter()
                .position(|e| self.display.contains_key(&e.from))
            {
                cycle.rotate_left(pos);
            }
            let start = cycle
                .first()
                .map(|e| e.from)
                .or(comp.shapes.first().copied());
            let Some(start) = start else { continue };
            let name = self.label(start).unwrap_or_else(|| format!("@{}", start.0));
            let mut path = name.clone();
            for e in &cycle {
                if e.polarity == shifty_opt::Polarity::Negative {
                    let what = match self.schema.arena.get(e.from) {
                        Shape::Not(_) => "(not)",
                        Shape::Count { .. } => "(upper bound)",
                        _ => "(negative)",
                    };
                    path.push_str(&format!("  {what}"));
                }
                if let Some(l) = self.label(e.to) {
                    path.push_str(&format!("  -->  {l}"));
                }
            }
            let negatives = cycle
                .iter()
                .filter(|e| e.polarity == shifty_opt::Polarity::Negative)
                .count();
            let loc = self
                .schema
                .origins_of(start)
                .iter()
                .find_map(|o| o.location.clone());
            let mut d = Diagnostic::error(
                format!(
                    "shape `{}` has no consistent meaning",
                    name.trim_matches('`')
                ),
                loc,
            );
            d.notes.push(path);
            d.notes.push(format!(
                "this loop passes through {negatives} negation{} (an odd number); a node could be neither valid nor invalid",
                if negatives == 1 { "" } else { "s" }
            ));
            d.hint = Some(
                "recursion through `none of`, `at most ... is matches`, or a `when` condition must be balanced; `each matches` is always safe".into(),
            );
            self.diags.push(d);
        }
        let analysis = shifty_opt::analyze(&self.schema.arena);
        self.portability.recursive = analysis.recursive().next().is_some();
    }
}

/// `at least n of {B_0} ... {B_{k-1}}` as the memoized dynamic program
/// `T(i, j) = (B_i ∧ T(i+1, j-1)) ∨ T(i+1, j)` over shared slots: O(k·n)
/// terms forming a DAG, never k-choose-n.
fn threshold(a: &mut shifty_algebra::ShapeArena, branches: &[ShapeId], n: usize) -> ShapeId {
    let k = branches.len();
    if n == 0 {
        return a.top();
    }
    if n > k {
        return a.bottom();
    }
    let mut memo: HashMap<(usize, usize), ShapeId> = HashMap::new();
    fn t(
        a: &mut shifty_algebra::ShapeArena,
        b: &[ShapeId],
        memo: &mut HashMap<(usize, usize), ShapeId>,
        i: usize,
        j: usize,
    ) -> ShapeId {
        if let Some(s) = memo.get(&(i, j)) {
            return *s;
        }
        let k = b.len();
        let s = if j == 0 {
            a.top()
        } else if k - i == j {
            a.and(b[i..].to_vec())
        } else {
            let rest = t(a, b, memo, i + 1, j - 1);
            let take = a.and(vec![b[i], rest]);
            let skip = t(a, b, memo, i + 1, j);
            a.or(vec![take, skip])
        };
        memo.insert((i, j), s);
        s
    }
    t(a, branches, &mut memo, 0, n)
}

fn number_literal(n: &Number) -> Literal {
    let dt = if n.is_integer() {
        "integer"
    } else if n.lexical.contains(['e', 'E']) {
        "double"
    } else {
        "decimal"
    };
    Literal::new_typed_literal(
        n.lexical.clone(),
        NamedNode::new_unchecked(format!("{XSD}{dt}")),
    )
}

/// FNV-1a, rendered as 8 hex digits: stable across builds and platforms.
pub fn stable_hash(s: &str) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{:08x}", (h >> 32) as u32 ^ (h as u32))
}

fn collect_field_names(lines: &[Line], out: &mut Vec<(String, Span)>) {
    for l in lines {
        match &l.kind {
            LineKind::Field(f) => {
                if let Some(n) = &f.name {
                    out.push((n.clone(), l.span));
                }
            }
            LineKind::Boolean(b) => {
                for blk in &b.blocks {
                    collect_field_names(&blk.lines, out);
                }
            }
            _ => {}
        }
    }
}

/// Walks a template body recording how each parameter is used.
struct SortVisitor<'a, 'b, 'p> {
    b: &'a Builder<'p>,
    m: usize,
    params: &'a HashSet<&'b str>,
    uses: &'a mut HashMap<String, Vec<(Sort, Span)>>,
}

impl SortVisitor<'_, '_, '_> {
    fn use_(&mut self, name: &str, sort: Sort, span: Span) {
        if self.params.contains(name) {
            self.uses
                .entry(name.to_string())
                .or_default()
                .push((sort, span));
        }
    }

    fn node(&mut self, n: &NodeRef, span: Span) {
        if let NodeRef::Name(x) = n {
            self.use_(x, Sort::Node, span);
        }
    }

    fn value(&mut self, v: &Value, span: Span) {
        if let Value::Node(n) = v {
            self.node(n, span);
        }
    }

    fn int(&mut self, i: &IntArg, span: Span) {
        if let IntArg::Param(p) = i {
            self.use_(p, Sort::Int, span);
        }
    }

    fn path(&mut self, p: &PathExpr, span: Span) {
        match p {
            PathExpr::Node(NodeRef::Name(x)) => self.use_(x, Sort::Path, span),
            PathExpr::Node(_) | PathExpr::SelfPath => {}
            PathExpr::Inverse(i)
            | PathExpr::Star(i)
            | PathExpr::Plus(i)
            | PathExpr::Optional(i) => self.path(i, span),
            PathExpr::Seq(ps) | PathExpr::Alt(ps) => {
                for q in ps {
                    self.path(q, span);
                }
            }
        }
    }

    fn call(&mut self, c: &Call) {
        if self.params.contains(c.name.as_str()) {
            self.use_(&c.name, Sort::Shape, c.span);
        }
        let callee = self.b.lookup_in_module(self.m, &c.name, true);
        let sorts = match callee {
            Some(Resolved::Template(m, d)) => self.b.sorts.get(&(m, d)).cloned(),
            _ => None,
        };
        for (i, a) in c.args.iter().enumerate() {
            let expected = sorts.as_ref().and_then(|s| s.get(i).copied());
            match a {
                Arg::Node(NodeRef::Name(x)) => {
                    if let Some(s) = expected
                        && s != Sort::Any
                    {
                        self.use_(x, s, c.span);
                    }
                }
                Arg::Path(p) => self.path(p, c.span),
                Arg::Call(inner) => self.call(inner),
                _ => {}
            }
        }
    }

    fn test(&mut self, t: &Test, span: Span) {
        match t {
            Test::Class(n) => self.node(n, span),
            Test::Matches(c) => self.call(c),
            Test::Is(v) => self.value(v, span),
            Test::OneOf(vs) => {
                for v in vs {
                    self.value(v, span);
                }
            }
            Test::Datatype(DatatypeRef::Node(n)) => self.node(n, span),
            _ => {}
        }
    }

    fn line(&mut self, l: &Line) {
        let span = l.span;
        match &l.kind {
            LineKind::Field(f) => {
                self.path(&f.path, span);
                if let Some(c) = &f.count {
                    match c {
                        Count::Exactly(n) | Count::AtLeast(n) | Count::AtMost(n) => {
                            self.int(n, span)
                        }
                        Count::Between(a, b) => {
                            self.int(a, span);
                            self.int(b, span);
                        }
                        _ => {}
                    }
                }
                for q in &f.qualifiers {
                    match q {
                        Qualifier::Each(t) => self.test(t, span),
                        Qualifier::Qualified { n, test, .. } => {
                            self.int(n, span);
                            self.test(test, span);
                        }
                        Qualifier::SameAs(n)
                        | Qualifier::DifferentFrom(n)
                        | Qualifier::LessThan(n)
                        | Qualifier::AtMostProperty(n) => self.node(n, span),
                        Qualifier::UniqueLanguage => {}
                    }
                }
            }
            LineKind::Include(c) => self.call(c),
            LineKind::Boolean(b) => {
                for blk in &b.blocks {
                    for l in &blk.lines {
                        self.line(l);
                    }
                }
            }
            LineKind::Closed => {}
        }
    }
}
