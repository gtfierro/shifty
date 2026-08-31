//! Shape-map builder: typed key -> value bindings, one level above the
//! evidence trees.
//!
//! This is the C++ SDK port of `python/shifty/shapemap.py` (and the term
//! model of `python/shifty/terms.py`), operating on the same serialized run
//! an `EvidenceRun` carries so the two bindings observe identical semantics:
//! the run JSON is parsed into [`serde_json::Value`], the Python readers are
//! ported onto it, and only the pieces Python reaches through its
//! the prepared evaluator (`binding_names`, `shape_name_of`, `materialize_constraint`
//! / `evidence_for`, `resolve_path`) are supplied by the caller as closures.
//!
//! One difference from Python is deliberate: Python materializes the passing
//! keys of failing foci and the `value_paths` annotations lazily, on first
//! read. C++ materializes both eagerly at build time so a `ShapeMap` is a
//! plain value that never needs its session to outlive it.

use serde_json::{Map as JsonMap, Value};
use std::collections::{HashMap, HashSet};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

/// Evidence node kinds that contribute no binding of their own — pure
/// conjunction/disjunction containers that readers recurse through.
const TRANSPARENT: [&str; 4] = ["all_held", "any_held", "all", "any"];

// ── terms (port of python/shifty/terms.py) ─────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TermKind {
    Iri,
    Literal,
    BNode,
}

/// A typed RDF term: the components the C ABI hands out, plus an N-Triples
/// rendering matching `terms.py` (`<iri>`, `"lit"@lang`, `"lit"^^<dt>`, with
/// `xsd:string` datatypes omitted and lexical escapes applied).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TermInfo {
    pub kind: TermKind,
    pub value: String,
    pub datatype: Option<String>,
    pub language: Option<String>,
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

impl TermInfo {
    pub fn n3(&self) -> String {
        match self.kind {
            TermKind::Iri => return format!("<{}>", self.value),
            TermKind::BNode => return format!("_:{}", self.value),
            TermKind::Literal => {}
        }
        let escaped = escape(&self.value);
        if let Some(language) = &self.language {
            return format!("\"{escaped}\"@{language}");
        }
        if let Some(datatype) = &self.datatype
            && datatype != XSD_STRING
        {
            return format!("\"{escaped}\"^^<{datatype}>");
        }
        format!("\"{escaped}\"")
    }
}

/// Decode the SPARQL-JSON term encoding evidence trees use
/// (`{"type": "uri"|"bnode"|"literal", "value": …, "datatype"?: …,
/// "xml:lang"?: …}`).
fn term_from_json(term: &Value) -> TermInfo {
    let value = term
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    match term.get("type").and_then(Value::as_str) {
        Some("uri") => TermInfo {
            kind: TermKind::Iri,
            value,
            datatype: None,
            language: None,
        },
        Some("bnode") => TermInfo {
            kind: TermKind::BNode,
            value,
            datatype: None,
            language: None,
        },
        _ => TermInfo {
            kind: TermKind::Literal,
            value,
            datatype: term
                .get("datatype")
                .and_then(Value::as_str)
                .map(str::to_string),
            language: term
                .get("xml:lang")
                .or_else(|| term.get("lang"))
                .and_then(Value::as_str)
                .map(str::to_string),
        },
    }
}

/// The local name of an IRI: the segment after the last '#', '/', or ':'.
fn local(iri: &str) -> &str {
    for sep in ['#', '/', ':'] {
        if let Some((_, tail)) = iri.rsplit_once(sep)
            && !tail.is_empty()
        {
            return tail;
        }
    }
    iri
}

// ── paths (the serde spelling of shifty_algebra::Path) ─────────────────────────

/// The externally-tagged serde encoding of `shifty_algebra::Path` is
/// `"Id"` or `{"Pred": {"value": …}}` / `{"Inverse": …}` / `{"Seq": […]}` /
/// `{"Alt": […]}` / `{"Star": …}`. `path` may be JSON `null`.
fn path_tag(path: &Value) -> Option<&str> {
    match path {
        Value::String(tag) => Some(tag.as_str()),
        Value::Object(map) => map.keys().next().map(String::as_str),
        _ => None,
    }
}

/// The predicate IRI of a `Pred` path node, if it is one.
fn pred_iri(path: &Value) -> Option<&str> {
    path.get("Pred")
        .and_then(|pred| pred.get("value"))
        .and_then(Value::as_str)
}

/// True for the `rdf:type/rdfs:subClassOf*` shape of a class-membership path.
fn is_class_path(path: Option<&Value>) -> bool {
    let Some(Value::Object(map)) = path else {
        return false;
    };
    let Some(Value::Array(parts)) = map.get("Seq") else {
        return false;
    };
    parts.len() == 2
        && path_tag(&parts[0]) == Some("Pred")
        && pred_iri(&parts[0]) == Some(RDF_TYPE)
        && path_tag(&parts[1]) == Some("Star")
        && path_tag(parts[1].get("Star").unwrap_or(&Value::Null)) == Some("Pred")
        && pred_iri(parts[1].get("Star").unwrap_or(&Value::Null)) == Some(RDFS_SUBCLASS)
}

/// Join path children with a separator, or an empty string when the body is
/// not an array.
fn join_paths(body: &Value, sep: &str, compact: bool) -> String {
    body.as_array()
        .map(|parts| {
            parts
                .iter()
                .map(|part| path_str(part, compact))
                .collect::<Vec<_>>()
                .join(sep)
        })
        .unwrap_or_default()
}

fn path_str(path: &Value, compact: bool) -> String {
    match path {
        Value::String(tag) if tag == "Id" => "id".to_string(),
        Value::Object(map) => {
            let (tag, body) = map.iter().next().expect("path object is non-empty");
            match tag.as_str() {
                "Pred" => {
                    let iri = body.get("value").and_then(Value::as_str).unwrap_or("");
                    if compact {
                        local(iri).to_string()
                    } else {
                        format!("<{iri}>")
                    }
                }
                "Inverse" => format!("^{}", path_str(body, compact)),
                "Star" => format!("{}*", path_str(body, compact)),
                "Seq" => {
                    // `rdf:type/rdfs:subClassOf*` is class membership; render
                    // it like Turtle.
                    if is_class_path(Some(path)) {
                        "a".to_string()
                    } else {
                        join_paths(body, "/", compact)
                    }
                }
                "Alt" => join_paths(body, "|", compact),
                _ => String::new(),
            }
        }
        _ => String::new(),
    }
}

// ── qualifiers ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QualifierInfo {
    /// `sh:class C` / class-membership.
    Cls(String),
    /// `sh:hasValue` / `TestConst`.
    Const(TermInfo),
    /// `sh:datatype` (`TestType`).
    Datatype(String),
    /// `sh:node <named shape>`.
    ShapeRef(String),
}

fn qualifier_local(qualifier: &QualifierInfo) -> String {
    match qualifier {
        QualifierInfo::Cls(iri) | QualifierInfo::Datatype(iri) | QualifierInfo::ShapeRef(iri) => {
            local(iri).to_string()
        }
        QualifierInfo::Const(term) => match term.kind {
            TermKind::Iri => local(&term.value).to_string(),
            TermKind::Literal => term.value.clone(),
            TermKind::BNode => term.n3(),
        },
    }
}

// ── source constraint catalog ─────────────────────────────────────────────────

/// One side (source or normalized) of a run's constraint catalog, keyed by
/// record id and holding the `Shape` JSON each id resolves to.
struct Catalog {
    by_id: HashMap<u32, Value>,
}

impl Catalog {
    fn new(records: &Value) -> Self {
        let mut by_id = HashMap::new();
        if let Some(records) = records.as_array() {
            for record in records {
                if let (Some(id), Some(constraint)) = (
                    record.get("id").and_then(Value::as_u64),
                    record.get("constraint"),
                ) {
                    by_id.insert(id as u32, constraint.clone());
                }
            }
        }
        Catalog { by_id }
    }

    fn get(&self, id: Option<u32>) -> Option<&Value> {
        id.and_then(|id| self.by_id.get(&id))
    }

    /// Follow `Annotated` wrappers down to the logical constraint id.
    fn unwrap(&self, id: Option<u32>) -> Option<u32> {
        let mut seen = HashSet::new();
        let mut id = id;
        while let Some(current) = id {
            if !seen.insert(current) {
                break;
            }
            match self.by_id.get(&current) {
                Some(Value::Object(map)) if map.contains_key("Annotated") => {
                    id = map["Annotated"]["shape"].as_u64().map(|v| v as u32);
                }
                _ => break,
            }
        }
        id
    }

    fn logical(&self, id: Option<u32>) -> Option<&Value> {
        self.get(self.unwrap(id))
    }

    /// Like [`Self::unwrap`], but checks `shape_name_of` at *every* wrapper
    /// along the way, not just the outermost. A blank `sh:qualifiedValueShape`
    /// whose sole content is `sh:node <named>` doubly-wraps: the blank node's
    /// own `Annotated` wraps the *named* shape's `Annotated` directly, so a
    /// plain outermost-only check would unwrap straight past the name.
    /// Returns `(name, final_unwrapped_id)`.
    fn unwrap_checking_names(
        &self,
        shape_name_of: &dyn Fn(u32) -> Option<String>,
        id: Option<u32>,
    ) -> (Option<String>, Option<u32>) {
        let mut seen = HashSet::new();
        let mut current = id;
        while let Some(cid) = current {
            if !seen.insert(cid) {
                break;
            }
            if let Some(name) = shape_name_of(cid) {
                return (Some(name), current);
            }
            match self.by_id.get(&cid) {
                Some(Value::Object(map)) if map.contains_key("Annotated") => {
                    current = map["Annotated"]["shape"].as_u64().map(|v| v as u32);
                }
                _ => break,
            }
        }
        (None, current)
    }
}

/// The serde variant tag of a `Shape`, lowercased — the fallback kind tag for
/// pathless keys.
fn kind_tag(constraint: Option<&Value>) -> String {
    match constraint {
        Some(Value::Object(map)) => map
            .keys()
            .next()
            .map(|tag| tag.to_lowercase())
            .unwrap_or_default(),
        Some(Value::String(tag)) => tag.to_lowercase(),
        _ => "none".to_string(),
    }
}

/// The `Qualifier` a count qualifier demands, when one is evident. Handles the
/// common encodings: `sh:class` (class-membership count over a `TestConst`),
/// `sh:hasValue`/`TestConst`, `sh:datatype`/`TestType`, a `Not` from the
/// ∀-encoding, and a conjunction whose first labeled child wins. When
/// `shape_name_of` resolves the qualifier's own (possibly `Not`-wrapped) id to
/// a named shape, that name wins as a `ShapeRef` — a `sh:node <named-shape>`
/// reference names itself, regardless of what it expands to.
fn qualifier_from_json(
    catalog: &Catalog,
    shape_name_of: &dyn Fn(u32) -> Option<String>,
    qualifier_id: Option<u32>,
) -> Option<QualifierInfo> {
    let mut lookup_id = qualifier_id;
    // `∀π.φ ≡ ∃≤0 π.¬φ`: the qualifier label may sit under a `Not`.
    if let Some(Value::Object(map)) = catalog.get(qualifier_id)
        && map.contains_key("Not")
    {
        lookup_id = map["Not"].as_u64().map(|v| v as u32);
    }
    let (name, unwrapped) = catalog.unwrap_checking_names(shape_name_of, lookup_id);
    if let Some(name) = name {
        return Some(QualifierInfo::ShapeRef(name));
    }
    let constraint = catalog.get(unwrapped)?;
    let Value::Object(map) = constraint else {
        return None;
    };
    if let Some(test_const) = map.get("TestConst") {
        return Some(QualifierInfo::Const(term_from_json(test_const)));
    }
    if let Some(test_type) = map.get("TestType") {
        if let Some(datatype) = test_type.get("Datatype") {
            return datatype
                .get("value")
                .and_then(Value::as_str)
                .map(|value| QualifierInfo::Datatype(value.to_string()));
        }
        return None;
    }
    if let Some(count) = map.get("Count") {
        if is_class_path(count.get("path")) {
            let inner = qualifier_from_json(
                catalog,
                shape_name_of,
                count
                    .get("qualifier")
                    .and_then(Value::as_u64)
                    .map(|v| v as u32),
            );
            if let Some(QualifierInfo::Const(term)) = &inner
                && term.kind == TermKind::Iri
            {
                return Some(QualifierInfo::Cls(term.value.clone()));
            }
            return inner;
        }
        return None;
    }
    if map.contains_key("And") || map.contains_key("Or") {
        if let Some(children) = map
            .get("And")
            .or_else(|| map.get("Or"))
            .and_then(Value::as_array)
        {
            for child in children {
                if let Some(found) =
                    qualifier_from_json(catalog, shape_name_of, child.as_u64().map(|v| v as u32))
                {
                    return Some(found);
                }
            }
        }
        return None;
    }
    None
}

/// The ingredients of a key, derived from the *source* constraint a progress
/// child (or statement) names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyInfo {
    pub path: Option<Value>,
    pub qualifier: Option<QualifierInfo>,
    pub kind: String,
}

fn derive_key_info(
    catalog: &Catalog,
    shape_name_of: &dyn Fn(u32) -> Option<String>,
    source_id: u32,
) -> KeyInfo {
    let constraint = catalog.logical(Some(source_id));
    if let Some(Value::Object(map)) = constraint {
        if let Some(count) = map.get("Count") {
            let path = count.get("path").cloned();
            let qualifier = qualifier_from_json(
                catalog,
                shape_name_of,
                count
                    .get("qualifier")
                    .and_then(Value::as_u64)
                    .map(|v| v as u32),
            );
            return KeyInfo {
                path,
                qualifier,
                kind: "count".to_string(),
            };
        }
        let is_and = map.contains_key("And");
        if is_and || map.contains_key("Or") {
            let infos: Vec<KeyInfo> = map
                .get("And")
                .or_else(|| map.get("Or"))
                .and_then(Value::as_array)
                .map(|children| {
                    children
                        .iter()
                        .filter_map(Value::as_u64)
                        .map(|id| derive_key_info(catalog, shape_name_of, id as u32))
                        .collect()
                })
                .unwrap_or_default();
            let paths: HashSet<&Value> =
                infos.iter().filter_map(|info| info.path.as_ref()).collect();
            if paths.len() == 1 {
                let path = paths.into_iter().next().cloned();
                let qualifier = infos.iter().find_map(|info| info.qualifier.clone());
                return KeyInfo {
                    path,
                    qualifier,
                    kind: "count".to_string(),
                };
            }
            return KeyInfo {
                path: None,
                qualifier: None,
                kind: if is_and { "and" } else { "or" }.to_string(),
            };
        }
    }
    KeyInfo {
        path: None,
        qualifier: None,
        kind: kind_tag(constraint),
    }
}

/// The rendered key: `path→qualifier`, or the kind tag for a pathless key,
/// with an `#N` ordinal disambiguator when several keys share a
/// `(path, qualifier, kind)`.
pub fn key_str(info: &KeyInfo, ordinal: u32) -> String {
    let base = match &info.path {
        Some(path) => {
            let rendered = path_str(path, true);
            match &info.qualifier {
                Some(qualifier) => format!("{rendered}→{}", qualifier_local(qualifier)),
                None => rendered,
            }
        }
        None => info.kind.clone(),
    };
    if ordinal > 1 {
        format!("{base}#{ordinal}")
    } else {
        base
    }
}

/// `(min, max)` from the *source* constraint tree, through `And` containers
/// only — the collapsed-datatype-plus-minCount case takes the tightest bounds
/// across the conjuncts.
fn collect_bounds(catalog: &Catalog, constraint_id: Option<u32>) -> (Option<u64>, Option<u64>) {
    let constraint = catalog.logical(constraint_id);
    if let Some(Value::Object(map)) = constraint {
        if let Some(count) = map.get("Count") {
            let qualifier_body = count
                .get("qualifier")
                .and_then(Value::as_u64)
                .and_then(|id| catalog.get(Some(id as u32)));
            if let Some(Value::Object(qualifier_map)) = qualifier_body {
                // The `∀π.φ ≡ ∃≤0 π.¬φ` encoding: its `max=0` describes
                // counterexamples to `φ`, not the property's real cardinality.
                if qualifier_map.contains_key("Not") {
                    return (None, None);
                }
            }
            return (
                count.get("min").and_then(Value::as_u64),
                count.get("max").and_then(Value::as_u64),
            );
        }
        if let Some(children) = map.get("And").and_then(Value::as_array) {
            let mut mins = Vec::new();
            let mut maxs = Vec::new();
            for child in children {
                if let Some(id) = child.as_u64() {
                    let (cmin, cmax) = collect_bounds(catalog, Some(id as u32));
                    if let Some(cmin) = cmin {
                        mins.push(cmin);
                    }
                    if let Some(cmax) = cmax {
                        maxs.push(cmax);
                    }
                }
            }
            return (mins.into_iter().max(), maxs.into_iter().min());
        }
    }
    (None, None)
}

// ── evidence-tree readers (ported from shapemap.py onto the run JSON) ─────────

fn details_map(node: &Value) -> Option<&JsonMap<String, Value>> {
    node.get("details").and_then(Value::as_object)
}

fn node_type(node: &Value) -> Option<&str> {
    node.get("type").and_then(Value::as_str)
}

fn direct_children(node: &Value) -> Vec<&Value> {
    let Some(details) = details_map(node) else {
        return Vec::new();
    };
    for field in ["children", "failed", "branches", "satisfied"] {
        if let Some(children) = details.get(field).and_then(Value::as_array) {
            return children.iter().collect();
        }
    }
    Vec::new()
}

/// The values bound at the *top level* of an evidence subtree: what the
/// property's own path matched, without descending into nested qualifier
/// checks (whose matches are class/type terms, not bindings).
/// The `term_from_json` of each entry in a JSON array, deduplicated in order.
fn terms_from(items: &[Value], field: Option<&str>) -> Vec<TermInfo> {
    let mut out: Vec<TermInfo> = Vec::new();
    for item in items {
        let entry = match field {
            None => item.get(0),
            Some(field) => item.get(field),
        };
        let term = entry.map(term_from_json);
        if let Some(term) = term
            && !out.contains(&term)
        {
            out.push(term);
        }
    }
    out
}

fn top_values(node: Option<&Value>) -> Vec<TermInfo> {
    let Some(node) = node else { return Vec::new() };
    let kind = node_type(node).unwrap_or("");
    let details = details_map(node);

    let found: Vec<TermInfo> = match kind {
        "count_held" => details
            .and_then(|d| d.get("matches"))
            .and_then(Value::as_array)
            .map_or_else(Vec::new, |items| terms_from(items, None)),
        "for_all_held" => details
            .and_then(|d| d.get("values"))
            .and_then(Value::as_array)
            .map_or_else(Vec::new, |items| terms_from(items, None)),
        "count_low" => details
            .and_then(|d| d.get("qualifying_matches"))
            .and_then(Value::as_array)
            .map_or_else(Vec::new, |items| terms_from(items, Some("value"))),
        "count_high" => details
            .and_then(|d| d.get("matched"))
            .and_then(Value::as_array)
            .map_or_else(Vec::new, |items| terms_from(items, None)),
        _ if TRANSPARENT.contains(&kind) => direct_children(node)
            .into_iter()
            .flat_map(|child| top_values(Some(child)))
            .collect(),
        _ => Vec::new(),
    };
    found
}

/// The subtree's own count nodes: reached through AND/OR containers only,
/// never through a nested qualifier trace (whose counts describe a *value*,
/// not this binding).
fn top_counts<'a>(node: Option<&'a Value>, out: &mut Vec<&'a Value>) {
    let Some(node) = node else { return };
    let kind = node_type(node).unwrap_or("");
    if TRANSPARENT.contains(&kind) {
        for child in direct_children(node) {
            top_counts(Some(child), out);
        }
    } else if matches!(
        kind,
        "count_low" | "count_high" | "count_held" | "for_all_held"
    ) {
        out.push(node);
    }
}

fn missing_count(node: Option<&Value>) -> u64 {
    let mut counts = Vec::new();
    top_counts(node, &mut counts);
    counts
        .iter()
        .filter(|node| node_type(node).unwrap_or("") == "count_low")
        .map(|node| {
            let details = details_map(node);
            let min = details
                .and_then(|d| d.get("min"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let have = details
                .and_then(|d| d.get("have"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            min.saturating_sub(have)
        })
        .sum()
}

fn observed_count(node: Option<&Value>) -> Option<u64> {
    let mut counts = Vec::new();
    top_counts(node, &mut counts);
    for node in counts {
        let details = details_map(node);
        match node_type(node).unwrap_or("") {
            "count_held" => {
                if let Some(observed) = details
                    .and_then(|d| d.get("observed_count"))
                    .and_then(Value::as_u64)
                {
                    return Some(observed);
                }
            }
            "count_low" => {
                if let Some(have) = details.and_then(|d| d.get("have")).and_then(Value::as_u64) {
                    return Some(have);
                }
            }
            _ => {}
        }
    }
    None
}

fn rejected_values(node: Option<&Value>) -> Vec<TermInfo> {
    let mut counts = Vec::new();
    top_counts(node, &mut counts);
    let mut out: Vec<TermInfo> = Vec::new();
    for node in counts {
        if let Some(candidates) = details_map(node)
            .and_then(|d| d.get("rejected_candidates"))
            .and_then(Value::as_array)
        {
            for candidate in candidates {
                if let Some(value) = candidate.get("value") {
                    let term = term_from_json(value);
                    if !out.contains(&term) {
                        out.push(term);
                    }
                }
            }
        }
    }
    out
}

// ── public objects ─────────────────────────────────────────────────────────────

/// One bound value's `value_paths` annotations for one label: the bound term
/// and the terms `path` reaches from it over the session's evaluation graph.
pub struct AnnotationGroup {
    pub label: String,
    pub entries: Vec<(TermInfo, Vec<TermInfo>)>,
}

/// One key of a mapping: a property obligation and what it bound to.
pub struct ShapeMapBinding {
    pub key: String,
    pub key_path_json: String,
    pub qualifier: Option<QualifierInfo>,
    pub ordinal: u32,
    pub kind: String,
    /// `"pass"` (bound) or `"fail"` (unbound).
    pub status: String,
    pub names: Vec<String>,
    pub min: Option<u64>,
    pub max: Option<u64>,
    pub observed: Option<u64>,
    pub missing: u64,
    /// The values the key's path bound; for a failing key, the qualifying
    /// near-matches.
    pub values: Vec<TermInfo>,
    /// Near-miss candidates the path reached but the qualifier rejected.
    pub rejected_values: Vec<TermInfo>,
    /// `value_paths` annotations, one group per label.
    pub annotations: Vec<AnnotationGroup>,
}

impl ShapeMapBinding {
    pub fn ok(&self) -> bool {
        self.status == "pass"
    }

    pub fn name(&self) -> Option<&str> {
        self.names.first().map(String::as_str)
    }
}

/// One `(focus node, shape statement)` association with its key bindings.
pub struct ShapeMapMapping {
    pub focus: String,
    pub shape_name: String,
    pub target: String,
    pub conforms: bool,
    pub bindings: Vec<ShapeMapBinding>,
}

/// All mappings of one shape identity (a named shape, or a
/// `_:statement-N` placeholder for an anonymous one).
pub struct ShapeMapShape {
    pub name: String,
    pub mappings: Vec<ShapeMapMapping>,
}

/// The full shape map: key -> value bindings for every selected
/// `(shape, focus)` pair of a run.
pub struct ShapeMapData {
    pub conforms: bool,
    pub shapes: Vec<ShapeMapShape>,
    pub json: String,
}

/// A `{"type": "irrefutable", "details": {}}` subtree for a constraint that
/// normalized away as trivially true: bound, with nothing to show.
fn irrefutable_node() -> Value {
    serde_json::json!({ "type": "irrefutable", "details": {} })
}

/// The session-backed pieces the builder cannot derive from the run alone.
type BindingNamesFn<'a> = dyn Fn(Option<&str>) -> Result<HashMap<u32, Vec<String>>, String> + 'a;
type BindingValuesFn<'a> = dyn Fn(&str, u32) -> Result<Vec<TermInfo>, String> + 'a;
type MaterializeConstraintFn<'a> = dyn Fn(&str, u32) -> Result<Option<Value>, String> + 'a;
type ResolvePathFn<'a> =
    dyn Fn(&[String], &str) -> Result<HashMap<String, Vec<String>>, String> + 'a;

pub struct ShapeMapBuildInputs<'a> {
    /// The raw schema's shape name for a source constraint id.
    pub shape_name_of: &'a dyn Fn(u32) -> Option<String>,
    /// `name_path` -> constraint id -> reached names over the shapes graph.
    pub binding_names: &'a BindingNamesFn<'a>,
    /// Values for an authored property constraint normalized away as `Top`.
    pub binding_values: &'a BindingValuesFn<'a>,
    /// Materialize evidence for one `(focus, normalized constraint)` pair;
    /// returns the satisfaction trace as JSON (`None` when the constraint is
    /// not a normalized arena id).
    pub materialize_constraint: &'a MaterializeConstraintFn<'a>,
    /// Batch-evaluate a path from N-Triples nodes over the session's graph.
    pub resolve_path: &'a ResolvePathFn<'a>,
}

/// Per-focus metadata parallel to the run JSON.
pub struct FocusMeta {
    pub focus: String,
}

/// Per-statement rendering from the C++ side of the ABI, parallel to the
/// `"statements"` array of the run JSON.
pub struct StatementMeta {
    pub source_statement_id: usize,
    pub source_constraint_id: u32,
    pub normalized_constraint_id: Option<u32>,
    pub target: String,
    pub foci: Vec<FocusMeta>,
}

/// Build the shape map from a parsed run. `name_path` (`None` to skip) names
/// each slot from the shapes graph; `value_paths` annotates each bound value
/// from the data graph, resolved in one batched call per label.
pub fn build(
    run: &Value,
    name_path: Option<&str>,
    value_paths: &[(String, String)],
    statements: &[StatementMeta],
    inputs: &ShapeMapBuildInputs,
) -> Result<ShapeMapData, String> {
    let conforms = run
        .get("conforms")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let source_catalog = Catalog::new(run.pointer("/constraints/source").unwrap_or(&Value::Null));
    let normalized_catalog = Catalog::new(
        run.pointer("/constraints/normalized")
            .unwrap_or(&Value::Null),
    );

    let names_table = if name_path.is_some() {
        (inputs.binding_names)(name_path)?
    } else {
        HashMap::new()
    };

    let mut shapes: Vec<ShapeMapShape> = Vec::new();
    let mut shape_index: HashMap<String, usize> = HashMap::new();

    let json_statements = run
        .get("statements")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    for (statement_index, statement) in json_statements.iter().enumerate() {
        let meta = statements
            .get(statement_index)
            .ok_or_else(|| format!("statement {statement_index} has no C++-side metadata"))?;
        let shape_name = (inputs.shape_name_of)(meta.source_constraint_id);
        let group_key = shape_name
            .clone()
            .unwrap_or_else(|| format!("_:statement-{}", meta.source_statement_id));
        let group_index = match shape_index.get(&group_key) {
            Some(&index) => index,
            None => {
                shape_index.insert(group_key.clone(), shapes.len());
                shapes.push(ShapeMapShape {
                    name: group_key,
                    mappings: Vec::new(),
                });
                shapes.len() - 1
            }
        };

        let json_foci = statement
            .get("selected_foci")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        for (focus_index, focus) in json_foci.iter().enumerate() {
            let focus_meta = meta.foci.get(focus_index).ok_or_else(|| {
                format!(
                    "focus {focus_index} of statement {statement_index} has no C++-side metadata"
                )
            })?;
            let mapping = build_mapping(
                shape_name.clone(),
                focus,
                meta,
                focus_meta,
                &source_catalog,
                &normalized_catalog,
                inputs,
                &names_table,
            )?;
            shapes[group_index].mappings.push(mapping);
        }
    }

    if !value_paths.is_empty() {
        annotate_values(&mut shapes, value_paths, inputs.resolve_path)?;
    }

    let json = to_dict_json(&shapes, conforms);
    Ok(ShapeMapData {
        conforms,
        shapes,
        json,
    })
}

/// The statement's direct children, keyed by their logical constraint id.
/// Progress children are the authored conjunction's members; a child absent
/// from a failure tree is a passing sibling the witness elided.
fn collect_subtrees<'a>(root: &'a Value, out: &mut HashMap<u32, &'a Value>) {
    if let Some(shape) = root.pointer("/details/shape").and_then(Value::as_u64) {
        out.insert(shape as u32, root);
    }
    for child in direct_children(root) {
        if let Some(shape) = child.pointer("/details/shape").and_then(Value::as_u64) {
            out.insert(shape as u32, child);
        }
    }
}

/// One authored obligation of a focus: either a progress child (a conjunction
/// member) or the statement's own constraint.
enum Entry<'a> {
    Progress { child: &'a Value },
    Statement,
}

#[allow(clippy::too_many_arguments)]
fn build_mapping(
    shape_name: Option<String>,
    focus: &Value,
    meta: &StatementMeta,
    focus_meta: &FocusMeta,
    source_catalog: &Catalog,
    normalized_catalog: &Catalog,
    inputs: &ShapeMapBuildInputs,
    names_table: &HashMap<u32, Vec<String>>,
) -> Result<ShapeMapMapping, String> {
    let conforms = focus.pointer("/evidence/status").and_then(Value::as_str) == Some("pass");
    let root = focus.pointer("/evidence/evidence").unwrap_or(&Value::Null);
    let progress = focus
        .get("progress")
        .and_then(|p| p.get("evaluated_children"))
        .and_then(Value::as_array);

    let mut subtrees: HashMap<u32, &Value> = HashMap::new();
    collect_subtrees(root, &mut subtrees);

    let mut entries: Vec<Entry> = Vec::new();
    if let Some(children) = progress {
        for child in children {
            entries.push(Entry::Progress { child });
        }
    } else {
        entries.push(Entry::Statement);
    }

    let mut bindings: Vec<ShapeMapBinding> = Vec::new();
    let mut ordinals: HashMap<(Option<Value>, Option<QualifierInfo>, String), u32> = HashMap::new();

    for entry in entries {
        let (info, status, source_id, normalized_ref, subtree): (
            KeyInfo,
            String,
            u32,
            Option<u32>,
            Option<&Value>,
        ) = match entry {
            Entry::Progress { child } => {
                let source_id = child
                    .get("source_constraint_ref")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as u32;
                let info = derive_key_info(source_catalog, inputs.shape_name_of, source_id);
                let normalized_ref = child
                    .get("normalized_constraint_ref")
                    .and_then(Value::as_u64)
                    .map(|v| v as u32);
                let logical = normalized_catalog.unwrap(normalized_ref);
                let subtree = logical.and_then(|id| subtrees.get(&id)).copied();
                let status = child
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("fail")
                    .to_string();
                (info, status, source_id, normalized_ref, subtree)
            }
            Entry::Statement => {
                let info = derive_key_info(
                    source_catalog,
                    inputs.shape_name_of,
                    meta.source_constraint_id,
                );
                let status = focus
                    .pointer("/evidence/status")
                    .and_then(Value::as_str)
                    .unwrap_or("fail")
                    .to_string();
                (
                    info,
                    status,
                    meta.source_constraint_id,
                    meta.normalized_constraint_id,
                    Some(root),
                )
            }
        };

        let dedup_key = (info.path.clone(), info.qualifier.clone(), info.kind.clone());
        let ordinal = {
            let counter = ordinals.entry(dedup_key).or_insert(0);
            *counter += 1;
            *counter
        };
        let key = key_str(&info, ordinal);

        // Materialize the subtree: the run carries it for failing keys and
        // conforming foci; a passing key of a failing focus needs a fresh
        // evaluation against the normalized constraint.
        let owned_subtree: Option<Value> = match subtree {
            Some(node) => Some(node.clone()),
            None if status == "pass" => match normalized_ref {
                None => Some(irrefutable_node()),
                Some(ref_id) => (inputs.materialize_constraint)(&focus_meta.focus, ref_id)?,
            },
            None => None,
        };

        let (min, max) = collect_bounds(source_catalog, Some(source_id));
        let names = binding_names(source_catalog, names_table, source_id);
        let values = if status == "pass"
            && source_catalog.logical(Some(source_id)) == Some(&Value::String("Top".to_string()))
        {
            (inputs.binding_values)(&focus_meta.focus, source_id)?
        } else {
            top_values(owned_subtree.as_ref())
        };
        let missing = if status == "fail" {
            missing_count(owned_subtree.as_ref())
        } else {
            0
        };
        let observed = observed_count(owned_subtree.as_ref());
        let rejected = if status == "fail" {
            rejected_values(owned_subtree.as_ref())
        } else {
            Vec::new()
        };
        let key_path_json = info.path.as_ref().map(Value::to_string).unwrap_or_default();

        bindings.push(ShapeMapBinding {
            key,
            key_path_json,
            qualifier: info.qualifier,
            ordinal,
            kind: info.kind,
            status,
            names,
            min,
            max,
            observed,
            missing,
            values,
            rejected_values: rejected,
            annotations: Vec::new(),
        });
    }

    Ok(ShapeMapMapping {
        focus: focus_meta.focus.to_string(),
        shape_name: shape_name.unwrap_or_default(),
        target: meta.target.to_string(),
        conforms,
        bindings,
    })
}

/// Names from a property's own source node, following transparent authored
/// wrappers left when a singleton conjunction is normalized away.
fn binding_names(
    catalog: &Catalog,
    names_table: &HashMap<u32, Vec<String>>,
    source_id: u32,
) -> Vec<String> {
    let mut current = Some(source_id);
    let mut seen = HashSet::new();
    while let Some(id) = current {
        if !seen.insert(id) {
            break;
        }
        if let Some(names) = names_table.get(&id)
            && !names.is_empty()
        {
            return names.clone();
        }
        current = catalog
            .get(Some(id))
            .and_then(|constraint| constraint.get("Annotated"))
            .and_then(|annotated| annotated.get("shape"))
            .and_then(Value::as_u64)
            .map(|id| id as u32);
    }
    Vec::new()
}

/// One batched `resolve_path` call per label, over every distinct bound value
/// of every mapping (Python's `_ValueAnnotationResolver`).
fn annotate_values(
    shapes: &mut [ShapeMapShape],
    value_paths: &[(String, String)],
    resolve_path: &ResolvePathFn<'_>,
) -> Result<(), String> {
    let mut nodes: Vec<String> = Vec::new();
    for shape in shapes.iter() {
        for mapping in &shape.mappings {
            for binding in &mapping.bindings {
                for value in &binding.values {
                    let n3 = value.n3();
                    if !nodes.contains(&n3) {
                        nodes.push(n3);
                    }
                }
            }
        }
    }
    nodes.sort();

    let mut cache: HashMap<String, HashMap<String, Vec<TermInfo>>> = HashMap::new();
    for (label, path) in value_paths {
        let reached = if nodes.is_empty() {
            HashMap::new()
        } else {
            resolve_path(&nodes, path)?
        };
        let mut table = HashMap::new();
        for (node, values) in reached {
            table.insert(
                node,
                values.iter().map(|value| parse_n3_term(value)).collect(),
            );
        }
        cache.insert(label.clone(), table);
    }

    for shape in shapes.iter_mut() {
        for mapping in &mut shape.mappings {
            for binding in &mut mapping.bindings {
                let mut groups = Vec::new();
                for (label, table) in &cache {
                    let mut entries = Vec::new();
                    for value in &binding.values {
                        let reached = table.get(&value.n3()).cloned().unwrap_or_default();
                        entries.push((value.clone(), reached));
                    }
                    groups.push(AnnotationGroup {
                        label: label.clone(),
                        entries,
                    });
                }
                binding.annotations = groups;
            }
        }
    }
    Ok(())
}

/// The plain-JSON summary `ShapeMap.to_dict()` produces.
fn to_dict_json(shapes: &[ShapeMapShape], conforms: bool) -> String {
    let mut shape_entries = serde_json::Map::new();
    for shape in shapes {
        let mappings: Vec<Value> = shape
            .mappings
            .iter()
            .map(|mapping| {
                let mut bindings = serde_json::Map::new();
                for binding in &mapping.bindings {
                    bindings.insert(
                        binding.key.clone(),
                        serde_json::json!({
                            "status": binding.status,
                            "values": binding.values.iter().map(TermInfo::n3).collect::<Vec<_>>(),
                            "missing": binding.missing,
                            "name": binding.name(),
                        }),
                    );
                }
                serde_json::json!({
                    "focus": mapping.focus,
                    "target": mapping.target,
                    "conforms": mapping.conforms,
                    "bindings": bindings,
                })
            })
            .collect();
        shape_entries.insert(shape.name.clone(), Value::Array(mappings));
    }
    serde_json::json!({ "conforms": conforms, "shapes": shape_entries }).to_string()
}

/// Parse one N-Triples spelling (`<…>` / `"…"` / `_:…`) back into a term.
/// The values `resolve_path` reaches are rendered by the engine, so this
/// round-trips them; falls back to the bare spelling as a literal.
fn parse_n3_term(text: &str) -> TermInfo {
    match crate::parse_term(text) {
        Ok(term) => term_from_oxrdf(&term),
        Err(_) => TermInfo {
            kind: TermKind::Literal,
            value: text.to_string(),
            datatype: None,
            language: None,
        },
    }
}

pub(crate) fn term_from_oxrdf(term: &oxrdf::Term) -> TermInfo {
    match term {
        oxrdf::Term::NamedNode(node) => TermInfo {
            kind: TermKind::Iri,
            value: node.as_str().to_string(),
            datatype: None,
            language: None,
        },
        oxrdf::Term::BlankNode(node) => TermInfo {
            kind: TermKind::BNode,
            value: node.as_str().to_string(),
            datatype: None,
            language: None,
        },
        oxrdf::Term::Literal(literal) => TermInfo {
            kind: TermKind::Literal,
            value: literal.value().to_string(),
            datatype: Some(literal.datatype().as_str().to_string()),
            language: literal.language().map(ToString::to_string),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_json_uri_decodes_as_iri() {
        let term = serde_json::json!({"type": "uri", "value": "urn:x"});
        let info = term_from_json(&term);
        assert_eq!(
            info.kind,
            TermKind::Iri,
            "expected Iri, got {:?}",
            info.kind
        );
        assert_eq!(info.n3(), "<urn:x>");
        assert_eq!(info.value, "urn:x");
    }

    #[test]
    fn term_json_literal_decodes_as_literal() {
        let term = serde_json::json!({"type": "literal", "value": "Alice", "datatype": "http://www.w3.org/2001/XMLSchema#string"});
        let info = term_from_json(&term);
        assert_eq!(info.kind, TermKind::Literal);
        assert_eq!(info.n3(), "\"Alice\"");
    }

    #[test]
    fn key_str_renders_path_qualifier_ordinal() {
        let info = KeyInfo {
            path: Some(serde_json::json!({"Pred": {"value": "https://br#hasPoint"}})),
            qualifier: Some(QualifierInfo::Cls("https://br#Sensor".to_string())),
            kind: "count".to_string(),
        };
        assert_eq!(key_str(&info, 1), "hasPoint→Sensor");
        assert_eq!(key_str(&info, 2), "hasPoint→Sensor#2");
    }

    #[test]
    fn class_membership_path_detects_a() {
        let path = serde_json::json!({"Seq": [
            {"Pred": {"value": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"}},
            {"Star": {"Pred": {"value": "http://www.w3.org/2000/01/rdf-schema#subClassOf"}}}
        ]});
        assert!(is_class_path(Some(&path)));
        assert_eq!(path_str(&path, true), "a");
    }
}
