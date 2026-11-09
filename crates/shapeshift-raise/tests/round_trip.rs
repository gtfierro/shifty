//! The raiser's correctness property: lowering the raised SHACL with
//! `shifty-parse` gives back the builder's schema, up to transparent
//! annotations and the order of conjuncts/disjuncts; and both schemas validate
//! the example data identically.

use shapeshift_import::{FsResolver, resolve_file};
use shapeshift_macro::diagnostic::render_all;
use shapeshift_macro::{BuildOutput, build, build_module};
use shapeshift_raise::{RaiseOptions, raise};
use shifty_algebra::{Schema, Selector, Severity, Shape, ShapeId};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

fn example(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/shapeshift")
        .join(name)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum C {
    Ref(String),
    Top,
    Not(Box<C>),
    And(Vec<C>),
    Or(Vec<C>),
    Count(String, Option<u64>, Option<u64>, Box<C>),
    Annot(String, Vec<String>, Box<C>),
    Leaf(String),
}

fn mk_and(parts: Vec<C>) -> C {
    let mut flat = Vec::new();
    for p in parts {
        match p {
            C::And(inner) => flat.extend(inner),
            C::Top => {}
            other => flat.push(other),
        }
    }
    flat.sort();
    flat.dedup();
    match flat.len() {
        0 => C::Top,
        1 => flat.pop().unwrap(),
        _ => C::And(flat),
    }
}

fn mk_or(parts: Vec<C>) -> C {
    let mut flat = Vec::new();
    for p in parts {
        match p {
            C::Or(inner) => flat.extend(inner),
            other => flat.push(other),
        }
    }
    flat.sort();
    flat.dedup();
    if flat.len() == 1 {
        flat.pop().unwrap()
    } else {
        C::Or(flat)
    }
}

struct Canon<'a> {
    s: &'a Schema,
    bodies: BTreeMap<String, C>,
    /// Expand named shapes in place (to compare schemas that name
    /// different intermediate shapes), stopping only at recursion.
    inline: bool,
    stack: Vec<ShapeId>,
}

impl Canon<'_> {
    fn shape(&mut self, id: ShapeId, top: bool) -> C {
        if self.inline {
            if let Some(name) = self.s.name_of(id)
                && self.stack.contains(&id)
            {
                return C::Ref(name.to_string());
            }
            self.stack.push(id);
            let c = self.shape_body(id);
            self.stack.pop();
            return c;
        }
        if !top && let Some(name) = self.s.name_of(id) {
            let name = name.to_string();
            if !self.bodies.contains_key(&name) {
                self.bodies.insert(name.clone(), C::Top);
                let body = self.shape(id, true);
                self.bodies.insert(name.clone(), body);
            }
            return C::Ref(name);
        }
        self.shape_body(id)
    }

    fn shape_body(&mut self, id: ShapeId) -> C {
        match self.s.arena.get(id).clone() {
            Shape::Annotated {
                severity,
                messages,
                shape,
            } => {
                let inner = self.shape(shape, false);
                if severity == Severity::Violation && messages.is_empty() {
                    inner
                } else {
                    C::Annot(
                        severity.label().to_string(),
                        messages.iter().map(|m| m.to_string()).collect(),
                        Box::new(inner),
                    )
                }
            }
            Shape::Top => C::Top,
            Shape::Not(c) => match self.shape(c, false) {
                C::Not(x) => *x,
                other => C::Not(Box::new(other)),
            },
            Shape::And(cs) => {
                let parts = cs.iter().map(|c| self.shape(*c, false)).collect();
                mk_and(parts)
            }
            Shape::Or(cs) => {
                let parts = cs.iter().map(|c| self.shape(*c, false)).collect();
                mk_or(parts)
            }
            Shape::Count {
                path,
                min,
                max,
                qualifier,
            } => {
                // `at least 0` is no constraint at all
                let min = min.filter(|m| *m > 0);
                if min.is_none() && max.is_none() {
                    return C::Top;
                }
                C::Count(
                    format!("{path:?}"),
                    min,
                    max,
                    Box::new(self.shape(qualifier, false)),
                )
            }
            other => C::Leaf(format!("{other:?}")),
        }
    }

    fn selector(&mut self, sel: &Selector) -> String {
        match sel {
            Selector::HasPath(p, q) => format!("HasPath({p:?}, {:?})", self.shape(*q, false)),
            other => format!("{other:?}"),
        }
    }
}

fn canonical(s: &Schema, skip: &BTreeSet<usize>) -> (BTreeSet<String>, BTreeMap<String, C>) {
    let mut c = Canon {
        s,
        bodies: BTreeMap::new(),
        inline: false,
        stack: Vec::new(),
    };
    let mut statements = BTreeSet::new();
    for (i, st) in s.statements.iter().enumerate() {
        if skip.contains(&i) {
            continue;
        }
        let sel = c.selector(&st.selector);
        let shape = c.shape(st.shape, false);
        statements.insert(format!("{sel} => {shape:?}"));
    }
    (statements, c.bodies)
}

fn check_round_trip(built: &BuildOutput) -> String {
    let raised = raise(built, RaiseOptions::default()).expect("raises");
    let ttl = raised.to_turtle();
    let lowered = shifty_parse::parse_turtle(ttl.as_bytes(), None)
        .unwrap_or_else(|e| panic!("raised Turtle does not parse: {e}\n{ttl}"));
    let errors: Vec<_> = lowered
        .diagnostics
        .iter()
        .filter(|d| d.level != shifty_parse::DiagLevel::Warning)
        .collect();
    assert!(errors.is_empty(), "lowering diagnostics: {errors:?}\n{ttl}");
    let skip: BTreeSet<usize> = built.imported_statements.iter().copied().collect();
    let (a_st, a_bodies) = canonical(&built.schema, &skip);
    let (b_st, b_bodies) = canonical(&lowered.schema, &BTreeSet::new());
    assert_eq!(a_st, b_st, "statements differ after the round trip\n{ttl}");
    for (name, body) in &a_bodies {
        assert_eq!(
            Some(body),
            b_bodies.get(name),
            "body of {name} differs after the round trip\n{ttl}"
        );
    }
    ttl
}

#[test]
fn hvac_round_trips_through_shacl() {
    let (resolved, _) = resolve_file(&example("hvac.shm"), &FsResolver::default(), true).unwrap();
    let built = build(&resolved.program);
    assert!(!built.has_errors(), "{}", render_all(&built.diagnostics));
    let ttl = check_round_trip(&built);
    println!("{ttl}");

    // Semantic check: identical violations on the example data.
    let data = std::fs::read(example("hvac-data.ttl")).unwrap();
    let data = shifty_parse::load_turtle(&data, None).unwrap().graph;
    let lowered = shifty_parse::parse_turtle(ttl.as_bytes(), None)
        .unwrap()
        .schema;
    let a = shifty_engine::validate(&data, &built.schema).unwrap();
    let b = shifty_engine::validate(&data, &lowered).unwrap();
    let foci = |o: &shifty_engine::ValidationOutcome| {
        o.violations
            .iter()
            .map(|v| v.focus.to_string())
            .collect::<BTreeSet<_>>()
    };
    assert_eq!(foci(&a), foci(&b));
    assert_eq!(a.conforms, b.conforms);
}

#[test]
fn shape_targets_and_sparql_targets_round_trip() {
    let src = r#"
base <https://example.org/t>
prefix ex: <https://example.org/>

shape Named { ex:name required, each string }
shape Labelled for every node matching Named {
  ex:label required
}
shape Odd for sparql "SELECT ?this WHERE { ?this ex:weird true }" {
  ex:reason required
}
"#;
    let parsed = shapeshift_parse::parse(src, "t.shm");
    assert!(
        parsed.diagnostics.is_empty(),
        "{}",
        render_all(&parsed.diagnostics)
    );
    let built = build_module("t.shm", &parsed.module);
    assert!(!built.has_errors(), "{}", render_all(&built.diagnostics));
    let ttl = check_round_trip(&built);
    assert!(ttl.contains("shapeshift:targetShape"), "{ttl}");
    assert!(raise(&built, RaiseOptions { core: true }).is_err());
}

#[test]
fn values_facets_and_pairs_round_trip() {
    let src = r#"
base <https://example.org/v>
prefix ex: <https://example.org/>

shape Contractor { ex:agency required }

shape Person for every ex:Person {
  ex:name required, each string, each length 1 to 80, each pattern "^[A-Z]" flags "i"
  ex:age optional, each integer, each from 0 to 150
  ex:email at most 3, each iri
  ex:nick each language "en" "fr"
  ex:label unique language
  ex:start less than ex:end
  ex:givenName different from ex:familyName
  ex:status each one of ex:Active ex:Retired "unknown" 3
  ex:knows/ex:name or ^ex:parent each literal
  ex:tag at least 2 is "important"
  ex:manager at most 1 is matches Contractor
  none of { ex:deceased is true }
  any of { ex:phone required } { ex:email required }
}
"#;
    let parsed = shapeshift_parse::parse(src, "v.shm");
    assert!(
        parsed.diagnostics.is_empty(),
        "{}",
        render_all(&parsed.diagnostics)
    );
    let built = build_module("v.shm", &parsed.module);
    assert!(!built.has_errors(), "{}", render_all(&built.diagnostics));
    check_round_trip(&built);
}

fn g36_files() -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(example("g36"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "shm"))
        .collect();
    files.sort();
    files
}

fn build_file(path: &std::path::Path) -> BuildOutput {
    let (resolved, diags) = resolve_file(path, &FsResolver::default(), true).unwrap();
    assert!(
        diags.is_empty(),
        "{}: {}",
        path.display(),
        render_all(&diags)
    );
    let built = build(&resolved.program);
    assert!(
        !built.has_errors(),
        "{}: {}",
        path.display(),
        render_all(&built.diagnostics)
    );
    built
}

#[test]
fn g36_round_trips_through_shacl() {
    for f in g36_files() {
        check_round_trip(&build_file(&f));
    }
}

/// Every shape in BuildingMOTIF's Guideline 36 library means the same as its
/// Shapeshift translation in `examples/shapeshift/g36`. Point
/// `BUILDINGMOTIF_G36` at `BuildingMOTIF/libraries/ashrae/guideline36` and
/// run with `--ignored`.
///
/// BuildingMOTIF composes shapes with `rdfs:subClassOf`; the translation
/// writes `include`. The original is read with that composition made explicit
/// (`sh:node`), as BuildingMOTIF applies it.
#[test]
#[ignore = "needs a BuildingMOTIF checkout (set BUILDINGMOTIF_G36)"]
fn g36_matches_buildingmotif() {
    let Some(dir) = std::env::var_os("BUILDINGMOTIF_G36") else {
        panic!("set BUILDINGMOTIF_G36 to BuildingMOTIF/libraries/ashrae/guideline36");
    };
    let mut entries: Vec<PathBuf> = std::fs::read_dir(PathBuf::from(dir))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "ttl"))
        .collect();
    entries.sort();
    let mut original: Option<shifty_parse::Loaded> = None;
    for p in entries {
        let loaded = shifty_parse::load_turtle(&std::fs::read(&p).unwrap(), None).unwrap();
        match &mut original {
            None => original = Some(loaded),
            Some(o) => {
                for t in loaded.graph.iter() {
                    o.graph.insert(t);
                }
            }
        }
    }
    let mut original = original.expect("Turtle files in BUILDINGMOTIF_G36");
    let sub = oxrdf::NamedNode::new_unchecked("http://www.w3.org/2000/01/rdf-schema#subClassOf");
    let node = oxrdf::NamedNode::new_unchecked("http://www.w3.org/ns/shacl#node");
    let composed: Vec<oxrdf::Triple> = original
        .graph
        .triples_for_predicate(&sub)
        .filter(|t| matches!(t.object, oxrdf::TermRef::NamedNode(n) if n.as_str().starts_with("urn:ashrae/g36/")))
        .map(|t| oxrdf::Triple::new(t.subject.into_owned(), node.clone(), t.object.into_owned()))
        .collect();
    for t in &composed {
        original.graph.insert(t);
    }
    let lowered = shifty_parse::parse_loaded(&original).schema;

    let builds: Vec<BuildOutput> = g36_files().iter().map(|f| build_file(f)).collect();
    // The translation fixes one bug on purpose: 5.17.4 FC#1 places
    // `sh:qualifiedMinCount` inside the value shape, where SHACL ignores it.
    let deliberate = ["urn:ashrae/g36/5.17.4/dual-fan-dual-duct-heating-vav-ahu-afdd/fc-1"];
    let canon = |s: &Schema, id: ShapeId| {
        Canon {
            s,
            bodies: BTreeMap::new(),
            inline: true,
            stack: Vec::new(),
        }
        .shape(id, true)
    };
    let mut compared = 0;
    let mut absent = Vec::new();
    let mut differ = Vec::new();
    let mut names: Vec<String> = (0..lowered.arena.len())
        .filter_map(|i| lowered.name_of(ShapeId(i as u32)).map(str::to_string))
        .filter(|n| n.starts_with("urn:ashrae/g36/"))
        .collect();
    names.sort();
    names.dedup();
    for name in names {
        let a = lowered.slot_named(&name).unwrap();
        let Some((b, ours)) = builds
            .iter()
            .find_map(|bo| bo.schema.slot_named(&name).map(|s| (s, bo)))
        else {
            absent.push(name);
            continue;
        };
        compared += 1;
        let (ca, cb) = (canon(&lowered, a), canon(&ours.schema, b));
        if ca != cb && !deliberate.contains(&name.as_str()) {
            differ.push(format!(
                "{name}\n  BuildingMOTIF: {ca:?}\n  Shapeshift:    {cb:?}"
            ));
        }
    }
    println!("compared {compared} shapes; not separate shapes in the translation: {absent:?}");
    assert!(
        differ.is_empty(),
        "{} shapes differ:\n{}",
        differ.len(),
        differ.join("\n")
    );
    assert!(compared > 80, "only {compared} shapes compared");
}
