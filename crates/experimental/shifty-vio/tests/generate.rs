//! A universe over the running example of arXiv 2611.07342 (Fig. 6),
//! extended with a datatype, a class, an `sh:in`, a disjunction, and a
//! negation.

use shifty_vio::{Error, Injection, OperationKind, Options, Triple, Universe, Weights};
use std::collections::{BTreeMap, BTreeSet};

const SHAPES: &str = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex/> .
ex:AHUShape a sh:NodeShape ;
    sh:targetClass ex:AHU ;
    sh:property [
        sh:path ex:hasPoint ;
        sh:qualifiedValueShape ex:NamedPoint ;
        sh:qualifiedMinCount 1
    ] ;
    sh:property [ sh:path ex:label ; sh:datatype xsd:string ; sh:minCount 1 ; sh:maxCount 1 ] ;
    sh:property [ sh:path ex:feeds ; sh:class ex:Zone ; sh:maxCount 2 ] ;
    sh:property [ sh:path ex:status ; sh:in ( "on" "off" ) ] .
ex:NamedPoint a sh:NodeShape ;
    sh:property [ sh:path ex:description ; sh:minCount 1 ; sh:minLength 3 ] .
ex:ZoneShape a sh:NodeShape ;
    sh:targetClass ex:Zone ;
    sh:or ( [ sh:path ex:area ; sh:minCount 1 ] [ sh:path ex:volume ; sh:minCount 1 ] ) ;
    sh:not [ sh:path ex:deprecated ; sh:hasValue true ] .
"#;

const DATA: &str = r#"
@prefix ex: <http://ex/> .
ex:ahu_1 a ex:AHU ;
    ex:hasPoint ex:RAT_Sensor ;
    ex:label "AHU-1" ;
    ex:feeds ex:z1 ;
    ex:status "on" .
ex:RAT_Sensor ex:description "RAT_01" .
ex:z1 a ex:Zone ; ex:area 30 .
ex:z2 a ex:Zone ; ex:volume 90 ; ex:area 12 ; ex:deprecated false .
"#;

fn universe() -> Universe {
    let shapes = shifty_parse::load_turtle(SHAPES.as_bytes(), None).unwrap();
    let data = shifty_parse::load_turtle(DATA.as_bytes(), None)
        .unwrap()
        .graph;
    Universe::new(shapes, data, Options::default()).unwrap()
}

fn ex(name: &str) -> String {
    format!("<http://ex/{name}>")
}

fn t(s: &str, p: &str, o: &str) -> Triple {
    (s.to_owned(), p.to_owned(), o.to_owned())
}

fn keys<'a>(injections: impl IntoIterator<Item = &'a Injection>) -> Vec<String> {
    injections.into_iter().map(|e| e.key().to_owned()).collect()
}

#[test]
fn expansion_reaches_the_leaves_of_the_running_example() {
    let u = universe();
    assert!(u.baseline_failures().is_empty());
    let foci: BTreeSet<&str> = u.iter().map(|e| e.focus.as_str()).collect();
    assert_eq!(
        foci,
        BTreeSet::from([ex("ahu_1"), ex("z1"), ex("z2")])
            .iter()
            .map(String::as_str)
            .collect()
    );
    let ahu = u.filter(|e| e.focus == ex("ahu_1"));
    let has_point = t(&ex("ahu_1"), &ex("hasPoint"), &ex("RAT_Sensor"));
    let description = t(&ex("RAT_Sensor"), &ex("description"), "\"RAT_01\"");
    assert!(
        ahu.iter()
            .any(|e| e.deletes() == BTreeSet::from([has_point.clone()]) && e.adds().is_empty())
    );
    assert!(ahu.iter().any(|e| {
        e.deletes() == BTreeSet::from([description.clone()])
            && e.adds().iter().any(|a| a.2 == "\"RA\"")
            && e.kinds() == [OperationKind::Substitute]
    }));
}

#[test]
fn every_injection_is_distinct_and_breaks_its_target_alone() {
    let u = universe();
    assert!(u.len() > 20);
    let all = keys(&u);
    assert_eq!(all.iter().collect::<BTreeSet<_>>().len(), u.len());
    for e in &u {
        let key = e.key().to_owned();
        let one = u.filter(move |x| x.key() == key);
        let case = one.draw(1, 0).unwrap();
        assert_eq!(keys(&case.injections), [e.key()], "{}", e.to_json());
        assert!(
            case.failures
                .iter()
                .any(|f| f.focus == e.focus && f.injections == [0])
        );
    }
}

#[test]
fn negation_flips_to_the_satisfying_direction() {
    let u = universe();
    let truth = "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>";
    assert!(
        u.iter()
            .any(|e| e.adds() == BTreeSet::from([t(&ex("z1"), &ex("deprecated"), truth)]))
    );
}

#[test]
fn drawn_cases_apply_together_and_are_realized() {
    let u = universe();
    for seed in 0..5 {
        let case = u.draw(4, seed).unwrap();
        assert_eq!((case.k, case.seed), (4, seed));
        assert!((1..=4).contains(&case.injections.len()));
        for (i, a) in case.injections.iter().enumerate() {
            for b in &case.injections[i + 1..] {
                assert!(a.deletes().is_disjoint(&b.adds()));
                assert!(a.adds().is_disjoint(&b.deletes()));
            }
        }
        let realized: BTreeSet<usize> = case
            .failures
            .iter()
            .flat_map(|f| f.injections.iter().copied())
            .collect();
        assert_eq!(realized, (0..case.injections.len()).collect());
        let graph = case.graph_ntriples();
        assert_eq!(
            graph.lines().count(),
            u.reference().len() - case.deletes().len() + case.adds().len()
        );
        assert_eq!(case.delta().add.len(), case.adds().len());
        assert_eq!(case.repair().add, case.delta().delete);
        assert!(case.repair_sparql().ends_with('\n'));
    }
}

#[test]
fn draws_reproduce_and_seeds_vary() {
    let a = universe();
    let b = universe();
    assert_eq!(keys(&a), keys(&b));
    let draw = |u: &Universe, seed| keys(&u.draw(3, seed).unwrap().injections);
    assert_eq!(draw(&a, 7), draw(&b, 7));
    let distinct: BTreeSet<Vec<String>> = (0..6).map(|s| draw(&a, s)).collect();
    assert!(distinct.len() > 1);
}

#[test]
fn filter_restricts_the_draw() {
    let u = universe();
    let zones = u.filter(|e| e.shape.as_deref() == Some("http://ex/ZoneShape"));
    assert!(!zones.is_empty() && zones.len() < u.len());
    let members: BTreeSet<String> = keys(&zones).into_iter().collect();
    for seed in 0..3 {
        let case = zones.draw(2, seed).unwrap();
        assert!(!case.injections.is_empty());
        assert!(case.injections.iter().all(|e| members.contains(e.key())));
    }
    assert_eq!(u.filter(|_| false).draw(1, 0).unwrap_err(), Error::Empty);
    assert_eq!(u.draw(0, 0).unwrap_err(), Error::Budget);
}

#[test]
fn weights_steer_the_draw() {
    let u = universe();
    let removes = u
        .weighted(Weights {
            kinds: BTreeMap::from([(OperationKind::Remove, 1e6)]),
            ..Weights::default()
        })
        .unwrap();
    assert_eq!(removes.len(), u.len());
    let case = removes.draw(2, 0).unwrap();
    for e in &case.injections {
        assert!(e.kinds().iter().all(|k| *k == OperationKind::Remove));
    }

    let status = ex("status");
    let on_status = u
        .weighted(Weights {
            predicates: BTreeMap::from([(status.clone(), 1e6)]),
            ..Weights::default()
        })
        .unwrap();
    let first = &on_status.draw(1, 0).unwrap().injections[0];
    assert!(first.predicates().contains(&status));

    assert!(matches!(
        u.weighted(Weights {
            kinds: BTreeMap::from([(OperationKind::AddValue, 0.0)]),
            ..Weights::default()
        }),
        Err(Error::Weight { .. })
    ));
}

#[test]
fn several_injections_may_break_one_property() {
    // `sh:in ( "on" "off" )` on ahu_1's status is one property, and several
    // compatible injections break it: a draw can stack them.
    let u = universe();
    let status = u.filter(|e| e.predicates().contains(&ex("status")));
    assert!(status.len() >= 2);
    let case = status.draw(2, 0).unwrap();
    assert_eq!(case.injections.len(), 2);
    assert!(case.injections.iter().all(|e| e.focus == ex("ahu_1")));
    let failure = case
        .failures
        .iter()
        .find(|f| f.focus == ex("ahu_1"))
        .unwrap();
    assert_eq!(failure.injections, [0, 1]);
}

#[test]
fn streamed_and_cached_draws_agree() {
    // A fresh universe streams its draws; listing it caches the expansion,
    // and later draws read the cache. The reservoir does not depend on
    // arrival order, so both give the same cases.
    let streamed = universe();
    let zones = |u: &Universe| u.filter(|e| e.shape.as_deref() == Some("http://ex/ZoneShape"));
    let before: Vec<Vec<String>> = [streamed.draw(3, 7), zones(&streamed).draw(2, 1)]
        .into_iter()
        .map(|c| keys(&c.unwrap().injections))
        .collect();
    let cached = universe();
    assert!(cached.len() > 20);
    let after: Vec<Vec<String>> = [cached.draw(3, 7), zones(&cached).draw(2, 1)]
        .into_iter()
        .map(|c| keys(&c.unwrap().injections))
        .collect();
    assert_eq!(before, after);
}

#[test]
fn batched_draws_match_single_draws() {
    let u = universe();
    let requests = [(1, 0), (3, 7), (3, 8), (5, 2)];
    let batch: Vec<Vec<String>> = u
        .draws(&requests)
        .unwrap()
        .iter()
        .map(|c| keys(&c.injections))
        .collect();
    let single: Vec<Vec<String>> = requests
        .iter()
        .map(|&(k, seed)| keys(&universe().draw(k, seed).unwrap().injections))
        .collect();
    assert_eq!(batch, single);
    assert_eq!(u.draws(&[(2, 0), (0, 1)]).unwrap_err(), Error::Budget);
}
