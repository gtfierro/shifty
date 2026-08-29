//! How much an [`EvidenceRun`] shares across independently addressable
//! validation results.
//!
//! [`compact`](crate::compact) answers a question about *bytes*: how much of a
//! serialized run collapses under hash-consing. This module answers a question
//! about *work*: how much of the derivation is reached from more than one
//! addressable result, and whether a judgment determines its own evidence. The
//! two are measured differently on purpose — this one never touches JSON, so
//! its counts are an independent check on the encoder's.
//!
//! Three definitions carry the measurement.
//!
//! **The address of a result.** Interior results are addressed by
//! `(constraint, node, polarity)` — the shape memo's `(ShapeId, Term)` key
//! (`validate.rs`) plus which way it came out. Every `Witness` and `SatTrace`
//! carries the first two; the enum itself carries the third. *Constraint* and
//! *node*, not *shape* and *focus*: `focus` is reserved for the top-level
//! selected node, and interior judgments are usually about some other node
//! reached from it.
//!
//! **The unit that is independently addressable.** A run holds one record per
//! *authored* `(statement, focus)` pair, because a report must name the
//! statement its reader wrote. But two authored statements that normalize
//! together are one *request*: the engine evaluates it once, and materializing
//! it twice is duplication, not sharing. Sharing is therefore counted across
//! normalized requests — [`normalized_requests`](ResultSharing::normalized_requests)
//! — while the authored records are retained and counted separately, so
//! source traceability costs a number rather than hiding inside the sharing
//! one.
//!
//! **Payload divergence.** A key is only an *address* if it determines what is
//! stored there. It may not: `Witness::Atom` carries `reached_by` and
//! `produced_by`, which describe how the node was reached rather than what was
//! decided about it, so one key can occur with several payloads.
//! [`divergent_keys`](ResultSharing::divergent_keys) measures how often that
//! happens, which is what decides whether evidence can be memoized on the
//! judgment the way conformance already is.
//!
//! That gives a bracket rather than a single number:
//! [`payload_redundancy`](ResultSharing::payload_redundancy) is what structure
//! sharing collapses losslessly today, and
//! [`key_redundancy`](ResultSharing::key_redundancy) is what a memo keyed like
//! the conformance memo could collapse if divergence were zero. The gap between
//! them is the evidence that depends on its derivation context.

use crate::witness::{EvaluationStatus, Evidence, EvidenceNodeRef, EvidenceRun};
use oxrdf::Term;
use rustc_hash::FxHashMap;
use shifty_algebra::ShapeId;
use std::hash::{Hash, Hasher};

/// Distinct payloads retained per key before the count saturates.
///
/// Divergence is detected by comparing a payload against the ones already seen
/// for its key, which is exact but quadratic in the number of *distinct*
/// payloads one key accumulates. The cap bounds that; whether a key diverges at
/// all is still exact, since the first disagreement is enough. Only
/// [`distinct_payloads_per_key`](ResultSharing::distinct_payloads_per_key)
/// saturates, and [`keys_over_payload_cap`](ResultSharing::keys_over_payload_cap)
/// reports when it did.
const PAYLOAD_CAP: usize = 32;

/// Which statement a record answers, after normalization has had its say.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Statement {
    /// The normalized statement this record was evaluated against. Authored
    /// statements that normalize together share one of these.
    Normalized(usize),
    /// No normalized counterpart, so the authored statement is its own request.
    Authored(usize),
}

/// Sharing and divergence over one run's evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResultSharing {
    /// `(authored statement, focus)` records the run reports.
    pub authored_records: usize,
    /// Distinct `(normalized statement, focus)` requests behind them.
    pub normalized_requests: usize,
    /// Records answering a request some earlier record already answered.
    pub duplicate_records: usize,
    /// Duplicates whose evidence is *not* equal to the first record's — which
    /// would mean the duplication is not pure and cannot simply be shared.
    pub divergent_duplicates: usize,

    /// Judgment nodes over every authored record. Equals `run.walk().len()`.
    pub occurrences: usize,
    /// Judgment nodes over one record per request: the materialization view,
    /// with source-preserving duplication removed.
    pub canonical_occurrences: usize,
    /// Structurally distinct judgment nodes.
    pub distinct_payloads: usize,
    /// Distinct payloads reached from two or more requests.
    pub shared_payloads: usize,
    /// Canonical occurrences of payloads reached from two or more requests.
    pub shared_canonical_occurrences: usize,
    /// Sum over distinct payloads of the requests reaching each.
    pub request_reaches: usize,
    /// Requests reaching the most widely shared payload.
    pub max_payload_requests: usize,

    /// Distinct `(constraint, node, polarity)` keys.
    pub distinct_keys: usize,
    /// Keys occurring more than once — the ones that *could* diverge.
    pub multi_occurrence_keys: usize,
    /// Keys occurring with two or more distinct payloads.
    pub divergent_keys: usize,
    /// Occurrences belonging to a divergent key.
    pub divergent_occurrences: usize,
    /// Distinct payloads summed over keys, saturating at [`PAYLOAD_CAP`].
    pub distinct_payloads_per_key: usize,
    /// Keys whose distinct-payload count saturated.
    pub keys_over_payload_cap: usize,
    /// `(constraint, node)` addresses appearing with *both* polarities in one
    /// run. A judgment has one truth value per run, so this should be zero;
    /// it is measured rather than assumed.
    pub both_polarity_addresses: usize,
}

impl ResultSharing {
    /// Occurrences per structurally distinct judgment node: what hash-consing
    /// collapses losslessly, and the lower bound of the bracket.
    pub fn payload_redundancy(&self) -> f64 {
        ratio(self.occurrences, self.distinct_payloads)
    }

    /// Occurrences per distinct key: what a memo keyed on the judgment could
    /// collapse, and the upper bound of the bracket. Reachable only to the
    /// extent that [`divergent_keys`](Self::divergent_keys) is small.
    pub fn key_redundancy(&self) -> f64 {
        ratio(self.occurrences, self.distinct_keys)
    }

    /// Share of distinct payloads reached from more than one request.
    pub fn shared_payload_fraction(&self) -> f64 {
        fraction(self.shared_payloads, self.distinct_payloads)
    }

    /// Share of canonical occurrences that are of a payload more than one
    /// request reaches.
    pub fn shared_occurrence_fraction(&self) -> f64 {
        fraction(
            self.shared_canonical_occurrences,
            self.canonical_occurrences,
        )
    }

    /// Requests reaching the average distinct payload; 1.0 when nothing is
    /// shared across requests.
    pub fn requests_per_payload(&self) -> f64 {
        ratio(self.request_reaches, self.distinct_payloads)
    }

    /// Share of the keys that occur more than once whose payloads disagree.
    /// The quantity that decides whether evidence can be memoized on the
    /// judgment; 0.0 when no key occurs twice, since none can diverge.
    pub fn divergence_fraction(&self) -> f64 {
        fraction(self.divergent_keys, self.multi_occurrence_keys)
    }
}

fn ratio(occurrences: usize, distinct: usize) -> f64 {
    if distinct == 0 {
        1.0
    } else {
        occurrences as f64 / distinct as f64
    }
}

fn fraction(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64
    }
}

/// A judgment node compared and hashed by the structure it holds, so that
/// identical derivations collapse to one payload id.
///
/// `EvidenceNodeRef` is `Copy` and deliberately not `Eq`: it is a cursor, and
/// two cursors onto equal trees are not the same node for most purposes. Here
/// they are exactly what should merge, so the equality lives on this wrapper
/// rather than on the public cursor.
#[derive(Clone, Copy)]
struct Payload<'a>(EvidenceNodeRef<'a>);

impl PartialEq for Payload<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self.0, other.0) {
            (EvidenceNodeRef::Satisfaction(left), EvidenceNodeRef::Satisfaction(right)) => {
                left == right
            }
            (EvidenceNodeRef::Failure(left), EvidenceNodeRef::Failure(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for Payload<'_> {}

impl Hash for Payload<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.0 {
            EvidenceNodeRef::Satisfaction(value) => {
                state.write_u8(0);
                value.hash(state);
            }
            EvidenceNodeRef::Failure(value) => {
                state.write_u8(1);
                value.hash(state);
            }
        }
    }
}

/// The address of an interior judgment: the memo's key plus its polarity.
type Key<'a> = (ShapeId, Option<&'a Term>, EvaluationStatus);

#[derive(Default)]
struct KeyStats {
    occurrences: usize,
    payloads: Vec<u32>,
    over_cap: bool,
}

/// Per-payload accumulators, indexed by payload id.
#[derive(Default)]
struct Payloads {
    occurrences: Vec<u32>,
    canonical_occurrences: Vec<u32>,
    /// The last request that reached this payload, so that a request reaching
    /// it many times counts once. Sound only because records are visited
    /// grouped by request.
    last_request: Vec<u32>,
    requests: Vec<u32>,
}

impl Payloads {
    fn push(&mut self) {
        self.occurrences.push(0);
        self.canonical_occurrences.push(0);
        self.last_request.push(u32::MAX);
        self.requests.push(0);
    }
}

/// Measure sharing and divergence over a run's evidence.
///
/// One pass over the typed evidence; no serialization, and nothing is retained
/// beyond the counts.
pub fn result_sharing(run: &EvidenceRun) -> ResultSharing {
    // Address every authored record first, so requests can be numbered before
    // anything is counted against them.
    let mut request_ids: FxHashMap<(Statement, &Term), u32> = FxHashMap::default();
    let mut records: Vec<(u32, &Evidence)> = Vec::new();
    for statement in &run.statements {
        let which = statement
            .normalized_statement_id
            .map_or(Statement::Authored(statement.source_statement_id), |id| {
                Statement::Normalized(id)
            });
        for focus in &statement.selected_foci {
            let next = request_ids.len() as u32;
            let id = *request_ids.entry((which, &focus.focus)).or_insert(next);
            records.push((id, &focus.evidence));
        }
    }
    // Grouping by request is what makes the per-payload request count exact
    // with a single `last_request` slot instead of a set per payload. The sort
    // is stable, so the first record of each request stays first and remains
    // the canonical one.
    records.sort_by_key(|(request, _)| *request);

    let mut payload_ids: FxHashMap<Payload<'_>, u32> = FxHashMap::default();
    let mut payloads = Payloads::default();
    let mut keys: FxHashMap<Key<'_>, KeyStats> = FxHashMap::default();

    let mut measured = ResultSharing {
        authored_records: records.len(),
        normalized_requests: request_ids.len(),
        ..ResultSharing::default()
    };
    let mut canonical: Vec<&Evidence> = Vec::with_capacity(request_ids.len());
    let mut current = None;

    for (request, evidence) in records {
        let first = current != Some(request);
        current = Some(request);
        if first {
            canonical.push(evidence);
        } else {
            measured.duplicate_records += 1;
            if canonical[request as usize] != evidence {
                measured.divergent_duplicates += 1;
            }
        }

        for node in evidence.walk() {
            let next = payload_ids.len() as u32;
            let payload = *payload_ids.entry(Payload(node)).or_insert_with(|| {
                payloads.push();
                next
            });
            let slot = payload as usize;

            payloads.occurrences[slot] += 1;
            if first {
                payloads.canonical_occurrences[slot] += 1;
            }
            if payloads.last_request[slot] != request {
                payloads.last_request[slot] = request;
                payloads.requests[slot] += 1;
            }

            let stats = keys
                .entry((node.constraint_id(), node.node(), node.status()))
                .or_default();
            stats.occurrences += 1;
            if !stats.payloads.contains(&payload) {
                if stats.payloads.len() < PAYLOAD_CAP {
                    stats.payloads.push(payload);
                } else {
                    stats.over_cap = true;
                }
            }
        }
    }

    measured.distinct_payloads = payloads.occurrences.len();
    measured.occurrences = payloads
        .occurrences
        .iter()
        .map(|&count| count as usize)
        .sum();
    measured.canonical_occurrences = payloads
        .canonical_occurrences
        .iter()
        .map(|&count| count as usize)
        .sum();
    for (slot, &requests) in payloads.requests.iter().enumerate() {
        measured.request_reaches += requests as usize;
        measured.max_payload_requests = measured.max_payload_requests.max(requests as usize);
        if requests >= 2 {
            measured.shared_payloads += 1;
            measured.shared_canonical_occurrences += payloads.canonical_occurrences[slot] as usize;
        }
    }

    // Polarity is part of the key, so a `(constraint, node)` reached with both
    // polarities shows up as two keys. Fold them back to check for it.
    let mut polarities: FxHashMap<(ShapeId, Option<&Term>), u8> = FxHashMap::default();
    measured.distinct_keys = keys.len();
    for ((constraint, node, status), stats) in &keys {
        if stats.occurrences >= 2 {
            measured.multi_occurrence_keys += 1;
        }
        if stats.payloads.len() >= 2 {
            measured.divergent_keys += 1;
            measured.divergent_occurrences += stats.occurrences;
        }
        measured.distinct_payloads_per_key += stats.payloads.len();
        measured.keys_over_payload_cap += usize::from(stats.over_cap);

        let seen = polarities.entry((*constraint, *node)).or_default();
        *seen |= match status {
            EvaluationStatus::Pass => 1,
            EvaluationStatus::Fail => 2,
        };
    }
    measured.both_polarity_addresses = polarities.values().filter(|&&seen| seen == 3).count();

    measured
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validate_with_evidence;
    use shifty_parse::{load_turtle, parse_turtle};

    // Two shapes declare the same property constraint and select the same
    // focus nodes, so identical judgments are reached from separate authored
    // statements — the sharing this module exists to count.
    const TTL: &str = r#"
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex: <http://ex/> .
        ex:S a sh:NodeShape ; sh:targetClass ex:T ;
          sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:class ex:C ] ;
          sh:property [ sh:path ex:n ; sh:datatype xsd:integer ] .
        ex:T2 a sh:NodeShape ; sh:targetClass ex:T ;
          sh:property [ sh:path ex:p ; sh:minCount 1 ; sh:class ex:C ] .
        ex:good a ex:T ; ex:p ex:c1 ; ex:n 3 .
        ex:bad a ex:T ; ex:n "not a number" .
        ex:c1 a ex:C .
    "#;

    fn run() -> EvidenceRun {
        let parsed = parse_turtle(TTL.as_bytes(), None).unwrap();
        let loaded = load_turtle(TTL.as_bytes(), None).unwrap();
        validate_with_evidence(&loaded.graph, &parsed.schema).unwrap()
    }

    // The identity that ties this module to the encoder: two traversals — one
    // over the types here, one over serialized JSON there — must see the same
    // judgment nodes and the same number of distinct ones.
    #[test]
    fn the_typed_pass_agrees_with_the_encoder() {
        let original = run();
        let measured = result_sharing(&original);
        let encoded = crate::compact::sharing(&original).unwrap();

        assert_eq!(measured.occurrences, original.walk().len());
        assert_eq!(measured.occurrences, encoded.result_occurrences);
        assert_eq!(measured.distinct_payloads, encoded.distinct_results);
    }

    #[test]
    fn requests_collapse_authored_records_without_losing_them() {
        let original = run();
        let measured = result_sharing(&original);

        assert_eq!(
            measured.authored_records,
            original
                .statements
                .iter()
                .map(|statement| statement.selected_foci.len())
                .sum::<usize>()
        );
        assert!(
            measured.normalized_requests <= measured.authored_records,
            "normalizing cannot invent requests"
        );
        assert_eq!(
            measured.authored_records - measured.normalized_requests,
            measured.duplicate_records
        );
        assert_eq!(
            measured.divergent_duplicates, 0,
            "records answering one request must agree"
        );
    }

    #[test]
    fn the_bracket_is_ordered() {
        let measured = result_sharing(&run());

        // Structural equality implies key equality but not the reverse, so
        // distinct keys can only be fewer, and the collapse a key-addressed
        // memo could reach can only be larger.
        assert!(measured.distinct_keys <= measured.distinct_payloads);
        assert!(measured.key_redundancy() >= measured.payload_redundancy());
        assert!(measured.payload_redundancy() >= 1.0);

        assert!(measured.canonical_occurrences <= measured.occurrences);
        assert!(measured.shared_payloads <= measured.distinct_payloads);
        assert!(measured.divergent_keys <= measured.multi_occurrence_keys);
        assert_eq!(
            measured.both_polarity_addresses, 0,
            "one truth value per (constraint, node) per run"
        );
    }

    #[test]
    fn the_fixture_actually_shares() {
        let measured = result_sharing(&run());
        assert!(
            measured.shared_payloads > 0,
            "ex:S and ex:T2 reach the same judgments about the same nodes"
        );
        assert!(measured.requests_per_payload() > 1.0);
    }

    #[test]
    fn sharing_of_nothing_is_one() {
        let empty = ResultSharing::default();
        assert_eq!(empty.payload_redundancy(), 1.0);
        assert_eq!(empty.key_redundancy(), 1.0);
        assert_eq!(empty.requests_per_payload(), 1.0);
        assert_eq!(empty.divergence_fraction(), 0.0);
        assert_eq!(empty.shared_payload_fraction(), 0.0);
    }
}
