#ifndef SHIFTY_SHIFTY_H
#define SHIFTY_SHIFTY_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SHIFTY_ABI_VERSION 5u

typedef uint32_t ShiftyStatus;
enum {
    SHIFTY_STATUS_OK = 0,
    SHIFTY_STATUS_INVALID_ARGUMENT = 1,
    SHIFTY_STATUS_IO_ERROR = 2,
    SHIFTY_STATUS_PARSE_ERROR = 3,
    SHIFTY_STATUS_QUERY_ERROR = 4,
    SHIFTY_STATUS_VALIDATION_ERROR = 5,
    SHIFTY_STATUS_INTERNAL_ERROR = 255
};

typedef uint32_t ShiftyRdfFormat;
enum {
    SHIFTY_RDF_FORMAT_TURTLE = 0,
    SHIFTY_RDF_FORMAT_NTRIPLES = 1,
    SHIFTY_RDF_FORMAT_AUTO = 2
};

typedef uint32_t ShiftyGraphMode;
enum {
    SHIFTY_GRAPH_MODE_DATA = 0,
    SHIFTY_GRAPH_MODE_UNION = 1,
    SHIFTY_GRAPH_MODE_UNION_ALL = 2
};

/* Lowest result severity that makes a validation outcome non-conforming.
 * Findings below the threshold are still reported (they appear in the W3C
 * report graph / AlgebraResult.violations); they just don't fail conforms.
 * Matches the `minimum_severity` option of the Python / WASM / CLI APIs. */
typedef uint32_t ShiftySeverity;
enum {
    SHIFTY_SEVERITY_INFO = 0,
    SHIFTY_SEVERITY_WARNING = 1,
    SHIFTY_SEVERITY_VIOLATION = 2
};

typedef uint32_t ShiftyQueryResultKind;
enum {
    SHIFTY_QUERY_RESULT_BOOLEAN = 0,
    SHIFTY_QUERY_RESULT_SOLUTIONS = 1,
    SHIFTY_QUERY_RESULT_GRAPH = 2
};

typedef struct ShiftyStringView {
    const char *data;
    size_t len;
} ShiftyStringView;

/* A (label, value) string pair, used for `value_paths`. */
typedef struct ShiftyStringPair {
    ShiftyStringView first;
    ShiftyStringView second;
} ShiftyStringPair;

/* The three RDF term kinds the shape-map ABI reports. */
typedef uint32_t ShiftyTermKind;
enum {
    SHIFTY_TERM_IRI = 0,
    SHIFTY_TERM_LITERAL = 1,
    SHIFTY_TERM_BNODE = 2
};

/* One RDF term, returned by value with the string components pointing into
 * the owning handle. `datatype` and `language` are set only for literals and
 * are empty otherwise. */
typedef struct ShiftyTerm {
    ShiftyTermKind kind;
    ShiftyStringView value;
    ShiftyStringView datatype;
    ShiftyStringView language;
} ShiftyTerm;

/* The four qualifier kinds a shape-map key can carry. */
typedef uint32_t ShiftyQualifierKind;
enum {
    SHIFTY_QUALIFIER_CLS = 0,
    SHIFTY_QUALIFIER_CONST = 1,
    SHIFTY_QUALIFIER_DATATYPE = 2,
    SHIFTY_QUALIFIER_SHAPE_REF = 3
};

/* The polarity of the evidence produced for one (statement, focus) pair. */
typedef uint32_t ShiftyEvaluationStatus;
enum {
    SHIFTY_EVALUATION_PASS = 0,
    SHIFTY_EVALUATION_FAIL = 1
};

/* Reported for an absent normalized identity or an out-of-range index. */
#define SHIFTY_EVIDENCE_NO_INDEX ((size_t)-1)
#define SHIFTY_EVIDENCE_NO_CONSTRAINT ((uint32_t)-1)

typedef struct ShiftyDataset ShiftyDataset;
typedef struct ShiftyPreparedValidator ShiftyPreparedValidator;
typedef struct ShiftyQueryResult ShiftyQueryResult;
typedef struct ShiftyValidationResult ShiftyValidationResult;
typedef struct ShiftyPropertyWitnessList ShiftyPropertyWitnessList;
typedef struct ShiftyAlgebraResult ShiftyAlgebraResult;
typedef struct ShiftyEvidenceSession ShiftyEvidenceSession;
typedef struct ShiftyEvidenceRun ShiftyEvidenceRun;
typedef struct ShiftyFailureList ShiftyFailureList;
typedef struct ShiftyString ShiftyString;
typedef struct ShiftyBindingNameList ShiftyBindingNameList;
typedef struct ShiftyPathResolutionList ShiftyPathResolutionList;
typedef struct ShiftyShapeMap ShiftyShapeMap;

/* Conformance-only totals over *normalized* (statement, focus) pairs — the
 * pairs evidence is materialized against, before authored statements that
 * normalize together fan the same evidence back out. Returned by value. */
typedef struct ShiftyConformanceRun {
    uint8_t conforms;
    size_t selected_pairs;
    size_t passed;
    size_t failed;
} ShiftyConformanceRun;

/*
 * Pointer contract:
 * - input buffers must remain valid for the duration of the call;
 * - opaque handles must originate from this library and must not be reused
 *   after their matching destroy function;
 * - output pointers must point to writable storage.
 *
 * All functions that return ShiftyStatus catch Rust panics and report failures
 * through shifty_last_error_message(). The returned error pointer remains valid
 * until the next status-returning SDK call on the same thread.
 */
uint32_t shifty_abi_version(void);
const char *shifty_last_error_message(void);

ShiftyStatus shifty_dataset_create(ShiftyDataset **out);
void shifty_dataset_destroy(ShiftyDataset *dataset);
ShiftyStatus shifty_dataset_load_memory(
    ShiftyDataset *dataset,
    const uint8_t *data,
    size_t len,
    ShiftyRdfFormat format,
    const char *base,
    size_t base_len);
ShiftyStatus shifty_dataset_load_file(
    ShiftyDataset *dataset,
    const char *path,
    size_t path_len,
    ShiftyRdfFormat format,
    const char *base,
    size_t base_len);
size_t shifty_dataset_len(const ShiftyDataset *dataset);
ShiftyStatus shifty_dataset_ntriples(
    const ShiftyDataset *dataset,
    ShiftyQueryResult **out);
ShiftyStatus shifty_dataset_query(
    const ShiftyDataset *dataset,
    const char *query,
    size_t query_len,
    ShiftyQueryResult **out);

void shifty_query_result_destroy(ShiftyQueryResult *result);
ShiftyQueryResultKind shifty_query_result_kind(const ShiftyQueryResult *result);
uint8_t shifty_query_result_boolean(const ShiftyQueryResult *result);
ShiftyStringView shifty_query_result_data(const ShiftyQueryResult *result);
ShiftyStringView shifty_query_result_media_type(const ShiftyQueryResult *result);

ShiftyStatus shifty_prepared_validator_create_memory(
    const uint8_t *data,
    size_t len,
    ShiftyRdfFormat format,
    const char *base,
    size_t base_len,
    ShiftyPreparedValidator **out);
ShiftyStatus shifty_prepared_validator_create_file(
    const char *path,
    size_t path_len,
    ShiftyRdfFormat format,
    const char *base,
    size_t base_len,
    ShiftyPreparedValidator **out);
void shifty_prepared_validator_destroy(ShiftyPreparedValidator *validator);
ShiftyStringView shifty_prepared_validator_diagnostics_json(
    const ShiftyPreparedValidator *validator);
ShiftyStatus shifty_prepared_validator_validate(
    const ShiftyPreparedValidator *validator,
    const ShiftyDataset *dataset,
    ShiftyGraphMode graph_mode,
    uint8_t run_inference,
    ShiftySeverity minimum_severity,
    ShiftyValidationResult **out);
/*
 * Like shifty_prepared_validator_validate, but validates only the named shapes
 * in shape_names as top-level entry points. shape_names is an array of
 * length-delimited UTF-8 IRIs; bare IRIs and <iri> forms are accepted. Helper
 * shapes referenced from the selected entries are still evaluated normally.
 * Pass NULL with length 0 to validate every target-bearing shape.
 */
ShiftyStatus shifty_prepared_validator_validate_with_shapes(
    const ShiftyPreparedValidator *validator,
    const ShiftyDataset *dataset,
    ShiftyGraphMode graph_mode,
    uint8_t run_inference,
    ShiftySeverity minimum_severity,
    const ShiftyStringView *shape_names,
    size_t shape_names_len,
    ShiftyValidationResult **out);

void shifty_validation_result_destroy(ShiftyValidationResult *result);
uint8_t shifty_validation_result_conforms(const ShiftyValidationResult *result);
ShiftyStringView shifty_validation_result_report_turtle(
    const ShiftyValidationResult *result);
ShiftyStringView shifty_validation_result_results_text(
    const ShiftyValidationResult *result);

/*
 * Property witnesses: the observed sh:property bindings at conforming focus
 * nodes (the inverse of a violation report). `key_path` (may be NULL/empty)
 * is a SPARQL 1.1 property path expression (e.g. "zea:roleName",
 * "zea:role/zea:roleName", "^zea:describes/zea:roleName") evaluated from each
 * sh:property shape's own node *over the shapes graph* to produce a stable
 * key (e.g. reaching a "zea:roleName \"outsideAirTemp\"" style annotation);
 * property shapes where it resolves to no value report their own shape node
 * as the key instead. Prefixes are resolved against the shapes document's
 * declared @prefixes.
 */
ShiftyStatus shifty_prepared_validator_witnesses(
    const ShiftyPreparedValidator *validator,
    const ShiftyDataset *dataset,
    const char *key_path,
    size_t key_path_len,
    ShiftyGraphMode graph_mode,
    uint8_t run_inference,
    ShiftyPropertyWitnessList **out);

void shifty_property_witness_list_destroy(ShiftyPropertyWitnessList *list);
size_t shifty_property_witness_list_len(const ShiftyPropertyWitnessList *list);
ShiftyStringView shifty_property_witness_focus(
    const ShiftyPropertyWitnessList *list, size_t index);
ShiftyStringView shifty_property_witness_shape(
    const ShiftyPropertyWitnessList *list, size_t index);
ShiftyStringView shifty_property_witness_key(
    const ShiftyPropertyWitnessList *list, size_t index);
size_t shifty_property_witness_value_count(
    const ShiftyPropertyWitnessList *list, size_t index);
ShiftyStringView shifty_property_witness_value(
    const ShiftyPropertyWitnessList *list, size_t index, size_t value_index);

/*
 * Algebra-path validation: the engine's own conformance oracle, run directly
 * against the SHACL algebra rather than compiled to a W3C sh:ValidationReport.
 * Produces a structured violation/reason tree instead of an RDF report graph.
 * An absent shape_name, path, or author_message is reported as an empty
 * ShiftyStringView.
 */
ShiftyStatus shifty_prepared_validator_validate_algebra(
    const ShiftyPreparedValidator *validator,
    const ShiftyDataset *dataset,
    ShiftyGraphMode graph_mode,
    uint8_t run_inference,
    ShiftySeverity minimum_severity,
    ShiftyAlgebraResult **out);
/*
 * Like shifty_prepared_validator_validate_algebra, but validates only the
 * named shapes in shape_names as top-level entry points. Dependencies of those
 * entries are still evaluated normally. Pass NULL with length 0 to validate
 * every target-bearing shape.
 */
ShiftyStatus shifty_prepared_validator_validate_algebra_with_shapes(
    const ShiftyPreparedValidator *validator,
    const ShiftyDataset *dataset,
    ShiftyGraphMode graph_mode,
    uint8_t run_inference,
    ShiftySeverity minimum_severity,
    const ShiftyStringView *shape_names,
    size_t shape_names_len,
    ShiftyAlgebraResult **out);

void shifty_algebra_result_destroy(ShiftyAlgebraResult *result);
uint8_t shifty_algebra_result_conforms(const ShiftyAlgebraResult *result);
ShiftyStringView shifty_algebra_result_results_text(
    const ShiftyAlgebraResult *result);
size_t shifty_algebra_result_violation_count(const ShiftyAlgebraResult *result);
ShiftyStringView shifty_algebra_violation_focus(
    const ShiftyAlgebraResult *result, size_t index);
ShiftyStringView shifty_algebra_violation_shape_name(
    const ShiftyAlgebraResult *result, size_t index);
ShiftyStringView shifty_algebra_violation_severity(
    const ShiftyAlgebraResult *result, size_t index);
size_t shifty_algebra_violation_reason_count(
    const ShiftyAlgebraResult *result, size_t index);
ShiftyStringView shifty_algebra_reason_value(
    const ShiftyAlgebraResult *result, size_t index, size_t reason_index);
ShiftyStringView shifty_algebra_reason_path(
    const ShiftyAlgebraResult *result, size_t index, size_t reason_index);
ShiftyStringView shifty_algebra_reason_message(
    const ShiftyAlgebraResult *result, size_t index, size_t reason_index);
ShiftyStringView shifty_algebra_reason_author_message(
    const ShiftyAlgebraResult *result, size_t index, size_t reason_index);
ShiftyStringView shifty_algebra_reason_severity(
    const ShiftyAlgebraResult *result, size_t index, size_t reason_index);

/*
 * Evidence-carrying validation: every selected (authored statement, focus)
 * pair carries exactly one evidence polarity — a satisfaction trace when it
 * passes, a failure witness when it does not — rather than only the failures a
 * validation report contains. Statements whose selector chose no focus nodes
 * are reported with an empty focus list, so the run covers the whole schema.
 *
 * A session prepares one immutable snapshot: inference (when requested),
 * normalization, stratification, indexing, and SPARQL preparation happen once
 * here and are reused by every call below. graph_mode and run_inference define
 * the snapshot and so are fixed at creation; minimum_severity and shape_names
 * are per-call.
 */
ShiftyStatus shifty_evidence_session_create(
    const ShiftyPreparedValidator *validator,
    const ShiftyDataset *dataset,
    ShiftyGraphMode graph_mode,
    uint8_t run_inference,
    ShiftyEvidenceSession **out);
void shifty_evidence_session_destroy(ShiftyEvidenceSession *session);

/* The source/normalized constraint catalogs this snapshot's evidence refers to
 * by id, as JSON. Fixed per snapshot: take it once rather than per run. */
ShiftyStringView shifty_evidence_session_constraints_json(
    const ShiftyEvidenceSession *session);

/*
 * Shape-map v2 session helpers. In `binding_names`, a NULL/empty `name_path`
 * means `sh:name` (matching the Python `binding_names(None)`). In the
 * `shape_map` call, a NULL `name_path` pointer with length 0 means *skip* name
 * resolution; a non-NULL path is used verbatim, and an empty path falls back
 * to `sh:name`.
 */

/* For every source constraint with shapes-graph provenance, the values
 * `name_path` reaches from its originating shapes-graph node (evaluated over
 * the shapes graph). The result is an opaque list of (constraint id, names)
 * entries, sorted by constraint id. */
ShiftyStatus shifty_evidence_session_binding_names(
    const ShiftyEvidenceSession *session,
    const char *name_path,
    size_t name_path_len,
    ShiftyBindingNameList **out);

void shifty_binding_name_list_destroy(ShiftyBindingNameList *list);
size_t shifty_binding_name_list_len(const ShiftyBindingNameList *list);
uint32_t shifty_binding_name_constraint(
    const ShiftyBindingNameList *list, size_t index);
size_t shifty_binding_name_value_count(
    const ShiftyBindingNameList *list, size_t index);
ShiftyStringView shifty_binding_name_value(
    const ShiftyBindingNameList *list, size_t index, size_t value_index);

/* The raw schema's shape name for `constraint_id` — the IRI of the named
 * (non-blank) RDF node it was lowered from. *out is NULL when there is none. */
ShiftyStatus shifty_evidence_session_shape_name_of(
    const ShiftyEvidenceSession *session,
    uint32_t constraint_id,
    ShiftyString **out);

/* Batch-evaluate `path` (a SPARQL 1.1 property path, same grammar as
 * name_path) from each of `nodes` (N-Triples spellings) over the session's
 * evaluation graph: the data graph for `data` mode, unioned with the shapes
 * graph for `union`/`union_all`. The result is an opaque list of
 * (node, reached) entries in input order, the reached values sorted in each
 * entry. */
ShiftyStatus shifty_evidence_session_resolve_path(
    const ShiftyEvidenceSession *session,
    const ShiftyStringView *nodes,
    size_t nodes_len,
    const char *path,
    size_t path_len,
    ShiftyPathResolutionList **out);

void shifty_path_resolution_list_destroy(ShiftyPathResolutionList *list);
size_t shifty_path_resolution_list_len(const ShiftyPathResolutionList *list);
ShiftyStringView shifty_path_resolution_node(
    const ShiftyPathResolutionList *list, size_t index);
size_t shifty_path_resolution_value_count(
    const ShiftyPathResolutionList *list, size_t index);
ShiftyStringView shifty_path_resolution_value(
    const ShiftyPathResolutionList *list, size_t index, size_t value_index);

/* The complete coverage horizon: every authored statement, every selected
 * focus, one evidence polarity each. */
ShiftyStatus shifty_evidence_session_validate(
    const ShiftyEvidenceSession *session,
    ShiftySeverity minimum_severity,
    const ShiftyStringView *shape_names,
    size_t shape_names_len,
    ShiftyEvidenceRun **out);

/*
 * The same snapshot, target selection, and evaluator, deciding each pair with
 * one short-circuiting satisfaction test instead of materializing evidence.
 * Use it for a verdict with counts, or as the baseline that isolates what
 * evidence tracing costs. It does not honor a minimum severity: with no failure
 * evidence there is no per-constraint severity to weigh, so any failing pair
 * makes the run non-conforming.
 */
ShiftyStatus shifty_evidence_session_validate_conformance(
    const ShiftyEvidenceSession *session,
    const ShiftyStringView *shape_names,
    size_t shape_names_len,
    ShiftyConformanceRun *out);

/*
 * The same single pass, additionally retaining the pairs that failed. On
 * corpora where failures are a small fraction of selected pairs, this plus
 * explaining each failing pair costs far less than materializing evidence for
 * everything and discarding the passes.
 */
ShiftyStatus shifty_evidence_session_find_failures(
    const ShiftyEvidenceSession *session,
    const ShiftyStringView *shape_names,
    size_t shape_names_len,
    ShiftyFailureList **out);

void shifty_failure_list_destroy(ShiftyFailureList *list);
size_t shifty_failure_list_len(const ShiftyFailureList *list);
ShiftyConformanceRun shifty_failure_list_conformance(const ShiftyFailureList *list);
/* Index of the *normalized* statement of failure `index`, or
 * SHIFTY_EVIDENCE_NO_INDEX when out of range. */
size_t shifty_failure_statement(const ShiftyFailureList *list, size_t index);
ShiftyStringView shifty_failure_focus(const ShiftyFailureList *list, size_t index);

/*
 * Materialize evidence for one pair from a failure list. Target selection is
 * not re-run — the pair is taken as already selected, which is the point.
 * The returned run carries an empty constraint catalog; take the catalog once
 * from shifty_evidence_session_constraints_json.
 */
ShiftyStatus shifty_evidence_session_explain_failure(
    const ShiftyEvidenceSession *session,
    const ShiftyFailureList *failures,
    size_t index,
    ShiftyEvidenceRun **out);

/*
 * Explain an arbitrary pair, naming the focus by its N-Triples rendering
 * (`<iri>`, `_:label`, `"lit"@lang`, `"lit"^^<datatype>`) — the same spelling
 * shifty_failure_focus and shifty_evidence_focus_node produce. `statement`
 * indexes the *normalized* statements. A focus the statement never selected
 * still yields well-defined evidence; it just describes a pair no run contained.
 */
ShiftyStatus shifty_evidence_session_explain(
    const ShiftyEvidenceSession *session,
    size_t statement,
    const char *focus,
    size_t focus_len,
    ShiftyEvidenceRun **out);

void shifty_evidence_run_destroy(ShiftyEvidenceRun *run);
uint8_t shifty_evidence_run_conforms(const ShiftyEvidenceRun *run);
/* The whole run as JSON, evidence trees included. */
ShiftyStringView shifty_evidence_run_json(const ShiftyEvidenceRun *run);

/*
 * The same run with evidence nodes and RDF terms hash-consed into shared
 * tables and referenced by index. Lossless: shifty_evidence_expand_json
 * restores exactly what shifty_evidence_run_json returned. Pass
 * include_catalog = 0 to elide the constraint catalog for a consumer that
 * already holds the schema; expanding such an encoding then requires the
 * catalog. The result is caller-owned — release it with shifty_string_destroy.
 */
ShiftyStatus shifty_evidence_run_compact_json(
    const ShiftyEvidenceRun *run,
    uint8_t include_catalog,
    ShiftyString **out);

/* Restore a compacted run. `catalog` (NULL with length 0 to omit) supplies the
 * catalog for an encoding written without one; it is the "constraints" value of
 * the original run, which shifty_evidence_session_constraints_json returns. */
ShiftyStatus shifty_evidence_expand_json(
    const char *compact,
    size_t compact_len,
    const char *catalog,
    size_t catalog_len,
    ShiftyString **out);

void shifty_string_destroy(ShiftyString *value);
ShiftyStringView shifty_string_data(const ShiftyString *value);

size_t shifty_evidence_run_statement_count(const ShiftyEvidenceRun *run);
size_t shifty_evidence_statement_source_id(
    const ShiftyEvidenceRun *run, size_t index);
/* SHIFTY_EVIDENCE_NO_INDEX when the authored statement has no normalized
 * counterpart (and for an out-of-range index). */
size_t shifty_evidence_statement_normalized_id(
    const ShiftyEvidenceRun *run, size_t index);
uint32_t shifty_evidence_statement_source_constraint(
    const ShiftyEvidenceRun *run, size_t index);
/* SHIFTY_EVIDENCE_NO_CONSTRAINT when there is no normalized counterpart. */
uint32_t shifty_evidence_statement_normalized_constraint(
    const ShiftyEvidenceRun *run, size_t index);
ShiftyStringView shifty_evidence_statement_constraint_kind(
    const ShiftyEvidenceRun *run, size_t index);
/* The authored selector, rendered (e.g. "targetClass(ex:Person)"). */
ShiftyStringView shifty_evidence_statement_target(
    const ShiftyEvidenceRun *run, size_t index);
size_t shifty_evidence_statement_focus_count(
    const ShiftyEvidenceRun *run, size_t index);

ShiftyStringView shifty_evidence_focus_node(
    const ShiftyEvidenceRun *run, size_t index, size_t focus_index);
/* SHIFTY_EVALUATION_FAIL for an out-of-range index; check the focus count. */
ShiftyEvaluationStatus shifty_evidence_focus_status(
    const ShiftyEvidenceRun *run, size_t index, size_t focus_index);
/* This focus's evidence subtree alone, as JSON: a `{"status": "pass"|"fail",
 * "evidence": ...}` object. */
ShiftyStringView shifty_evidence_focus_evidence_json(
    const ShiftyEvidenceRun *run, size_t index, size_t focus_index);
/* A human-readable rendering of the same evidence. */
ShiftyStringView shifty_evidence_focus_explanation(
    const ShiftyEvidenceRun *run, size_t index, size_t focus_index);

/*
 * Shape map: typed key -> value bindings for every selected (shape, focus)
 * pair of a run — the flat view one level above the evidence trees.
 *
 * `name_path` is a SPARQL 1.1 property path (non-NULL) evaluated from each
 * property shape's own node over the shapes graph to carry the author's name
 * for a slot; an empty path falls back to `sh:name`. Pass a NULL pointer with
 * length 0 to *skip* name resolution entirely.
 *
 * `value_paths` is an array of (label, path) pairs; each path is evaluated
 * from each bound value over the session's data graph to annotate it. Pass
 * NULL with length 0 for none.
 *
 * All ShiftyStringView / ShiftyTerm values handed out by the accessors below
 * point into the returned handle and stay valid until shifty_shape_map_destroy.
 * Absent min/max/observed are reported as SHIFTY_EVIDENCE_NO_INDEX; an absent
 * normalized constraint as SHIFTY_EVIDENCE_NO_CONSTRAINT.
 */
ShiftyStatus shifty_evidence_session_shape_map(
    const ShiftyEvidenceSession *session,
    const ShiftyEvidenceRun *run,
    const char *name_path,
    size_t name_path_len,
    const ShiftyStringPair *value_paths,
    size_t value_paths_len,
    ShiftyShapeMap **out);

void shifty_shape_map_destroy(ShiftyShapeMap *map);
uint8_t shifty_shape_map_conforms(const ShiftyShapeMap *map);
/* The plain-JSON summary ShapeMap.to_dict() would produce. */
ShiftyStringView shifty_shape_map_to_json(const ShiftyShapeMap *map);

size_t shifty_shape_map_shape_count(const ShiftyShapeMap *map);
ShiftyStringView shifty_shape_map_shape_name(
    const ShiftyShapeMap *map, size_t shape_index);
size_t shifty_shape_map_mapping_count(
    const ShiftyShapeMap *map, size_t shape_index);

ShiftyStringView shifty_shape_map_mapping_focus(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index);
ShiftyStringView shifty_shape_map_mapping_shape_name(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index);
ShiftyStringView shifty_shape_map_mapping_target(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index);
uint8_t shifty_shape_map_mapping_conforms(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index);
ShiftyStringView shifty_shape_map_mapping_evidence_json(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index);
ShiftyStringView shifty_shape_map_mapping_explanation(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index);
size_t shifty_shape_map_mapping_binding_count(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index);

ShiftyStringView shifty_shape_map_binding_key(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
/* The key's path as the externally-tagged serde encoding of the algebra Path
 * ("Id" or {"Pred": {"value": ...}} / {"Inverse": ...} / {"Seq": [...]} /
 * {"Alt": [...]} / {"Star": ...}), or the empty string for a pathless key. */
ShiftyStringView shifty_shape_map_binding_key_path_json(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
ShiftyStringView shifty_shape_map_binding_key_kind(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
size_t shifty_shape_map_binding_key_ordinal(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
ShiftyEvaluationStatus shifty_shape_map_binding_status(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
uint32_t shifty_shape_map_binding_source_constraint(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
uint32_t shifty_shape_map_binding_normalized_constraint(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
ShiftyStringView shifty_shape_map_binding_severity(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);

size_t shifty_shape_map_binding_name_count(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
ShiftyStringView shifty_shape_map_binding_name(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index, size_t name_index);

size_t shifty_shape_map_binding_min(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
size_t shifty_shape_map_binding_max(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
size_t shifty_shape_map_binding_observed(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
size_t shifty_shape_map_binding_missing(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);

uint8_t shifty_shape_map_binding_has_qualifier(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
ShiftyQualifierKind shifty_shape_map_binding_qualifier_kind(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
/* The qualifier IRI for Cls/Datatype/ShapeRef; empty for Const. */
ShiftyStringView shifty_shape_map_binding_qualifier_iri(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
/* The qualifier term for Const; an empty literal otherwise. */
ShiftyTerm shifty_shape_map_binding_qualifier_term(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);

size_t shifty_shape_map_binding_value_count(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
ShiftyTerm shifty_shape_map_binding_value(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index, size_t value_index);
size_t shifty_shape_map_binding_rejected_value_count(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
ShiftyTerm shifty_shape_map_binding_rejected_value(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index, size_t value_index);

/* Empty when the binding's evidence was not materialized. */
ShiftyStringView shifty_shape_map_binding_evidence_json(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
/* The same evidence rendered as indented text. */
ShiftyStringView shifty_shape_map_binding_explain(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);

/* value_paths annotations, one label per confirmed group. */
size_t shifty_shape_map_binding_annotation_label_count(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index);
ShiftyStringView shifty_shape_map_binding_annotation_label(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index, size_t label_index);
size_t shifty_shape_map_binding_annotation_term_count(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index, size_t label_index);
ShiftyTerm shifty_shape_map_binding_annotation_term(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index, size_t label_index, size_t term_index);
size_t shifty_shape_map_binding_annotation_reached_count(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index, size_t label_index, size_t term_index);
ShiftyTerm shifty_shape_map_binding_annotation_reached(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index,
    size_t binding_index, size_t label_index, size_t term_index,
    size_t reached_index);

#ifdef __cplusplus
}
#endif

#endif
