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

/* Whether a shape-map key is bound. */
typedef uint32_t ShiftyBindingStatus;
enum {
    SHIFTY_BINDING_BOUND = 0,
    SHIFTY_BINDING_UNBOUND = 1
};

/* Reported for an absent numeric value or an out-of-range index. */
#define SHIFTY_NO_INDEX ((size_t)-1)

typedef struct ShiftyDataset ShiftyDataset;
typedef struct ShiftyPreparedValidator ShiftyPreparedValidator;
typedef struct ShiftyQueryResult ShiftyQueryResult;
typedef struct ShiftyValidationResult ShiftyValidationResult;
typedef struct ShiftyAlgebraResult ShiftyAlgebraResult;
typedef struct ShiftyShapeMap ShiftyShapeMap;

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
 * Shape map: typed key -> value bindings for every selected (shape, focus)
 * pair.
 *
 * `name_path` is a SPARQL 1.1 property path (non-NULL) evaluated from each
 * property shape's own node over the shapes graph to carry the author's name
 * for a slot; an empty path falls back to `sh:name`. Pass a NULL pointer with
 * length 0 to *skip* name resolution entirely.
 *
 * `value_paths` is an array of (label, path) pairs; each path is evaluated
 * from each bound value over the evaluation graph to annotate it. Pass
 * NULL with length 0 for none.
 *
 * All ShiftyStringView / ShiftyTerm values handed out by the accessors below
 * point into the returned handle and stay valid until shifty_shape_map_destroy.
 * Absent min/max/observed are reported as SHIFTY_NO_INDEX.
 */
ShiftyStatus shifty_prepared_validator_shape_map(
    const ShiftyPreparedValidator *validator,
    const ShiftyDataset *dataset,
    ShiftyGraphMode graph_mode,
    uint8_t run_inference,
    ShiftySeverity minimum_severity,
    const ShiftyStringView *shape_names,
    size_t shape_names_len,
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
size_t shifty_shape_map_mapping_binding_count(
    const ShiftyShapeMap *map, size_t shape_index, size_t mapping_index);

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
ShiftyBindingStatus shifty_shape_map_binding_status(
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
