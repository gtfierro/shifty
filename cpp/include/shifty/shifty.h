#ifndef SHIFTY_SHIFTY_H
#define SHIFTY_SHIFTY_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SHIFTY_ABI_VERSION 1u

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

typedef struct ShiftyDataset ShiftyDataset;
typedef struct ShiftyPreparedValidator ShiftyPreparedValidator;
typedef struct ShiftyQueryResult ShiftyQueryResult;
typedef struct ShiftyValidationResult ShiftyValidationResult;
typedef struct ShiftyPropertyWitnessList ShiftyPropertyWitnessList;
typedef struct ShiftyAlgebraResult ShiftyAlgebraResult;

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

#ifdef __cplusplus
}
#endif

#endif
