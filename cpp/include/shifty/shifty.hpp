#ifndef SHIFTY_SHIFTY_HPP
#define SHIFTY_SHIFTY_HPP

#include "shifty/shifty.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace shifty {

/// RDF serialization formats accepted by the SDK.
enum class RdfFormat {
    Turtle,
    NTriples,
    Auto,
};

/// Controls which triples participate in focus discovery and evaluation.
enum class GraphMode {
    Data,
    Union,
    UnionAll,
};

/// Lowest result severity that fails validation. Findings below the threshold
/// are still reported (in the W3C report graph / AlgebraResult::violations());
/// they just don't make conforms() return false. Mirrors the
/// `minimum_severity` option of the Python / WASM / CLI APIs.
enum class Severity {
    Info,
    Warning,
    Violation,
};

/// Identifies the form of a SPARQL query result.
enum class QueryResultKind {
    Boolean,
    Solutions,
    Graph,
};

/// Options applied when validating a dataset.
struct ValidationOptions {
    /// Use the data/shapes union for evaluation while discovering focus nodes
    /// from the data graph.
    GraphMode graph_mode = GraphMode::Union;

    /// Run SHACL-AF rules to a fixed point before validation.
    bool run_inference = true;

    /// Lowest result severity that makes conforms() false. Defaults to
    /// `Severity::Info`, so any finding fails validation. Set to
    /// `Severity::Warning` to treat Info findings as non-failing, or
    /// `Severity::Violation` to fail only on Violations. Applied by both
    /// validate() and validate_algebra(); lower-severity findings remain
    /// available in the report / AlgebraResult::violations() regardless.
    Severity minimum_severity = Severity::Info;

    /// Named shape IRIs to use as validation entry points. When empty (the
    /// default), every target-bearing shape is used. Referenced helper shapes
    /// are still evaluated normally from the selected entries.
    std::vector<std::string> shape_names;

    /// A SPARQL 1.1 property path expression (e.g. "zea:roleName",
    /// "zea:role/zea:roleName", "^zea:describes/zea:roleName") evaluated from
    /// each `sh:property` shape's own node, over the shapes graph, to produce
    /// a stable key — used only by PreparedValidator::witnesses(). Prefixes
    /// resolve against the shapes document's declared `@prefix`es. When empty
    /// (the default), a property shape's own IRI/blank-node id is used as its
    /// key instead.
    std::string key_path;
};

/// Exception raised when an SDK operation fails.
class Error : public std::runtime_error {
public:
    /// Constructs an exception from a C ABI status and message.
    Error(ShiftyStatus status, std::string message)
        : std::runtime_error(std::move(message)), status_(status) {}

    /// Returns the machine-readable status associated with the failure.
    [[nodiscard]] ShiftyStatus status() const noexcept { return status_; }

private:
    ShiftyStatus status_;
};

/// Owned result of a SPARQL query.
class QueryResult {
public:
    /// Returns the query result form.
    [[nodiscard]] QueryResultKind kind() const noexcept { return kind_; }

    /// Returns the ASK result.
    ///
    /// \throws std::logic_error if this is not a Boolean result.
    [[nodiscard]] bool boolean_value() const {
        if (kind_ != QueryResultKind::Boolean) {
            throw std::logic_error("SPARQL result is not Boolean");
        }
        return boolean_value_;
    }

    /// Returns serialized query data.
    ///
    /// SELECT results use SPARQL Results JSON. CONSTRUCT and DESCRIBE results
    /// use N-Triples. ASK results use SPARQL Results JSON and are also
    /// available through boolean_value().
    [[nodiscard]] const std::string &data() const noexcept { return data_; }

    /// Returns the MIME type of data().
    [[nodiscard]] const std::string &media_type() const noexcept {
        return media_type_;
    }

private:
    friend class Dataset;

    QueryResult(
        QueryResultKind kind,
        bool boolean_value,
        std::string data,
        std::string media_type)
        : kind_(kind),
          boolean_value_(boolean_value),
          data_(std::move(data)),
          media_type_(std::move(media_type)) {}

    QueryResultKind kind_;
    bool boolean_value_;
    std::string data_;
    std::string media_type_;
};

/// Owned result of SHACL validation.
class ValidationResult {
public:
    /// Returns true when the dataset conforms to all shapes.
    [[nodiscard]] bool conforms() const noexcept { return conforms_; }

    /// Returns the W3C sh:ValidationReport serialized as Turtle.
    [[nodiscard]] const std::string &report_turtle() const noexcept {
        return report_turtle_;
    }

    /// Returns a human-readable validation summary.
    [[nodiscard]] const std::string &results_text() const noexcept {
        return results_text_;
    }

private:
    friend class PreparedValidator;

    ValidationResult(
        bool conforms,
        std::string report_turtle,
        std::string results_text)
        : conforms_(conforms),
          report_turtle_(std::move(report_turtle)),
          results_text_(std::move(results_text)) {}

    bool conforms_;
    std::string report_turtle_;
    std::string results_text_;
};

/// The observed binding of one `sh:property` shape at one *conforming* focus
/// node — the inverse of a violation: not what failed, but what a passing
/// property shape's `sh:path` actually resolved to.
struct PropertyWitness {
    /// The focus node (e.g. an equipment IRI) that conformed.
    std::string focus_node;

    /// The node shape (application profile) `focus_node` conformed to.
    std::string shape_id;

    /// A stable id for the `sh:property` shape: the lexical value reached by
    /// evaluating `ValidationOptions::key_path` from the property shape's own
    /// node when it resolves to a value, otherwise the property shape's own
    /// IRI/blank-node id.
    std::string key;

    /// The `sh:path` value nodes, deduped. Each entry is rendered in full
    /// (`<iri>`, `_:label`, `"lit"`, `"lit"@lang`, or `"lit"^^<datatype>`) so
    /// IRI and literal bindings (and a literal's datatype/language) stay
    /// distinguishable. When the property shape declares a
    /// `sh:qualifiedValueShape`, these are narrowed to the values satisfying
    /// the qualifier rather than every raw path value.
    std::vector<std::string> value_nodes;
};

/// One failed atomic constraint within an AlgebraViolation. An absent `path`
/// or `author_message` is represented as an empty string.
struct AlgebraReason {
    /// The node at which the constraint failed.
    std::string value;

    /// Path from the focus node to `value`, in π notation (e.g. `ex:name`),
    /// or empty when the failure is not value-scoped.
    std::string path;

    /// Engine-generated description of the failing constraint — always set.
    std::string message;

    /// The source shape's `sh:message`, if the author supplied one (with
    /// `{$this}`/`{?var}` resolved), otherwise empty. Prefer this over
    /// `message` when non-empty.
    std::string author_message;

    /// SHACL severity (`"Violation"`, `"Warning"`, `"Info"`, or a custom IRI).
    std::string severity;
};

/// One focus node that failed a shape, from the algebra validation path: the
/// engine's own conformance oracle evaluated directly against the SHACL
/// algebra, as a structured tree rather than a W3C sh:ValidationReport graph.
struct AlgebraViolation {
    /// The focus node that failed.
    std::string focus_node;

    /// Named shape IRI, or empty if the violated statement was an anonymous
    /// (blank-node) shape.
    std::string shape_name;

    /// Most severe reason in this grouped finding.
    std::string severity;

    /// The individual failing constraints that make up this finding.
    std::vector<AlgebraReason> reasons;
};

/// Owned result of algebra-path SHACL validation.
class AlgebraResult {
public:
    /// Returns true when the dataset conforms to all shapes.
    [[nodiscard]] bool conforms() const noexcept { return conforms_; }

    /// Returns the violations found, if any.
    [[nodiscard]] const std::vector<AlgebraViolation> &violations() const noexcept {
        return violations_;
    }

    /// Returns a human-readable validation summary.
    [[nodiscard]] const std::string &results_text() const noexcept {
        return results_text_;
    }

private:
    friend class PreparedValidator;

    AlgebraResult(
        bool conforms,
        std::vector<AlgebraViolation> violations,
        std::string results_text)
        : conforms_(conforms),
          violations_(std::move(violations)),
          results_text_(std::move(results_text)) {}

    bool conforms_;
    std::vector<AlgebraViolation> violations_;
    std::string results_text_;
};

/// The evidence polarity produced for one selected `(statement, focus)` pair.
enum class EvaluationStatus {
    Pass,
    Fail,
};

/// Conformance-only totals from one prepared snapshot. Counts are over
/// *normalized* `(statement, focus)` pairs — the pairs evidence is materialized
/// against, before authored statements that normalize together fan the same
/// evidence back out, so they need not match the number of
/// StatementEvidence::selected_foci entries an EvidenceRun reports.
struct ConformanceRun {
    /// True when no selected pair failed.
    bool conforms = true;

    /// Pairs target selection produced.
    std::size_t selected_pairs = 0;

    /// Pairs whose shape held.
    std::size_t passed = 0;

    /// Pairs whose shape did not hold.
    std::size_t failed = 0;
};

/// One selected focus node under one authored statement, and the single
/// evidence polarity that applies to it.
struct FocusEvidence {
    /// The focus node, rendered in full (`<iri>`, `_:label`, `"lit"@lang`,
    /// `"lit"^^<datatype>`).
    std::string focus_node;

    /// Whether the shape held at this focus.
    EvaluationStatus status = EvaluationStatus::Fail;

    /// This focus's evidence tree as JSON: a `{"status": ..., "evidence": ...}`
    /// object holding a satisfaction trace or a failure witness. Constraint ids
    /// inside it resolve against EvidenceSession::constraints_json().
    std::string evidence_json;

    /// A human-readable rendering of the same evidence.
    std::string explanation;

    /// True when the shape held at this focus.
    [[nodiscard]] bool passed() const noexcept {
        return status == EvaluationStatus::Pass;
    }
};

/// One authored statement and every focus its selector chose. A statement whose
/// selector chose nothing is still reported, with an empty `selected_foci` —
/// that is what makes a run a coverage horizon rather than a findings list.
struct StatementEvidence {
    /// Index of this statement in the authored (pre-normalization) schema.
    std::size_t source_statement_id = 0;

    /// Index in the normalized schema, absent when the authored statement has
    /// no normalized counterpart.
    std::optional<std::size_t> normalized_statement_id;

    /// The authored constraint this statement carries.
    std::uint32_t source_constraint_id = 0;

    /// Its normalized counterpart, absent when there is none. This is the id to
    /// look up in the `normalized` catalog of constraints_json().
    std::optional<std::uint32_t> normalized_constraint_id;

    /// Stable semantic kind of the constraint (e.g. `"ClassMembership"`),
    /// spelled as it appears in the run's JSON.
    std::string constraint_kind;

    /// The authored selector, rendered (e.g. `"targetClass(ex:Person)"`).
    std::string target;

    /// Every focus this statement selected, each with one evidence polarity.
    std::vector<FocusEvidence> selected_foci;
};

/// One `(normalized statement, focus)` pair, as target selection produced it —
/// enough to name a pair, and nothing that costs anything to carry.
struct SelectedPair {
    /// Index into the *normalized* statements.
    std::size_t statement = 0;

    /// The focus node, rendered in full.
    std::string focus_node;
};

namespace detail {

inline void check_abi() {
    const auto actual = shifty_abi_version();
    if (actual != SHIFTY_ABI_VERSION) {
        throw std::runtime_error(
            "shifty C++ header/library ABI mismatch: header=" +
            std::to_string(SHIFTY_ABI_VERSION) +
            ", library=" + std::to_string(actual));
    }
}

inline ShiftyRdfFormat to_c(RdfFormat format) {
    switch (format) {
    case RdfFormat::Turtle:
        return SHIFTY_RDF_FORMAT_TURTLE;
    case RdfFormat::NTriples:
        return SHIFTY_RDF_FORMAT_NTRIPLES;
    case RdfFormat::Auto:
        return SHIFTY_RDF_FORMAT_AUTO;
    }
    throw std::invalid_argument("unknown RDF format");
}

inline ShiftyGraphMode to_c(GraphMode mode) {
    switch (mode) {
    case GraphMode::Data:
        return SHIFTY_GRAPH_MODE_DATA;
    case GraphMode::Union:
        return SHIFTY_GRAPH_MODE_UNION;
    case GraphMode::UnionAll:
        return SHIFTY_GRAPH_MODE_UNION_ALL;
    }
    throw std::invalid_argument("unknown graph mode");
}

inline ShiftySeverity to_c(Severity severity) {
    switch (severity) {
    case Severity::Info:
        return SHIFTY_SEVERITY_INFO;
    case Severity::Warning:
        return SHIFTY_SEVERITY_WARNING;
    case Severity::Violation:
        return SHIFTY_SEVERITY_VIOLATION;
    }
    throw std::invalid_argument("unknown severity");
}

inline QueryResultKind from_c(ShiftyQueryResultKind kind) {
    switch (kind) {
    case SHIFTY_QUERY_RESULT_BOOLEAN:
        return QueryResultKind::Boolean;
    case SHIFTY_QUERY_RESULT_SOLUTIONS:
        return QueryResultKind::Solutions;
    case SHIFTY_QUERY_RESULT_GRAPH:
        return QueryResultKind::Graph;
    }
    throw std::runtime_error("unknown query result kind returned by shifty");
}

inline std::string copy(ShiftyStringView value) {
    if (value.data == nullptr || value.len == 0) {
        return {};
    }
    return std::string(value.data, value.len);
}

inline void check(ShiftyStatus status) {
    if (status != SHIFTY_STATUS_OK) {
        const char *message = shifty_last_error_message();
        throw Error(status, message == nullptr ? "unknown shifty error" : message);
    }
}

inline const char *optional_data(std::string_view value) noexcept {
    return value.empty() ? nullptr : value.data();
}

inline std::vector<ShiftyStringView> string_views(
    const std::vector<std::string> &values) {
    std::vector<ShiftyStringView> out;
    out.reserve(values.size());
    for (const auto &value : values) {
        out.push_back(ShiftyStringView{value.data(), value.size()});
    }
    return out;
}

inline std::string path_utf8(const std::filesystem::path &path) {
    const auto value = path.u8string();
#if defined(__cpp_lib_char8_t)
    return std::string(
        reinterpret_cast<const char *>(value.data()), value.size());
#else
    return value;
#endif
}

struct DatasetDeleter {
    void operator()(ShiftyDataset *value) const noexcept {
        shifty_dataset_destroy(value);
    }
};

struct ValidatorDeleter {
    void operator()(ShiftyPreparedValidator *value) const noexcept {
        shifty_prepared_validator_destroy(value);
    }
};

struct QueryResultDeleter {
    void operator()(ShiftyQueryResult *value) const noexcept {
        shifty_query_result_destroy(value);
    }
};

struct ValidationResultDeleter {
    void operator()(ShiftyValidationResult *value) const noexcept {
        shifty_validation_result_destroy(value);
    }
};

struct PropertyWitnessListDeleter {
    void operator()(ShiftyPropertyWitnessList *value) const noexcept {
        shifty_property_witness_list_destroy(value);
    }
};

struct AlgebraResultDeleter {
    void operator()(ShiftyAlgebraResult *value) const noexcept {
        shifty_algebra_result_destroy(value);
    }
};

struct EvidenceSessionDeleter {
    void operator()(ShiftyEvidenceSession *value) const noexcept {
        shifty_evidence_session_destroy(value);
    }
};

struct EvidenceRunDeleter {
    void operator()(ShiftyEvidenceRun *value) const noexcept {
        shifty_evidence_run_destroy(value);
    }
};

struct FailureListDeleter {
    void operator()(ShiftyFailureList *value) const noexcept {
        shifty_failure_list_destroy(value);
    }
};

struct StringDeleter {
    void operator()(ShiftyString *value) const noexcept {
        shifty_string_destroy(value);
    }
};

struct BindingNameListDeleter {
    void operator()(ShiftyBindingNameList *value) const noexcept {
        shifty_binding_name_list_destroy(value);
    }
};

struct PathResolutionListDeleter {
    void operator()(ShiftyPathResolutionList *value) const noexcept {
        shifty_path_resolution_list_destroy(value);
    }
};

struct ShapeMapDeleter {
    void operator()(ShiftyShapeMap *value) const noexcept {
        shifty_shape_map_destroy(value);
    }
};

using OwnedString = std::unique_ptr<ShiftyString, StringDeleter>;

using OwnedBindingNames =
    std::unique_ptr<ShiftyBindingNameList, BindingNameListDeleter>;
using OwnedPathResolutions =
    std::unique_ptr<ShiftyPathResolutionList, PathResolutionListDeleter>;

/// Convert the C++ `value_paths` (label -> path) list into the ABI's
/// parallel string-pair array, valid for the duration of the call.
inline std::vector<ShiftyStringPair> build_value_path_pairs(
    const std::vector<std::pair<std::string, std::string>> &value_paths) {
    std::vector<ShiftyStringPair> out;
    out.reserve(value_paths.size());
    for (const auto &[label, path] : value_paths) {
        out.push_back(ShiftyStringPair{
            ShiftyStringView{label.data(), label.size()},
            ShiftyStringView{path.data(), path.size()},
        });
    }
    return out;
}

/// Not an overload of from_c(): ShiftyEvaluationStatus and
/// ShiftyQueryResultKind are both uint32_t typedefs, so they cannot be
/// distinguished by overload resolution.
inline EvaluationStatus evaluation_status_from_c(ShiftyEvaluationStatus status) {
    switch (status) {
    case SHIFTY_EVALUATION_PASS:
        return EvaluationStatus::Pass;
    case SHIFTY_EVALUATION_FAIL:
        return EvaluationStatus::Fail;
    }
    throw std::runtime_error("unknown evaluation status returned by shifty");
}

inline ConformanceRun from_c(const ShiftyConformanceRun &run) {
    ConformanceRun out;
    out.conforms = run.conforms != 0;
    out.selected_pairs = run.selected_pairs;
    out.passed = run.passed;
    out.failed = run.failed;
    return out;
}

inline std::optional<std::size_t> optional_index(std::size_t value) {
    if (value == SHIFTY_EVIDENCE_NO_INDEX) {
        return std::nullopt;
    }
    return value;
}

inline std::optional<std::uint32_t> optional_constraint(std::uint32_t value) {
    if (value == SHIFTY_EVIDENCE_NO_CONSTRAINT) {
        return std::nullopt;
    }
    return value;
}

} // namespace detail

/// The statement-oriented coverage horizon of one evidence-carrying validation
/// run: every authored statement, every focus its selector chose, and exactly
/// one evidence polarity per pair — a satisfaction trace where the shape held, a
/// failure witness where it did not. Unlike a validation report, passing pairs
/// are present too, which is what makes the run usable as provenance rather
/// than only as a findings list.
///
/// Move-only: the structured view is materialized eagerly, while json() and
/// compact_json() are served from the retained engine handle.
class EvidenceRun {
public:
    EvidenceRun(const EvidenceRun &) = delete;
    EvidenceRun &operator=(const EvidenceRun &) = delete;
    EvidenceRun(EvidenceRun &&) noexcept = default;
    EvidenceRun &operator=(EvidenceRun &&) noexcept = default;
    ~EvidenceRun() = default;

    /// Returns true when no selected pair failed at or above the run's
    /// minimum severity.
    [[nodiscard]] bool conforms() const noexcept { return conforms_; }

    /// Returns every authored statement, including those that selected nothing.
    [[nodiscard]] const std::vector<StatementEvidence> &statements() const noexcept {
        return statements_;
    }

    /// Returns the whole run as JSON, evidence trees included.
    [[nodiscard]] std::string json() const {
        return detail::copy(shifty_evidence_run_json(handle_.get()));
    }

    /// Returns the run with evidence nodes and RDF terms hash-consed into
    /// shared tables and referenced by index. Lossless: expand_evidence()
    /// restores exactly what json() returns.
    ///
    /// \param include_catalog Keep the constraint catalog in the encoding. Pass
    /// false for a consumer that already holds the schema — on a small graph the
    /// catalog is most of the payload — and supply it to expand_evidence()
    /// separately, from EvidenceSession::constraints_json().
    /// \throws Error if the run cannot be encoded.
    [[nodiscard]] std::string compact_json(bool include_catalog = true) const {
        ShiftyString *raw = nullptr;
        detail::check(shifty_evidence_run_compact_json(
            handle_.get(),
            static_cast<std::uint8_t>(include_catalog),
            &raw));
        detail::OwnedString owned(raw);
        return detail::copy(shifty_string_data(owned.get()));
    }

    /// Returns true when the run conforms, so a run can be tested directly.
    explicit operator bool() const noexcept { return conforms_; }

private:
    friend class EvidenceSession;
    using Handle = std::unique_ptr<ShiftyEvidenceRun, detail::EvidenceRunDeleter>;

    explicit EvidenceRun(ShiftyEvidenceRun *raw) : handle_(raw) {
        conforms_ = shifty_evidence_run_conforms(handle_.get()) != 0;
        const std::size_t count =
            shifty_evidence_run_statement_count(handle_.get());
        statements_.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            StatementEvidence statement;
            statement.source_statement_id =
                shifty_evidence_statement_source_id(handle_.get(), i);
            statement.normalized_statement_id = detail::optional_index(
                shifty_evidence_statement_normalized_id(handle_.get(), i));
            statement.source_constraint_id =
                shifty_evidence_statement_source_constraint(handle_.get(), i);
            statement.normalized_constraint_id = detail::optional_constraint(
                shifty_evidence_statement_normalized_constraint(handle_.get(), i));
            statement.constraint_kind = detail::copy(
                shifty_evidence_statement_constraint_kind(handle_.get(), i));
            statement.target =
                detail::copy(shifty_evidence_statement_target(handle_.get(), i));

            const std::size_t focus_count =
                shifty_evidence_statement_focus_count(handle_.get(), i);
            statement.selected_foci.reserve(focus_count);
            for (std::size_t f = 0; f < focus_count; ++f) {
                FocusEvidence focus;
                focus.focus_node =
                    detail::copy(shifty_evidence_focus_node(handle_.get(), i, f));
                focus.status = detail::evaluation_status_from_c(
                    shifty_evidence_focus_status(handle_.get(), i, f));
                focus.evidence_json = detail::copy(
                    shifty_evidence_focus_evidence_json(handle_.get(), i, f));
                focus.explanation = detail::copy(
                    shifty_evidence_focus_explanation(handle_.get(), i, f));
                statement.selected_foci.push_back(std::move(focus));
            }
            statements_.push_back(std::move(statement));
        }
    }

    Handle handle_;
    bool conforms_ = false;
    std::vector<StatementEvidence> statements_;
};

// ── shape-map v2: typed key -> value bindings ────────────────────────────────
// The flat view one level above the evidence trees: for each selected
// (shape, focus) pair, which property obligations bound to which values and
// which are still unbound, with a typed Key -> Binding vocabulary. This is the
// C++ port of the Python `shifty.ShapeMap` / `shifty.shape_map()`.

/// The three RDF term kinds a shape-map value can carry.
enum class TermKind {
    Iri,
    Literal,
    BNode,
};

/// A typed RDF term: IRI, literal, or blank node, with an N-Triples
/// rendering (`n3()`) that matches `terms.py` — `xsd:string` datatypes are
/// omitted and lexical escapes applied, so `"lit"^^<..string>` reads `"lit"`.
class Term {
public:
    Term() = default;
    Term(TermKind kind, std::string value, std::string datatype = {},
         std::string language = {})
        : kind_(kind),
          value_(std::move(value)),
          datatype_(std::move(datatype)),
          language_(std::move(language)) {}

    /// The term kind.
    [[nodiscard]] TermKind kind() const noexcept { return kind_; }
    /// The IRI text (no brackets), literal lexical form, or blank-node label.
    [[nodiscard]] const std::string &value() const noexcept { return value_; }
    /// The datatype IRI for a literal; empty otherwise. `xsd:string` is
    /// preserved here but omitted from `n3()`.
    [[nodiscard]] const std::string &datatype() const noexcept { return datatype_; }
    /// The language tag for a literal; empty otherwise.
    [[nodiscard]] const std::string &language() const noexcept { return language_; }

    [[nodiscard]] bool is_iri() const noexcept { return kind_ == TermKind::Iri; }
    [[nodiscard]] bool is_literal() const noexcept { return kind_ == TermKind::Literal; }
    [[nodiscard]] bool is_bnode() const noexcept { return kind_ == TermKind::BNode; }

    /// The full N-Triples rendering (`<iri>`, `"lit"@lang`, `"lit"^^<dt>`,
    /// `_:label`).
    [[nodiscard]] std::string n3() const {
        switch (kind_) {
        case TermKind::Iri:
            return "<" + value_ + ">";
        case TermKind::BNode:
            return "_:" + value_;
        case TermKind::Literal:
            break;
        }
        std::string escaped = value_;
        std::size_t pos = 0;
        while ((pos = escaped.find_first_of("\\\"\n", pos)) != std::string::npos) {
            const char replacement = escaped[pos] == '\n' ? 'n' : escaped[pos];
            escaped.replace(pos, 1, "\\");
            escaped.insert(pos + 1, 1, replacement);
            pos += 2;
        }
        constexpr const char *XSD_STRING_IRI =
            "http://www.w3.org/2001/XMLSchema#string";
        if (!language_.empty()) {
            return "\"" + escaped + "\"@" + language_;
        }
        if (!datatype_.empty() && datatype_ != XSD_STRING_IRI) {
            return "\"" + escaped + "\"^^<" + datatype_ + ">";
        }
        return "\"" + escaped + "\"";
    }

    bool operator==(const Term &other) const noexcept {
        return kind_ == other.kind_ && value_ == other.value_ &&
               datatype_ == other.datatype_ && language_ == other.language_;
    }
    bool operator!=(const Term &other) const noexcept { return !(*this == other); }
    bool operator<(const Term &other) const noexcept {
        if (kind_ != other.kind_) return kind_ < other.kind_;
        if (value_ != other.value_) return value_ < other.value_;
        if (datatype_ != other.datatype_) return datatype_ < other.datatype_;
        return language_ < other.language_;
    }

private:
    friend class Binding;
    static Term from_c(const ShiftyTerm &value) {
        Term out;
        out.kind_ = static_cast<TermKind>(value.kind);
        out.value_ = detail::copy(value.value);
        out.datatype_ = detail::copy(value.datatype);
        out.language_ = detail::copy(value.language);
        return out;
    }

    TermKind kind_ = TermKind::Literal;
    std::string value_;
    std::string datatype_;
    std::string language_;
};

/// The five forms of a SPARQL property path (mirrors the algebra `Path`).
enum class PathKind {
    Id,
    Pred,
    Inverse,
    Seq,
    Alt,
    Star,
};

/// A typed property path: a single predicate step, or a composition.
/// `Seq`/`Alt` hold ordered children; `Inverse`/`Star` hold exactly one.
/// `Id` and `Pred` hold none.
class Path {
public:
    Path() = default;

    [[nodiscard]] PathKind kind() const noexcept { return kind_; }
    /// The predicate IRI for a `Pred`; empty otherwise.
    [[nodiscard]] const std::string &iri() const noexcept { return iri_; }
    /// The children (`Seq`/`Alt`: all; `Inverse`/`Star`: the single inner).
    [[nodiscard]] const std::vector<Path> &children() const noexcept {
        return children_;
    }

    /// True for the `rdf:type/rdfs:subClassOf*` class-membership path (renders
    /// as `a`).
    [[nodiscard]] bool is_class_path() const noexcept;

    /// The rendered path: compact local names by default (e.g.
    /// `hasPoint->Supply_Air_Flow_Sensor`), full IRIs otherwise.
    [[nodiscard]] std::string str(bool compact = true) const;

    bool operator==(const Path &other) const noexcept {
        return kind_ == other.kind_ && iri_ == other.iri_ &&
               children_ == other.children_;
    }
    bool operator!=(const Path &other) const noexcept { return !(*this == other); }

    /// Parse the externally-tagged serde encoding of the algebra `Path` that
    /// `Binding::key()` carries (`"Id"`, `{"Pred": {"value": "…"}}`,
    /// `{"Inverse": …}`, `{"Seq": […]}`, `{"Alt": […]}`, `{"Star": …}`).
    /// Returns std::nullopt for malformed input.
    [[nodiscard]] static std::optional<Path> parse_json(std::string_view json);

private:
    friend class Binding;
    Path(PathKind kind, std::string iri, std::vector<Path> children)
        : kind_(kind), iri_(std::move(iri)), children_(std::move(children)) {}

    PathKind kind_ = PathKind::Id;
    std::string iri_;
    std::vector<Path> children_;
};

/// The four qualifier kinds a key can carry.
enum class QualifierKind {
    Cls,
    Const,
    Datatype,
    ShapeRef,
};

/// The optional qualifier of a shape-map key: a class, a constant value, a
/// datatype, or a named-shape reference.
class Qualifier {
public:
    Qualifier(QualifierKind kind, std::string iri, Term term)
        : kind_(kind), iri_(std::move(iri)), term_(std::move(term)) {}

    [[nodiscard]] QualifierKind kind() const noexcept { return kind_; }
    /// The IRI for `Cls`/`Datatype`/`ShapeRef`; empty for `Const`.
    [[nodiscard]] const std::string &iri() const noexcept { return iri_; }
    /// The constant term for `Const`; an empty literal otherwise.
    [[nodiscard]] const Term &term() const noexcept { return term_; }

    /// The compact rendering (e.g. `Supply_Air_Flow_Sensor`).
    [[nodiscard]] std::string str() const;

    bool operator==(const Qualifier &other) const noexcept {
        return kind_ == other.kind_ && iri_ == other.iri_ && term_ == other.term_;
    }
    bool operator!=(const Qualifier &other) const noexcept { return !(*this == other); }

private:
    QualifierKind kind_;
    std::string iri_;
    Term term_;
};

/// A typed, hashable shape-map key: the property shape's path plus its
/// qualifier class when one is declared, disambiguated by ordinal when several
/// bindings share a `(path, qualifier)`. `str()` reads
/// `hasPoint->Supply_Air_Flow_Sensor`, or the `kind` tag for a pathless key.
class Key {
public:
    Key() = default;
    Key(std::optional<Path> path, std::optional<Qualifier> qualifier,
        std::size_t ordinal = 1, std::string kind = "count")
        : path_(std::move(path)),
          qualifier_(std::move(qualifier)),
          ordinal_(ordinal),
          kind_(std::move(kind)) {}

    /// The path, or std::nullopt for a pathless key (nodeKind, …).
    [[nodiscard]] const std::optional<Path> &path() const noexcept { return path_; }
    /// The qualifier, if one is declared.
    [[nodiscard]] const std::optional<Qualifier> &qualifier() const noexcept {
        return qualifier_;
    }
    /// Disambiguates identical `(path, qualifier)` pairs; the n-th in
    /// lowering order.
    [[nodiscard]] std::size_t ordinal() const noexcept { return ordinal_; }
    /// The constraint tag fallback for pathless keys (e.g. `count`).
    [[nodiscard]] const std::string &kind() const noexcept { return kind_; }

    /// The rendered key, reading e.g. `hasPoint->Supply_Air_Flow_Sensor`.
    [[nodiscard]] std::string str() const;
    explicit operator std::string() const { return str(); }

    bool operator==(const Key &other) const noexcept {
        return path_ == other.path_ && qualifier_ == other.qualifier_ &&
               ordinal_ == other.ordinal_ && kind_ == other.kind_;
    }
    bool operator!=(const Key &other) const noexcept { return !(*this == other); }
    bool operator<(const Key &other) const noexcept;  // for std::map<Key, …>

private:
    std::optional<Path> path_;
    std::optional<Qualifier> qualifier_;
    std::size_t ordinal_ = 1;
    std::string kind_ = "count";
};

/// One bound value plus its `value_paths` annotations.
struct BoundValue {
    /// The bound value.
    Term term;
    /// `label -> reached` for each configured `value_paths` label, empty per
    /// value when nothing is reached.
    std::map<std::string, std::vector<Term>> annotations;
};

/// One key of a mapping: a property obligation and what it bound to. A passing
/// (`ok()`) binding carries `values()`; a failing one carries
/// `missing()`/`rejected_values()` and the `evidence_json()` witness subtree.
class Binding {
public:
    Binding() = default;

    [[nodiscard]] const Key &key() const noexcept { return key_; }
    [[nodiscard]] const std::optional<Path> &path() const noexcept {
        return key_.path();
    }
    [[nodiscard]] const std::optional<Qualifier> &qualifier() const noexcept {
        return key_.qualifier();
    }

    /// True when bound (`status() == "pass"`).
    [[nodiscard]] bool ok() const noexcept { return ok_; }
    /// `"pass"` (bound) or `"fail"` (unbound).
    [[nodiscard]] const std::string &status() const noexcept { return status_; }

    /// The loaded source constraint id.
    [[nodiscard]] std::uint32_t source_constraint_id() const noexcept {
        return source_constraint_id_;
    }
    /// The normalized constraint id, absent when there is none.
    [[nodiscard]] const std::optional<std::uint32_t> &constraint_id() const noexcept {
        return constraint_id_;
    }

    /// SHACL severity (`"violation"`/`"warning"`/`"info"`, lowercased).
    [[nodiscard]] const std::string &severity() const noexcept { return severity_; }
    /// The author's names for the slot (`name_path`), if any.
    [[nodiscard]] const std::vector<std::string> &names() const noexcept {
        return names_;
    }
    /// The author's name for the slot — the first value of `names()`, or
    /// nullptr when there is none.
    [[nodiscard]] const std::string *name() const noexcept {
        return names_.empty() ? nullptr : &names_.front();
    }

    /// The values the key's path bound. For a failing key these are the
    /// qualifying near-matches (same as `partial_values()`). Empty only when
    /// the evidence was unavailable.
    [[nodiscard]] const std::vector<Term> &values() const noexcept {
        return values_;
    }
    /// Whether the source constraint expects exactly one value.
    [[nodiscard]] bool expects_single() const noexcept {
        return min().has_value() && *min() == 1 && max().has_value() && *max() == 1;
    }
    /// The declared lower bound, present even when evidence was never
    /// materialized.
    [[nodiscard]] const std::optional<std::size_t> &min() const noexcept { return min_; }
    /// The declared upper bound.
    [[nodiscard]] const std::optional<std::size_t> &max() const noexcept { return max_; }
    /// How many qualifying values are still owed (0 for a bound key).
    [[nodiscard]] std::size_t missing() const noexcept { return missing_; }
    /// The count observed, when evidence carried it.
    [[nodiscard]] const std::optional<std::size_t> &observed() const noexcept {
        return observed_;
    }
    /// Values that did qualify under a failing count (never enough) — the same
    /// as `values()` for a failing key.
    [[nodiscard]] std::vector<Term> partial_values() const {
        return ok_ ? std::vector<Term>{} : values_;
    }
    /// Near-miss candidates the path reached but the qualifier rejected.
    [[nodiscard]] const std::vector<Term> &rejected_values() const noexcept {
        return rejected_values_;
    }

    /// Every bound value paired with its `value_paths` annotations (empty
    /// per-value when `value_paths` was not configured).
    [[nodiscard]] const std::vector<BoundValue> &annotated_values() const noexcept {
        return annotated_values_;
    }
    /// `label -> value -> reached`, pivoted from `annotated_values()`.
    [[nodiscard]] std::map<std::string, std::map<Term, std::vector<Term>>>
    annotations() const {
        std::map<std::string, std::map<Term, std::vector<Term>>> out;
        for (const auto &bound : annotated_values_) {
            for (const auto &entry : bound.annotations) {
                out[entry.first][bound.term] = entry.second;
            }
        }
        return out;
    }

    /// This key's evidence subtree as JSON. Empty when the evidence was not
    /// materialized.
    [[nodiscard]] const std::string &evidence_json() const noexcept {
        return evidence_json_;
    }
    /// The same evidence as indented text.
    [[nodiscard]] const std::string &explain() const noexcept { return explain_; }

private:
    friend class Mapping;
    friend class ShapeMap;
    static Binding from_c(const ShiftyShapeMap *map, std::size_t shape,
                          std::size_t mapping, std::size_t index);

    Key key_;
    bool ok_ = false;
    std::string status_;
    std::uint32_t source_constraint_id_ = 0;
    std::optional<std::uint32_t> constraint_id_;
    std::string severity_;
    std::vector<std::string> names_;
    std::optional<std::size_t> min_;
    std::optional<std::size_t> max_;
    std::optional<std::size_t> observed_;
    std::size_t missing_ = 0;
    std::vector<Term> values_;
    std::vector<Term> rejected_values_;
    std::vector<BoundValue> annotated_values_;
    std::string evidence_json_;
    std::string explain_;
};

/// One `(focus node, shape statement)` association with its key bindings.
class Mapping {
public:
    Mapping() = default;

    /// The focus node, rendered in full (`<iri>`, `_:label`, `"lit"@lang`, …)
    /// — the same spelling `FocusEvidence::focus_node` carries.
    [[nodiscard]] const std::string &focus() const noexcept { return focus_; }
    /// The named shape IRI, or empty for an anonymous shape.
    [[nodiscard]] const std::string &shape_name() const noexcept { return shape_name_; }
    /// The authored selector, rendered.
    [[nodiscard]] const std::string &target() const noexcept { return target_; }
    /// True when the focus conformed to the shape.
    [[nodiscard]] bool conforms() const noexcept { return conforms_; }
    /// Every key -> binding, in authored order.
    [[nodiscard]] const std::vector<Binding> &bindings() const noexcept {
        return bindings_;
    }
    /// The underlying focus evaluation: `evaluation().passed()`, its
    /// `evidence_json` / `explanation`.
    [[nodiscard]] const FocusEvidence &evaluation() const noexcept {
        return evaluation_;
    }

    /// Every bound key, in authored order.
    [[nodiscard]] std::vector<const Binding *> successful() const {
        std::vector<const Binding *> out;
        for (const auto &binding : bindings_) {
            if (binding.ok()) out.push_back(&binding);
        }
        return out;
    }
    /// Every unbound key; the binding carries the witness subtree, shortfall
    /// counts, and near-misses.
    [[nodiscard]] std::vector<const Binding *> unsuccessful() const {
        std::vector<const Binding *> out;
        for (const auto &binding : bindings_) {
            if (!binding.ok()) out.push_back(&binding);
        }
        return out;
    }

    /// Number of bindings.
    [[nodiscard]] std::size_t size() const noexcept { return bindings_.size(); }
    [[nodiscard]] bool empty() const noexcept { return bindings_.empty(); }

    /// The first binding whose `name()` matches (names are not guaranteed
    /// unique). Throws std::out_of_range when none matches.
    [[nodiscard]] const Binding &by_name(const std::string &name) const {
        for (const auto &binding : bindings_) {
            if (const auto n = binding.name(); n != nullptr && *n == name) return binding;
        }
        throw std::out_of_range("Mapping::by_name: no binding named " + name);
    }
    /// The binding with the given typed `Key` (or its `str()`), or nullptr.
    [[nodiscard]] const Binding *find(const Key &key) const {
        for (const auto &binding : bindings_) {
            if (binding.key() == key) return &binding;
        }
        return nullptr;
    }
    [[nodiscard]] const Binding *find(const std::string &key) const {
        for (const auto &binding : bindings_) {
            if (binding.key().str() == key) return &binding;
        }
        return nullptr;
    }

    /// Bound keys only, projected for application configuration. `by_name`
    /// keys the result by `binding.name()`, falling back to `str(key)`.
    [[nodiscard]] std::map<Key, std::vector<Term>> value_map() const {
        std::map<Key, std::vector<Term>> out;
        for (const auto &binding : bindings_) {
            if (!binding.ok() || binding.values().empty()) continue;
            out[binding.key()] = binding.values();
        }
        return out;
    }
    [[nodiscard]] std::map<std::string, std::vector<Term>> value_map_by_name() const {
        std::map<std::string, std::vector<Term>> out;
        for (const auto &binding : bindings_) {
            if (!binding.ok() || binding.values().empty()) continue;
            const auto n = binding.name();
            out[n ? *n : binding.key().str()] = binding.values();
        }
        return out;
    }

private:
    friend class ShapeMap;
    static Mapping from_c(const ShiftyShapeMap *map, std::size_t shape,
                          std::size_t index);

    std::string focus_;
    std::string shape_name_;
    std::string target_;
    bool conforms_ = false;
    std::vector<Binding> bindings_;
    FocusEvidence evaluation_;
};

/// Options applied to `EvidenceSession::shape_map()`.
struct ShapeMapOptions {
    /// A SPARQL 1.1 property path evaluated from each property shape's own
    /// node over the shapes graph to carry the author's name for a slot.
    /// Defaults to `sh:name`; set to empty to skip name resolution.
    std::string name_path = "sh:name";

    /// `label -> path` pairs evaluated from each bound value over the data
    /// graph, annotating it (`Binding::annotated_values()`).
    std::vector<std::pair<std::string, std::string>> value_paths;
};

/// Key -> value bindings for every selected (shape, focus) pair of a run,
/// grouped by shape identity. Built from `EvidenceSession::shape_map()`.
///
/// Move-only: the structured view is materialized eagerly, while `to_json()`
/// is served from the retained engine handle.
class ShapeMap {
public:
    ShapeMap(const ShapeMap &) = delete;
    ShapeMap &operator=(const ShapeMap &) = delete;
    ShapeMap(ShapeMap &&) noexcept = default;
    ShapeMap &operator=(ShapeMap &&) noexcept = default;
    ~ShapeMap() = default;

    /// True when no selected `(shape, focus)` pair failed.
    [[nodiscard]] bool conforms() const noexcept { return conforms_; }

    /// Every shape identity with at least one authored statement — named shape
    /// IRIs, or `_:statement-N` placeholders for anonymous shapes.
    [[nodiscard]] const std::vector<std::string> &shape_names() const noexcept {
        return shape_names_;
    }

    /// The mappings of one shape, in selection order. Throws std::out_of_range
    /// when `shape_name` is not a shape of this map.
    [[nodiscard]] const std::vector<Mapping> &mappings(
        const std::string &shape_name) const;

    /// The mappings of the shape at `index`.
    [[nodiscard]] const std::vector<Mapping> &mappings(std::size_t index) const {
        return mappings_.at(index);
    }

    /// Every mapping across shapes.
    [[nodiscard]] std::vector<const Mapping *> all() const {
        std::vector<const Mapping *> out;
        for (const auto &group : mappings_) {
            for (const auto &mapping : group) out.push_back(&mapping);
        }
        return out;
    }

    /// Every mapping whose focus is `focus` (compared as N-Triples), across
    /// shapes. A bare IRI string is wrapped in angle brackets first.
    [[nodiscard]] std::vector<const Mapping *> for_focus(const Term &focus) const {
        return for_focus(focus.n3());
    }
    [[nodiscard]] std::vector<const Mapping *> for_focus(std::string_view focus) const {
        std::string key(focus);
        if (!key.empty() && key[0] != '<' && key[0] != '_' && key[0] != '"') {
            key = '<' + key + '>';
        }
        std::vector<const Mapping *> out;
        for (const auto &mapping : all()) {
            if (mapping->focus() == key) out.push_back(mapping);
        }
        return out;
    }

    /// The conforming mappings of one shape.
    [[nodiscard]] std::vector<const Mapping *> conforming(
        const std::string &shape_name) const {
        std::vector<const Mapping *> out;
        for (const auto &mapping : mappings(shape_name)) {
            if (mapping.conforms()) out.push_back(&mapping);
        }
        return out;
    }
    /// The non-conforming mappings of one shape.
    [[nodiscard]] std::vector<const Mapping *> nonconforming(
        const std::string &shape_name) const {
        std::vector<const Mapping *> out;
        for (const auto &mapping : mappings(shape_name)) {
            if (!mapping.conforms()) out.push_back(&mapping);
        }
        return out;
    }

    /// Size of the `i`-th shape's group.
    [[nodiscard]] std::size_t mapping_count(std::size_t index) const {
        return mappings_.at(index).size();
    }
    [[nodiscard]] std::size_t shape_count() const noexcept { return mappings_.size(); }
    /// Total number of mappings across all shapes.
    [[nodiscard]] std::size_t total_mappings() const noexcept { return total_mappings_; }

    /// A plain-JSON summary: `conforms`, `shapes`, each mapping's focus,
    /// target, conforms, and key -> `{status, values, missing, name}`.
    [[nodiscard]] const std::string &to_json() const noexcept { return json_; }

private:
    friend class EvidenceSession;
    using Handle = std::unique_ptr<ShiftyShapeMap, detail::ShapeMapDeleter>;

    explicit ShapeMap(ShiftyShapeMap *raw);

    Handle handle_;
    bool conforms_ = false;
    std::string json_;
    std::vector<std::string> shape_names_;
    std::vector<std::vector<Mapping>> mappings_;
    std::size_t total_mappings_ = 0;
};

// ── shape-map class method definitions ───────────────────────────────────────

/// The local name of an IRI: the segment after the last '#', '/', or ':'.
inline std::string iri_local(const std::string &iri) {
    const std::size_t pos = iri.find_last_of("#/:");
    if (pos != std::string::npos && pos + 1 < iri.size()) {
        return iri.substr(pos + 1);
    }
    return iri;
}

inline bool Path::is_class_path() const noexcept {
    constexpr const char *RDF_TYPE =
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    constexpr const char *RDFS_SUBCLASS =
        "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    if (kind_ != PathKind::Seq || children_.size() != 2) return false;
    const auto &first = children_[0];
    const auto &second = children_[1];
    return first.kind_ == PathKind::Pred && first.iri_ == RDF_TYPE &&
           second.kind_ == PathKind::Star && second.children_.size() == 1 &&
           second.children_[0].kind_ == PathKind::Pred &&
           second.children_[0].iri_ == RDFS_SUBCLASS;
}

inline std::string Path::str(bool compact) const {
    switch (kind_) {
    case PathKind::Id:
        return "id";
    case PathKind::Pred:
        return compact ? iri_local(iri_) : "<" + iri_ + ">";
    case PathKind::Inverse:
        return "^" + (children_.empty() ? Path{} : children_[0]).str(compact);
    case PathKind::Star:
        return (children_.empty() ? Path{} : children_[0]).str(compact) + "*";
    case PathKind::Seq: {
        // `rdf:type/rdfs:subClassOf*` is class membership; render it like
        // Turtle.
        if (is_class_path()) return "a";
        std::string out;
        for (std::size_t i = 0; i < children_.size(); ++i) {
            if (i) out += "/";
            out += children_[i].str(compact);
        }
        return out;
    }
    case PathKind::Alt: {
        std::string out;
        for (std::size_t i = 0; i < children_.size(); ++i) {
            if (i) out += "|";
            out += children_[i].str(compact);
        }
        return out;
    }
    }
    return {};
}

namespace detail {

/// A minimal parser for the externally-tagged serde encoding of the algebra
/// `Path` that `key_path_json` returns. Only the structures the encoding uses
/// (`"Id"`, `{"Pred": {"value": …}}`, `{"Inverse": …}`, `{"Seq": […]}`,
/// `{"Alt": […]}`, `{"Star": …}`) are accepted, so the grammar is tiny.
inline void json_skip_ws(std::string_view &s) {
    while (!s.empty() &&
           (s.front() == ' ' || s.front() == '\t' || s.front() == '\n' ||
            s.front() == '\r')) {
        s.remove_prefix(1);
    }
}

inline bool json_take(std::string_view &s, char c) {
    if (!s.empty() && s.front() == c) {
        s.remove_prefix(1);
        return true;
    }
    return false;
}

inline std::optional<std::string> json_parse_string(std::string_view &s) {
    detail::json_skip_ws(s);
    if (s.empty() || s.front() != '"') return std::nullopt;
    s.remove_prefix(1);
    std::string out;
    while (!s.empty()) {
        const char c = s.front();
        s.remove_prefix(1);
        if (c == '"') return out;
        if (c == '\\') {
            if (s.empty()) return std::nullopt;
            const char e = s.front();
            s.remove_prefix(1);
            switch (e) {
            case '"': out.push_back('"'); break;
            case '\\': out.push_back('\\'); break;
            case '/': out.push_back('/'); break;
            case 'n': out.push_back('\n'); break;
            case 't': out.push_back('\t'); break;
            case 'r': out.push_back('\r'); break;
            case 'b': out.push_back('\b'); break;
            case 'f': out.push_back('\f'); break;
            case 'u': {
                if (s.size() < 4) return std::nullopt;
                unsigned cp = 0;
                for (int i = 0; i < 4; ++i) {
                    const char h = s.front();
                    s.remove_prefix(1);
                    if (h >= '0' && h <= '9')
                        cp = cp * 16 + (h - '0');
                    else if (h >= 'a' && h <= 'f')
                        cp = cp * 16 + (h - 'a' + 10);
                    else if (h >= 'A' && h <= 'F')
                        cp = cp * 16 + (h - 'A' + 10);
                    else
                        return std::nullopt;
                }
                if (cp < 0x80) {
                    out.push_back(static_cast<char>(cp));
                } else if (cp < 0x800) {
                    out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
                    out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
                } else {
                    out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
                    out.push_back(
                        static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                    out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
                }
                break;
            }
            default:
                return std::nullopt;
            }
        } else {
            out.push_back(c);
        }
    }
    return std::nullopt;
}

}  // namespace detail

inline std::optional<Path> Path::parse_json(std::string_view json) {
    std::string_view s = json;
    std::function<std::optional<Path>(std::string_view &)> parse_path;
    parse_path = [&](std::string_view &s) -> std::optional<Path> {
        detail::json_skip_ws(s);
        if (s.empty()) return std::nullopt;
        if (s.front() == '"') {
            auto tag = detail::json_parse_string(s);
            if (tag && *tag == "Id") return Path(PathKind::Id, {}, {});
            return std::nullopt;
        }
        if (!detail::json_take(s, '{')) return std::nullopt;
        auto tag = detail::json_parse_string(s);
        if (!tag) return std::nullopt;
        if (!detail::json_take(s, ':')) return std::nullopt;
        std::optional<Path> result;
        if (*tag == "Pred") {
            if (!detail::json_take(s, '{')) return std::nullopt;
            auto key = detail::json_parse_string(s);
            if (!key || *key != "value") return std::nullopt;
            if (!detail::json_take(s, ':')) return std::nullopt;
            auto iri = detail::json_parse_string(s);
            if (!iri || !detail::json_take(s, '}')) return std::nullopt;
            result = Path(PathKind::Pred, *iri, {});
        } else if (*tag == "Inverse" || *tag == "Star") {
            auto inner = parse_path(s);
            if (!inner) return std::nullopt;
            std::vector<Path> one{std::move(*inner)};
            result = Path(*tag == "Inverse" ? PathKind::Inverse : PathKind::Star,
                          {}, std::move(one));
        } else if (*tag == "Seq" || *tag == "Alt") {
            if (!detail::json_take(s, '[')) return std::nullopt;
            std::vector<Path> parts;
            detail::json_skip_ws(s);
            if (!detail::json_take(s, ']')) {
                for (;;) {
                    auto part = parse_path(s);
                    if (!part) return std::nullopt;
                    parts.push_back(std::move(*part));
                    detail::json_skip_ws(s);
                    if (detail::json_take(s, ']')) break;
                    if (!detail::json_take(s, ',')) return std::nullopt;
                    detail::json_skip_ws(s);
                }
            }
            result = Path(*tag == "Seq" ? PathKind::Seq : PathKind::Alt, {},
                          std::move(parts));
        } else {
            return std::nullopt;
        }
        detail::json_skip_ws(s);
        if (!detail::json_take(s, '}')) return std::nullopt;
        return result;
    };
    auto result = parse_path(s);
    detail::json_skip_ws(s);
    if (!result || !s.empty()) return std::nullopt;
    return *result;
}

inline std::string Qualifier::str() const {
    switch (kind_) {
    case QualifierKind::Cls:
    case QualifierKind::Datatype:
    case QualifierKind::ShapeRef:
        return iri_local(iri_);
    case QualifierKind::Const:
        if (term_.is_iri()) return iri_local(term_.value());
        if (term_.is_literal()) return term_.value();
        return term_.n3();  // blank node
    }
    return {};
}

inline bool operator<(const Path &a, const Path &b) {
    if (a.kind() != b.kind()) return a.kind() < b.kind();
    if (a.iri() != b.iri()) return a.iri() < b.iri();
    return std::lexicographical_compare(a.children().begin(), a.children().end(),
                                        b.children().begin(), b.children().end());
}

inline bool operator<(const Qualifier &a, const Qualifier &b) {
    if (a.kind() != b.kind()) return a.kind() < b.kind();
    if (a.iri() != b.iri()) return a.iri() < b.iri();
    return a.term() < b.term();
}

inline std::string Key::str() const {
    std::string base;
    if (path_.has_value()) {
        base = path_->str(true);
        if (qualifier_.has_value()) {
            base += "\u2192" + qualifier_->str();  // →
        }
    } else {
        base = kind_;
    }
    if (ordinal_ > 1) {
        base += "#" + std::to_string(ordinal_);
    }
    return base;
}

inline bool Key::operator<(const Key &other) const noexcept {
    if (path_.has_value() != other.path_.has_value()) {
        return path_.has_value() < other.path_.has_value();
    }
    if (path_.has_value() && *path_ != *other.path_) return *path_ < *other.path_;
    if (qualifier_.has_value() != other.qualifier_.has_value()) {
        return qualifier_.has_value() < other.qualifier_.has_value();
    }
    if (qualifier_.has_value() && *qualifier_ != *other.qualifier_) {
        return *qualifier_ < *other.qualifier_;
    }
    if (ordinal_ != other.ordinal_) return ordinal_ < other.ordinal_;
    return kind_ < other.kind_;
}

inline const std::vector<Mapping> &ShapeMap::mappings(
    const std::string &shape_name) const {
    for (std::size_t i = 0; i < shape_names_.size(); ++i) {
        if (shape_names_[i] == shape_name) return mappings_[i];
    }
    throw std::out_of_range("ShapeMap::mappings: no shape named " + shape_name);
}

inline ShapeMap::ShapeMap(ShiftyShapeMap *raw) : handle_(raw) {
    conforms_ = shifty_shape_map_conforms(handle_.get()) != 0;
    json_ = detail::copy(shifty_shape_map_to_json(handle_.get()));
    const std::size_t shape_count = shifty_shape_map_shape_count(handle_.get());
    shape_names_.reserve(shape_count);
    mappings_.reserve(shape_count);
    for (std::size_t s = 0; s < shape_count; ++s) {
        shape_names_.push_back(
            detail::copy(shifty_shape_map_shape_name(handle_.get(), s)));
        const std::size_t mapping_count =
            shifty_shape_map_mapping_count(handle_.get(), s);
        std::vector<Mapping> group;
        group.reserve(mapping_count);
        for (std::size_t m = 0; m < mapping_count; ++m) {
            group.push_back(Mapping::from_c(handle_.get(), s, m));
        }
        total_mappings_ += group.size();
        mappings_.push_back(std::move(group));
    }
}

inline Binding Binding::from_c(const ShiftyShapeMap *map, std::size_t shape,
                               std::size_t mapping, std::size_t index) {
    Binding out;

    const std::string path_json = detail::copy(
        shifty_shape_map_binding_key_path_json(map, shape, mapping, index));
    std::optional<Path> path;
    if (!path_json.empty()) path = Path::parse_json(path_json);
    std::optional<Qualifier> qualifier;
    if (shifty_shape_map_binding_has_qualifier(map, shape, mapping, index)) {
        qualifier = Qualifier(
            static_cast<QualifierKind>(shifty_shape_map_binding_qualifier_kind(
                map, shape, mapping, index)),
            detail::copy(shifty_shape_map_binding_qualifier_iri(
                map, shape, mapping, index)),
            Term::from_c(shifty_shape_map_binding_qualifier_term(
                map, shape, mapping, index)));
    }
    out.key_ = Key(std::move(path), std::move(qualifier),
                   shifty_shape_map_binding_key_ordinal(map, shape, mapping,
                                                        index),
                   detail::copy(shifty_shape_map_binding_key_kind(
                       map, shape, mapping, index)));

    out.ok_ = shifty_shape_map_binding_status(map, shape, mapping, index) ==
              SHIFTY_EVALUATION_PASS;
    out.status_ = out.ok_ ? "pass" : "fail";
    out.source_constraint_id_ =
        shifty_shape_map_binding_source_constraint(map, shape, mapping, index);
    out.constraint_id_ = detail::optional_constraint(
        shifty_shape_map_binding_normalized_constraint(map, shape, mapping,
                                                       index));
    out.severity_ = detail::copy(
        shifty_shape_map_binding_severity(map, shape, mapping, index));

    const std::size_t name_count =
        shifty_shape_map_binding_name_count(map, shape, mapping, index);
    out.names_.reserve(name_count);
    for (std::size_t n = 0; n < name_count; ++n) {
        out.names_.push_back(detail::copy(
            shifty_shape_map_binding_name(map, shape, mapping, index, n)));
    }

    out.min_ = detail::optional_index(
        shifty_shape_map_binding_min(map, shape, mapping, index));
    out.max_ = detail::optional_index(
        shifty_shape_map_binding_max(map, shape, mapping, index));
    out.observed_ = detail::optional_index(
        shifty_shape_map_binding_observed(map, shape, mapping, index));
    out.missing_ = shifty_shape_map_binding_missing(map, shape, mapping, index);

    const std::size_t value_count =
        shifty_shape_map_binding_value_count(map, shape, mapping, index);
    out.values_.reserve(value_count);
    for (std::size_t v = 0; v < value_count; ++v) {
        out.values_.push_back(Term::from_c(
            shifty_shape_map_binding_value(map, shape, mapping, index, v)));
    }
    const std::size_t rejected_count =
        shifty_shape_map_binding_rejected_value_count(map, shape, mapping,
                                                     index);
    out.rejected_values_.reserve(rejected_count);
    for (std::size_t v = 0; v < rejected_count; ++v) {
        out.rejected_values_.push_back(Term::from_c(
            shifty_shape_map_binding_rejected_value(map, shape, mapping, index,
                                                    v)));
    }

    out.evidence_json_ = detail::copy(
        shifty_shape_map_binding_evidence_json(map, shape, mapping, index));
    out.explain_ = detail::copy(
        shifty_shape_map_binding_explain(map, shape, mapping, index));

    // value_paths annotations: one group per label, each holding an entry per
    // bound value in `values()` order.
    out.annotated_values_.reserve(out.values_.size());
    for (const auto &value : out.values_) {
        out.annotated_values_.push_back(BoundValue{value, {}});
    }
    const std::size_t label_count =
        shifty_shape_map_binding_annotation_label_count(map, shape, mapping,
                                                        index);
    for (std::size_t l = 0; l < label_count; ++l) {
        const std::string label = detail::copy(
            shifty_shape_map_binding_annotation_label(map, shape, mapping,
                                                      index, l));
        const std::size_t entry_count =
            shifty_shape_map_binding_annotation_term_count(map, shape, mapping,
                                                           index, l);
        for (std::size_t e = 0; e < entry_count && e < out.annotated_values_.size(); ++e) {
            std::vector<Term> reached;
            const std::size_t reached_count =
                shifty_shape_map_binding_annotation_reached_count(
                    map, shape, mapping, index, l, e);
            reached.reserve(reached_count);
            for (std::size_t r = 0; r < reached_count; ++r) {
                reached.push_back(Term::from_c(
                    shifty_shape_map_binding_annotation_reached(
                        map, shape, mapping, index, l, e, r)));
            }
            out.annotated_values_[e].annotations[label] = std::move(reached);
        }
    }

    return out;
}

inline Mapping Mapping::from_c(const ShiftyShapeMap *map, std::size_t shape,
                               std::size_t index) {
    Mapping out;
    out.focus_ = detail::copy(shifty_shape_map_mapping_focus(map, shape, index));
    out.shape_name_ =
        detail::copy(shifty_shape_map_mapping_shape_name(map, shape, index));
    out.target_ = detail::copy(shifty_shape_map_mapping_target(map, shape, index));
    out.conforms_ = shifty_shape_map_mapping_conforms(map, shape, index) != 0;
    out.evaluation_.focus_node = out.focus_;
    out.evaluation_.status = out.conforms_ ? EvaluationStatus::Pass
                                           : EvaluationStatus::Fail;
    out.evaluation_.evidence_json = detail::copy(
        shifty_shape_map_mapping_evidence_json(map, shape, index));
    out.evaluation_.explanation = detail::copy(
        shifty_shape_map_mapping_explanation(map, shape, index));
    const std::size_t binding_count =
        shifty_shape_map_mapping_binding_count(map, shape, index);
    out.bindings_.reserve(binding_count);
    for (std::size_t b = 0; b < binding_count; ++b) {
        out.bindings_.push_back(Binding::from_c(map, shape, index, b));
    }
    return out;
}


/// The failing pairs of one conformance scan, with the totals that scan
/// produced. Move-only: the engine-side pairs are retained so explaining one
/// costs no re-selection and no term re-parsing.
class Failures {
public:
    Failures(const Failures &) = delete;
    Failures &operator=(const Failures &) = delete;
    Failures(Failures &&) noexcept = default;
    Failures &operator=(Failures &&) noexcept = default;
    ~Failures() = default;

    /// Returns the conformance totals of the scan that produced these pairs.
    [[nodiscard]] const ConformanceRun &conformance() const noexcept {
        return conformance_;
    }

    /// Returns the pairs that failed, in selection order.
    [[nodiscard]] const std::vector<SelectedPair> &pairs() const noexcept {
        return pairs_;
    }

    /// Returns the number of failing pairs.
    [[nodiscard]] std::size_t size() const noexcept { return pairs_.size(); }

    /// Returns true when nothing failed.
    [[nodiscard]] bool empty() const noexcept { return pairs_.empty(); }

private:
    friend class EvidenceSession;
    using Handle = std::unique_ptr<ShiftyFailureList, detail::FailureListDeleter>;

    explicit Failures(ShiftyFailureList *raw) : handle_(raw) {
        conformance_ =
            detail::from_c(shifty_failure_list_conformance(handle_.get()));
        const std::size_t count = shifty_failure_list_len(handle_.get());
        pairs_.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            SelectedPair pair;
            pair.statement = shifty_failure_statement(handle_.get(), i);
            pair.focus_node =
                detail::copy(shifty_failure_focus(handle_.get(), i));
            pairs_.push_back(std::move(pair));
        }
    }

    Handle handle_;
    ConformanceRun conformance_;
    std::vector<SelectedPair> pairs_;
};

/// An in-memory RDF graph owned by the Rust engine.
///
/// Dataset is move-only. Read-only operations may run concurrently only when
/// the caller provides external synchronization against load operations.
///
/// Multiple RDF sources are unioned: call `load` / `load_file` repeatedly to
/// accumulate triples from several documents (e.g. several data files) into
/// one dataset — the C++ analogue of the CLI's repeatable `--data`.
class Dataset {
public:
    /// Constructs an empty dataset.
    Dataset() {
        detail::check_abi();
        ShiftyDataset *raw = nullptr;
        detail::check(shifty_dataset_create(&raw));
        handle_.reset(raw);
    }

    Dataset(const Dataset &) = delete;
    Dataset &operator=(const Dataset &) = delete;
    Dataset(Dataset &&) noexcept = default;
    Dataset &operator=(Dataset &&) noexcept = default;
    ~Dataset() = default;

    /// Parses RDF from memory and adds it to this dataset.
    ///
    /// \param data UTF-8 RDF input.
    /// \param format Input serialization.
    /// \param base_iri Optional base IRI used for Turtle resolution.
    /// \throws Error on parse failure.
    void load(
        std::string_view data,
        RdfFormat format = RdfFormat::Auto,
        std::string_view base_iri = {}) {
        detail::check(shifty_dataset_load_memory(
            handle_.get(),
            reinterpret_cast<const std::uint8_t *>(data.data()),
            data.size(),
            detail::to_c(format),
            detail::optional_data(base_iri),
            base_iri.size()));
    }

    /// Parses an RDF file and adds it to this dataset.
    ///
    /// \throws Error on file access or parse failure.
    void load_file(
        const std::filesystem::path &path,
        RdfFormat format = RdfFormat::Auto,
        std::string_view base_iri = {}) {
        const auto utf8 = detail::path_utf8(path);
        detail::check(shifty_dataset_load_file(
            handle_.get(),
            utf8.data(),
            utf8.size(),
            detail::to_c(format),
            detail::optional_data(base_iri),
            base_iri.size()));
    }

    /// Returns the number of unique triples in the dataset.
    [[nodiscard]] std::size_t size() const noexcept {
        return shifty_dataset_len(handle_.get());
    }

    /// Serializes the complete dataset as N-Triples.
    ///
    /// \throws Error if serialization fails.
    [[nodiscard]] std::string ntriples() const {
        return consume_query(shifty_dataset_ntriples);
    }

    /// Executes a SPARQL SELECT, ASK, CONSTRUCT, or DESCRIBE query.
    ///
    /// \param sparql SPARQL query text.
    /// \returns An owned result using standard RDF/SPARQL serialization.
    /// \throws Error on query parsing or evaluation failure.
    [[nodiscard]] QueryResult query(std::string_view sparql) const {
        ShiftyQueryResult *raw = nullptr;
        detail::check(shifty_dataset_query(
            handle_.get(), sparql.data(), sparql.size(), &raw));
        return from_raw(raw);
    }

private:
    friend class PreparedValidator;
    friend class EvidenceSession;
    using Handle = std::unique_ptr<ShiftyDataset, detail::DatasetDeleter>;

    using DatasetResultFunction =
        ShiftyStatus (*)(const ShiftyDataset *, ShiftyQueryResult **);

    [[nodiscard]] std::string consume_query(DatasetResultFunction function) const {
        ShiftyQueryResult *raw = nullptr;
        detail::check(function(handle_.get(), &raw));
        return from_raw(raw).data();
    }

    static QueryResult from_raw(ShiftyQueryResult *raw) {
        std::unique_ptr<ShiftyQueryResult, detail::QueryResultDeleter> result(raw);
        return QueryResult(
            detail::from_c(shifty_query_result_kind(result.get())),
            shifty_query_result_boolean(result.get()) != 0,
            detail::copy(shifty_query_result_data(result.get())),
            detail::copy(shifty_query_result_media_type(result.get())));
    }

    Handle handle_;
};

/// Parsed and normalized SHACL shapes reusable across datasets.
class PreparedValidator {
public:
    /// Parses and prepares shapes from memory.
    ///
    /// \param shapes UTF-8 RDF shapes.
    /// \param format Input serialization.
    /// \param base_iri Optional base IRI used for Turtle resolution.
    /// \throws Error on parse failure.
    explicit PreparedValidator(
        std::string_view shapes,
        RdfFormat format = RdfFormat::Auto,
        std::string_view base_iri = {}) {
        detail::check_abi();
        ShiftyPreparedValidator *raw = nullptr;
        detail::check(shifty_prepared_validator_create_memory(
            reinterpret_cast<const std::uint8_t *>(shapes.data()),
            shapes.size(),
            detail::to_c(format),
            detail::optional_data(base_iri),
            base_iri.size(),
            &raw));
        handle_.reset(raw);
    }

    /// Parses and prepares shapes from a file.
    ///
    /// \throws Error on file access or parse failure.
    [[nodiscard]] static PreparedValidator from_file(
        const std::filesystem::path &path,
        RdfFormat format = RdfFormat::Auto,
        std::string_view base_iri = {}) {
        detail::check_abi();
        const auto utf8 = detail::path_utf8(path);
        ShiftyPreparedValidator *raw = nullptr;
        detail::check(shifty_prepared_validator_create_file(
            utf8.data(),
            utf8.size(),
            detail::to_c(format),
            detail::optional_data(base_iri),
            base_iri.size(),
            &raw));
        return PreparedValidator(raw);
    }

    /// Parses and prepares shapes from multiple files, unioning them at the
    /// RDF triple level before planning — the C++ analogue of the CLI's
    /// repeatable `--shapes`. Each file is parsed individually (so per-file
    /// `@prefix`es and relative IRIs resolve in their own document) and the
    /// resulting triples are merged into one graph.
    ///
    /// \param paths Filesystem paths to RDF shapes files.
    /// \param format Input serialization applied to every file.
    /// \param base_iri Optional base IRI used for Turtle resolution.
    /// \throws Error on file access or parse failure.
    [[nodiscard]] static PreparedValidator from_files(
        const std::vector<std::filesystem::path> &paths,
        RdfFormat format = RdfFormat::Auto,
        std::string_view base_iri = {}) {
        detail::check_abi();
        const std::string ntriples =
            merge_sources_to_ntriples(paths, {}, format, base_iri);
        ShiftyPreparedValidator *raw = nullptr;
        detail::check(shifty_prepared_validator_create_memory(
            reinterpret_cast<const std::uint8_t *>(ntriples.data()),
            ntriples.size(),
            SHIFTY_RDF_FORMAT_NTRIPLES,
            detail::optional_data(base_iri),
            base_iri.size(),
            &raw));
        return PreparedValidator(raw);
    }

    /// Parses and prepares shapes from multiple in-memory documents, unioning
    /// them at the RDF triple level before planning — the C++ analogue of the
    /// CLI's repeatable `--shapes`. Each document is parsed individually (so
    /// per-document `@prefix`es resolve in their own context) and the resulting
    /// triples are merged into one graph.
    ///
    /// \param shapes UTF-8 RDF shapes documents (must outlive this call).
    /// \param format Input serialization applied to every document.
    /// \param base_iri Optional base IRI used for Turtle resolution.
    /// \throws Error on parse failure.
    [[nodiscard]] static PreparedValidator from_memory(
        const std::vector<std::string_view> &shapes,
        RdfFormat format = RdfFormat::Auto,
        std::string_view base_iri = {}) {
        detail::check_abi();
        const std::string ntriples =
            merge_sources_to_ntriples({}, shapes, format, base_iri);
        ShiftyPreparedValidator *raw = nullptr;
        detail::check(shifty_prepared_validator_create_memory(
            reinterpret_cast<const std::uint8_t *>(ntriples.data()),
            ntriples.size(),
            SHIFTY_RDF_FORMAT_NTRIPLES,
            detail::optional_data(base_iri),
            base_iri.size(),
            &raw));
        return PreparedValidator(raw);
    }

    PreparedValidator(const PreparedValidator &) = delete;
    PreparedValidator &operator=(const PreparedValidator &) = delete;
    PreparedValidator(PreparedValidator &&) noexcept = default;
    PreparedValidator &operator=(PreparedValidator &&) noexcept = default;
    ~PreparedValidator() = default;

    /// Returns parser/lowering diagnostics encoded as a JSON string array.
    [[nodiscard]] std::string diagnostics_json() const {
        return detail::copy(
            shifty_prepared_validator_diagnostics_json(handle_.get()));
    }

    /// Validates a dataset using the cached shapes representation.
    /// `options.shape_names`, when non-empty, limits validation to those named
    /// shapes as top-level entry points while preserving normal dependency
    /// evaluation.
    ///
    /// \throws Error for non-stratifiable shapes or validation failures.
    [[nodiscard]] ValidationResult validate(
        const Dataset &dataset,
        ValidationOptions options = {}) const {
        ShiftyValidationResult *raw = nullptr;
        const auto shape_names = detail::string_views(options.shape_names);
        detail::check(shifty_prepared_validator_validate_with_shapes(
            handle_.get(),
            dataset.handle_.get(),
            detail::to_c(options.graph_mode),
            static_cast<std::uint8_t>(options.run_inference),
            detail::to_c(options.minimum_severity),
            shape_names.data(),
            shape_names.size(),
            &raw));
        std::unique_ptr<
            ShiftyValidationResult,
            detail::ValidationResultDeleter>
            result(raw);
        return ValidationResult(
            shifty_validation_result_conforms(result.get()) != 0,
            detail::copy(shifty_validation_result_report_turtle(result.get())),
            detail::copy(shifty_validation_result_results_text(result.get())));
    }

    /// Returns the observed `sh:property` bindings for every focus node that
    /// conforms to a target/profile node shape — the inverse of validate():
    /// successful bindings rather than violations. `options.key_path` (when
    /// set) is a SPARQL 1.1 property path expression evaluated from each
    /// `sh:property` shape's own node to produce a stable key; see
    /// [`PropertyWitness::key`].
    ///
    /// \throws Error for a malformed `key_path`, non-stratifiable shapes, or
    /// validation failures.
    [[nodiscard]] std::vector<PropertyWitness> witnesses(
        const Dataset &dataset,
        ValidationOptions options = {}) const {
        ShiftyPropertyWitnessList *raw = nullptr;
        detail::check(shifty_prepared_validator_witnesses(
            handle_.get(),
            dataset.handle_.get(),
            detail::optional_data(options.key_path),
            options.key_path.size(),
            detail::to_c(options.graph_mode),
            static_cast<std::uint8_t>(options.run_inference),
            &raw));
        std::unique_ptr<ShiftyPropertyWitnessList, detail::PropertyWitnessListDeleter>
            list(raw);

        const std::size_t count = shifty_property_witness_list_len(list.get());
        std::vector<PropertyWitness> out;
        out.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            PropertyWitness witness;
            witness.focus_node = detail::copy(shifty_property_witness_focus(list.get(), i));
            witness.shape_id = detail::copy(shifty_property_witness_shape(list.get(), i));
            witness.key = detail::copy(shifty_property_witness_key(list.get(), i));

            const std::size_t value_count =
                shifty_property_witness_value_count(list.get(), i);
            witness.value_nodes.reserve(value_count);
            for (std::size_t v = 0; v < value_count; ++v) {
                witness.value_nodes.push_back(
                    detail::copy(shifty_property_witness_value(list.get(), i, v)));
            }
            out.push_back(std::move(witness));
        }
        return out;
    }

    /// Validates a dataset using the algebra path: the engine's own
    /// conformance oracle, evaluated directly against the SHACL algebra and
    /// returned as a structured violation/reason tree rather than a W3C
    /// sh:ValidationReport graph. Prefer this over validate() when the
    /// caller wants to inspect findings programmatically instead of parsing
    /// Turtle. `options.shape_names`, when non-empty, limits validation to
    /// those named shapes as top-level entry points while preserving normal
    /// dependency evaluation.
    ///
    /// \throws Error for non-stratifiable shapes or validation failures.
    [[nodiscard]] AlgebraResult validate_algebra(
        const Dataset &dataset,
        ValidationOptions options = {}) const {
        ShiftyAlgebraResult *raw = nullptr;
        const auto shape_names = detail::string_views(options.shape_names);
        detail::check(shifty_prepared_validator_validate_algebra_with_shapes(
            handle_.get(),
            dataset.handle_.get(),
            detail::to_c(options.graph_mode),
            static_cast<std::uint8_t>(options.run_inference),
            detail::to_c(options.minimum_severity),
            shape_names.data(),
            shape_names.size(),
            &raw));
        std::unique_ptr<ShiftyAlgebraResult, detail::AlgebraResultDeleter> result(raw);

        const std::size_t violation_count =
            shifty_algebra_result_violation_count(result.get());
        std::vector<AlgebraViolation> violations;
        violations.reserve(violation_count);
        for (std::size_t i = 0; i < violation_count; ++i) {
            AlgebraViolation violation;
            violation.focus_node =
                detail::copy(shifty_algebra_violation_focus(result.get(), i));
            violation.shape_name =
                detail::copy(shifty_algebra_violation_shape_name(result.get(), i));
            violation.severity =
                detail::copy(shifty_algebra_violation_severity(result.get(), i));

            const std::size_t reason_count =
                shifty_algebra_violation_reason_count(result.get(), i);
            violation.reasons.reserve(reason_count);
            for (std::size_t r = 0; r < reason_count; ++r) {
                AlgebraReason reason;
                reason.value = detail::copy(shifty_algebra_reason_value(result.get(), i, r));
                reason.path = detail::copy(shifty_algebra_reason_path(result.get(), i, r));
                reason.message = detail::copy(shifty_algebra_reason_message(result.get(), i, r));
                reason.author_message =
                    detail::copy(shifty_algebra_reason_author_message(result.get(), i, r));
                reason.severity =
                    detail::copy(shifty_algebra_reason_severity(result.get(), i, r));
                violation.reasons.push_back(std::move(reason));
            }
            violations.push_back(std::move(violation));
        }

        return AlgebraResult(
            shifty_algebra_result_conforms(result.get()) != 0,
            std::move(violations),
            detail::copy(shifty_algebra_result_results_text(result.get())));
    }

private:
    friend class EvidenceSession;

    explicit PreparedValidator(ShiftyPreparedValidator *raw) : handle_(raw) {}

    /// Union several shape sources into one N-Triples buffer by loading them
    /// into a temporary `Dataset` (which already merges at the triple level)
    /// and serializing the result. Each source is parsed individually so its
    /// own `@prefix`es / relative IRIs resolve in their own document.
    static std::string merge_sources_to_ntriples(
        const std::vector<std::filesystem::path> &files,
        const std::vector<std::string_view> &memories,
        RdfFormat format,
        std::string_view base_iri) {
        Dataset dataset;
        for (const auto &path : files) {
            dataset.load_file(path, format, base_iri);
        }
        for (const auto &shapes : memories) {
            dataset.load(shapes, format, base_iri);
        }
        return dataset.ntriples();
    }

    using Handle =
        std::unique_ptr<ShiftyPreparedValidator, detail::ValidatorDeleter>;
    Handle handle_;
};

/// Evidence-carrying validation over one immutable shapes/data snapshot.
///
/// Where validate() reports what failed, an evidence run reports what was
/// *decided*: every authored statement, every focus it selected, and one
/// evidence polarity each — a satisfaction trace where the shape held, a failure
/// witness where it did not. Statements that selected no focus nodes are
/// reported too, so a run is a coverage horizon over the schema rather than a
/// list of findings.
///
/// Inference, normalization, stratification, indexing, and SPARQL preparation
/// happen once in the constructor and are reused by every call. `graph_mode` and
/// `run_inference` define the snapshot, so they are read from the options here
/// and ignored afterwards; `minimum_severity` and `shape_names` are per-call.
///
/// On a corpus where failures are a small fraction of selected pairs, prefer
/// find_failures() plus explain() to validate(): the scan decides each pair
/// with one short-circuiting test, and evidence is materialized only for the
/// pairs you ask about.
///
/// EvidenceSession is move-only and independent of the PreparedValidator and
/// Dataset it was built from — neither needs to outlive it.
class EvidenceSession {
public:
    /// Prepares an evidence snapshot over a validator's shapes and a dataset.
    ///
    /// Only `options.graph_mode` and `options.run_inference` are read; the rest
    /// apply per call. As with validate(), a dataset holding only data uses the
    /// shapes graph for evaluation under the default GraphMode::Union — load the
    /// shapes into the dataset as well for the embedded-data pattern, where
    /// focus nodes are discovered from the shapes document too.
    ///
    /// \throws Error for non-stratifiable shapes or inference failures.
    EvidenceSession(
        const PreparedValidator &validator,
        const Dataset &dataset,
        ValidationOptions options = {}) {
        detail::check_abi();
        ShiftyEvidenceSession *raw = nullptr;
        detail::check(shifty_evidence_session_create(
            validator.handle_.get(),
            dataset.handle_.get(),
            detail::to_c(options.graph_mode),
            static_cast<std::uint8_t>(options.run_inference),
            &raw));
        handle_.reset(raw);
    }

    EvidenceSession(const EvidenceSession &) = delete;
    EvidenceSession &operator=(const EvidenceSession &) = delete;
    EvidenceSession(EvidenceSession &&) noexcept = default;
    EvidenceSession &operator=(EvidenceSession &&) noexcept = default;
    ~EvidenceSession() = default;

    /// Returns the source/normalized constraint catalogs this snapshot's
    /// evidence refers to by id, as JSON. Fixed per snapshot, so take it once
    /// rather than per run — on a small model the catalog is the majority of a
    /// run's serialized bytes.
    [[nodiscard]] std::string constraints_json() const {
        return detail::copy(
            shifty_evidence_session_constraints_json(handle_.get()));
    }

    /// Returns the complete coverage horizon for this snapshot.
    ///
    /// `options.shape_names`, when non-empty, limits validation to those named
    /// shapes as top-level entry points; referenced helper shapes are still
    /// evaluated normally.
    ///
    /// \throws Error on validation failure.
    [[nodiscard]] EvidenceRun validate(ValidationOptions options = {}) const {
        ShiftyEvidenceRun *raw = nullptr;
        const auto shape_names = detail::string_views(options.shape_names);
        detail::check(shifty_evidence_session_validate(
            handle_.get(),
            detail::to_c(options.minimum_severity),
            shape_names.data(),
            shape_names.size(),
            &raw));
        return EvidenceRun(raw);
    }

    /// Decides the same pairs with one short-circuiting satisfaction test
    /// instead of materializing evidence — a verdict with counts, and the
    /// baseline that isolates what evidence tracing costs.
    ///
    /// `options.minimum_severity` is not honored: with no failure evidence
    /// there is no per-constraint severity to weigh, so any failing pair makes
    /// the run non-conforming. Only `options.shape_names` applies.
    ///
    /// \throws Error on validation failure.
    [[nodiscard]] ConformanceRun validate_conformance(
        ValidationOptions options = {}) const {
        ShiftyConformanceRun run{};
        const auto shape_names = detail::string_views(options.shape_names);
        detail::check(shifty_evidence_session_validate_conformance(
            handle_.get(), shape_names.data(), shape_names.size(), &run));
        return detail::from_c(run);
    }

    /// Runs the same single pass and additionally retains the pairs that
    /// failed, ready for explain(). Same severity caveat as
    /// validate_conformance().
    ///
    /// \throws Error on validation failure.
    [[nodiscard]] Failures find_failures(ValidationOptions options = {}) const {
        ShiftyFailureList *raw = nullptr;
        const auto shape_names = detail::string_views(options.shape_names);
        detail::check(shifty_evidence_session_find_failures(
            handle_.get(), shape_names.data(), shape_names.size(), &raw));
        return Failures(raw);
    }

    /// Materializes evidence for one pair of a failure list. Target selection
    /// is not re-run — the pair is taken as already selected, which is the
    /// point: re-deriving the selection would cost what the whole pass costs.
    ///
    /// The returned run carries an empty constraint catalog; take the catalog
    /// once from constraints_json().
    ///
    /// \throws Error if `index` is out of bounds, or on evaluation failure.
    [[nodiscard]] EvidenceRun explain(
        const Failures &failures,
        std::size_t index) const {
        ShiftyEvidenceRun *raw = nullptr;
        detail::check(shifty_evidence_session_explain_failure(
            handle_.get(), failures.handle_.get(), index, &raw));
        return EvidenceRun(raw);
    }

    /// Explains an arbitrary pair, naming the focus by its full rendering
    /// (`<iri>`, `_:label`, `"lit"@lang`, `"lit"^^<datatype>`) — the spelling
    /// SelectedPair::focus_node and FocusEvidence::focus_node carry.
    /// `statement` indexes the *normalized* statements. A focus the statement
    /// never selected still yields well-defined evidence; it just describes a
    /// pair no run contained.
    ///
    /// \throws Error for a malformed focus term, or on evaluation failure.
    [[nodiscard]] EvidenceRun explain(
        std::size_t statement,
        std::string_view focus_node) const {
        ShiftyEvidenceRun *raw = nullptr;
        detail::check(shifty_evidence_session_explain(
            handle_.get(),
            statement,
            focus_node.data(),
            focus_node.size(),
            &raw));
        return EvidenceRun(raw);
    }

    /// Builds the shape map for `run` of the *same snapshot* (a run returned
    /// by `validate()`). `options.name_path` (a SPARQL 1.1 property path,
    /// default `sh:name`) carries the author's name for each slot, evaluated
    /// from the property shape's own node over the shapes graph; set empty to
    /// skip. `options.value_paths` (`label -> path`) annotates each bound
    /// *value* from the data graph. Materialized eagerly, unlike Python's lazy
    /// `ShapeMap`.
    ///
    /// \throws Error for a malformed `name_path`/`value_paths` path.
    [[nodiscard]] ShapeMap shape_map(
        const EvidenceRun &run, ShapeMapOptions options = {}) const {
        ShiftyShapeMap *raw = nullptr;
        const auto value_pairs =
            detail::build_value_path_pairs(options.value_paths);
        detail::check(shifty_evidence_session_shape_map(
            handle_.get(),
            run.handle_.get(),
            detail::optional_data(options.name_path),
            options.name_path.size(),
            value_pairs.data(),
            value_pairs.size(),
            &raw));
        return ShapeMap(raw);
    }

    /// Maps raw (source) constraint id to the values `name_path` reaches from
    /// that constraint's originating shapes-graph node, evaluated over the
    /// shapes graph. `name_path` defaults to `sh:name`. Constraints with no
    /// source-node provenance, or where `name_path` resolves to nothing, are
    /// omitted.
    ///
    /// \throws Error for a malformed `name_path`.
    [[nodiscard]] std::map<std::uint32_t, std::vector<std::string>>
    binding_names(std::string_view name_path = "sh:name") const {
        ShiftyBindingNameList *raw = nullptr;
        detail::check(shifty_evidence_session_binding_names(
            handle_.get(), name_path.data(), name_path.size(), &raw));
        detail::OwnedBindingNames owned(raw);
        std::map<std::uint32_t, std::vector<std::string>> out;
        const std::size_t count = shifty_binding_name_list_len(owned.get());
        for (std::size_t i = 0; i < count; ++i) {
            std::vector<std::string> names;
            const std::size_t count_v =
                shifty_binding_name_value_count(owned.get(), i);
            names.reserve(count_v);
            for (std::size_t v = 0; v < count_v; ++v) {
                names.push_back(detail::copy(
                    shifty_binding_name_value(owned.get(), i, v)));
            }
            out[shifty_binding_name_constraint(owned.get(), i)] = std::move(names);
        }
        return out;
    }

    /// The raw schema's shape name for `constraint_id` — the IRI of the named
    /// (non-blank) RDF node it was lowered from. `std::nullopt` when the
    /// constraint has no name.
    [[nodiscard]] std::optional<std::string> shape_name_of(
        std::uint32_t constraint_id) const {
        ShiftyString *raw = nullptr;
        detail::check(shifty_evidence_session_shape_name_of(
            handle_.get(), constraint_id, &raw));
        if (raw == nullptr) return std::nullopt;
        detail::OwnedString owned(raw);
        return detail::copy(shifty_string_data(owned.get()));
    }

    /// Batch-evaluates `path` (a SPARQL 1.1 property path, same grammar as
    /// `name_path`) from each of `nodes` (N-Triples spellings) over this
    /// session's evaluation graph — the data graph, unioned with the shapes
    /// graph to match this session's own `graph_mode`. Returns each input
    /// node's N-Triples spelling mapped to the N-Triples spellings it reaches,
    /// in input order.
    ///
    /// \throws Error for a malformed node or `path`.
    [[nodiscard]] std::vector<std::pair<std::string, std::vector<std::string>>>
    resolve_path(
        const std::vector<std::string> &nodes, std::string_view path) const {
        const auto views = detail::string_views(nodes);
        ShiftyPathResolutionList *raw = nullptr;
        detail::check(shifty_evidence_session_resolve_path(
            handle_.get(), views.data(), views.size(), path.data(), path.size(),
            &raw));
        detail::OwnedPathResolutions owned(raw);
        std::vector<std::pair<std::string, std::vector<std::string>>> out;
        const std::size_t count = shifty_path_resolution_list_len(owned.get());
        out.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            std::vector<std::string> reached;
            const std::size_t count_v =
                shifty_path_resolution_value_count(owned.get(), i);
            reached.reserve(count_v);
            for (std::size_t v = 0; v < count_v; ++v) {
                reached.push_back(detail::copy(
                    shifty_path_resolution_value(owned.get(), i, v)));
            }
            out.emplace_back(
                detail::copy(shifty_path_resolution_node(owned.get(), i)),
                std::move(reached));
        }
        return out;
    }

private:
    using Handle =
        std::unique_ptr<ShiftyEvidenceSession, detail::EvidenceSessionDeleter>;
    Handle handle_;
};


/// Restores a run compacted by EvidenceRun::compact_json(), returning the same
/// JSON EvidenceRun::json() produced.
///
/// \param compact The compact encoding.
/// \param catalog The constraint catalog for an encoding written with
/// `include_catalog = false` — the `"constraints"` value of the original run,
/// which EvidenceSession::constraints_json() returns. Leave empty when the
/// encoding carries its own.
/// \throws Error if the encoding is malformed, was written by another version
/// of the format, or omits a catalog without one being supplied.
[[nodiscard]] inline std::string expand_evidence(
    std::string_view compact,
    std::string_view catalog = {}) {
    ShiftyString *raw = nullptr;
    detail::check(shifty_evidence_expand_json(
        compact.data(),
        compact.size(),
        detail::optional_data(catalog),
        catalog.size(),
        &raw));
    detail::OwnedString owned(raw);
    return detail::copy(shifty_string_data(owned.get()));
}

/// Returns the ABI version implemented by the linked static library.
[[nodiscard]] inline std::uint32_t abi_version() noexcept {
    return shifty_abi_version();
}

} // namespace shifty

#endif
