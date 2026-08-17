#ifndef SHIFTY_SHIFTY_HPP
#define SHIFTY_SHIFTY_HPP

#include "shifty/shifty.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
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

using OwnedString = std::unique_ptr<ShiftyString, StringDeleter>;

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
