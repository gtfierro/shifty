#ifndef SHIFTY_SHIFTY_HPP
#define SHIFTY_SHIFTY_HPP

#include "shifty/shifty.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

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

} // namespace detail

/// An in-memory RDF graph owned by the Rust engine.
///
/// Dataset is move-only. Read-only operations may run concurrently only when
/// the caller provides external synchronization against load operations.
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
    ///
    /// \throws Error for non-stratifiable shapes or validation failures.
    [[nodiscard]] ValidationResult validate(
        const Dataset &dataset,
        ValidationOptions options = {}) const {
        ShiftyValidationResult *raw = nullptr;
        detail::check(shifty_prepared_validator_validate(
            handle_.get(),
            dataset.handle_.get(),
            detail::to_c(options.graph_mode),
            static_cast<std::uint8_t>(options.run_inference),
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

private:
    explicit PreparedValidator(ShiftyPreparedValidator *raw) : handle_(raw) {}

    using Handle =
        std::unique_ptr<ShiftyPreparedValidator, detail::ValidatorDeleter>;
    Handle handle_;
};

/// Returns the ABI version implemented by the linked static library.
[[nodiscard]] inline std::uint32_t abi_version() noexcept {
    return shifty_abi_version();
}

} // namespace shifty

#endif
