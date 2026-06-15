#include <shifty/shifty.hpp>

#include <filesystem>
#include <iostream>

int main(int argc, char **argv) {
    const std::filesystem::path shapes =
        argc > 1 ? argv[1] : "s223/223p.ttl";

    const std::filesystem::path model =
        argc > 2 ? argv[2] : "s223/models/nist-bdg1-1.ttl";

    try {
        std::cout << "Shifty ABI: " << shifty::abi_version() << '\n';

        // Parse and normalize the 223P shapes once. Reuse this object to
        // validate any number of model datasets.
        auto validator =
            shifty::PreparedValidator::from_file(shapes);

        const auto diagnostics = validator.diagnostics_json();
        std::cout << "Shape diagnostics: " << diagnostics << "\n\n";

        // Load the model into a Rust-owned RDF dataset.
        shifty::Dataset dataset;
        dataset.load_file(model);

        std::cout << "Loaded triples: " << dataset.size() << '\n';

        // Optional: inspect the model using SPARQL before validation.
        const auto classes = dataset.query(R"(
            SELECT ?class (COUNT(?instance) AS ?count)
            WHERE {
                ?instance a ?class .
            }
            GROUP BY ?class
            ORDER BY DESC(?count)
            LIMIT 10
        )");

        std::cout << "\nSPARQL result type: "
                  << classes.media_type() << '\n';
        std::cout << classes.data() << "\n\n";

        shifty::ValidationOptions options;
        options.graph_mode = shifty::GraphMode::Union;
        options.run_inference = true;

        const auto result = validator.validate(dataset, options);

        std::cout << "Conforms: "
                  << (result.conforms() ? "yes" : "no")
                  << "\n\n";

        // Human-readable violations.
        std::cout << result.results_text() << '\n';

        // Complete W3C sh:ValidationReport in Turtle.
        std::cout << "\n--- RDF validation report ---\n";
        std::cout << result.report_turtle() << '\n';

        return result.conforms() ? 0 : 2;
    } catch (const shifty::Error &error) {
        std::cerr << "Shifty error " << error.status()
                  << ": " << error.what() << '\n';
        return 1;
    } catch (const std::exception &error) {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
}
