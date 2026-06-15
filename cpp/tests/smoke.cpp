#include <shifty/shifty.hpp>

#include <cassert>
#include <string>

int main() {
    constexpr std::string_view shapes = R"(
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.com/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

        ex:PersonShape a sh:NodeShape ;
            sh:targetClass ex:Person ;
            sh:property [
                sh:path ex:name ;
                sh:minCount 1 ;
                sh:datatype xsd:string
            ] .
    )";

    constexpr std::string_view data = R"(
        @prefix ex: <http://example.com/> .
        ex:alice a ex:Person ; ex:name "Alice" .
        ex:bob a ex:Person .
    )";

    shifty::Dataset dataset;
    dataset.load(data);
    assert(dataset.size() == 3);

    const auto select = dataset.query(R"(
        SELECT ?person WHERE {
            ?person a <http://example.com/Person>
        }
        ORDER BY ?person
    )");
    assert(select.kind() == shifty::QueryResultKind::Solutions);
    assert(select.media_type() == "application/sparql-results+json");
    assert(select.data().find("alice") != std::string::npos);
    assert(select.data().find("bob") != std::string::npos);

    const auto ask = dataset.query(R"(
        ASK {
            <http://example.com/alice>
                <http://example.com/name> "Alice"
        }
    )");
    assert(ask.kind() == shifty::QueryResultKind::Boolean);
    assert(ask.boolean_value());

    const auto graph = dataset.query(R"(
        CONSTRUCT {
            ?person <http://example.com/selected> ?person
        }
        WHERE {
            ?person a <http://example.com/Person>
        }
    )");
    assert(graph.kind() == shifty::QueryResultKind::Graph);
    assert(graph.media_type() == "application/n-triples");

    shifty::PreparedValidator validator(shapes);
    assert(validator.diagnostics_json() == "[]");

    const auto validation = validator.validate(dataset);
    assert(!validation.conforms());
    assert(validation.report_turtle().find("ValidationReport") != std::string::npos);
    assert(validation.results_text().find("bob") != std::string::npos);

    return 0;
}
