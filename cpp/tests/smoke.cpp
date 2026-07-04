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

    // Algebra-path validation: same non-conformance, but as a structured
    // violation/reason tree instead of a W3C sh:ValidationReport graph.
    const auto algebra = validator.validate_algebra(dataset);
    assert(!algebra.conforms());
    assert(algebra.violations().size() == 1);
    const auto &algebra_violation = algebra.violations().front();
    assert(algebra_violation.focus_node == "<http://example.com/bob>");
    assert(algebra_violation.shape_name == "http://example.com/PersonShape");
    assert(algebra_violation.severity == "Violation");
    assert(algebra_violation.reasons.size() == 1);
    assert(algebra_violation.reasons.front().severity == "Violation");
    assert(algebra.results_text().find("bob") != std::string::npos);

    // Property witnesses: the observed sh:property bindings for conforming
    // focus nodes, disambiguated via sh:qualifiedValueShape (the "four
    // same-quantity-kind temperature sensors" scenario). The key isn't a
    // direct annotation on the property shape — it's one hop further,
    // through an intermediate "role descriptor" node — to exercise the
    // multi-hop key_path (a plain single-predicate lookup couldn't reach it).
    constexpr std::string_view witness_shapes = R"(
        @prefix sh:  <http://www.w3.org/ns/shacl#> .
        @prefix zea: <http://zea.example/ns#> .
        @prefix ex:  <http://example.com/> .

        ex:VavProfile a sh:NodeShape ;
            sh:targetClass ex:Vav ;
            sh:property [
                zea:role ex:OutsideAirTempRole ;
                sh:path ex:hasPoint ;
                sh:qualifiedValueShape [ sh:hasValue ex:oat ] ;
                sh:qualifiedMinCount 1 ;
                sh:qualifiedMaxCount 1 ;
            ] ;
            sh:property [
                zea:role ex:ReturnAirTempRole ;
                sh:path ex:hasPoint ;
                sh:qualifiedValueShape [ sh:hasValue ex:rat ] ;
                sh:qualifiedMinCount 1 ;
                sh:qualifiedMaxCount 1 ;
            ] .
        ex:OutsideAirTempRole zea:roleName "outsideAirTemp" .
        ex:ReturnAirTempRole zea:roleName "returnAirTemp" .
    )";

    constexpr std::string_view witness_data = R"(
        @prefix ex: <http://example.com/> .
        ex:vav1 a ex:Vav ; ex:hasPoint ex:oat, ex:rat, ex:sat, ex:mat .
        ex:vav2 a ex:Vav ; ex:hasPoint ex:sat .
    )";

    shifty::Dataset witness_dataset;
    witness_dataset.load(witness_data);

    shifty::PreparedValidator witness_validator(witness_shapes);

    shifty::ValidationOptions witness_options;
    witness_options.key_path = "zea:role/zea:roleName";
    const auto witnesses = witness_validator.witnesses(witness_dataset, witness_options);

    // Only ex:vav1 conforms (ex:vav2 is missing both qualified points), so
    // only its two key bindings are reported.
    assert(witnesses.size() == 2);
    for (const auto &w : witnesses) {
        assert(w.focus_node == "<http://example.com/vav1>");
        assert(w.shape_id == "<http://example.com/VavProfile>");
    }

    const auto find_key = [&witnesses](const std::string &key) -> const shifty::PropertyWitness & {
        for (const auto &w : witnesses) {
            if (w.key == key) {
                return w;
            }
        }
        throw std::runtime_error("key not found: " + key);
    };

    const auto &outside_air = find_key("outsideAirTemp");
    assert(outside_air.value_nodes.size() == 1);
    assert(outside_air.value_nodes[0] == "<http://example.com/oat>");

    const auto &return_air = find_key("returnAirTemp");
    assert(return_air.value_nodes.size() == 1);
    assert(return_air.value_nodes[0] == "<http://example.com/rat>");

    return 0;
}
