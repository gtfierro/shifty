#include <shifty/shifty.hpp>

#include <cassert>

int main() {
    shifty::Dataset dataset;
    dataset.load(R"(
        @prefix ex: <http://example.com/> .
        ex:subject ex:predicate ex:object .
    )");

    const auto result = dataset.query(R"(
        ASK {
            <http://example.com/subject>
            <http://example.com/predicate>
            <http://example.com/object>
        }
    )");

    assert(result.boolean_value());
    return 0;
}
