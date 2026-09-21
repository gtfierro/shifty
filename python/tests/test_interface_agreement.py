"""The three validation interfaces must agree about a schema and its graphs.

W3C reporting, algebraic validation, and evidence each answer "does this graph
conform" by a different route. Before 0.5 they also each assembled their own
schema admission, graph roles, and SPARQL function registry, and disagreed
about all three: `development_docs/0.5-architecture-review.md` reproduced one
case per row of its findings table. They now share `CompiledShapes`, and these
tests pin that agreement at the Python boundary, where a routing regression in
`python/src/lib.rs` would not be caught by the engine's own session tests.

Each case asserts agreement, not a particular verdict: the point is that no
interface is the odd one out.
"""

import warnings

import pytest

import shifty

PREFIX = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://ex/> .
"""


def _verdicts(shapes, data, *, graph_mode="union"):
    """Each interface's answer, as a verdict or the error it raised."""

    def run(call):
        try:
            return ("ok", call())
        except ValueError as error:
            return ("error", str(error))

    return {
        "w3c": run(
            lambda: shifty.validate(data, shapes, infer=False, graph_mode=graph_mode)[0]
        ),
        "algebra": run(
            lambda: (
                shifty.validate_algebra(
                    data, shapes, infer=False, graph_mode=graph_mode
                ).conforms
            )
        ),
        "evidence": run(
            lambda: (
                shifty.EvidenceSession(shapes, data, infer=False, graph_mode=graph_mode)
                .validate()
                .conforms
            )
        ),
    }


def _agree(verdicts):
    kinds = {kind for kind, _ in verdicts.values()}
    assert len(kinds) == 1, f"interfaces disagreed about outcome kind: {verdicts}"
    if kinds == {"ok"}:
        values = {value for _, value in verdicts.values()}
        assert len(values) == 1, f"interfaces disagreed about conformance: {verdicts}"
    return next(iter(verdicts.values()))


def test_recursion_through_negation_is_rejected_by_every_interface():
    """Admission is shared, so no interface answers for a schema the others refuse."""
    shapes = PREFIX + "ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:not ex:S ."
    data = PREFIX + "ex:x ex:p ex:o ."
    kind, message = _agree(_verdicts(shapes, data))
    assert kind == "error"
    assert "non-stratifiable" in message
    # The cycle names the shape the author wrote, not a bare arena slot.
    assert "ex:S" in message


def test_pure_sparql_function_is_supported_by_every_interface():
    """Functions are registered once, so algebra and evidence see them too."""
    shapes = (
        PREFIX
        + """
    ex:isOk a sh:SPARQLFunction ;
        sh:parameter [ sh:path ex:arg ] ;
        sh:ask 'ASK { FILTER (STR($arg) = "ok") }' .
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:sparql [ sh:select '''SELECT $this ?value WHERE {
            $this <http://ex/val> ?value .
            FILTER (! <http://ex/isOk>(?value))
        }''' ] .
    """
    )
    conforming = PREFIX + 'ex:x ex:val "ok" .'
    failing = PREFIX + 'ex:x ex:val "nope" .'

    kind, verdict = _agree(_verdicts(shapes, conforming))
    assert (kind, verdict) == ("ok", True)

    # And the function actually decides something, rather than passing vacuously.
    kind, verdict = _agree(_verdicts(shapes, failing))
    assert (kind, verdict) == ("ok", False)


@pytest.mark.parametrize("graph_mode", ["union", "union-all"])
def test_shapes_graph_names_the_authored_source_in_every_interface(graph_mode):
    """$shapesGraph is the authored document, never the data, in any graph mode."""
    shapes = (
        PREFIX
        + """ex:S a sh:NodeShape ; sh:targetNode ex:data ;
        sh:sparql [ sh:select '''SELECT $this WHERE {
            GRAPH $shapesGraph {
                <http://ex/data> <http://ex/p> <http://ex/o>
            }
        }''' ] ."""
    )
    data = PREFIX + "ex:data ex:p ex:o ."
    kind, verdict = _agree(_verdicts(shapes, data, graph_mode=graph_mode))
    # The triple lives only in the data graph, so the query selects nothing and
    # the constraint reports no violation.
    assert (kind, verdict) == ("ok", True)


def test_unsupported_policy_reaches_validations_automatic_inference():
    """`on_unsupported` is not silently downgraded for the pre-validation rules."""
    shapes = (
        PREFIX
        + """
    ex:exists a sh:SPARQLFunction ;
        sh:ask "ASK { ?s <http://ex/marker> ?o }" .
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:rule [ a sh:SPARQLRule ;
            sh:construct "CONSTRUCT { $this <http://ex/flag> true } WHERE { FILTER (! <http://ex/exists>()) }" ] ;
        sh:property [ sh:path ex:flag ; sh:maxCount 0 ] .
    """
    )
    data = PREFIX + 'ex:x ex:marker "present" .'

    with pytest.raises(ValueError, match="strict inference failed"):
        shifty.infer(data, shapes, on_unsupported="error")

    for call in (shifty.validate, shifty.validate_algebra):
        with pytest.raises(ValueError, match="strict inference failed"):
            call(data, shapes, on_unsupported="error")


SKIPPED_RULE_SHAPES = (
    PREFIX
    + """
ex:S a sh:NodeShape ; sh:targetNode ex:x ;
    sh:rule [ a sh:SPARQLRule ;
        sh:construct "CONSTRUCT { $this ex:p [] } WHERE {}" ] .
"""
)
SKIPPED_RULE_DATA = PREFIX + "ex:x ex:q ex:y ."


def test_every_interface_reports_inference_diagnostics():
    """A run whose rules could not execute says so, whichever result type it returns."""
    algebra = shifty.validate_algebra(SKIPPED_RULE_DATA, SKIPPED_RULE_SHAPES)
    assert any("blank nodes" in message for message in algebra.diagnostics)

    inferred = shifty.infer(SKIPPED_RULE_DATA, SKIPPED_RULE_SHAPES)
    assert any("blank nodes" in message for message in inferred.diagnostics)


def test_w3c_path_warns_because_its_tuple_cannot_carry_diagnostics():
    """`validate()` returns pyshacl's 3-tuple, so a skipped rule must warn.

    Without this the caller sees `conforms=True` and nothing at all about the
    rule that never ran.
    """
    with pytest.warns(shifty.ShaclDiagnosticWarning, match="blank nodes"):
        conforms, _, _ = shifty.validate(SKIPPED_RULE_DATA, SKIPPED_RULE_SHAPES)
    assert conforms is True

    prepared = shifty.PreparedValidator(SKIPPED_RULE_SHAPES)
    with pytest.warns(shifty.ShaclDiagnosticWarning, match="blank nodes"):
        prepared.validate(SKIPPED_RULE_DATA)


def test_a_clean_run_warns_about_nothing():
    """The warning has to stay rare enough that callers do not filter it away."""
    shapes = PREFIX + "ex:S a sh:NodeShape ; sh:targetNode ex:x ."
    data = PREFIX + "ex:x ex:p ex:o ."
    with warnings.catch_warnings():
        warnings.simplefilter("error", shifty.ShaclDiagnosticWarning)
        shifty.validate(data, shapes)
