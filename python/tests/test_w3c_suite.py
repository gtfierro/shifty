"""
W3C SHACL test suite integration tests.

Runs all sht:Validate test cases from the official SHACL test suite and
asserts that validate() and validate_algebra() agree on conforms=True/False,
matching the expected outcome encoded in each test's mf:result.

Test files in testdata/test-suite/advanced/ use @base <urn:shacl-advanced-*:>
which rdflib cannot resolve as a relative-URI base; those files are silently
skipped during collection (they are duplicate/alternate encodings of the same
tests already covered by core/ and sparql/).
"""

import pathlib

import pytest
import rdflib

import shifty

MF = rdflib.Namespace("http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#")
SHT = rdflib.Namespace("http://www.w3.org/ns/shacl-test#")
SH = rdflib.Namespace("http://www.w3.org/ns/shacl#")

TEST_SUITE_DIR = (
    pathlib.Path(__file__).parent.parent.parent / "testdata" / "test-suite"
)


def _build_urn_map():
    """Return a mapping of manifest-URN string -> file path.

    Each test file declares itself as a manifest with a URN subject, e.g.:
      <urn:and-001.ttl> a mf:Manifest ; mf:entries ( <urn:and-001> ) .
    We scan every .ttl file and record those URNs so we can resolve
    sht:dataGraph / sht:shapesGraph references later.
    """
    urn_map: dict[str, pathlib.Path] = {}
    for ttl_file in TEST_SUITE_DIR.rglob("*.ttl"):
        try:
            g = rdflib.Graph()
            g.parse(str(ttl_file), format="turtle")
        except Exception:
            continue
        for subject in g.subjects(rdflib.RDF.type, MF.Manifest):
            if isinstance(subject, rdflib.URIRef):
                urn_map[str(subject)] = ttl_file
    return urn_map


def _resolve_graph_uri(uri, test_file: pathlib.Path, urn_map: dict) -> pathlib.Path | None:
    uri_str = str(uri)
    if uri_str.startswith("urn:"):
        return urn_map.get(uri_str)
    # Relative file reference — resolve against the test file's directory.
    candidate = test_file.parent / uri_str
    return candidate if candidate.exists() else None


def _collect_test_cases() -> list:
    urn_map = _build_urn_map()
    seen_entries: set[str] = set()
    params: list = []

    for ttl_file in sorted(TEST_SUITE_DIR.rglob("*.ttl")):
        try:
            g = rdflib.Graph()
            g.parse(str(ttl_file), format="turtle")
        except Exception:
            continue

        for entry in g.subjects(rdflib.RDF.type, SHT.Validate):
            entry_str = str(entry)
            if entry_str in seen_entries:
                continue
            seen_entries.add(entry_str)

            action = g.value(entry, MF.action)
            result_node = g.value(entry, MF.result)
            label = g.value(entry, rdflib.RDFS.label)

            if action is None or result_node is None:
                continue

            data_uri = g.value(action, SHT.dataGraph)
            shapes_uri = g.value(action, SHT.shapesGraph)
            expected_lit = g.value(result_node, SH.conforms)

            if data_uri is None or shapes_uri is None or expected_lit is None:
                continue

            data_file = _resolve_graph_uri(data_uri, ttl_file, urn_map)
            shapes_file = _resolve_graph_uri(shapes_uri, ttl_file, urn_map)

            if data_file is None or shapes_file is None:
                continue

            if not isinstance(expected_lit, rdflib.Literal):
                continue
            expected_conforms = bool(expected_lit.toPython())
            rel = ttl_file.relative_to(TEST_SUITE_DIR).with_suffix("")
            test_id = f"{rel}/{str(label) if label else entry_str.split(':')[-1]}"

            params.append(
                pytest.param(
                    data_file,
                    shapes_file,
                    expected_conforms,
                    id=test_id,
                )
            )

    return params


_TEST_CASES = _collect_test_cases()


@pytest.mark.parametrize("data_file,shapes_file,expected_conforms", _TEST_CASES)
def test_suite_validate_algebra_matches_expected(
    data_file: pathlib.Path,
    shapes_file: pathlib.Path,
    expected_conforms: bool,
) -> None:
    data = data_file.read_bytes()
    shapes = shapes_file.read_bytes()
    result = shifty.validate_algebra(data, shapes, infer=False)
    assert result.conforms == expected_conforms


@pytest.mark.parametrize("data_file,shapes_file,expected_conforms", _TEST_CASES)
def test_suite_validate_matches_expected(
    data_file: pathlib.Path,
    shapes_file: pathlib.Path,
    expected_conforms: bool,
) -> None:
    data = data_file.read_bytes()
    shapes = shapes_file.read_bytes()
    conforms, _, _ = shifty.validate(data, shapes, infer=False)
    assert conforms == expected_conforms


@pytest.mark.parametrize("data_file,shapes_file,expected_conforms", _TEST_CASES)
def test_suite_validate_and_algebra_agree(
    data_file: pathlib.Path,
    shapes_file: pathlib.Path,
    expected_conforms: bool,
) -> None:
    data = data_file.read_bytes()
    shapes = shapes_file.read_bytes()
    conforms_w3c, _, _ = shifty.validate(data, shapes, infer=False)
    conforms_algebra = shifty.validate_algebra(data, shapes, infer=False).conforms
    assert conforms_w3c == conforms_algebra, (
        f"validate() returned {conforms_w3c} but validate_algebra() returned "
        f"{conforms_algebra} (expected {expected_conforms})"
    )
