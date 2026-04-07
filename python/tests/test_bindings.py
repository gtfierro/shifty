from __future__ import annotations

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed

from rdflib import Graph, Literal, Namespace, RDF, RDFS
from rdflib.namespace import SH

import shifty

EX = Namespace("http://example.com/ns#")


def build_data_graph(*, valid: bool) -> Graph:
    graph = Graph()
    graph.bind("ex", EX)
    graph.add((EX.Person1, RDF.type, EX.Person))
    graph.add((EX.Person1, RDFS.label, Literal("Alice")))
    graph.add((EX.Person2, RDF.type, EX.Person))
    if valid:
        graph.add((EX.Person2, RDFS.label, Literal("Bob")))
    return graph


def build_constraint_shapes_graph() -> Graph:
    graph = Graph()
    graph.parse(
        data="""
            PREFIX sh: <http://www.w3.org/ns/shacl#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX ex: <http://example.com/ns#>

            ex:PersonShape
                a sh:NodeShape ;
                sh:targetClass ex:Person ;
                sh:property [
                    sh:path rdfs:label ;
                    sh:minCount 1 ;
                    sh:maxCount 1 ;
                ] .
        """,
        format="turtle",
    )
    return graph


def build_rule_shapes_graph() -> Graph:
    graph = build_constraint_shapes_graph()
    graph.parse(
        data="""
            PREFIX sh: <http://www.w3.org/ns/shacl#>
            PREFIX ex: <http://example.com/ns#>

            ex:PersonShape
                sh:rule [
                    a sh:TripleRule ;
                    sh:subject sh:this ;
                    sh:predicate ex:validated ;
                    sh:object true ;
                ] .
        """,
        format="turtle",
    )
    return graph


class ShiftyBindingsTest(unittest.TestCase):
    def test_validate_reports_nonconformance(self) -> None:
        conforms, report_graph, report_text = shifty.validate(
            build_data_graph(valid=False),
            build_constraint_shapes_graph(),
            do_imports=False,
        )

        self.assertFalse(conforms)
        self.assertIn((None, RDF.type, SH.ValidationReport), report_graph)
        self.assertIn((None, SH.conforms, Literal(False)), report_graph)
        self.assertIn("ValidationReport", report_text)

    def test_infer_returns_expected_triples_and_diagnostics(self) -> None:
        inferred_graph, diagnostics = shifty.infer(
            build_data_graph(valid=True),
            build_rule_shapes_graph(),
            do_imports=False,
            union=False,
            return_inference_outcome=True,
            run_until_converged=True,
        )

        self.assertIn((EX.Person1, EX.validated, Literal(True)), inferred_graph)
        self.assertIn((EX.Person2, EX.validated, Literal(True)), inferred_graph)
        self.assertEqual(diagnostics["inference_outcome"]["triples_added"], 2)
        self.assertTrue(diagnostics["inference_outcome"]["converged"])

    def test_generate_ir_matches_one_shot_validate(self) -> None:
        data_graph = build_data_graph(valid=False)
        shapes_graph = build_constraint_shapes_graph()

        direct_conforms, direct_report_graph, direct_report_text = shifty.validate(
            data_graph,
            shapes_graph,
            do_imports=False,
        )

        compiled = shifty.generate_ir(shapes_graph, do_imports=False)
        cached_conforms, cached_report_graph, cached_report_text = compiled.validate(
            build_data_graph(valid=False),
            do_imports=False,
        )

        self.assertEqual(cached_conforms, direct_conforms)
        self.assertEqual(len(cached_report_graph), len(direct_report_graph))
        self.assertIn("ValidationReport", direct_report_text)
        self.assertIn("ValidationReport", cached_report_text)

    def test_compiled_shape_graph_supports_multithreaded_calls(self) -> None:
        compiled = shifty.generate_ir(build_rule_shapes_graph(), do_imports=False)
        worker_count = 6
        barrier = threading.Barrier(worker_count)

        def validate_worker() -> tuple[str, bool]:
            barrier.wait(timeout=10)
            conforms, report_graph, report_text, diagnostics = compiled.validate(
                build_data_graph(valid=True),
                run_inference=True,
                do_imports=False,
                graphviz=True,
                return_inference_outcome=True,
            )
            self.assertTrue(conforms)
            self.assertGreater(len(report_graph), 0)
            self.assertIn("ValidationReport", report_text)
            self.assertIn("graphviz", diagnostics)
            self.assertEqual(diagnostics["inference_outcome"]["triples_added"], 2)
            return ("validate", conforms)

        def infer_worker() -> tuple[str, int]:
            barrier.wait(timeout=10)
            inferred_graph, diagnostics = compiled.infer(
                build_data_graph(valid=True),
                do_imports=False,
                union=False,
                return_inference_outcome=True,
            )
            self.assertIn((EX.Person1, EX.validated, Literal(True)), inferred_graph)
            self.assertIn((EX.Person2, EX.validated, Literal(True)), inferred_graph)
            self.assertEqual(diagnostics["inference_outcome"]["triples_added"], 2)
            return ("infer", len(inferred_graph))

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = []
            for index in range(worker_count):
                if index % 2 == 0:
                    futures.append(executor.submit(validate_worker))
                else:
                    futures.append(executor.submit(infer_worker))

            results = [future.result(timeout=30) for future in as_completed(futures, timeout=30)]

        self.assertEqual(len(results), worker_count)
        self.assertEqual(sum(1 for kind, _ in results if kind == "validate"), worker_count // 2)
        self.assertEqual(sum(1 for kind, _ in results if kind == "infer"), worker_count // 2)


if __name__ == "__main__":
    unittest.main()
