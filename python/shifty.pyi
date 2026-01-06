from __future__ import annotations

"""Typed interface for the `shifty` Python extension."""

from os import PathLike
from typing import Any, Literal, Mapping, TypedDict

from rdflib import Graph

Pathish = str | PathLike[str]


class InferenceOptions(TypedDict, total=False):
    """Accepted keys for the `inference` options mapping."""

    run: bool
    enabled: bool
    run_inference: bool
    min_iterations: int
    max_iterations: int
    run_until_converged: bool
    no_converge: bool
    inference_min_iterations: int
    inference_max_iterations: int
    inference_no_converge: bool
    error_on_blank_nodes: bool
    inference_error_on_blank_nodes: bool
    debug: bool
    inference_debug: bool


class InferenceOutcome(TypedDict):
    """Summary of an inference run."""

    iterations_executed: int
    triples_added: int
    converged: bool


class TraceEvent(TypedDict, total=False):
    """Tracing event emitted during validation/inference."""

    type: Literal[
        "EnterNodeShape",
        "EnterPropertyShape",
        "ComponentPassed",
        "ComponentFailed",
        "SparqlQuery",
        "RuleApplied",
    ]
    node_shape_id: int
    property_shape_id: int
    component_id: int
    focus: str
    value: str | None
    message: str
    label: str
    rule_id: int
    inserted: int


class Diagnostics(TypedDict, total=False):
    """Extra outputs returned when diagnostics are requested."""

    graphviz: str
    heatmap: str
    trace_events: list[TraceEvent]
    inference_outcome: InferenceOutcome


class CompiledShapeGraph:
    """Cached ShapeIR that can repeatedly validate or infer against new data."""

    def infer(
        self,
        data_graph: Graph,
        *,
        min_iterations: int | None = ...,
        max_iterations: int | None = ...,
        run_until_converged: bool | None = ...,
        no_converge: bool | None = ...,
        error_on_blank_nodes: bool | None = ...,
        enable_af: bool = ...,
        enable_rules: bool = ...,
        debug: bool | None = ...,
        skip_invalid_rules: bool = ...,
        warnings_are_errors: bool = ...,
        do_imports: bool = ...,
        graphviz: bool = ...,
        heatmap: bool = ...,
        heatmap_all: bool = ...,
        trace_events: bool = ...,
        trace_file: Pathish | None = ...,
        trace_jsonl: Pathish | None = ...,
        return_inference_outcome: bool = ...,
        union: bool = ...,
    ) -> Graph | tuple[Graph, Diagnostics]:
        """Run SHACL rule inference using the cached shapes."""

    def validate(
        self,
        data_graph: Graph,
        *,
        run_inference: bool = ...,
        inference: bool | InferenceOptions | None = ...,
        min_iterations: int | None = ...,
        max_iterations: int | None = ...,
        run_until_converged: bool | None = ...,
        no_converge: bool | None = ...,
        inference_min_iterations: int | None = ...,
        inference_max_iterations: int | None = ...,
        inference_no_converge: bool | None = ...,
        error_on_blank_nodes: bool | None = ...,
        inference_error_on_blank_nodes: bool | None = ...,
        enable_af: bool = ...,
        enable_rules: bool = ...,
        debug: bool | None = ...,
        inference_debug: bool | None = ...,
        skip_invalid_rules: bool = ...,
        warnings_are_errors: bool = ...,
        do_imports: bool = ...,
        graphviz: bool = ...,
        heatmap: bool = ...,
        heatmap_all: bool = ...,
        trace_events: bool = ...,
        trace_file: Pathish | None = ...,
        trace_jsonl: Pathish | None = ...,
        return_inference_outcome: bool = ...,
    ) -> tuple[bool, Graph, str] | tuple[bool, Graph, str, Diagnostics]:
        """Validate data against cached shapes, optionally running inference."""


def generate_ir(
    shapes_graph: Graph,
    *,
    enable_af: bool = ...,
    enable_rules: bool = ...,
    skip_invalid_rules: bool = ...,
    warnings_are_errors: bool = ...,
    do_imports: bool = ...,
) -> CompiledShapeGraph:
    """Compile and cache the ShapeIR for the provided shapes graph."""


def infer(
    data_graph: Graph,
    shapes_graph: Graph,
    *,
    min_iterations: int | None = ...,
    max_iterations: int | None = ...,
    run_until_converged: bool | None = ...,
    no_converge: bool | None = ...,
    error_on_blank_nodes: bool | None = ...,
    enable_af: bool = ...,
    enable_rules: bool = ...,
    debug: bool | None = ...,
    skip_invalid_rules: bool = ...,
    warnings_are_errors: bool = ...,
    do_imports: bool = ...,
    graphviz: bool = ...,
    heatmap: bool = ...,
    heatmap_all: bool = ...,
    trace_events: bool = ...,
    trace_file: Pathish | None = ...,
    trace_jsonl: Pathish | None = ...,
    return_inference_outcome: bool = ...,
    union: bool = ...,
) -> Graph | tuple[Graph, Diagnostics]:
    """Run inference directly from RDFLib graphs."""


def validate(
    data_graph: Graph,
    shapes_graph: Graph,
    *,
    run_inference: bool = ...,
    inference: bool | InferenceOptions | None = ...,
    min_iterations: int | None = ...,
    max_iterations: int | None = ...,
    run_until_converged: bool | None = ...,
    no_converge: bool | None = ...,
    inference_min_iterations: int | None = ...,
    inference_max_iterations: int | None = ...,
    inference_no_converge: bool | None = ...,
    error_on_blank_nodes: bool | None = ...,
    inference_error_on_blank_nodes: bool | None = ...,
    enable_af: bool = ...,
    enable_rules: bool = ...,
    debug: bool | None = ...,
    inference_debug: bool | None = ...,
    skip_invalid_rules: bool = ...,
    warnings_are_errors: bool = ...,
    do_imports: bool = ...,
    graphviz: bool = ...,
    heatmap: bool = ...,
    heatmap_all: bool = ...,
    trace_events: bool = ...,
    trace_file: Pathish | None = ...,
    trace_jsonl: Pathish | None = ...,
    return_inference_outcome: bool = ...,
) -> tuple[bool, Graph, str] | tuple[bool, Graph, str, Diagnostics]:
    """Validate RDFLib graphs against SHACL shapes."""
