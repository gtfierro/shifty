from __future__ import annotations

from os import PathLike
from typing import Any, Mapping

from rdflib import Graph

Pathish = str | PathLike[str]
Diagnostics = Mapping[str, Any]
InferenceOptions = Mapping[str, Any]


class ShapeIrCache:
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
) -> ShapeIrCache:
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
