#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "brick-tq-shacl>=0.4.1",
#   "matplotlib",
#   "pyshacl",
#   "rdflib",
# ]
# ///
"""Benchmark pyshifty against pySHACL and TopQuadrant on benchmark models.

The script times two operations independently:

  1. SHACL-AF rule inference to a fixed point.
  2. Inference plus validation against the same ontology/shapes graph.

For pyshifty, operation 2 is `validate_algebra(..., infer=True)`.
For reported chart values, each row uses the best wall-clock time from three
runs by default. Raw run timings and best-of-N summaries are written to CSV.
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import gc
import importlib.util
import math
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional
import types

import rdflib
from rdflib.util import guess_format


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
SHIFTY_PYTHON = REPO_ROOT / "python"
if SHIFTY_PYTHON.exists() and str(SHIFTY_PYTHON) not in sys.path:
    sys.path.insert(0, str(SHIFTY_PYTHON))


@dataclass(frozen=True)
class Suite:
    name: str
    shapes: Path
    models_dir: Path


@dataclass(frozen=True)
class Engine:
    name: str
    infer: Optional[Callable[[rdflib.Graph, rdflib.Graph], rdflib.Graph]] = None
    infer_validate: Optional[Callable[[rdflib.Graph, rdflib.Graph], bool]] = None
    run_model: Optional[Callable[["Engine", "Suite", Path, rdflib.Graph, rdflib.Graph, int], "RunResult"]] = None
    union_shapes_into_data: bool = False


@dataclass(frozen=True)
class RunResult:
    suite: str
    model: str
    triples: int
    engine: str
    run: int
    inference_seconds: float
    infer_plus_validation_seconds: float
    conforms: bool | None
    inferred_triples: int | None
    error: str


@dataclass(frozen=True)
class ModelInfo:
    suite: str
    path: Path
    triples: int


class RunTimeoutError(TimeoutError):
    pass


@contextlib.contextmanager
def timeout_after(seconds: int):
    if seconds <= 0:
        yield
        return

    def handle_timeout(_signum, _frame):
        raise RunTimeoutError(f"run exceeded timeout of {seconds} seconds")

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, seconds)
    signal.signal(signal.SIGALRM, handle_timeout)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, *previous_timer)
        signal.signal(signal.SIGALRM, previous_handler)


def load_graph(path: Path) -> rdflib.Graph:
    graph = rdflib.Graph()
    graph.parse(path, format=guess_format(str(path)) or "turtle")
    return graph


def clone_graph(graph: rdflib.Graph) -> rdflib.Graph:
    cloned = rdflib.Graph()
    for triple in graph:
        cloned.add(triple)
    return cloned


def _graph_to_nt_bytes(graph: rdflib.Graph) -> bytes:
    data = graph.serialize(format="nt", encoding="utf-8")
    if isinstance(data, str):
        return data.encode("utf-8")
    return data


def _shifty_extension_path() -> Path:
    release_extension = REPO_ROOT / "target" / "release" / "lib_shifty.so"
    if release_extension.exists():
        return release_extension
    copied_extension = next((SHIFTY_PYTHON / "shifty").glob("_shifty*.so"), None)
    if copied_extension is not None:
        return copied_extension
    raise RuntimeError(
        "could not find a pyshifty extension; build release first with "
        "`cd python && maturin develop --release` or `cargo build --release -p pyshifty`"
    )


def _local_shifty_extension():
    package = sys.modules.get("shifty")
    if package is None:
        package = types.ModuleType("shifty")
        package.__path__ = [str(SHIFTY_PYTHON / "shifty")]  # type: ignore[attr-defined]
        sys.modules["shifty"] = package

    module = sys.modules.get("shifty._shifty")
    if module is not None:
        return module

    extension = _shifty_extension_path()
    spec = importlib.util.spec_from_file_location("shifty._shifty", extension)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load extension spec for {extension}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["shifty._shifty"] = module
    spec.loader.exec_module(module)
    return module


def _native_supports_on_unsupported(func) -> bool:
    return "on_unsupported" in (getattr(func, "__text_signature__", "") or "")


def _shifty_api():
    native = _local_shifty_extension()

    class _InferResult:
        def __init__(self, inner):
            self._inner = inner

        def graph(self) -> rdflib.Graph:
            graph = rdflib.Graph()
            graph.parse(data=self._inner.graph_ntriples, format="nt")
            return graph

    class _FallbackShifty:
        @staticmethod
        def infer(data_graph: rdflib.Graph, shapes_graph: rdflib.Graph):
            args = [
                _graph_to_nt_bytes(data_graph),
                None,
                "nt",
                _graph_to_nt_bytes(shapes_graph),
                None,
                "nt",
            ]
            if _native_supports_on_unsupported(native._infer):
                args.append("ignore")
            args.append(None)
            inner = native._infer(*args)
            return _InferResult(inner)

        @staticmethod
        def validate_algebra(
            data_graph: rdflib.Graph,
            shacl_graph: rdflib.Graph,
            *,
            graph_mode: str = "union",
            infer: bool = True,
        ):
            args = [
                _graph_to_nt_bytes(data_graph),
                None,
                "nt",
                _graph_to_nt_bytes(shacl_graph),
                None,
                "nt",
                graph_mode,
                None,  # entry_shape_names
                infer,
                "info",
                True,
            ]
            if _native_supports_on_unsupported(native._validate_algebra):
                args.append("ignore")
            args.append(None)
            result = native._validate_algebra(*args)
            return result

        @staticmethod
        def validate(
            data_graph: rdflib.Graph,
            shacl_graph: rdflib.Graph,
            *,
            graph_mode: str = "union",
            infer: bool = True,
        ):
            result = _FallbackShifty.validate_algebra(
                data_graph,
                shacl_graph,
                graph_mode=graph_mode,
                infer=infer,
            )
            return bool(result.conforms), None, ""

    return _FallbackShifty


def infer_pyshifty(data_graph: rdflib.Graph, shapes_graph: rdflib.Graph) -> rdflib.Graph:
    shifty = _shifty_api()
    result = shifty.infer(data_graph, shapes_graph)
    return result.graph()


def validate_pyshifty(data_graph: rdflib.Graph, shapes_graph: rdflib.Graph) -> bool:
    shifty = _shifty_api()
    result = shifty.validate_algebra(
        data_graph,
        shapes_graph,
        infer=True,
    )
    return bool(result.conforms)


def infer_pyshacl(data_graph: rdflib.Graph, shapes_graph: rdflib.Graph) -> rdflib.Graph:
    import pyshacl

    if not hasattr(pyshacl, "shacl_rules"):
        raise RuntimeError("installed pySHACL does not expose pyshacl.shacl_rules")
    inferred = pyshacl.shacl_rules(
        data_graph=data_graph,
        shacl_graph=shapes_graph,
        advanced=True,
        inplace=True,
        iterate_rules=True,
    )
    if not isinstance(inferred, rdflib.Graph):
        raise RuntimeError(f"pyshacl.shacl_rules returned {type(inferred).__name__}, expected rdflib.Graph")
    return inferred


def validate_pyshacl(data_graph: rdflib.Graph, shapes_graph: rdflib.Graph) -> bool:
    import pyshacl

    conforms, _, _ = pyshacl.validate(
        data_graph=data_graph,
        shacl_graph=shapes_graph,
        advanced=True,
        inplace=False,
        iterate_rules=True,
        allow_warnings=True,
    )
    return bool(conforms)


def infer_topquadrant(data_graph: rdflib.Graph, shapes_graph: rdflib.Graph) -> rdflib.Graph:
    from brick_tq_shacl import infer

    return infer(data_graph, shapes_graph, max_iterations=100, early_isomorphic_exit=True)


def validate_topquadrant(data_graph: rdflib.Graph, shapes_graph: rdflib.Graph) -> bool:
    from brick_tq_shacl import validate

    conforms, _, _ = validate(
        data_graph,
        shapes_graph,
        max_iterations=100,
        early_isomorphic_exit=True,
    )
    return bool(conforms)


ENGINES: dict[str, Engine] = {
    "pyshifty": Engine("pyshifty", infer_pyshifty, validate_pyshifty),
    "pySHACL": Engine("pySHACL", infer_pyshacl, validate_pyshacl, union_shapes_into_data=True),
    "TopQuadrant": Engine("TopQuadrant", infer_topquadrant, validate_topquadrant),
}


def default_suites() -> list[Suite]:
    return [
        Suite("brick", REPO_ROOT / "benchmark" / "brick" / "Brick-closure.ttl", REPO_ROOT / "benchmark" / "brick" / "models"),
        Suite("s223", REPO_ROOT / "benchmark" / "s223" / "223p-closure.ttl", REPO_ROOT / "benchmark" / "s223" / "models"),
    ]


def iter_models(models_dir: Path, limit: int | None) -> list[Path]:
    models = sorted(models_dir.glob("*.ttl"), key=lambda p: p.name)
    if limit is not None:
        return models[:limit]
    return models


def count_model_triples(path: Path) -> int:
    return len(load_graph(path))


def model_infos(suite: Suite, limit: int | None) -> list[ModelInfo]:
    return [
        ModelInfo(suite.name, model_path, count_model_triples(model_path))
        for model_path in iter_models(suite.models_dir, limit)
    ]


def log_sample_model_infos(models: list[ModelInfo], models_per_decade: int) -> list[ModelInfo]:
    if models_per_decade < 1:
        raise SystemExit("--models-per-decade must be at least 1")
    if not models:
        return []
    if any(model.triples <= 0 for model in models):
        bad = ", ".join(model.path.name for model in models if model.triples <= 0)
        raise SystemExit(f"cannot log-sample models with zero triples: {bad}")

    selected: dict[Path, ModelInfo] = {}
    ordered = sorted(models, key=lambda model: (model.triples, model.path.name))
    selected[ordered[0].path] = ordered[0]
    selected[ordered[-1].path] = ordered[-1]

    decades: dict[int, list[ModelInfo]] = {}
    for model in ordered:
        decades.setdefault(math.floor(math.log10(model.triples)), []).append(model)

    for group in decades.values():
        group = sorted(group, key=lambda model: (model.triples, model.path.name))
        if len(group) <= models_per_decade:
            for model in group:
                selected[model.path] = model
            continue

        logs = [math.log10(model.triples) for model in group]
        if models_per_decade == 1:
            targets = [(logs[0] + logs[-1]) / 2]
        else:
            step = (logs[-1] - logs[0]) / (models_per_decade - 1)
            targets = [logs[0] + step * index for index in range(models_per_decade)]

        remaining = group[:]
        for target in targets:
            closest = min(
                remaining,
                key=lambda model: (abs(math.log10(model.triples) - target), model.triples, model.path.name),
            )
            selected[closest.path] = closest
            remaining.remove(closest)

    return sorted(selected.values(), key=lambda model: (model.triples, model.path.name))


def read_model_manifest(path: Path, suites: list[Suite]) -> dict[str, set[str]]:
    suites_by_name = {suite.name for suite in suites}
    selected: dict[str, set[str]] = {suite.name: set() for suite in suites}
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        required = {"suite", "model"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise SystemExit(f"{path} is missing required columns: {', '.join(sorted(missing))}")
        for row in reader:
            suite = row["suite"]
            model = row["model"]
            if suite not in suites_by_name:
                continue
            selected[suite].add(model)
    return selected


def apply_model_manifest(models: list[ModelInfo], manifest: dict[str, set[str]]) -> list[ModelInfo]:
    selected_names = manifest.get(models[0].suite, set()) if models else set()
    by_name = {model.path.name: model for model in models}
    missing = sorted(selected_names.difference(by_name))
    if missing:
        suite = models[0].suite if models else "unknown"
        raise SystemExit(f"manifest references missing {suite} models: {', '.join(missing)}")
    return sorted((by_name[name] for name in selected_names), key=lambda model: (model.triples, model.path.name))


def write_model_manifest(path: Path, selected_by_suite: dict[str, list[ModelInfo]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["suite", "model", "triples"])
        writer.writeheader()
        for suite in sorted(selected_by_suite):
            for model in selected_by_suite[suite]:
                writer.writerow({"suite": suite, "model": model.path.name, "triples": model.triples})


def time_one(engine: Engine, suite: Suite, model_path: Path, shapes_graph: rdflib.Graph, data_graph: rdflib.Graph, run: int) -> RunResult:
    if engine.run_model is not None:
        return engine.run_model(engine, suite, model_path, shapes_graph, data_graph, run)
    assert engine.infer is not None
    assert engine.infer_validate is not None
    gc.collect()
    inference_data = clone_graph(data_graph)
    infer_validate_data = clone_graph(data_graph)
    baseline_graph = clone_graph(data_graph)
    if engine.union_shapes_into_data:
        inference_data += shapes_graph
        infer_validate_data += shapes_graph
        baseline_graph += shapes_graph
    before = len(baseline_graph)

    t0 = time.perf_counter()
    inferred_graph = engine.infer(inference_data, shapes_graph)
    t1 = time.perf_counter()
    conforms = engine.infer_validate(infer_validate_data, shapes_graph)
    t2 = time.perf_counter()

    inferred_delta = max(0, len(inferred_graph) - before)
    inference_seconds = t1 - t0
    return RunResult(
        suite=suite.name,
        model=model_path.name,
        triples=len(data_graph),
        engine=engine.name,
        run=run,
        inference_seconds=inference_seconds,
        infer_plus_validation_seconds=t2 - t1,
        conforms=conforms,
        inferred_triples=inferred_delta,
        error="",
    )


def failed_result(engine: Engine, suite: Suite, model_path: Path, data_graph: rdflib.Graph, run: int, exc: BaseException) -> RunResult:
    return RunResult(
        suite=suite.name,
        model=model_path.name,
        triples=len(data_graph),
        engine=engine.name,
        run=run,
        inference_seconds=math.nan,
        infer_plus_validation_seconds=math.nan,
        conforms=None,
        inferred_triples=None,
        error=f"{type(exc).__name__}: {exc}",
    )


def write_results_csv(path: Path, rows: Iterable[RunResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "suite",
        "model",
        "triples",
        "engine",
        "run",
        "inference_seconds",
        "infer_plus_validation_seconds",
        "conforms",
        "inferred_triples",
        "error",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "suite": row.suite,
                "model": row.model,
                "triples": row.triples,
                "engine": row.engine,
                "run": row.run,
                "inference_seconds": row.inference_seconds,
                "infer_plus_validation_seconds": row.infer_plus_validation_seconds,
                "conforms": "" if row.conforms is None else row.conforms,
                "inferred_triples": "" if row.inferred_triples is None else row.inferred_triples,
                "error": row.error,
            })


def best_rows(rows: Iterable[RunResult]) -> list[dict[str, str | int | float]]:
    grouped: dict[tuple[str, str, str], list[RunResult]] = {}
    for row in rows:
        if row.error:
            continue
        grouped.setdefault((row.suite, row.model, row.engine), []).append(row)

    best: list[dict[str, str | int | float]] = []
    for (suite, model, engine), group in sorted(grouped.items()):
        best_inference = min(r.inference_seconds for r in group)
        best_total = min(r.infer_plus_validation_seconds for r in group)
        exemplar = group[0]
        best.append({
            "suite": suite,
            "model": model,
            "triples": exemplar.triples,
            "engine": engine,
            "best_inference_seconds": best_inference,
            "best_infer_plus_validation_seconds": best_total,
            "runs": len(group),
            "conforms": all(bool(r.conforms) for r in group),
            "inferred_triples": max(r.inferred_triples or 0 for r in group),
        })
    return best


def write_best_csv(path: Path, rows: list[dict[str, str | int | float]]) -> None:
    fieldnames = [
        "suite",
        "model",
        "triples",
        "engine",
        "best_inference_seconds",
        "best_infer_plus_validation_seconds",
        "runs",
        "conforms",
        "inferred_triples",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def make_plots(best: list[dict[str, str | int | float]], output_dir: Path) -> None:
    import matplotlib.pyplot as plt

    metrics = [
        ("best_inference_seconds", "Inference to fixed point", output_dir / "inference_only.png"),
        ("best_infer_plus_validation_seconds", "Inference plus validation", output_dir / "inference_plus_validation.png"),
    ]
    suites = ["brick", "s223"]
    engine_names = [engine.name for engine in ENGINES.values()]
    colors = {
        "pyshifty": "#2563eb",
        "pySHACL": "#d97706",
        "TopQuadrant": "#059669",
    }

    for metric, title, path in metrics:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5), constrained_layout=True, sharey=False)
        fig.suptitle(title)
        for ax, suite in zip(axes, suites, strict=True):
            subset = [row for row in best if row["suite"] == suite]
            for engine in engine_names:
                points = sorted(
                    (
                        (int(row["triples"]), float(row[metric]))
                        for row in subset
                        if row["engine"] == engine
                    ),
                    key=lambda item: item[0],
                )
                if not points:
                    continue
                xs, ys = zip(*points, strict=True)
                ax.plot(xs, ys, marker="o", linewidth=1.5, markersize=3, label=engine, color=colors.get(engine))
            ax.set_title("Brick" if suite == "brick" else "ASHRAE 223")
            ax.set_xlabel("Data graph triples")
            ax.set_ylabel("Best wall time (seconds)")
            ax.grid(True, alpha=0.3)
            handles, labels = ax.get_legend_handles_labels()
            if handles:
                ax.legend(handles, labels)
        fig.savefig(path, dpi=180)
        plt.close(fig)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=3, help="Runs per engine/model; charts use the best successful run (default: 3)")
    parser.add_argument(
        "--engine-runs",
        action="append",
        default=[],
        metavar="ENGINE=N",
        help="Override --runs for one engine, e.g. --engine-runs pySHACL=1. Repeat for multiple engines.",
    )
    parser.add_argument("--out-dir", type=Path, default=SCRIPT_DIR, help="Directory for CSV and PNG outputs")
    parser.add_argument("--raw-csv", default="performance_comparison_raw.csv", help="Raw per-run CSV filename")
    parser.add_argument("--best-csv", default="performance_comparison_best.csv", help="Best-of-N summary CSV filename")
    parser.add_argument("--suite", choices=["brick", "s223"], action="append", help="Limit to one suite; repeat for both")
    parser.add_argument("--engine", choices=list(ENGINES), action="append", help="Limit to one engine; repeat for several")
    parser.add_argument("--limit-models", type=int, help="Limit each suite to the first N sorted models")
    parser.add_argument(
        "--sample-models",
        choices=["log"],
        help="Select a deterministic model sample per suite. 'log' spreads models across orders of magnitude by triple count.",
    )
    parser.add_argument(
        "--models-per-decade",
        type=int,
        default=3,
        help="Number of representatives to keep from each populated log10 triple-count decade when --sample-models log is used (default: 3)",
    )
    parser.add_argument(
        "--model-manifest",
        type=Path,
        help="CSV manifest of models to benchmark. Requires columns: suite, model. Extra columns are ignored.",
    )
    parser.add_argument(
        "--write-model-manifest",
        type=Path,
        help="Write the selected models to this CSV before running benchmarks.",
    )
    parser.add_argument(
        "--dry-run-models",
        action="store_true",
        help="Select and optionally write model manifests, then exit before loading shapes or running engines.",
    )
    parser.add_argument(
        "--run-timeout-seconds",
        type=int,
        default=600,
        help="Maximum wall time for one engine/model/run before recording a timeout error. Use 0 to disable (default: 600).",
    )
    parser.add_argument("--keep-going", action="store_true", help="Record errors and continue instead of stopping at the first failure")
    return parser.parse_args()


def parse_engine_runs(overrides: list[str]) -> dict[str, int]:
    parsed: dict[str, int] = {}
    for override in overrides:
        if "=" not in override:
            raise SystemExit(f"--engine-runs must be ENGINE=N, got {override!r}")
        engine, value = override.split("=", 1)
        if engine not in ENGINES:
            choices = ", ".join(ENGINES)
            raise SystemExit(f"unknown engine {engine!r} in --engine-runs; choices: {choices}")
        try:
            runs = int(value)
        except ValueError as exc:
            raise SystemExit(f"invalid run count in --engine-runs {override!r}") from exc
        if runs < 1:
            raise SystemExit(f"run count must be at least 1 in --engine-runs {override!r}")
        parsed[engine] = runs
    return parsed


def main() -> int:
    args = parse_args()
    if args.runs < 1:
        raise SystemExit("--runs must be at least 1")
    if args.models_per_decade < 1:
        raise SystemExit("--models-per-decade must be at least 1")
    if args.run_timeout_seconds < 0:
        raise SystemExit("--run-timeout-seconds must be non-negative")
    if args.model_manifest is not None and args.sample_models is not None:
        raise SystemExit("--model-manifest and --sample-models are mutually exclusive")
    engine_runs = parse_engine_runs(args.engine_runs)

    output_dir = args.out_dir.resolve()
    suites = [suite for suite in default_suites() if args.suite is None or suite.name in args.suite]
    engines = [engine for key, engine in ENGINES.items() if args.engine is None or key in args.engine]
    if not suites:
        raise SystemExit("no suites selected")
    if not engines:
        raise SystemExit("no engines selected")

    manifest = read_model_manifest(args.model_manifest, suites) if args.model_manifest is not None else None
    selected_by_suite: dict[str, list[ModelInfo]] = {}
    for suite in suites:
        candidates = model_infos(suite, args.limit_models)
        if manifest is not None:
            selected = apply_model_manifest(candidates, manifest)
        elif args.sample_models == "log":
            selected = log_sample_model_infos(candidates, args.models_per_decade)
        else:
            selected = candidates
        selected_by_suite[suite.name] = selected
        print(f"[{suite.name}] selected {len(selected)} of {len(candidates)} models", flush=True)

    if args.write_model_manifest is not None:
        manifest_path = args.write_model_manifest.resolve()
        write_model_manifest(manifest_path, selected_by_suite)
        print(f"Wrote {manifest_path}", flush=True)

    if args.dry_run_models:
        for suite in suites:
            print(f"\n[{suite.name}] model selection", flush=True)
            for model in selected_by_suite[suite.name]:
                print(f"  {model.triples:8d} {model.path.name}", flush=True)
        return 0

    all_rows: list[RunResult] = []
    for suite in suites:
        print(f"\n[{suite.name}] loading shapes: {suite.shapes}", flush=True)
        shapes_graph = load_graph(suite.shapes)
        for model in selected_by_suite[suite.name]:
            model_path = model.path
            data_graph = load_graph(model_path)
            print(f"[{suite.name}] {model_path.name}: {len(data_graph)} triples", flush=True)
            for engine in engines:
                runs = engine_runs.get(engine.name, args.runs)
                for run in range(1, runs + 1):
                    print(f"  {engine.name} run {run}/{runs} ... ", end="", flush=True)
                    try:
                        with timeout_after(args.run_timeout_seconds):
                            row = time_one(engine, suite, model_path, shapes_graph, data_graph, run)
                        print(
                            f"infer={row.inference_seconds:.3f}s "
                            f"infer+validate={row.infer_plus_validation_seconds:.3f}s",
                            flush=True,
                        )
                    except Exception as exc:
                        row = failed_result(engine, suite, model_path, data_graph, run, exc)
                        print(f"ERROR: {row.error}", flush=True)
                        if not args.keep_going:
                            all_rows.append(row)
                            write_results_csv(output_dir / args.raw_csv, all_rows)
                            return 1
                    all_rows.append(row)

    raw_path = output_dir / args.raw_csv
    best_path = output_dir / args.best_csv
    write_results_csv(raw_path, all_rows)
    best = best_rows(all_rows)
    write_best_csv(best_path, best)
    make_plots(best, output_dir)
    print(f"\nWrote {raw_path}")
    print(f"Wrote {best_path}")
    print(f"Wrote {output_dir / 'inference_only.png'}")
    print(f"Wrote {output_dir / 'inference_plus_validation.png'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
