"""Regression coverage for release-history ingestion and failed measurements."""

from __future__ import annotations

import importlib
import json
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def benchmarks(monkeypatch, tmp_path):
    monkeypatch.syspath_prepend(str(ROOT / "benchmark"))
    results = importlib.import_module("process_results")
    runner = importlib.import_module("run")
    monkeypatch.setattr(results, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    for suite in results.DATASETS:
        directory = tmp_path / "benchmark" / suite
        (directory / "models").mkdir(parents=True)
        (directory / "models/a.ttl").write_text("<urn:a> <urn:p> <urn:b> .")
        shapes = "Brick-closure.ttl" if suite == "brick" else "223p-closure.ttl"
        (directory / shapes).write_text("<urn:shape> <urn:p> <urn:b> .")
    return results, runner


def test_release_order_includes_prereleases_and_head(benchmarks):
    results, _ = benchmarks
    versions = ["HEAD", "v0.6.0", "v0.6.0-alpha.10", "v0.5.1", "v0.6.0-alpha.2"]
    assert sorted(versions, key=results.version_key) == [
        "v0.5.1",
        "v0.6.0-alpha.2",
        "v0.6.0-alpha.10",
        "v0.6.0",
        "HEAD",
    ]


@pytest.mark.parametrize("extra_columns", ["", " 0 0"])
def test_current_and_historical_table_formats(benchmarks, tmp_path, extra_columns):
    results, _ = benchmarks
    table = tmp_path / "timings.txt"
    table.write_text(f"a.ttl 8.125 0.25 10.75 1.5{extra_columns}\n")
    model = results.parse_bench_file(table)["a.ttl"]
    assert model["val_ms"] == 10.75
    assert model["infer_ms"] == 8.125


def test_history_reads_all_recorded_versions(benchmarks, tmp_path):
    results, _ = benchmarks
    output = tmp_path / "results"
    for version in ("v0.5.0", "v0.5.1", "v0.6.0-alpha.2"):
        directory = output / version
        directory.mkdir(parents=True)
        (directory / "brick.txt").write_text("a.ttl 10 1 9.5 2 0 0\n")
    data = results.build_data(output)
    brick, s223 = data["datasets"]
    assert [run["version"] for run in brick["runs"]] == [
        "v0.5.0",
        "v0.5.1",
        "v0.6.0-alpha.2",
    ]
    assert s223["runs"] == []
    assert brick["runs"][0]["mean_ms"] == 9.5
    assert brick["runs"][0]["infer_mean_ms"] == 10
    assert brick["runs"][0]["complete"]


def test_json_recording_is_complete_without_legacy_table(benchmarks, tmp_path):
    results, _ = benchmarks
    output = tmp_path / "results/v0.6.0-alpha.4"
    output.mkdir(parents=True)
    (output / "brick.json").write_text(
        json.dumps(
            {
                "models": {
                    "a.ttl": {
                        "infer_ms": 12.5,
                        "infer_spread_ms": 1,
                        "val_ms": 16.5,
                        "val_spread_ms": 2,
                    }
                }
            }
        )
    )
    data = results.build_data(output.parent)
    assert data["datasets"][0]["runs"][0]["mean_ms"] == 16.5


def test_tagged_text_recordings_resolve_revisions_without_git(benchmarks, tmp_path):
    results, _ = benchmarks
    output = tmp_path / "results"
    for version in ("v0.5.1", "HEAD"):
        directory = output / version
        directory.mkdir(parents=True)
        (directory / "brick.txt").write_text("a.ttl 10 1 12 2\n")
    (output / "revisions.json").write_text(json.dumps({"v0.5.1": "release-commit"}))
    runs = results.build_data(output)["datasets"][0]["runs"]
    assert runs[0]["revision"] == "release-commit"
    assert runs[0]["metadata"] is None
    assert runs[1]["revision"] is None


def test_captured_revision_takes_precedence_over_tag_map(benchmarks, tmp_path):
    results, _ = benchmarks
    output = tmp_path / "results/v0.5.1"
    output.mkdir(parents=True)
    (output / "brick.txt").write_text("a.ttl 10 1 12 2\n")
    (output / "brick.json").write_text(json.dumps({"revision": "measured-commit"}))
    (output.parent / "revisions.json").write_text(json.dumps({"v0.5.1": "tag-commit"}))
    run = results.build_data(output.parent)["datasets"][0]["runs"][0]
    assert run["revision"] == "measured-commit"


def test_release_revision_map_dereferences_tags(benchmarks, monkeypatch, tmp_path):
    _, runner = benchmarks
    monkeypatch.setattr(runner, "release_tags", lambda: ["v0.5.1"])

    def checked(command):
        assert command == ["git", "rev-parse", "refs/tags/v0.5.1^{commit}"]
        return "commit-id"

    monkeypatch.setattr(runner, "checked", checked)
    output = tmp_path / "results"
    output.mkdir()
    (output / "revisions.json").write_text(json.dumps({"v0.5.0": "earlier-commit"}))
    runner.write_release_revisions(output)
    revisions = json.loads((output / "revisions.json").read_text())
    assert revisions == {"v0.5.0": "earlier-commit", "v0.5.1": "commit-id"}


@pytest.mark.parametrize(
    "operation,code,stdout,valid",
    [
        ("validate", 1, "conforms: false\n", True),
        ("validate", 1, "", False),
        ("validate", 0, "", False),
        ("infer", 1, "", False),
    ],
)
def test_execution_errors_are_not_recorded_as_timings(
    benchmarks, monkeypatch, operation, code, stdout, valid
):
    _, runner = benchmarks
    monkeypatch.setattr(
        runner.subprocess,
        "run",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args[0], code, stdout, "execution failed"
        ),
    )
    if valid:
        median, spread, samples = runner.measure(["shifty", operation], 2)
        assert median > 0 and spread >= 0 and len(samples) == 2
    else:
        with pytest.raises(RuntimeError, match="Command failed"):
            runner.measure(["shifty", operation], 2)


def test_failed_suite_preserves_previous_recording(benchmarks, monkeypatch, tmp_path):
    _, runner = benchmarks
    output = tmp_path / "results/HEAD"
    output.mkdir(parents=True)
    table = output / "brick.txt"
    metadata = output / "brick.json"
    table.write_text("old table")
    metadata.write_text("old metadata")
    monkeypatch.setattr(runner, "checked", lambda *args: "")

    def fail(*args):
        raise RuntimeError("measurement failed")

    monkeypatch.setattr(runner, "measure", fail)
    with pytest.raises(RuntimeError, match="measurement failed"):
        runner.record_suite(Path("shifty"), "brick", "HEAD", 3, output.parent)
    assert table.read_text() == "old table"
    assert metadata.read_text() == "old metadata"


def test_changed_fixture_invalidates_recorded_timings(benchmarks, tmp_path):
    _, runner = benchmarks
    shapes, models = runner.suite_inputs("brick")
    output = tmp_path / "results/v0.5.1"
    output.mkdir(parents=True)
    (output / "brick.json").write_text(
        json.dumps(
            {
                "models": {"a.ttl": {}},
                "iterations": 3,
                "inputs_sha256": runner.fingerprint([shapes, *models]),
            }
        )
    )
    assert runner.reusable("brick", "v0.5.1", output.parent, 3)
    models[0].write_text("<urn:a> <urn:p> <urn:c> .")
    assert not runner.reusable("brick", "v0.5.1", output.parent, 3)


def test_recording_resolves_annotated_tag_to_commit(benchmarks, monkeypatch, tmp_path):
    _, runner = benchmarks
    binary = tmp_path / "shifty"
    binary.write_text("fake binary")

    def checked(command):
        if command[:2] == ["git", "rev-parse"]:
            return "commit-id" if command[2].endswith("^{commit}") else "tag-object"
        return ""

    monkeypatch.setattr(runner, "checked", checked)
    monkeypatch.setattr(
        runner, "measure", lambda command, iterations: (3, 0, [3] * iterations)
    )
    output = tmp_path / "results"
    runner.record_suite(binary, "brick", "v0.5.1", 3, output)
    metadata = json.loads((output / "v0.5.1/brick.json").read_text())
    assert metadata["revision"] == "commit-id"
    assert runner.parse_bench_file(output / "v0.5.1/brick.txt")["a.ttl"]["val_ms"] == 3
