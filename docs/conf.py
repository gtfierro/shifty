"""Sphinx configuration for Shifty."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore


ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = Path(__file__).resolve().parent


def _read_version() -> str:
    cargo_toml = ROOT / "Cargo.toml"
    try:
        with cargo_toml.open("rb") as fh:
            cargo = tomllib.load(fh)
        return cargo.get("workspace", {}).get("package", {}).get("version", "0.0.0")
    except Exception:
        return "0.0.0"


# Generate chart data beside the build output, directly from recorded results.
# Building docs never runs benchmarks or rewrites checked-in source assets.
BENCHMARK_STATIC = DOCS_DIR / "_build" / "benchmark-static"
subprocess.run(
    [
        sys.executable,
        str(ROOT / "benchmark" / "process_results.py"),
        "--output",
        str(BENCHMARK_STATIC / "benchmark_data.js"),
    ],
    check=True,
)
benchmark_data = BENCHMARK_STATIC / "benchmark_data.js"
benchmark_digest = hashlib.sha256(benchmark_data.read_bytes()).hexdigest()[:12]
BENCHMARK_DATA_FILENAME = f"benchmark_data.{benchmark_digest}.js"
benchmark_data.replace(BENCHMARK_STATIC / BENCHMARK_DATA_FILENAME)


project = "Shifty"
author = "Gabe Fierro"
release = _read_version()
version = release

# Local extensions: `shapeshift_lexer` highlights `.. code-block:: shapeshift`.
sys.path.insert(0, str(DOCS_DIR / "_ext"))

extensions = [
    "sphinx.ext.intersphinx",
    "sphinxcontrib.programoutput",
    "shapeshift_lexer",
    # changelog.rst renders the root CHANGELOG.md in place, so the release
    # history is searchable here rather than only on GitHub.
    "myst_parser",
]

# Sphinx would otherwise treat every .md file it finds as a MyST source. The
# only Markdown this project renders is the changelog, and it is included
# explicitly, so leave .md out of the source suffixes.
source_suffix = {".rst": "restructuredtext"}

# changelog.rst supplies its own page title and includes CHANGELOG.md from its
# second line, so the Markdown's own `# Changelog` heading is skipped rather
# than rendered as a second H1. MyST then sees a fragment whose headings start
# at H2 and says so, once per release section.
suppress_warnings = ["myst.header"]

# Executable documentation examples use the debug CLI built from this checkout.
# The Pages workflow builds it before invoking Sphinx. This also makes a local
# build test the current source instead of any globally installed `shifty`.
cli_dir = ROOT / "target" / "debug"
os.environ["PATH"] = os.pathsep.join([str(cli_dir), os.environ.get("PATH", "")])

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "_doctrees",
    "Thumbs.db",
    ".DS_Store",
    ".venv",
    ".venv/**",
    "extra/**",
]

html_theme = "furo"
html_static_path = ["_static", str(BENCHMARK_STATIC)]
html_title = "Shifty"
html_css_files = ["custom.css"]
# Loaded via html_js_files (not a raw <script> in benchmark.rst) so Sphinx
# appends a content-hash ?v=… — otherwise browsers cache a stale copy and the
# benchmark plot never updates after a rebuild.
# A new data filename also makes Sphinx rebuild cached HTML when recordings
# change during an incremental build.
html_js_files = [BENCHMARK_DATA_FILENAME, "benchmarks.js"]

# The extra/ directory's contents are copied to the output root.
# extra/playground/ → <build>/playground/
html_extra_path = ["extra"]

html_theme_options = {
    "light_logo": None,
    "dark_logo": None,
    "source_repository": "https://github.com/gtfierro/shifty/",
    "source_branch": "main",
    "source_directory": "docs/",
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "rdflib": ("https://rdflib.readthedocs.io/en/stable", None),
}
