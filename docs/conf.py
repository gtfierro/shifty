"""Sphinx configuration for Shifty."""

from __future__ import annotations

import json
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


def _write_benchmark_js() -> None:
    """Bake benchmark_data.json into a JS file loaded by benchmark.rst."""
    data_file = ROOT / "benchmark" / "results" / "benchmark_data.json"
    out_file = DOCS_DIR / "_static" / "benchmark_data.js"
    if data_file.exists():
        data = json.loads(data_file.read_text())
    else:
        data = {}
    out_file.write_text(f"window.SHIFTY_BENCHMARK_DATA = {json.dumps(data)};\n")


_write_benchmark_js()


project = "Shifty"
author = "Gabe Fierro"
release = _read_version()
version = release

extensions = [
    "sphinx.ext.intersphinx",
]

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
html_static_path = ["_static"]
html_title = "Shifty"
html_css_files = ["custom.css"]

# The extra/ directory's contents are copied to the output root.
# extra/playground/ → <build>/playground/
html_extra_path = ["extra"]

html_theme_options = {
    "light_logo": None,
    "dark_logo": None,
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "rdflib": ("https://rdflib.readthedocs.io/en/stable", None),
}
