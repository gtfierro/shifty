"""Sphinx configuration for Shifty."""

from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore


ROOT = Path(__file__).resolve().parent.parent


def _read_version() -> str:
    cargo_toml = ROOT / "Cargo.toml"
    try:
        with cargo_toml.open("rb") as fh:
            cargo = tomllib.load(fh)
        return cargo.get("workspace", {}).get("package", {}).get("version", "0.0.0")
    except Exception:
        return "0.0.0"


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
