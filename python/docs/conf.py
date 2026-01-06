from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - py<3.11 fallback
    import tomli as tomllib  # type: ignore

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"


def _load_project_metadata() -> dict[str, str]:
    if not PYPROJECT.exists():
        return {"project": "pyshifty", "version": "unknown"}
    data = tomllib.loads(PYPROJECT.read_text())
    project = data.get("project", {})
    return {
        "project": project.get("name", "pyshifty"),
        "version": project.get("version", "unknown"),
    }


meta = _load_project_metadata()

project = "shifty (Python)"
author = "Gabe Fierro"
release = meta["version"]

extensions = [
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.napoleon",
]

autosectionlabel_prefix_document = True

templates_path = ["_templates"]
exclude_patterns = ["_build"]

html_theme = "alabaster"
html_static_path = ["_static"]

