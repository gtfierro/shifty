# Repository instructions

## Python changes

Work from `python/` and use the locked uv development environment:

```sh
uv sync --dev --frozen
```

While editing, format Python code with:

```sh
uv run ruff format .
```

Before handing off or committing Python changes, run the same non-mutating
quality gate used by CI and releases:

```sh
uv run ruff check .
uv run ruff format --check .
uv run ty check shifty
uv run pytest -q
```

Ruff checks all Python package, test, example, and benchmark code. `ty` checks
the shipped `shifty` package; pytest provides the runtime and extension-module
coverage. Fix diagnostics instead of weakening or skipping the checks unless a
documented compatibility constraint requires a narrow exception.
