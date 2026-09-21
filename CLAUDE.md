# Repository instructions

## Python changes

Work from `python/` and use the locked uv development environment:

```sh
uv sync --dev --frozen --reinstall-package pyshifty
```

`--reinstall-package pyshifty` is required whenever Rust sources have changed.
The extension is an editable build, and a plain `uv sync` will not rebuild it,
so `pytest` then exercises whichever `_shifty` shared object was compiled last
— reporting failures against code you already fixed, and passing against code
you have not.

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
