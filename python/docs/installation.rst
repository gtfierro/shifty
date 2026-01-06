Installation
============

From PyPI (package name ``pyshifty``, import name ``shifty``)::

   pip install pyshifty

From the repository root for local development::

   cd python
   uv sync  # or pip install -r requirements.txt
   uvx maturin develop --extras rdflib --release

The GitHub Actions workflow publishes the package to PyPI using
``python/README.md`` as the long description, so keep it current when the
bindings change.
