SPHINXOPTS    ?=
SPHINXBUILD   ?= uv run sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

.PHONY: help html Makefile

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

html:
	@cargo build --locked -p shifty-cli --manifest-path ../Cargo.toml
	@$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
