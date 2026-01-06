.PHONY: html llms

SPHINXBUILD ?= uv run --project .. --extra docs sphinx-build
SOURCEDIR = .
BUILDDIR = _build

html:
	$(SPHINXBUILD) -b html $(SOURCEDIR) $(BUILDDIR)/html

llms:
	$(SPHINXBUILD) -b text $(SOURCEDIR) $(BUILDDIR)/text
	@files=$$(find $(BUILDDIR)/text -type f -name '*.txt' -print | sort); \
	if [ -z "$$files" ]; then \
	  echo "No Sphinx text output found under $(BUILDDIR)/text" >&2; \
	  exit 1; \
	fi; \
	cat $$files > $(BUILDDIR)/llms.txt
	@mkdir -p $(BUILDDIR)/html
	@cp $(BUILDDIR)/llms.txt $(BUILDDIR)/html/llms.txt
