# SHACL-AF Test Suite Placeholder

This directory reserves a spot in the manifest hierarchy for SHACL Advanced Features conformance tests. The official test cases live in the W3C [`data-shapes`](https://github.com/w3c/data-shapes) repository under `tests/validation/advanced/` (structure subject to change).

To populate these fixtures locally:

1. Fetch the upstream repository (for example, `git clone https://github.com/w3c/data-shapes.git lib/tests/data-shapes-upstream` or update an existing checkout).
2. Copy the SHACL-AF test manifests and referenced data/shapes files into `lib/tests/test-suite/advanced/`, preserving relative paths expected by the manifests.
3. Update `manifest.ttl` in this directory to include the downloaded W3C manifests (e.g., `mf:include <w3c/manifest.ttl>`).
4. Run `cargo test --workspace` with any future `SHACL_W3C_ENABLE_AF` toggle (to be introduced) to execute the advanced suite.

Until the fixtures are copied in, the placeholder manifest advertises an empty test list so the build script can safely load the directory without generating tests.
