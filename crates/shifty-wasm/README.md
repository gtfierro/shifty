# shifty-wasm

WebAssembly bindings for the [shifty](../../README.md) SHACL engine. Run the full
**SHACL-AF inference + validation** pipeline entirely in the browser — no server,
no round-trips.

Three entry points, all taking RDF strings. Turtle, RDF/XML, N-Triples, N-Quads,
TriG, and N3 are detected from content where possible; fetched playground
dependencies also use response content type and URL extension hints.

| Function | Returns |
| --- | --- |
| `version()` | package version string |
| `validate(shapesRdf, dataRdf, options)` | `{ conforms, violations: [...], resultsText }` — structured "algebra" findings |
| `validateW3c(shapesRdf, dataRdf, options)` | `{ conforms, reportTurtle, resultsText }` — a W3C `sh:ValidationReport` |
| `infer(shapesRdf, dataRdf)` | `{ inferredCount, totalCount, graphNtriples, inferredNtriples, diagnostics }` — the union graph plus just the inferred delta, as N-Triples |
| `ntriplesToTurtle(ntriples)` | a prettified Turtle string — re-serialize a graph the UI only holds as N-Triples (e.g. the inference union) without re-running the engine |

`dataRdf` may be `null`/`""` to treat `shapesRdf` as a single combined
shapes+data graph (matching the embedded mode of the Python/CLI bindings).

### `options`

A plain object; every field is optional:

```js
{
  infer: true,              // run SHACL-AF rules before validating (default true)
  graphMode: "data",        // "data" | "union" | "union-all" (default "data")
  minimumSeverity: "info",  // lowest severity that fails: "info" | "warning" | "violation"
  sortResults: true,        // deterministic ordering (default true)
}
```

## Building / rebuilding

### One-time prerequisites

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.125   # must match Cargo.lock
# optional, for a smaller bundle: install binaryen (provides wasm-opt)
```

> The `wasm-bindgen-cli` version **must** match the `wasm-bindgen` dependency in
> `Cargo.lock`, or it refuses to process the module. To find the pinned version:
> `awk '/^name = "wasm-bindgen"$/{getline; print}' Cargo.lock`.

### Rebuild

[`build.sh`](build.sh) does the whole pipeline — compile → generate JS/TS
bindings → size-optimize — and writes to `crates/shifty-wasm/pkg/`:

```sh
./build.sh                    # -> pkg/ (ESM, target=web) — the default
./build.sh --target bundler   # for webpack / vite
./build.sh --target nodejs    # for Node
```

Then serve and open the playground (ES modules + wasm need a real origin, not
`file://`):

```sh
python3 -m http.server -d crates/shifty-wasm
# open http://localhost:8000/example/
```

**You only need to rebuild when the Rust changes.** Edits to the front-end
(`example/*.js`, `*.css`, `*.html`) take effect on a plain page reload — no build
step.

`pkg/` is a build artifact and is **git-ignored**; regenerate it rather than
committing it.

### Doing it by hand

`build.sh` is just these three steps if you'd rather run them yourself:

```sh
# 1. compile — the `wasm-release` profile strips debuginfo and optimizes for
#    size (the workspace `release` profile keeps full debuginfo: ~100 MB .wasm).
cargo build -p shifty-wasm --target wasm32-unknown-unknown --profile wasm-release

# 2. generate JS/TS bindings
wasm-bindgen target/wasm32-unknown-unknown/wasm-release/shifty_wasm.wasm \
  --out-dir crates/shifty-wasm/pkg --target web

# 3. (optional) shrink with binaryen; needs the post-MVP feature flags rustc
#    emits, e.g. --enable-bulk-memory (build.sh passes the full set)
wasm-opt -Oz --enable-bulk-memory pkg/shifty_wasm_bg.wasm -o pkg/shifty_wasm_bg.wasm
```

### wasm compatibility plumbing

Three things make the engine run on `wasm32-unknown-unknown`; if you touch the
build or bump dependencies, keep them in mind:

- **Randomness** — blank-node IDs go through `rand` → `getrandom`, which has no
  default backend on wasm. The `wasm_js` feature (this crate's `Cargo.toml`)
  plus `getrandom_backend="wasm_js"` (`.cargo/config.toml` rustflags) routes it
  to the browser's `crypto.getRandomValues`.
- **Wall-clock time** — `oxsdatatypes`' `js` feature (this crate's `Cargo.toml`)
  sources the SPARQL `NOW()` timestamp from `Date.now()` instead of std
  `SystemTime`, which panics on wasm.
- **Monotonic time** — `shifty-engine` uses `web_time::Instant` instead of
  `std::time::Instant` (also a wasm panic) for its profiling/timeout clocks.

These failure modes only surface at runtime on specific inputs (e.g. SPARQL
shapes), so the [`WASM` CI workflow](../../.github/workflows/wasm.yml) builds
for wasm32 and runs [`ci/smoke.mjs`](ci/smoke.mjs) through SPARQL fixtures in
Node — a reintroduced panic fails the build instead of shipping a module that
traps in the browser.

## Using it

```html
<script type="module">
  import init, { validate } from "./pkg/shifty_wasm.js";
  await init();

  const result = validate(shapesTurtle, dataTurtle, { infer: true });
  console.log(result.conforms, result.violations);

  // Limit validation to named top-level shape entry points. Referenced helper
  // shapes are still evaluated normally. `entryShapeNames` is also accepted.
  const scoped = validate(shapesTurtle, dataTurtle, {
    shapeNames: ["http://example.org/PersonShape"],
  });

  // Each violation is { focusNode, shapeName?, severity, reasons: [...] }.
  // Each reason is { value, path?, message, severity, authorMessage? }:
  //   message       — always present, engine-generated description.
  //   authorMessage  — present only when the source shape declared an
  //                    sh:message ({$this}/{?var} already resolved); prefer it.
  for (const v of result.violations) {
    for (const r of v.reasons) {
      console.log(r.authorMessage ?? r.message);
    }
  }
</script>
```

## Playground

[`example/`](example/) is a full browser playground (no framework, no build
step) that exercises the whole API:

- **File upload + drag-and-drop** for the shapes and data graphs.
- **Browser-side file cache** (IndexedDB): uploaded/saved graphs persist on the
  device and can be reloaded into either slot or managed from the **Files**
  dialog. Nothing is uploaded anywhere.
- **Rich report rendering**: a conform/violation banner, severity filter chips
  with counts, focus-node/message search, and collapsible findings with
  prefix-shortened IRIs (prefixes are read from your `@prefix` declarations).
- **W3C report tab**: view, copy, or download the standard
  `sh:ValidationReport` Turtle.
- **Inference downloads**: grab the union (data + inferred) or just the inferred
  delta, as Turtle or N-Triples.
- **Advanced-options accordion**: `infer`, `graphMode`, `minimumSeverity`,
  `sortResults`.
- Last session (buffers + options) is restored from `localStorage` on reload.

All engine calls run in a [Web Worker](example/worker.js) so a large model never
freezes the UI.

ES modules + wasm need a real origin (not `file://`), so serve over HTTP:

```sh
python3 -m http.server -d crates/shifty-wasm
# open http://localhost:8000/example/
```
