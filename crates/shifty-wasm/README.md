# shifty-wasm

WebAssembly bindings for the [shifty](../../README.md) SHACL engine. Run the full
**SHACL-AF inference + validation** pipeline entirely in the browser — no server,
no round-trips.

Three entry points, all taking Turtle strings:

| Function | Returns |
| --- | --- |
| `validate(shapesTtl, dataTtl, options)` | `{ conforms, violations: [...], resultsText }` — structured "algebra" findings |
| `validateW3c(shapesTtl, dataTtl, options)` | `{ conforms, reportTurtle, resultsText }` — a W3C `sh:ValidationReport` |
| `infer(shapesTtl, dataTtl)` | `{ inferredCount, totalCount, graphNtriples, inferredNtriples, diagnostics }` — the union graph plus just the inferred delta, as N-Triples |
| `ntriplesToTurtle(ntriples)` | a prettified Turtle string — re-serialize a graph the UI only holds as N-Triples (e.g. the inference union) without re-running the engine |

`dataTtl` may be `null`/`""` to treat `shapesTtl` as a single combined
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

## Building

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.125   # must match Cargo.lock
# optional, for a smaller bundle: install binaryen (provides wasm-opt)

./build.sh                 # -> crates/shifty-wasm/pkg/ (ESM, target=web)
./build.sh --target bundler   # for webpack/vite
./build.sh --target nodejs    # for Node
```

The build uses the workspace `wasm-release` profile (size-optimized, debuginfo
stripped). The `wasm_js` getrandom backend (browser `crypto.getRandomValues`,
needed for blank-node IDs) is wired up via this crate's `Cargo.toml` and the
`.cargo/config.toml` rustflags at the repo root.

## Using it

```html
<script type="module">
  import init, { validate } from "./pkg/shifty_wasm.js";
  await init();

  const result = validate(shapesTurtle, dataTurtle, { infer: true });
  console.log(result.conforms, result.violations);
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

ES modules + wasm need a real origin (not `file://`), so serve over HTTP:

```sh
python3 -m http.server -d crates/shifty-wasm
# open http://localhost:8000/example/
```
