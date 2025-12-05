# Next Steps (v2 wiring & traces)

- ~~Interpreter → ShapeIR: start iterating `context.shape_ir().node_shapes` / `property_shapes` in validation instead of `model.node_shapes`/`prop_shapes`; use IDs to fetch runtime components from the existing map. Keep behaviour identical while the IR proves out.~~ **Done**
- Backend everywhere: finish swapping any remaining direct `model.sparql`/`store` usage in inference and advanced target handling to `ValidationContext` backend helpers.
- CLI traces UX: consider `--trace-jsonl` to emit machine-readable events, and maybe gate verbose stderr output behind `--trace-events`.
- Warnings clean-up: once the refactor settles, drop the unused imports/vars and consider `#[allow(dead_code)]` only where transitional.
- Persistence: optionally serialize `ShapeIR` (via serde) and accept a `--shape-ir` input to skip parsing for warm-start runs.
