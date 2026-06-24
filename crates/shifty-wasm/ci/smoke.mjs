// CI guardrail: drive the wasm build through SPARQL-based shapes in headless
// Node so a wasm-only runtime regression (e.g. std SystemTime/Instant panicking
// on wasm32) fails the build instead of reaching the browser.
//
// Expects a Node-target wasm-bindgen package at ../pkg-node (built by the
// workflow). Reads fixtures from the repo's testdata/. Exits non-zero on any
// failed expectation or panic (a wasm trap surfaces as a thrown error here).
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "../../.."); // crates/shifty-wasm/ci -> repo root
const fixtures = resolve(repoRoot, "testdata/fixtures");
const read = (p) => readFileSync(resolve(fixtures, p), "utf8");

const { validate, infer } = await import("../pkg-node/shifty_wasm.js");

let failures = 0;
function check(name, cond, detail = "") {
  if (cond) {
    console.log(`  ok   ${name}`);
  } else {
    console.error(`  FAIL ${name}${detail ? ` — ${detail}` : ""}`);
    failures++;
  }
}

// af_target_*: SPARQL-target shapes — the exact shape kind that used to trap
// because spareval stamps every query with DateTime::now() (std SystemTime).
{
  const shapes = read("af_target_shapes.ttl");
  const data = read("af_target_data.ttl");
  console.log("SPARQL-target fixtures (af_target):");
  try {
    const r = validate(shapes, data, { infer: true });
    check("validate returns without trapping", r && typeof r.conforms === "boolean");
    check("non-conforming as expected", r.conforms === false, `conforms=${r.conforms}`);
    check("reports at least one violation", (r.violations?.length ?? 0) >= 1);
  } catch (e) {
    check("validate returns without trapping", false, String(e).split("\n")[0]);
  }
  try {
    const i = infer(shapes, data);
    check("infer returns without trapping", i && typeof i.inferredCount === "number");
  } catch (e) {
    check("infer returns without trapping", false, String(e).split("\n")[0]);
  }
}

// af_default_*: a conforming split shapes/data pair — sanity that the happy
// path still passes.
{
  const shapes = read("af_default_shapes.ttl");
  const data = read("af_default_data.ttl");
  console.log("conforming fixtures (af_default):");
  try {
    const r = validate(shapes, data, { infer: true });
    check("conforms as expected", r.conforms === true, `conforms=${r.conforms}`);
  } catch (e) {
    check("validate returns without trapping", false, String(e).split("\n")[0]);
  }
}

if (failures) {
  console.error(`\n${failures} check(s) failed`);
  process.exit(1);
}
console.log("\nall wasm smoke checks passed");
