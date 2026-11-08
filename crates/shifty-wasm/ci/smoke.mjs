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

const { validate, validateW3c, infer, version } = await import("../pkg-node/shifty_wasm.js");

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
check("version is exported", typeof version() === "string" && version().length > 0);

{
  const shapes = read("af_target_shapes.ttl");
  const data = read("af_target_data.ttl");
  console.log("SPARQL-target fixtures (af_target):");
  let strictRejected = false;
  try { validate(shapes, data); } catch (error) { strictRejected = String(error).includes("unsupported shapes features"); }
  check("unsupported custom targets reject by default", strictRejected);
  try {
    const r = validate(shapes, data, { infer: true, onUnsupported: "ignore" });
    check("validate returns without trapping", r && typeof r.conforms === "boolean");
    check("non-conforming as expected", r.conforms === false, `conforms=${r.conforms}`);
    check("reports at least one violation", (r.violations?.length ?? 0) >= 1);
    check("validate returns diagnostics", Array.isArray(r.diagnostics));
  } catch (e) {
    check("validate returns without trapping", false, String(e).split("\n")[0]);
  }
  try {
    const r = validateW3c(shapes, data, { infer: true, onUnsupported: "ignore" });
    check("W3C validation returns diagnostics", Array.isArray(r.diagnostics));
  } catch (e) {
    check("W3C validation returns diagnostics", false, String(e).split("\n")[0]);
  }
  try {
    const i = infer(shapes, data, { onUnsupported: "ignore" });
    check("infer returns without trapping", i && typeof i.inferredCount === "number");
    check("infer returns diagnostics", Array.isArray(i.diagnostics));
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
  const inferred = infer(shapes, data);
  check("infer keeps its two-argument API", typeof inferred.inferredCount === "number");

}

// Defaults must be independent of how the caller spells an empty options object.
{
  const prefixes = `@prefix sh: <http://www.w3.org/ns/shacl#> .
    @prefix ex: <http://ex/> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .`;
  const shapes = `${prefixes}
    ex:S a sh:NodeShape ; sh:targetNode ex:x ;
      sh:rule [ a sh:TripleRule ; sh:subject sh:this ; sh:predicate ex:flag ; sh:object true ] ;
      sh:property [ sh:path ex:flag ; sh:maxCount 0 ] .`;
  const data = `${prefixes} ex:x ex:p ex:y .`;
  for (const call of [validate, validateW3c]) {
    const expected = call(shapes, data, {});
    for (const opts of [undefined, null, {}]) {
      check(`${call.name}: empty options run inference consistently`,
        call(shapes, data, opts).conforms === expected.conforms && !expected.conforms);
    }
    check(`${call.name}: inference can be disabled`, call(shapes, data, { infer: false }).conforms);
    const hierarchy = `${prefixes} ex:A rdfs:subClassOf ex:B .
      ex:S a sh:NodeShape ; sh:targetNode ex:x ; sh:property [ sh:path ex:p ; sh:class ex:B ] .`;
    const typed = `${data} ex:y a ex:A .`;
    check(`${call.name}: default graph mode includes shapes hierarchy`, call(hierarchy, typed, { infer: false }).conforms);
    check(`${call.name}: data mode remains explicit`, !call(hierarchy, typed, { infer: false, graphMode: "data" }).conforms);
    const functions = `${prefixes}
      ex:exists a sh:SPARQLFunction ; sh:ask "ASK { ?s <http://ex/p> ?o }" .
      ex:S a sh:NodeShape ; sh:targetNode ex:x ;
        sh:sparql [ sh:select "SELECT $this WHERE { FILTER (<http://ex/exists>()) }" ] .`;
    for (const opts of [undefined, null, {}, { onUnsupported: "error" }]) {
      let rejected = false;
      try { call(functions, data, opts); } catch (error) { rejected = String(error).includes("graph-reading SPARQL function"); }
      check(`${call.name}: strict policy rejects unsupported function`, rejected);
    }
    check(`${call.name}: best effort requires explicit ignore`, call(functions, data, { onUnsupported: "ignore" }).conforms);
    let invalidOption = false;
    try { call(shapes, data, { onUnsupported: "typo" }); } catch (error) { invalidOption = String(error).includes("onUnsupported"); }
    check(`${call.name}: invalid policy is rejected`, invalidOption);
  }
}

if (failures) {
  console.error(`\n${failures} check(s) failed`);
  process.exit(1);
}
console.log("\nall wasm smoke checks passed");
