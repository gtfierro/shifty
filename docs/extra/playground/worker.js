// Web Worker: runs the (synchronous) wasm SHACL engine off the main thread so a
// large model can't freeze the UI. Speaks a tiny request/response RPC: the page
// posts { id, method, args }, we reply { id, ok, result } or { id, ok:false, error }.
import init, { validate, validateW3c, infer, ntriplesToTurtle, rdfToTurtle } from "./pkg/shifty_wasm.js";

const methods = { validate, validateW3c, infer, ntriplesToTurtle, rdfToTurtle };

// Initialize the wasm once; announce readiness (or failure) to the page.
const ready = init()
  .then(() => self.postMessage({ type: "ready" }))
  .catch((e) => self.postMessage({ type: "init-error", error: String(e?.message || e) }));

self.onmessage = async (e) => {
  const { id, method, args } = e.data;
  await ready;
  const fn = methods[method];
  if (!fn) {
    self.postMessage({ id, ok: false, error: `unknown method: ${method}` });
    return;
  }
  try {
    const result = fn(...args);
    self.postMessage({ id, ok: true, result });
  } catch (err) {
    self.postMessage({ id, ok: false, error: String(err?.message || err) });
  }
};
