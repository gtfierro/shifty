// shifty browser playground — drives the wasm SHACL engine with file caching,
// rich report rendering, configurable options, and downloadable inference output.
// All engine calls run in a Web Worker (see worker.js) so a large model never
// freezes the UI.

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

// ── tiny helpers ──────────────────────────────────────────────────────────────
const esc = (s) =>
  String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

let toastTimer;
function toast(msg) {
  const t = $("#toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove("show"), 1800);
}

function download(filename, text, mime = "text/plain") {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = Object.assign(document.createElement("a"), { href: url, download: filename });
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

// ── engine: a Web Worker running the wasm, behind a promise RPC ───────────────
const engine = (() => {
  const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
  const pending = new Map();
  let nextId = 1;
  let resolveReady, rejectReady;
  const ready = new Promise((res, rej) => {
    resolveReady = res;
    rejectReady = rej;
  });

  worker.onmessage = (e) => {
    const msg = e.data;
    if (msg.type === "ready") return resolveReady();
    if (msg.type === "init-error") return rejectReady(new Error(msg.error));
    const p = pending.get(msg.id);
    if (!p) return;
    pending.delete(msg.id);
    msg.ok ? p.resolve(msg.result) : p.reject(new Error(msg.error));
  };
  worker.onerror = (e) => rejectReady(new Error(e.message || "worker failed to load"));

  const call = (method, args) =>
    new Promise((resolve, reject) => {
      const id = nextId++;
      pending.set(id, { resolve, reject });
      worker.postMessage({ id, method, args });
    });

  return {
    ready,
    validate: (s, d, o) => call("validate", [s, d, o]),
    validateW3c: (s, d, o) => call("validateW3c", [s, d, o]),
    infer: (s, d) => call("infer", [s, d]),
    ntriplesToTurtle: (nt) => call("ntriplesToTurtle", [nt]),
  };
})();

// ── prefix-aware IRI shortening (read @prefix/PREFIX from the inputs) ──────────
let prefixMap = []; // [[namespace, prefix], …] longest-namespace first
function rebuildPrefixes() {
  const text =
    inputs.shapes.value +
    "\n" +
    inputs.data.value +
    "\n" +
    ontologyWorkbench.map((item) => item.content || "").join("\n");
  const map = new Map([
    ["http://www.w3.org/ns/shacl#", "sh"],
    ["http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf"],
    ["http://www.w3.org/2000/01/rdf-schema#", "rdfs"],
    ["http://www.w3.org/2001/XMLSchema#", "xsd"],
  ]);
  const re = /@?prefix\s+([\w-]*):\s*<([^>]+)>/gi;
  let m;
  while ((m = re.exec(text))) map.set(m[2], m[1]);
  prefixMap = [...map.entries()].sort((a, b) => b[0].length - a[0].length);
}
function shortenIri(iri) {
  for (const [ns, pfx] of prefixMap) {
    if (iri.startsWith(ns)) return `${pfx}:${iri.slice(ns.length)}`;
  }
  return `<${iri}>`;
}
// Shorten any <…> IRIs inside an oxrdf term string (IRIs, datatyped literals…).
function shortenTerm(term) {
  if (term == null) return term;
  return String(term).replace(/<([^>]+)>/g, (_, iri) => shortenIri(iri));
}

// ── IndexedDB file cache ──────────────────────────────────────────────────────
const DB_NAME = "shifty-files";
const STORE = "files";
function openDB() {
  return new Promise((res, rej) => {
    const r = indexedDB.open(DB_NAME, 1);
    r.onupgradeneeded = () => r.result.createObjectStore(STORE, { keyPath: "name" });
    r.onsuccess = () => res(r.result);
    r.onerror = () => rej(r.error);
  });
}
function tx(db, mode, fn) {
  return new Promise((res, rej) => {
    const t = db.transaction(STORE, mode);
    const req = fn(t.objectStore(STORE));
    t.oncomplete = () => res(req && req.result);
    t.onerror = () => rej(t.error);
  });
}
async function cacheSave(name, content) {
  const db = await openDB();
  await tx(db, "readwrite", (s) =>
    s.put({ name, content, size: content.length, addedAt: Date.now() }),
  );
}
async function cacheAll() {
  const db = await openDB();
  return (await tx(db, "readonly", (s) => s.getAll())) || [];
}
async function cacheGet(name) {
  const db = await openDB();
  return tx(db, "readonly", (s) => s.get(name));
}
async function cacheDelete(name) {
  const db = await openDB();
  await tx(db, "readwrite", (s) => s.delete(name));
}

// ── ontology dependency workbench ────────────────────────────────────────────
const WORKBENCH_KEY = "shifty-ontology-workbench";
let ontologyWorkbench = [];

function makeId() {
  return `onto-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function normalizeImportIri(iri, baseIri) {
  try {
    const url = baseIri ? new URL(iri, baseIri) : new URL(iri);
    if (url.protocol === "http:" && !["localhost", "127.0.0.1", "::1"].includes(url.hostname)) {
      url.protocol = "https:";
    }
    return url.href;
  } catch {
    return iri;
  }
}

function parseTurtlePrefixes(text) {
  const prefixes = new Map();
  const re = /@?prefix\s+([\w-]*):\s*<([^>]+)>/gi;
  let m;
  while ((m = re.exec(text))) prefixes.set(m[1], m[2]);
  return prefixes;
}

function parseImports(text, baseIri = "") {
  const imports = new Set();
  const prefixes = parseTurtlePrefixes(text);
  if (prefixes.get("owl") === "http://www.w3.org/2002/07/owl#") {
    const prefixed = /\bowl:imports\s+((?:<[^>]+>\s*,?\s*)+)/gi;
    let m;
    while ((m = prefixed.exec(text))) {
      const objectList = m[1];
      for (const iri of objectList.matchAll(/<([^>]+)>/g)) {
        imports.add(normalizeImportIri(iri[1], baseIri));
      }
    }
  }

  const iriPredicate = /(?:<http:\/\/www\.w3\.org\/2002\/07\/owl#imports>|<http:\/\/www\.w3\.org\/2002\/07\/owl#Imports>)\s+<([^>]+)>/gi;
  let m;
  while ((m = iriPredicate.exec(text))) imports.add(normalizeImportIri(m[1], baseIri));

  const xmlImports = /<owl:imports\b[^>]*(?:rdf:resource|resource)=["']([^"']+)["'][^>]*\/?>/gi;
  while ((m = xmlImports.exec(text))) imports.add(normalizeImportIri(m[1], baseIri));

  return [...imports];
}

function displayOntologyName(iriOrName) {
  if (!iriOrName) return "ontology";
  try {
    const url = new URL(iriOrName);
    const last = url.pathname.split("/").filter(Boolean).pop();
    return last || url.hostname;
  } catch {
    return iriOrName;
  }
}

function findWorkbenchRecord({ iri, name, content }) {
  return ontologyWorkbench.find(
    (r) =>
      (iri && r.iri === iri) ||
      (!iri && name && r.name === name) ||
      (content && r.content && r.content === content),
  );
}

function upsertWorkbenchRecord(record) {
  const existing = findWorkbenchRecord(record);
  if (existing) {
    Object.assign(existing, record, { id: existing.id });
    return existing;
  }
  const next = {
    id: makeId(),
    name: record.name || displayOntologyName(record.iri),
    iri: record.iri || "",
    content: record.content || "",
    include: record.include ?? true,
    status: record.status || (record.content ? "ready" : "missing"),
    source: record.source || "upload",
    error: record.error || "",
    dependsOn: record.dependsOn || "",
    addedAt: Date.now(),
  };
  ontologyWorkbench.push(next);
  return next;
}

function findReusableWorkbenchDependency(iri) {
  const name = displayOntologyName(iri);
  return ontologyWorkbench.find(
    (r) =>
      r.status === "ready" &&
      r.content?.trim() &&
      (r.iri === iri || r.name === name),
  );
}

function workbenchIncludedShapes() {
  const editorText = inputs.shapes.value.trim();
  const chunks = [];
  if (editorText) chunks.push(inputs.shapes.value);
  for (const item of ontologyWorkbench) {
    if (!item.include || item.status !== "ready" || !item.content.trim()) continue;
    if (editorText && item.content.trim() === editorText) continue;
    chunks.push(item.content);
  }
  return chunks.join("\n\n");
}

function workbenchStats() {
  const ready = ontologyWorkbench.filter((r) => r.status === "ready").length;
  const missing = ontologyWorkbench.filter((r) => r.status === "missing" || r.status === "error").length;
  const included = ontologyWorkbench.filter((r) => r.include && r.status === "ready").length;
  return { ready, missing, included };
}

function persistWorkbench() {
  try {
    localStorage.setItem(WORKBENCH_KEY, JSON.stringify(ontologyWorkbench));
  } catch {}
}

function restoreWorkbench() {
  try {
    const records = JSON.parse(localStorage.getItem(WORKBENCH_KEY) || "[]");
    if (Array.isArray(records)) {
      ontologyWorkbench = records.map((r) => ({
        id: r.id || makeId(),
        name: r.name || displayOntologyName(r.iri),
        iri: r.iri || "",
        content: r.content || "",
        include: r.include ?? true,
        status: r.status || (r.content ? "ready" : "missing"),
        source: r.source || "upload",
        error: r.error || "",
        dependsOn: r.dependsOn || "",
        addedAt: r.addedAt || Date.now(),
      }));
    }
  } catch {
    ontologyWorkbench = [];
  }
}

function renderWorkbench() {
  const list = $("#workbenchList");
  if (!list) return;
  if (!ontologyWorkbench.length) {
    list.innerHTML = `<div class="workbench-empty">Upload a shapes ontology to fetch its <code>owl:imports</code> dependencies.</div>`;
    return;
  }
  list.innerHTML = ontologyWorkbench
    .map((item) => {
      const status = item.status === "ready" ? "ready" : item.status === "fetching" ? "fetching" : "missing";
      const meta = item.status === "ready"
        ? `${fmtBytes(item.content.length)}${item.iri ? ` · ${esc(item.iri)}` : ""}`
        : item.error || item.iri || "waiting for a local file";
      return `
        <div class="workbench-item status-${status}" data-id="${esc(item.id)}">
          <label class="workbench-check">
            <input type="checkbox" ${item.include ? "checked" : ""} ${item.status === "ready" ? "" : "disabled"} data-include />
            <span class="workbench-name">${esc(item.name)}</span>
          </label>
          <span class="workbench-badge">${esc(item.status)}</span>
          <div class="workbench-meta">${meta}</div>
          <span class="workbench-spacer"></span>
          ${item.status !== "ready" ? `<button class="small btn-sm" data-upload-missing>Upload file</button>` : ""}
          <button class="small btn-sm danger" data-remove>Remove</button>
          <input type="file" accept=".ttl,.turtle,.n3,.nt,.rdf,.owl,application/rdf+xml,text/turtle,text/plain" hidden data-missing-file />
        </div>`;
    })
    .join("");

  $$(".workbench-item", list).forEach((row) => {
    const item = ontologyWorkbench.find((r) => r.id === row.dataset.id);
    if (!item) return;
    const checkbox = $("[data-include]", row);
    if (checkbox) {
      checkbox.addEventListener("change", () => {
        item.include = checkbox.checked;
        persistWorkbench();
        persistSession();
      });
    }
    $("[data-remove]", row).addEventListener("click", () => {
      ontologyWorkbench = ontologyWorkbench.filter((r) => r.id !== item.id);
      persistWorkbench();
      persistSession();
      renderWorkbench();
    });
    const uploadButton = $("[data-upload-missing]", row);
    const uploadInput = $("[data-missing-file]", row);
    if (uploadButton && uploadInput) {
      uploadButton.addEventListener("click", () => uploadInput.click());
      uploadInput.addEventListener("change", async () => {
        const file = uploadInput.files[0];
        if (!file) return;
        const text = await file.text();
        item.name = file.name;
        item.content = text;
        item.status = "ready";
        item.source = "upload";
        item.error = "";
        item.include = true;
        await cacheSave(file.name, text);
        await refreshLibraries();
        await fetchDependenciesFor(item);
        persistWorkbench();
        renderWorkbench();
        toast(`Loaded missing dependency “${file.name}”`);
        uploadInput.value = "";
      });
    }
  });
}

async function fetchOntology(iri) {
  const res = await fetch(iri, {
    headers: {
      Accept: "text/turtle, application/rdf+xml;q=0.9, application/n-triples;q=0.8, text/plain;q=0.5",
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
}

async function fetchDependenciesFor(root, seen = new Set()) {
  const imports = parseImports(root.content || "", root.iri);
  if (!imports.length) {
    renderWorkbench();
    persistWorkbench();
    return;
  }

  for (const iri of imports) {
    if (seen.has(iri)) continue;
    seen.add(iri);
    const reusable = findReusableWorkbenchDependency(iri);
    if (reusable) {
      reusable.iri ||= iri;
      reusable.dependsOn ||= root.id;
      reusable.include = true;
      await fetchDependenciesFor(reusable, seen);
      persistWorkbench();
      renderWorkbench();
      continue;
    }
    let dep = upsertWorkbenchRecord({
      iri,
      name: displayOntologyName(iri),
      status: "fetching",
      source: "import",
      include: true,
      dependsOn: root.id,
    });
    renderWorkbench();
    try {
      const content = await fetchOntology(iri);
      dep = upsertWorkbenchRecord({
        id: dep.id,
        iri,
        name: dep.name || displayOntologyName(iri),
        content,
        status: "ready",
        source: "import",
        include: true,
        error: "",
        dependsOn: root.id,
      });
      await fetchDependenciesFor(dep, seen);
    } catch (e) {
      upsertWorkbenchRecord({
        iri,
        name: dep.name || displayOntologyName(iri),
        content: "",
        status: "missing",
        source: "import",
        include: false,
        error: `Could not fetch from ${iri}: ${String(e?.message || e)}`,
        dependsOn: root.id,
      });
    }
    persistWorkbench();
    renderWorkbench();
  }
}

async function addUploadedOntologyToWorkbench(file, text) {
  const root = upsertWorkbenchRecord({
    name: file.name,
    iri: "",
    content: text,
    include: true,
    status: "ready",
    source: "upload",
  });
  renderWorkbench();
  persistWorkbench();
  await fetchDependenciesFor(root);
  const stats = workbenchStats();
  toast(`Loaded “${file.name}” with ${stats.included} workbench file(s) included`);
}

// ── panels (shapes / data) ────────────────────────────────────────────────────
const inputs = {}; // slot -> textarea
const SLOTS = ["shapes", "data"];

function updateMeta(slot) {
  const ta = inputs[slot];
  const meta = $(`#panel-${slot} [data-meta]`);
  const txt = ta.value;
  if (!txt.trim()) {
    meta.textContent = slot === "data" ? "optional — leave empty to use one combined graph" : "";
    return;
  }
  const lines = txt.split("\n").length;
  meta.textContent = `${fmtBytes(txt.length)} · ${lines} lines`;
}

function wirePanel(slot) {
  const panel = $(`#panel-${slot}`);
  const ta = $("[data-input]", panel);
  inputs[slot] = ta;

  ta.addEventListener("input", () => {
    updateMeta(slot);
    rebuildPrefixes();
    persistSession();
  });

  // upload from disk -> fill + auto-cache
  const fileInput = $("[data-file]", panel);
  $("[data-upload]", panel).addEventListener("click", () => fileInput.click());
  fileInput.addEventListener("change", async () => {
    const file = fileInput.files[0];
    if (!file) return;
    const text = await file.text();
    ta.value = text;
    updateMeta(slot);
    rebuildPrefixes();
    persistSession();
    await cacheSave(file.name, text);
    await refreshLibraries();
    if (slot === "shapes") await addUploadedOntologyToWorkbench(file, text);
    toast(`Loaded & cached “${file.name}”`);
    fileInput.value = "";
  });

  // load a cached file
  const lib = $("[data-library]", panel);
  lib.addEventListener("change", async () => {
    if (!lib.value) return;
    const rec = await cacheGet(lib.value);
    if (rec) {
      ta.value = rec.content;
      updateMeta(slot);
      rebuildPrefixes();
      persistSession();
      toast(`Loaded “${rec.name}”`);
    }
    lib.value = "";
  });

  // save current buffer to cache
  $("[data-save]", panel).addEventListener("click", async () => {
    if (!ta.value.trim()) return toast("Nothing to save");
    const name = prompt("Save buffer as:", `${slot}.ttl`);
    if (!name) return;
    await cacheSave(name, ta.value);
    await refreshLibraries();
    toast(`Cached “${name}”`);
  });

  $("[data-clear]", panel).addEventListener("click", () => {
    ta.value = "";
    updateMeta(slot);
    persistSession();
  });

  // drag & drop a file onto the panel
  panel.addEventListener("dragover", (e) => {
    e.preventDefault();
    panel.classList.add("drop");
  });
  panel.addEventListener("dragleave", () => panel.classList.remove("drop"));
  panel.addEventListener("drop", async (e) => {
    e.preventDefault();
    panel.classList.remove("drop");
    const file = e.dataTransfer.files[0];
    if (!file) return;
    const text = await file.text();
    ta.value = text;
    updateMeta(slot);
    rebuildPrefixes();
    persistSession();
    await cacheSave(file.name, text);
    await refreshLibraries();
    if (slot === "shapes") await addUploadedOntologyToWorkbench(file, text);
    toast(`Loaded & cached “${file.name}”`);
  });
}

async function refreshLibraries() {
  const files = await cacheAll();
  files.sort((a, b) => b.addedAt - a.addedAt);
  for (const slot of SLOTS) {
    const lib = $(`#panel-${slot} [data-library]`);
    const cur = lib.value;
    lib.innerHTML =
      `<option value="">cached…</option>` +
      files.map((f) => `<option value="${esc(f.name)}">${esc(f.name)} (${fmtBytes(f.size)})</option>`).join("");
    lib.value = cur;
  }
}

// ── cache management modal ────────────────────────────────────────────────────
async function renderCacheModal() {
  const list = $("#cacheList");
  const files = (await cacheAll()).sort((a, b) => b.addedAt - a.addedAt);
  if (!files.length) {
    list.innerHTML = `<p class="muted">No cached files yet. Upload or drop a file onto a panel.</p>`;
    return;
  }
  list.innerHTML = files
    .map(
      (f) => `
      <div class="file-item" data-name="${esc(f.name)}">
        <div>
          <div class="fname">${esc(f.name)}</div>
          <div class="fmeta">${fmtBytes(f.size)} · ${new Date(f.addedAt).toLocaleString()}</div>
        </div>
        <span class="grow"></span>
        <button class="small" data-to="shapes">→ Shapes</button>
        <button class="small" data-to="data">→ Data</button>
        <button class="small danger" data-del>Delete</button>
      </div>`,
    )
    .join("");

  $$(".file-item", list).forEach((row) => {
    const name = row.dataset.name;
    $$("[data-to]", row).forEach((b) =>
      b.addEventListener("click", async () => {
        const rec = await cacheGet(name);
        inputs[b.dataset.to].value = rec.content;
        updateMeta(b.dataset.to);
        rebuildPrefixes();
        persistSession();
        toast(`Loaded “${name}” → ${b.dataset.to}`);
      }),
    );
    $("[data-del]", row).addEventListener("click", async () => {
      if (!confirm(`Delete cached file “${name}”?`)) return;
      await cacheDelete(name);
      await refreshLibraries();
      renderCacheModal();
    });
  });
}

// ── session persistence (last buffers + options) ──────────────────────────────
const SESSION_KEY = "shifty-session";
let persistTimer;
function persistSession() {
  clearTimeout(persistTimer);
  persistTimer = setTimeout(() => {
    try {
      localStorage.setItem(
        SESSION_KEY,
        JSON.stringify({
          shapes: inputs.shapes.value,
          data: inputs.data.value,
          options: readOptions(),
          workbench: ontologyWorkbench,
        }),
      );
    } catch {}
  }, 250);
}
function restoreSession() {
  try {
    const s = JSON.parse(localStorage.getItem(SESSION_KEY) || "null");
    if (!s) return false;
    inputs.shapes.value = s.shapes || "";
    inputs.data.value = s.data || "";
    if (Array.isArray(s.workbench)) {
      ontologyWorkbench = s.workbench.map((r) => ({
        id: r.id || makeId(),
        name: r.name || displayOntologyName(r.iri),
        iri: r.iri || "",
        content: r.content || "",
        include: r.include ?? true,
        status: r.status || (r.content ? "ready" : "missing"),
        source: r.source || "upload",
        error: r.error || "",
        dependsOn: r.dependsOn || "",
        addedAt: r.addedAt || Date.now(),
      }));
      persistWorkbench();
    }
    if (s.options) {
      $("#opt-infer").checked = s.options.infer ?? true;
      $("#opt-graphmode").value = s.options.graphMode || "union";
      $("#opt-severity").value = s.options.minimumSeverity || "info";
      $("#opt-sort").checked = s.options.sortResults ?? true;
    }
    return !!(s.shapes || s.data || ontologyWorkbench.length);
  } catch {
    return false;
  }
}

// ── options & inputs ──────────────────────────────────────────────────────────
function readOptions() {
  return {
    infer: $("#opt-infer").checked,
    graphMode: $("#opt-graphmode").value,
    minimumSeverity: $("#opt-severity").value,
    sortResults: $("#opt-sort").checked,
  };
}
function dataArg() {
  const d = inputs.data.value;
  return d.trim() ? d : null;
}
function shapesArg() {
  return workbenchIncludedShapes();
}

// ── result rendering ──────────────────────────────────────────────────────────
const results = $("#results");
const SEV_ORDER = { Violation: 0, Warning: 1, Info: 2 };
const sevClass = (s) => (s || "").toLowerCase();

function renderValidation(res, ms) {
  const violations = res.violations || [];
  const counts = { Violation: 0, Warning: 0, Info: 0 };
  for (const v of violations) counts[v.severity] = (counts[v.severity] || 0) + 1;

  const banner = res.conforms
    ? `<div class="banner ok">✓ Conforms<span class="counts">no findings at or above the minimum severity</span></div>`
    : `<div class="banner bad">✗ Does not conform
         <span class="counts">${violations.length} finding(s) ·
           ${counts.Violation} violation, ${counts.Warning} warning, ${counts.Info} info</span></div>`;

  const chip = (label, key) => {
    const n = key === "all" ? violations.length : counts[key] || 0;
    const dot = key === "all" ? "" : `<span class="dot ${key.toLowerCase()}"></span>`;
    return `<button class="chip${key === "all" ? " active" : ""}" data-filter="${key}">${dot}${label} ${n}</button>`;
  };

  results.innerHTML = `
    <div class="result-card">
      ${banner}
      <div class="tabs">
        <button class="tab active" data-tab="findings">Findings</button>
        <button class="tab" data-tab="w3c">W3C report</button>
      </div>
      <div data-panel="findings">
        <div class="toolbar">
          ${chip("All", "all")} ${chip("Violations", "Violation")}
          ${chip("Warnings", "Warning")} ${chip("Info", "Info")}
          <input type="search" placeholder="filter by focus node or message…" data-search />
        </div>
        <div class="violations" data-list></div>
      </div>
      <div data-panel="w3c" hidden></div>
    </div>`;

  $("#timing").textContent = `validated in ${ms.toFixed(1)} ms`;

  const listEl = $("[data-list]", results);
  let activeFilter = "all";
  let query = "";

  function paint() {
    const shown = violations.filter((v) => {
      if (activeFilter !== "all" && v.severity !== activeFilter) return false;
      if (query) {
        const hay = (v.focusNode + " " + (v.shapeName || "") + " " +
          v.reasons.map((r) => r.message).join(" ")).toLowerCase();
        if (!hay.includes(query)) return false;
      }
      return true;
    });
    if (!violations.length) {
      listEl.innerHTML = `<div class="empty-ok">🎉 The data graph conforms — nothing to report.</div>`;
      return;
    }
    if (!shown.length) {
      listEl.innerHTML = `<div class="empty-ok">No findings match the current filter.</div>`;
      return;
    }
    shown.sort((a, b) => (SEV_ORDER[a.severity] ?? 9) - (SEV_ORDER[b.severity] ?? 9));
    listEl.innerHTML = shown.map(violationRow).join("");
  }

  function violationRow(v) {
    const sc = sevClass(v.severity);
    const reasons = v.reasons
      .map(
        (r) => `
        <div class="reason">
          <div class="msg">${esc(r.message)}</div>
          ${r.path ? `<div class="kv"><b>path</b> ${esc(shortenTerm(r.path))}</div>` : ""}
          <div class="kv"><b>value</b> ${esc(shortenTerm(r.value))}</div>
          ${r.severity !== v.severity ? `<div class="kv"><b>severity</b> ${esc(r.severity)}</div>` : ""}
        </div>`,
      )
      .join("");
    return `
      <details class="violation-row sev-${sc}" open>
        <summary>
          <span class="sev-badge ${sc}">${esc(v.severity)}</span>
          <span class="focus">${esc(shortenTerm(v.focusNode))}</span>
          ${v.shapeName ? `<span class="shape">${esc(shortenIri(v.shapeName))}</span>` : ""}
        </summary>
        <div class="reasons">${reasons}</div>
      </details>`;
  }

  paint();

  $$("[data-filter]", results).forEach((c) =>
    c.addEventListener("click", () => {
      activeFilter = c.dataset.filter;
      $$("[data-filter]", results).forEach((x) => x.classList.toggle("active", x === c));
      paint();
    }),
  );
  $("[data-search]", results).addEventListener("input", (e) => {
    query = e.target.value.toLowerCase().trim();
    paint();
  });

  // lazy W3C report tab
  let w3cLoaded = false;
  $$("[data-tab]", results).forEach((tab) =>
    tab.addEventListener("click", () => {
      const which = tab.dataset.tab;
      $$("[data-tab]", results).forEach((t) => t.classList.toggle("active", t === tab));
      $$("[data-panel]", results).forEach((p) => (p.hidden = p.dataset.panel !== which));
      if (which === "w3c" && !w3cLoaded) {
        w3cLoaded = true;
        renderW3cInto($('[data-panel="w3c"]', results));
      }
    }),
  );
}

async function renderW3cInto(el) {
  el.innerHTML = `<div class="empty-ok">Building W3C report…</div>`;
  let rep;
  try {
    rep = await engine.validateW3c(shapesArg(), dataArg(), readOptions());
  } catch (e) {
    el.innerHTML = `<div class="diag">${esc(String(e))}</div>`;
    return;
  }
  el.innerHTML = `
    <div class="downloads">
      <button class="small" data-copy>Copy Turtle</button>
      <button class="small" data-dl>Download report.ttl</button>
      <span class="muted" style="align-self:center">standard <code>sh:ValidationReport</code></span>
    </div>
    <pre class="code">${esc(rep.reportTurtle)}</pre>`;
  $("[data-copy]", el).addEventListener("click", () => {
    navigator.clipboard.writeText(rep.reportTurtle);
    toast("Report Turtle copied");
  });
  $("[data-dl]", el).addEventListener("click", () =>
    download("report.ttl", rep.reportTurtle, "text/turtle"),
  );
}

function renderInfer(res, ms) {
  const diag = res.diagnostics?.length
    ? `<div class="diag"><b>${res.diagnostics.length} unsupported rule feature(s):</b><br>${res.diagnostics
        .map(esc)
        .join("<br>")}</div>`
    : "";

  const preview = res.inferredNtriples.trim()
    ? esc(res.inferredNtriples.split("\n").slice(0, 200).join("\n")) +
      (res.inferredNtriples.split("\n").length > 200 ? "\n… (truncated; download for full graph)" : "")
    : "(no new triples were inferred)";

  results.innerHTML = `
    <div class="result-card">
      <div class="banner ok">✓ Inference complete</div>
      <div class="stat-row">
        <div class="stat"><span class="num add">+${res.inferredCount.toLocaleString()}</span><span class="lbl">inferred triples</span></div>
        <div class="stat"><span class="num">${res.totalCount.toLocaleString()}</span><span class="lbl">total in union</span></div>
      </div>
      ${diag}
      <div class="downloads">
        <span class="muted" style="align-self:center">Download union (data + inferred):</span>
        <button class="small" data-dl="union-ttl">Turtle</button>
        <button class="small" data-dl="union-nt">N-Triples</button>
        <span class="muted" style="align-self:center; margin-left:.6rem">Just the inferred delta:</span>
        <button class="small" data-dl="delta-ttl">Turtle</button>
        <button class="small" data-dl="delta-nt">N-Triples</button>
      </div>
      <div class="tabs"><button class="tab active" data-tab>Inferred triples (preview)</button></div>
      <pre class="code">${preview}</pre>
    </div>`;

  $("#timing").textContent = `inferred in ${ms.toFixed(1)} ms`;

  const handlers = {
    "union-ttl": async () =>
      download("union.ttl", await engine.ntriplesToTurtle(res.graphNtriples), "text/turtle"),
    "union-nt": async () =>
      download("union.nt", res.graphNtriples, "application/n-triples"),
    "delta-ttl": async () =>
      download("inferred.ttl", await engine.ntriplesToTurtle(res.inferredNtriples), "text/turtle"),
    "delta-nt": async () =>
      download("inferred.nt", res.inferredNtriples, "application/n-triples"),
  };
  // Turtle conversion is a worker round-trip; disable the button while it runs
  // so a huge union graph gives feedback instead of a dead click.
  $$("[data-dl]", results).forEach((b) =>
    b.addEventListener("click", async () => {
      b.disabled = true;
      try {
        await handlers[b.dataset.dl]?.();
      } catch (e) {
        toast("Conversion failed");
      } finally {
        b.disabled = false;
      }
    }),
  );
}

function renderError(e) {
  results.innerHTML = `<div class="result-card"><div class="banner bad">✗ Error</div><pre class="code">${esc(String(e))}</pre></div>`;
  $("#timing").textContent = "";
}

// ── run actions ───────────────────────────────────────────────────────────────
let busy = false;
function setBusy(on) {
  busy = on;
  $("#btnValidate").disabled = on;
  $("#btnInfer").disabled = on;
  const status = $("#engineStatus");
  const base = status.classList.contains("status-pill") ? "status-pill" : "pill";
  status.textContent = on ? "working…" : "engine ready";
  status.className = on ? `${base} busy` : `${base} ready`;
}

// The work runs in the worker, so the main thread stays responsive — the
// "Working…" banner paints and the page keeps scrolling while big models churn.
async function run(fn, render) {
  if (busy) return;
  if (!shapesArg().trim()) return toast("Add a shapes graph first");
  rebuildPrefixes();
  results.innerHTML = `<div class="result-card"><div class="banner">⏳ Working…</div></div>`;
  setBusy(true);
  const t0 = performance.now();
  try {
    const out = await fn();
    render(out, performance.now() - t0);
  } catch (e) {
    renderError(e);
  } finally {
    setBusy(false);
  }
}

// ── sample ────────────────────────────────────────────────────────────────────
const SAMPLE_SHAPES = `@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:   <http://example.org/> .

ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    # SHACL-AF rule: tag every Person so inference has something to do.
    sh:rule [
        a sh:TripleRule ;
        sh:subject sh:this ;
        sh:predicate ex:hasTag ;
        sh:object ex:Tagged
    ] ;
    sh:property [
        sh:path ex:name ;
        sh:minCount 1 ;
        sh:datatype xsd:string ;
        sh:message "every person needs exactly one string name"
    ] ;
    sh:property [
        sh:path ex:age ;
        sh:maxCount 1 ;
        sh:datatype xsd:integer ;
        sh:severity sh:Warning ;
        sh:message "age should be a single integer"
    ] .`;

const SAMPLE_DATA = `@prefix ex: <http://example.org/> .

ex:alice a ex:Person ;         # fully conforms
    ex:name "Alice" ;
    ex:age 30 .

ex:bob a ex:Person ;           # missing ex:name -> Violation
    ex:age "old" .             # + age not an integer -> Warning (same focus)

ex:carol a ex:Person ;         # has a name, so only the age rule fires…
    ex:name "Carol" ;
    ex:age "twenty" .          # …-> a Warning-only finding`;

// ── boot ──────────────────────────────────────────────────────────────────────
async function boot() {
  SLOTS.forEach(wirePanel);

  $("#btnSample").addEventListener("click", () => {
    inputs.shapes.value = SAMPLE_SHAPES;
    inputs.data.value = SAMPLE_DATA;
    ontologyWorkbench = [];
    renderWorkbench();
    persistWorkbench();
    SLOTS.forEach(updateMeta);
    rebuildPrefixes();
    persistSession();
  });
  $("#btnClearAll").addEventListener("click", () => {
    SLOTS.forEach((s) => {
      inputs[s].value = "";
      updateMeta(s);
    });
    ontologyWorkbench = [];
    renderWorkbench();
    persistWorkbench();
    results.innerHTML = "";
    $("#timing").textContent = "";
    persistSession();
  });

  $("#btnInfer").addEventListener("click", () =>
    run(() => engine.infer(shapesArg(), dataArg()), renderInfer),
  );
  $("#btnValidate").addEventListener("click", () =>
    run(() => engine.validate(shapesArg(), dataArg(), readOptions()), renderValidation),
  );

  $("#btnWorkbenchAll").addEventListener("click", () => {
    ontologyWorkbench.forEach((item) => {
      item.include = item.status === "ready";
    });
    persistWorkbench();
    persistSession();
    renderWorkbench();
  });
  $("#btnWorkbenchNone").addEventListener("click", () => {
    ontologyWorkbench.forEach((item) => {
      item.include = false;
    });
    persistWorkbench();
    persistSession();
    renderWorkbench();
  });

  // options persistence
  $$("#opt-infer, #opt-graphmode, #opt-severity, #opt-sort").forEach((el) =>
    el.addEventListener("change", persistSession),
  );

  // cache modal
  const modal = $("#cacheModal");
  $("#manageCache").addEventListener("click", async () => {
    await renderCacheModal();
    modal.showModal();
  });
  $("#closeCache").addEventListener("click", () => modal.close());

  restoreWorkbench();
  // restore previous session, else seed the sample
  if (!restoreSession()) {
    inputs.shapes.value = SAMPLE_SHAPES;
    inputs.data.value = SAMPLE_DATA;
    ontologyWorkbench = [];
  }
  SLOTS.forEach(updateMeta);
  renderWorkbench();
  rebuildPrefixes();
  await refreshLibraries();

  // wait for the worker to load + initialize the wasm
  try {
    await engine.ready;
    $("#engineStatus").textContent = "engine ready";
    $("#engineStatus").className = $("#engineStatus").classList.contains("status-pill")
      ? "status-pill ready"
      : "pill ready";
    $("#btnValidate").disabled = false;
    $("#btnInfer").disabled = false;
  } catch (e) {
    $("#engineStatus").textContent = "wasm failed to load";
    $("#engineStatus").className = $("#engineStatus").classList.contains("status-pill")
      ? "status-pill busy"
      : "pill busy";
    renderError(e);
  }
}

boot();
