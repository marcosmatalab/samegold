// Every ```mermaid block in every markdown file in this repository, through mermaid's own
// parser.
//
// A flowchart that does not parse renders on GitHub as a raw code block: something that
// announces itself as a diagram and is not one, which is the class this repository exists to
// catch. Nothing in the Python suite can answer that question - mermaid's grammar is mermaid's,
// and a second implementation of it in a test would be a lint agreeing with itself.
//
// So the real parser runs, in CI, on every push. `tests/fast/test_documentation.py` carries the
// structural checks that do not need it, and says in as many words that they are weaker.
//
//   npm install --no-save mermaid jsdom
//   node scripts/check_mermaid.mjs            # every .md in the repository
//   node scripts/check_mermaid.mjs README.md  # or the ones named
//
// IT WALKS THE REPOSITORY, and that is the whole of this revision. The first version took
// `README.md` on the command line because that was the only file with diagrams in it, which
// made the scope of the check a fact about today rather than a rule - the next diagram, in
// `docs/`, would have gone unparsed and nothing would have said so. Widening it while there is
// still one file is cheap; widening it after the fourth is a thing nobody remembers to do.
//
// mermaid.parse() needs a DOM (it installs a sanitiser hook at import time), so jsdom provides
// one. It does NOT render: what is checked is that the source is a diagram, not that it draws.

import { readFileSync, readdirSync } from "node:fs";
import { join, relative } from "node:path";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>");

// Assignment is not enough. Node 21 added a built-in `globalThis.navigator` defined as a
// getter with no setter, so `globalThis.navigator = ...` throws `Cannot set property navigator
// of #<Object> which has only a getter` - on Node 22, which is what the GitHub runner has,
// while Node 20 in the author's WSL took the assignment happily. The script worked on the
// machine it was written on and failed on the first push, which is the same class as every
// other "it works here" in FINDINGS.md; it is fixed by defining rather than assigning.
const provide = (name, value) => {
  try {
    globalThis[name] = value;
  } catch {
    Object.defineProperty(globalThis, name, { value, configurable: true, writable: true });
  }
};
provide("window", dom.window);
provide("document", dom.window.document);
provide("navigator", dom.window.navigator);

const mermaid = (await import("mermaid")).default;

// Directories with no documents of this repository's own in them. `node_modules` is what the
// CI step installs one directory up from here, and it ships markdown with mermaid in it: left
// in, this check would be parsing other people's diagrams and reporting them as ours.
const SKIP = new Set([".git", ".venv", "node_modules", "__pycache__", ".mypy_cache", ".ruff_cache", ".pytest_cache"]);
const ROOT = new URL("..", import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1");

function markdownUnder(directory) {
  const out = [];
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    if (entry.name.startsWith(".") && entry.name !== ".github") continue;
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (!SKIP.has(entry.name)) out.push(...markdownUnder(path));
    } else if (entry.name.endsWith(".md")) {
      out.push(path);
    }
  }
  return out;
}

const named = process.argv.slice(2);
const files = named.length ? named : markdownUnder(ROOT).sort();
if (files.length === 0) {
  console.error(`no markdown files under ${ROOT}`);
  process.exit(1);
}
const label = (file) => relative(ROOT, file).split("\\").join("/") || file;

let blocks = 0;
let bad = 0;
for (const file of files) {
  const text = readFileSync(file, "utf8");
  const found = [...text.matchAll(/```mermaid\r?\n([\s\S]*?)```/g)].map((m) => m[1]);
  for (const [i, source] of found.entries()) {
    blocks++;
    try {
      const result = await mermaid.parse(source);
      console.log(`${label(file)} block ${i + 1}: ${result.diagramType}`);
    } catch (error) {
      bad++;
      const message = (error && error.message ? error.message : String(error)).slice(0, 600);
      console.error(`${label(file)} block ${i + 1}: FAILED\n${message}`);
    }
  }
}

// Zero blocks is a failure, not a pass, and it is checked over the WHOLE walk rather than per
// file: most markdown here has no diagram in it and always will. A moved file or a changed
// fence tag would otherwise turn this step green by giving it nothing to do, which is the shape
// of every check in FINDINGS.md that reported the absence of work as success.
if (blocks === 0) {
  console.error(
    `no \`\`\`mermaid blocks in any of the ${files.length} markdown file(s) searched - ` +
      `nothing was checked, which is not the same as nothing being wrong`,
  );
  process.exit(1);
}
console.log(`${files.length} file(s), ${blocks} block(s) parsed, ${bad} failed`);
process.exit(bad ? 1 : 0);
