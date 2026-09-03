// Every ```mermaid block in the named files, through mermaid's own parser.
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
//   node scripts/check_mermaid.mjs README.md
//
// mermaid.parse() needs a DOM (it installs a sanitiser hook at import time), so jsdom provides
// one. It does NOT render: what is checked is that the source is a diagram, not that it draws.

import { readFileSync } from "node:fs";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>");
globalThis.window = dom.window;
globalThis.document = dom.window.document;
globalThis.navigator = dom.window.navigator;

const mermaid = (await import("mermaid")).default;

const files = process.argv.slice(2);
if (files.length === 0) {
  console.error("usage: node scripts/check_mermaid.mjs <file.md> [...]");
  process.exit(2);
}

let blocks = 0;
let bad = 0;
for (const file of files) {
  const text = readFileSync(file, "utf8");
  const found = [...text.matchAll(/```mermaid\r?\n([\s\S]*?)```/g)].map((m) => m[1]);
  for (const [i, source] of found.entries()) {
    blocks++;
    try {
      const result = await mermaid.parse(source);
      console.log(`${file} block ${i + 1}: ${result.diagramType}`);
    } catch (error) {
      bad++;
      const message = (error && error.message ? error.message : String(error)).slice(0, 600);
      console.error(`${file} block ${i + 1}: FAILED\n${message}`);
    }
  }
}

// Zero blocks is a failure, not a pass. A rename or a changed fence tag would otherwise turn
// this step green by giving it nothing to do, which is the shape of every check in FINDINGS.md
// that reported the absence of work as success.
if (blocks === 0) {
  console.error(`no \`\`\`mermaid blocks found in ${files.join(", ")} - nothing was checked`);
  process.exit(1);
}
console.log(`${blocks} block(s) parsed, ${bad} failed`);
process.exit(bad ? 1 : 0);
