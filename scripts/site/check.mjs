import assert from 'node:assert/strict';
import { readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseHTML } from 'linkedom';

const site = fileURLToPath(new URL('../../site/', import.meta.url));
const files = (await readdir(site)).filter((file) => file.endsWith('.html'));
const documents = new Map();
for (const file of files) {
  const { document } = parseHTML(await readFile(path.join(site, file), 'utf8'));
  documents.set(file, document);
  assert.equal(document.querySelectorAll('h1').length, 1, `${file}: expected one h1`);
  const ids = [...document.querySelectorAll('[id]')].map((element) => element.id);
  assert.equal(new Set(ids).size, ids.length, `${file}: duplicate IDs`);
  for (const button of document.querySelectorAll('[data-copy-target]')) {
    assert(document.getElementById(button.getAttribute('data-copy-target')), `${file}: missing copy target`);
  }
}
let checked = 0;
for (const [file, document] of documents) {
  for (const element of document.querySelectorAll('[href], [src]')) {
    const reference = element.getAttribute('href') ?? element.getAttribute('src');
    const url = new URL(reference, `https://site.test/${file}`);
    if (url.origin !== 'https://site.test') continue;
    const target = decodeURIComponent(url.pathname).slice(1) || 'index.html';
    assert((await stat(path.join(site, target))).isFile(), `${file}: missing file ${reference}`);
    if (url.hash && documents.has(target)) {
      assert(documents.get(target).getElementById(decodeURIComponent(url.hash.slice(1))), `${file}: missing fragment ${reference}`);
    }
    checked++;
  }
}
console.log(`Checked ${files.length} pages and ${checked} local links/assets, fragments and copy targets.`);
