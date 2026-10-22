import { copyFile, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import MarkdownIt from 'markdown-it';

const root = fileURLToPath(new URL('../../', import.meta.url));
const pages = [
  ['docs/index.md', 'documentation.html', 'Documentation', 'Installation, configuration, deployment and API guides for fleet-router.'],
  ['docs/getting-started.md', 'getting-started.html', 'Getting started', 'Install fleet-router, configure SHiP upstreams and check your first listener.'],
  ['docs/configuration.md', 'configuration.html', 'Configuration', 'Every fleet-router configuration field, including TLS, live reload and multiple networks.'],
  ['docs/operations.md', 'operations.html', 'Operations', 'Routing, failover, frozen ranges, service deployment and production operations.'],
  ['docs/observability.md', 'observability.html', 'Observability', 'Health, readiness, coverage, Prometheus metrics and alerts.'],
  ['docs/chain-api.md', 'chain-api.html', 'Chain API', 'Configure the HTTP chain API, supported endpoints, routing and request limits.'],
  ['docs/api.md', 'api.html', 'Dashboard & control API', 'Dashboard setup, read endpoints, live streams, controls and configuration editing.'],
  ['docs/frozen-ship.md', 'frozen-ship.html', 'Frozen SHiP', 'Deploy frozen Antelope SHiP archives and live nodes behind one stitched endpoint.'],
  ['docs/wire-network.md', 'wire-network.html', 'Wire Network', 'Upgrade to fleet-router 0.8.1 for Wire Network SHiP ABI support and verify your deployment.'],
  ['CHANGELOG.md', 'changelog.html', 'Changelog', 'fleet-router release notes and upgrade guidance.'],
];
const outputs = new Map(pages.map(([source, output]) => [source, output]));
const version = (await readFile(path.join(root, 'Cargo.toml'), 'utf8')).match(/^version\s*=\s*"([^"]+)"/m)[1];
const template = await readFile(new URL('./page.html', import.meta.url), 'utf8');
const escape = (value) => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
// Match GitHub heading fragments, including punctuation and duplicate headings.
const slug = (text) => text.toLowerCase().replace(/[^\p{L}\p{N}_\-\s]/gu, '').replace(/\s/g, '-');

for (const [source, output, label, description] of pages) {
  const md = new MarkdownIt({ html: false, linkify: false });
  let codeCount = 0;
  const headings = [];
  const used = new Set();
  const headingRule = md.renderer.rules.heading_open;
  md.renderer.rules.heading_open = (tokens, index, options, env, renderer) => {
    const token = tokens[index];
    const inline = tokens[index + 1];
    const text = inline.children.filter((t) => t.type === 'text' || t.type === 'code_inline').map((t) => t.content).join('');
    const base = slug(text);
    let id = base;
    for (let n = 1; used.has(id); n++) id = `${base}-${n}`;
    used.add(id);
    token.attrSet('id', id);
    if (token.tag === 'h2') headings.push({ id, text });
    return headingRule ? headingRule(tokens, index, options, env, renderer) : renderer.renderToken(tokens, index, options);
  };
  md.renderer.rules.fence = (tokens, index) => {
    const token = tokens[index];
    const language = token.info.trim().split(/\s/)[0] || 'text';
    const id = `guide-code-${++codeCount}`;
    return `<div class="codeblock" data-copy-name="${escape(language)} example"><div class="codeblock-bar"><span>${escape(language)}</span><button type="button" class="copy" data-copy-target="${id}" aria-label="Copy ${escape(language)} example ${codeCount}"><span class="copy-label"><span>Copy</span><span>Copied</span><span>Failed</span></span></button></div><pre tabindex="0"><code id="${id}" class="language-${escape(language)}">${escape(token.content)}</code></pre></div>\n`;
  };
  md.renderer.rules.table_open = () => '<div class="guide-table" role="region" aria-label="Reference table" tabindex="0"><table>\n';
  md.renderer.rules.table_close = () => '</table></div>\n';
  const linkRule = md.renderer.rules.link_open;
  md.renderer.rules.link_open = (tokens, index, options, env, renderer) => {
    const token = tokens[index];
    const href = token.attrGet('href');
    if (href && !/^(?:[a-z][a-z\d+.-]*:|#|\/)/i.test(href)) {
      const [target, fragment] = href.split('#');
      const resolved = path.posix.normalize(path.posix.join(path.posix.dirname(source), target));
      const destination = outputs.get(resolved);
      const suffix = fragment ? `#${fragment}` : '';
      if (destination) token.attrSet('href', destination + suffix);
      else if (resolved === 'deploy/frozen-ship.config.json') token.attrSet('href', 'frozen-ship.config.json');
      else token.attrSet('href', `https://github.com/eosrio/fleet-router/blob/main/${resolved}${suffix}`);
    }
    return linkRule ? linkRule(tokens, index, options, env, renderer) : renderer.renderToken(tokens, index, options);
  };

  const tokens = md.parse(await readFile(path.join(root, source), 'utf8'), {});
  // Replace the references' hand-written contents lists with generated links.
  for (let i = 0; i < tokens.length && !(tokens[i].type === 'heading_open' && tokens[i].tag === 'h2'); i++) {
    if (tokens[i].type !== 'bullet_list_open') continue;
    let end = i + 1;
    let depth = 1;
    for (; end < tokens.length; end++) {
      if (tokens[end].type === 'bullet_list_open') depth++;
      if (tokens[end].type === 'bullet_list_close') depth--;
      if (depth === 0) break;
    }
    const links = tokens.slice(i, end).flatMap((token) => token.children || []).filter((token) => token.type === 'link_open');
    if (links.length && links.every((token) => token.attrGet('href').startsWith('#'))) tokens.splice(i, end - i + 1);
  }
  const body = md.renderer.render(tokens, md.options, {});
  const split = body.indexOf('<h2');
  const title = body.match(/<h1[^>]*>([\s\S]*?)<\/h1>/)[1].replace(/<[^>]+>/g, '');
  const navigation = pages.map(([, destination, name]) => `<li><a href="${destination}"${destination === output ? ' aria-current="page"' : ''}>${escape(name)}</a></li>`).join('\n');
  const toc = headings.map(({ id, text }) => `<li><a href="#${id}">${escape(text)}</a></li>`).join('\n');
  const values = {
    title: escape(title), description: escape(description), output, navigation, toc, body,
    source, version, label: escape(label),
    intro: split < 0 ? body : body.slice(0, split),
    content: split < 0 ? '' : body.slice(split),
    breadcrumb: output === 'documentation.html' ? '' : ` / ${escape(label)}`,
  };
  const html = template.replace(/\{\{(\w+)\}\}/g, (_, key) => {
    if (!(key in values)) throw new Error(`Unknown template value: ${key}`);
    return values[key];
  });
  await writeFile(path.join(root, 'site', output), html);
}
console.log(`Built ${pages.length} documentation pages for fleet-router ${version}.`);
const homePath = path.join(root, 'site/index.html');
const home = await readFile(homePath, 'utf8');
await writeFile(homePath, home.replace(/(<code data-release-version>)v[^<]+(<\/code>)/, `$1v${version}$2`));
await copyFile(path.join(root, 'deploy/frozen-ship.config.json'), path.join(root, 'site/frozen-ship.config.json'));
