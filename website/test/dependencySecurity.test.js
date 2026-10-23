const assert = require('node:assert/strict');
const test = require('node:test');
const {createRequire} = require('node:module');
const {pathToFileURL} = require('node:url');

const requireFromDocusaurus = createRequire(
  require.resolve('@docusaurus/core/package.json'),
);
const requireFromMath = createRequire(require.resolve('rehype-katex'));

test('shell quoting rejects line terminators after a comment', () => {
  const {parse, quote} = requireFromDocusaurus('shell-quote');
  for (const terminator of ['\n', '\r', '\u2028', '\u2029']) {
    assert.throws(
      () => quote(['echo', {comment: 'note'}, `first${terminator}second`]),
      TypeError,
    );
  }
  assert.deepEqual(parse(quote(['echo', 'first\nsecond'])), [
    'echo',
    'first\nsecond',
  ]);
});

test('math rendering ignores inherited trust while retaining ordinary equations', () => {
  const katex = requireFromMath('katex');
  const options = Object.create({trust: true});
  const html = katex.renderToString(
    '\\href{https://example.com/}{untrusted}',
    options,
  );
  assert.doesNotMatch(html, /<a\b/);
  assert.match(katex.renderToString('x^2 + y^2 = z^2'), /class="katex"/);
  assert.match(
    katex.renderToString('\\href{https://example.com/}{trusted}', {trust: true}),
    /<a\b/,
  );
});

test('CSS optimization preserves selectors through the Docusaurus preset', async () => {
  const postcss = requireFromDocusaurus('postcss');
  const cssnano = requireFromDocusaurus('cssnano');
  const preset = requireFromDocusaurus('@docusaurus/cssnano-preset');
  const result = await postcss([cssnano({preset})]).process(
    '.card:is(.active, [data-state="open"]) > a:hover { color: #ffffff; margin: 0px 0px 0px 0px; }',
    {from: undefined},
  );
  assert.match(result.css, /\.card:is\(\.active,\s*\[data-state=open\]\)>a:hover/);
  assert.match(result.css, /color:#fff/);
  assert.match(result.css, /margin:0/);
  assert.deepEqual(result.warnings(), []);
});

test('Docusaurus worker pool retains worker data and ignores inherited run options', async () => {
  const {default: Tinypool} = await import(
    pathToFileURL(requireFromDocusaurus.resolve('tinypool')).href
  );
  const worker = `
    import {workerData} from 'node:worker_threads';
    export default function(task) {
      return {
        task,
        params: workerData[1].params,
        workerId: process.__tinypool_state__.workerId,
      };
    }
  `;
  const pool = new Tinypool({
    filename: `data:text/javascript,${encodeURIComponent(worker)}`,
    minThreads: 1,
    maxThreads: 1,
    concurrentTasksPerWorker: 1,
    runtime: 'worker_threads',
    isolateWorkers: false,
    workerData: {params: {baseUrl: '/SynapseML/'}},
  });
  try {
    const result = await pool.run(
      {pathnames: ['/SynapseML/docs/']},
      Object.create({name: 'notAnExport'}),
    );
    assert.deepEqual(result.task, {pathnames: ['/SynapseML/docs/']});
    assert.deepEqual(result.params, {baseUrl: '/SynapseML/'});
    assert.ok(result.workerId > 0);
  } finally {
    await pool.destroy();
  }
});
