const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const patchServeHandler = require('../scripts/patchServeHandler.cjs');

const OLD_IMPORT = "const minimatch = require('minimatch');";
const NEW_IMPORT = "const {minimatch} = require('minimatch');";

function createFixture(version = '6.1.7', source = OLD_IMPORT, name = 'serve-handler') {
  const websiteDir = fs.mkdtempSync(path.join(os.tmpdir(), 'serve-handler-patch-'));
  const packageDir = path.join(websiteDir, 'node_modules', 'serve-handler');
  fs.mkdirSync(path.join(packageDir, 'src'), {recursive: true});
  fs.writeFileSync(
    path.join(packageDir, 'package.json'),
    JSON.stringify({name, version}),
  );
  fs.writeFileSync(path.join(packageDir, 'src', 'index.js'), source);
  return {websiteDir, packageDir};
}

test('patches serve-handler for the minimatch 10 named export', (context) => {
  const {websiteDir, packageDir} = createFixture();
  context.after(() => fs.rmSync(websiteDir, {recursive: true, force: true}));

  assert.equal(patchServeHandler(websiteDir), true);
  assert.equal(
    fs.readFileSync(path.join(packageDir, 'src', 'index.js'), 'utf8'),
    NEW_IMPORT,
  );
  assert.equal(patchServeHandler(websiteDir), false);
});

test('rejects an unreviewed serve-handler version', (context) => {
  const {websiteDir} = createFixture('6.1.8');
  context.after(() => fs.rmSync(websiteDir, {recursive: true, force: true}));

  assert.throws(
    () => patchServeHandler(websiteDir),
    /Unsupported serve-handler package serve-handler@6\.1\.8/,
  );
});

test('rejects an unexpected package name', (context) => {
  const {websiteDir} = createFixture('6.1.7', OLD_IMPORT, 'other-package');
  context.after(() => fs.rmSync(websiteDir, {recursive: true, force: true}));

  assert.throws(
    () => patchServeHandler(websiteDir),
    /Unsupported serve-handler package other-package@6\.1\.7/,
  );
});

test('rejects an unexpected serve-handler source shape', (context) => {
  const {websiteDir} = createFixture('6.1.7', 'const other = true;');
  context.after(() => fs.rmSync(websiteDir, {recursive: true, force: true}));

  assert.throws(
    () => patchServeHandler(websiteDir),
    /minimatch import no longer matches the reviewed source/,
  );
});

test('rejects ambiguous serve-handler minimatch imports', (context) => {
  const {websiteDir} = createFixture('6.1.7', `${OLD_IMPORT}\n${NEW_IMPORT}`);
  context.after(() => fs.rmSync(websiteDir, {recursive: true, force: true}));

  assert.throws(
    () => patchServeHandler(websiteDir),
    /minimatch import no longer matches the reviewed source/,
  );
});
