const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const packageJson = require('../package.json');
const packageLock = require('../package-lock.json');

const websiteDir = path.resolve(__dirname, '..');
const vendoredBraceExpansion = 'vendor/brace-expansion-5.0.8.tgz';

function packageEntries(packageName) {
  return Object.entries(packageLock.packages).filter(([packagePath]) => {
    return (
      packagePath === `node_modules/${packageName}` ||
      packagePath.endsWith(`/node_modules/${packageName}`)
    );
  });
}

test('keeps Component Governance dependency remediations pinned', () => {
  const braceExpansion = packageLock.packages['node_modules/brace-expansion'];

  assert.equal(braceExpansion.version, '5.0.8');
  assert.equal(braceExpansion.resolved, `file:${vendoredBraceExpansion}`);
  assert.equal(packageLock.packages['node_modules/minimatch'].version, '10.2.5');
  assert.equal(packageLock.packages['node_modules/update-notifier'].version, '7.3.1');
  assert.equal(packageLock.packages['node_modules/webpack-dev-server'].version, '6.0.0');
  assert.deepEqual(packageEntries('keyv'), []);
  assert.deepEqual(packageEntries('string_decoder'), []);
});

test('verifies the vendored brace-expansion archive integrity', () => {
  const braceExpansion = packageLock.packages['node_modules/brace-expansion'];
  const [algorithm, expectedDigest] = braceExpansion.integrity.split('-', 2);
  const archive = fs.readFileSync(path.join(websiteDir, vendoredBraceExpansion));
  const actualDigest = crypto.createHash(algorithm).update(archive).digest('base64');

  assert.equal(actualDigest, expectedDigest);
});

test('requires strong integrity hashes for locked registry packages', () => {
  const weakIntegrityEntries = Object.entries(packageLock.packages)
    .filter(([, packageData]) => packageData.integrity?.startsWith('sha1-'))
    .map(([packagePath]) => packagePath);

  assert.deepEqual(weakIntegrityEntries, []);
});

test('denies nonessential dependency install scripts', () => {
  assert.deepEqual(packageJson.allowScripts, {
    'brace-expansion': false,
    'core-js': false,
  });
});

test('loads the overridden glob dependencies through their reviewed APIs', () => {
  const {expand} = require('brace-expansion');
  const {minimatch} = require('minimatch');
  const serveHandler = require('serve-handler');

  assert.deepEqual(expand('{docs,api}/*.html'), ['docs/*.html', 'api/*.html']);
  assert.equal(minimatch('docs/index.html', '**/*.html'), true);
  assert.equal(typeof serveHandler, 'function');
});
