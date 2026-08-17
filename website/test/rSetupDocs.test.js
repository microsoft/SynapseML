const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const repoRoot = path.resolve(__dirname, '..', '..');
const componentNames = [
  'core',
  'cognitive',
  'deep-learning',
  'lightgbm',
  'opencv',
  'vw',
];

function rSetupGuides() {
  const guides = [path.join(repoRoot, 'docs', 'Reference', 'R Setup.md')];
  const versionsRoot = path.join(repoRoot, 'website', 'versioned_docs');

  for (const directory of fs.readdirSync(versionsRoot)) {
    if (directory.startsWith('version-')) {
      guides.push(path.join(versionsRoot, directory, 'Reference', 'R Setup.md'));
    }
  }

  return guides;
}

for (const guide of rSetupGuides()) {
  test(`R archive and resolver versions agree in ${path.relative(repoRoot, guide)}`, () => {
    const markdown = fs.readFileSync(guide, 'utf8');
    const coordinateVersions = [
      ...markdown.matchAll(/com\.microsoft\.azure:synapseml_2\.12:([0-9.]+)/g),
    ].map((match) => match[1]);
    const uniqueVersions = [...new Set(coordinateVersions)];

    assert.equal(uniqueVersions.length, 1, 'expected one Maven coordinate version');
    const [version] = uniqueVersions;
    const versionDirectory = guide.match(/version-([0-9.]+)[\\/]Reference/);
    if (versionDirectory) {
      assert.equal(version, versionDirectory[1]);
    }

    const escapedVersion = version.replaceAll('.', '\\.');
    for (const component of componentNames) {
      assert.match(
        markdown,
        new RegExp(
          `https://mmlspark\\.blob\\.core\\.windows\\.net/rrr/` +
            `synapseml-${component}-${escapedVersion}\\.zip`,
        ),
      );
    }

    const archiveUrls = markdown.match(
      /https:\/\/mmlspark\.blob\.core\.windows\.net\/rrr\/[^"\s)]+\.zip/g,
    );
    assert.equal(archiveUrls?.length, componentNames.length);
    assert.doesNotMatch(
      markdown,
      /mmlspark\.blob\.core\.windows\.net\/rrr\/synapseml-[0-9.]+\.zip/,
      'combined R archives are not published',
    );
    assert.match(markdown, /config\$sparklyr\.shell\.repositories/);
    assert.match(markdown, /extensions = character\(\)/);
    assert.doesNotMatch(markdown, /mmlspark\.azureedge\.net/);
  });
}
