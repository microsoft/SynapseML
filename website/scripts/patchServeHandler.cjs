const fs = require('node:fs');
const path = require('node:path');

const SUPPORTED_VERSION = '6.1.7';
const OLD_IMPORT = "const minimatch = require('minimatch');";
const NEW_IMPORT = "const {minimatch} = require('minimatch');";

function patchServeHandler(websiteDir = path.resolve(__dirname, '..')) {
  const packageDir = path.join(websiteDir, 'node_modules', 'serve-handler');
  const packageJsonPath = path.join(packageDir, 'package.json');
  const sourcePath = path.join(packageDir, 'src', 'index.js');
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));

  if (packageJson.name !== 'serve-handler' || packageJson.version !== SUPPORTED_VERSION) {
    throw new Error(
      `Unsupported serve-handler package ${packageJson.name}@${packageJson.version}; ` +
        `expected serve-handler@${SUPPORTED_VERSION}`,
    );
  }

  const source = fs.readFileSync(sourcePath, 'utf8');
  const oldImportCount = source.split(OLD_IMPORT).length - 1;
  const newImportCount = source.split(NEW_IMPORT).length - 1;
  if (oldImportCount === 0 && newImportCount === 1) {
    return false;
  }
  if (oldImportCount !== 1 || newImportCount !== 0) {
    throw new Error('serve-handler minimatch import no longer matches the reviewed source');
  }

  fs.writeFileSync(sourcePath, source.replace(OLD_IMPORT, NEW_IMPORT));
  return true;
}

if (require.main === module) {
  patchServeHandler();
}

module.exports = patchServeHandler;
