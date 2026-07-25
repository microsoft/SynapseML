function normalizeLegacyMarkdownLine(line) {
  return line
    .replace(/<(https?:\/\/[^>\s]+)>/g, '[$1]($1)')
    .replaceAll('<PATH-DOTNET_WORKER_DIR>', '`PATH-DOTNET_WORKER_DIR`')
    .replaceAll('<PATH-DOTNET-WORKER-DIR>', 'PATH-DOTNET-WORKER-DIR')
    .replaceAll(
      '{number features to explain}',
      '`{number features to explain}`',
    )
    .replaceAll(
      '#environment-setup----reinstall-horovod-based-on-new-version-of-pytorch',
      '#environment-setup-on-databricks',
    )
    .replaceAll('](#slicing)', '](#model-slicing)')
    .replace(/^(\s*)<train classifier>$/, '$1// train classifier')
    .replace(/^(\s*)(\{"name": .+\})$/, '$1`$2`')
    .replace(
      /^(\|(?:[^|]*\|)+\s*)(\[?\{.*\}\]?)(\s*\|)$/,
      '$1`$2`$3',
    );
}

function findFence(line) {
  const match = line.match(/^\s*(`{3,}|~{3,})/);
  if (!match) {
    return undefined;
  }
  return {character: match[1][0], length: match[1].length, token: match[1]};
}

function preprocessLegacyMarkdown({fileContent}) {
  let openFence;

  return fileContent
    .split('\n')
    .map((line) => {
      const candidateFence = findFence(line);
      if (candidateFence) {
        if (!openFence) {
          openFence = candidateFence;
        } else if (
          candidateFence.character === openFence.character &&
          candidateFence.length >= openFence.length &&
          line.trim() === candidateFence.token
        ) {
          openFence = undefined;
        }
        return line;
      }

      return openFence ? line : normalizeLegacyMarkdownLine(line);
    })
    .join('\n');
}

module.exports = preprocessLegacyMarkdown;
