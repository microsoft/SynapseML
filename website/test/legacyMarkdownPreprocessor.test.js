const assert = require('node:assert/strict');
const test = require('node:test');

const preprocessLegacyMarkdown = require('../legacyMarkdownPreprocessor');

test('normalizes legacy MDX constructs outside code fences', () => {
  const input = [
    'Visit <http://localhost:8888/>.',
    'Replace <PATH-DOTNET_WORKER_DIR> before continuing.',
    '    setx /M DOTNET_WORKER_DIR <PATH-DOTNET-WORKER-DIR>',
    '    <train classifier>',
    '{"name": "capital-gain", "numSplits": 20}',
    '| text | {"name": "English", "confidenceScore": 0.99} |',
    'Explain {number features to explain} columns.',
    '[Slicing](#slicing)',
    '[Setup](#environment-setup----reinstall-horovod-based-on-new-version-of-pytorch)',
  ].join('\n');

  assert.equal(
    preprocessLegacyMarkdown({fileContent: input}),
    [
      'Visit [http://localhost:8888/](http://localhost:8888/).',
      'Replace `PATH-DOTNET_WORKER_DIR` before continuing.',
      '    setx /M DOTNET_WORKER_DIR PATH-DOTNET-WORKER-DIR',
      '    // train classifier',
      '`{"name": "capital-gain", "numSplits": 20}`',
      '| text | `{"name": "English", "confidenceScore": 0.99}` |',
      'Explain `{number features to explain}` columns.',
      '[Slicing](#model-slicing)',
      '[Setup](#environment-setup-on-databricks)',
    ].join('\n'),
  );
});

test('does not alter fenced code blocks', () => {
  const input = [
    '```python',
    '{"name": "capital-gain", "numSplits": 20}',
    '<http://localhost:8888/>',
    '```',
    '~~~bash',
    'export DOTNET_WORKER_DIR=<PATH-DOTNET-WORKER-DIR>',
    '~~~',
  ].join('\n');

  assert.equal(preprocessLegacyMarkdown({fileContent: input}), input);
});
