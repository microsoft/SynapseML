const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const markdown = fs.readFileSync(
  path.join(
    __dirname, '..', '..', 'docs', 'Explore Algorithms', 'Other Algorithms',
    'Smart Adaptive Recommendations.md',
  ),
  'utf8',
);
const [, similarityTable, affinityTable, ...examples] =
  markdown.match(/(?:^\|.*\|\r?\n)+/gm);

function tableRows(table) {
  return table.trim().split('\n').slice(2).map((line) =>
    line.split('|').slice(2, -1).map((cell) => cell.replaceAll('**', '').trim()),
  );
}

function numericRows(table) {
  return tableRows(table).map((row) =>
    row.map((cell) => {
      const value = Number(cell);
      assert.ok(Number.isFinite(value), `Invalid numeric table cell: ${cell}`);
      return value;
    }),
  );
}

test('SAR example co-occurrences count distinct users in the affinity table', () => {
  const similarity = numericRows(similarityTable);
  const affinities = tableRows(affinityTable);

  for (let i = 0; i < similarity.length; i++) {
    for (let j = 0; j < similarity.length; j++) {
      const users = affinities.filter((row) => Boolean(row[i]) && Boolean(row[j]));
      assert.equal(
        similarity[i][j], users.length,
        `Co-occurrence of items ${i + 1} and ${j + 1}`,
      );
    }
  }
});

test('SAR example recommendation scores equal similarity times affinity', () => {
  const similarity = numericRows(similarityTable);
  assert.ok(examples.length > 0 && examples.length % 2 === 0);
  assert.deepEqual(
    numericRows(examples[0]).map((row) => row[0]),
    numericRows(affinityTable)[0].slice(0, similarity.length),
    'User 1 vector matches the affinity table',
  );

  for (let i = 0; i < examples.length; i += 2) {
    const affinity = numericRows(examples[i]).map((row) => row[0]);
    const scores = numericRows(examples[i + 1]).map((row) => row[0]);
    assert.equal(affinity.length, similarity.length);
    assert.equal(scores.length, similarity.length);
    const calculated = similarity.map((row) =>
      row.reduce((total, value, item) => total + value * affinity[item], 0),
    );
    assert.deepEqual(scores, calculated, `Recommendation example ${i / 2 + 1}`);
  }
});

test('SAR interpretability arithmetic matches the User 1 Item 4 score', () => {
  const similarity = numericRows(similarityTable)[3];
  const affinity = numericRows(examples[0]).map((row) => row[0]);
  const interpretation = markdown.split('#### Interpretability')[1];
  const expansion = interpretation.match(/^_= (.+)_\r?$/m);
  const sum = interpretation.match(/^\\\*= (\*\*[\d.]+[^\r\n]*)\r?$/m);
  assert.ok(expansion && sum, 'Expected the worked numeric expansion and sum');

  const factors = [...expansion[1].matchAll(/([\d.]+)\s+\\\*\s+([\d.]+)/g)]
    .map((match) => [Number(match[1]), Number(match[2])]);
  assert.deepEqual(factors, similarity.map((value, i) => [value, affinity[i]]));

  const [terms, total] = sum[1].replace(/[*\\]/g, '').split('=');
  const products = terms.split('+').map((term) => Number(term.trim()));
  assert.deepEqual(products, similarity.map((value, i) => value * affinity[i]));
  assert.equal(Number(total), products.reduce((value, term) => value + term, 0));
  assert.equal(Number(total), numericRows(examples[1])[3][0]);
});
