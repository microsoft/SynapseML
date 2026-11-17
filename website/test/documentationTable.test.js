const assert = require("node:assert/strict");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const {createRequire} = require("node:module");
const React = require("react");
const {renderToStaticMarkup} = require("react-dom/server");

const requireFromDocusaurus = createRequire(
  require.resolve("@docusaurus/core/package.json"),
);
const {transformFileSync} = requireFromDocusaurus("@babel/core");
const {code} = transformFileSync(
  path.join(__dirname, "../src/theme/DocumentationTable/index.js"),
  {
    babelrc: false,
    configFile: false,
    presets: [requireFromDocusaurus.resolve("@babel/preset-react")],
    plugins: [
      requireFromDocusaurus.resolve("@babel/plugin-transform-modules-commonjs"),
    ],
  },
);
const componentExports = {};
vm.runInNewContext(code, {
  exports: componentExports,
  require(name) {
    if (name === "react") return React;
    if (name === "@docusaurus/useDocusaurusContext") {
      return () => ({siteConfig: {customFields: {version: "1.1.3"}}});
    }
    throw new Error(`Unexpected component import: ${name}`);
  },
});

const props = {
  className: "Example",
  py: "synapse.ml.example.html#module-synapse.ml.example.Example",
  scala: "com/microsoft/azure/synapse/ml/example/Example.html",
  sourceLink: "https://example.com/Example.scala",
};

function links(extraProps = {}) {
  const html = renderToStaticMarkup(
    React.createElement(componentExports.default, {...props, ...extraProps}),
  );
  return [...html.matchAll(/href="([^"]+)"/g)].map(match => match[1]);
}

test("API tables preserve the site documentation version by default", () => {
  assert.deepEqual(links(), [
    `https://mmlspark.blob.core.windows.net/docs/1.1.3/pyspark/${props.py}`,
    `https://mmlspark.blob.core.windows.net/docs/1.1.3/scala/${props.scala}`,
    props.sourceLink,
  ]);
});

test("a preview API table overrides both language links without changing other tables", () => {
  const docsVersion = "1.1.3-preview-SNAPSHOT";
  assert.deepEqual(links({docsVersion}), [
    `https://mmlspark.blob.core.windows.net/docs/${docsVersion}/pyspark/${props.py}`,
    `https://mmlspark.blob.core.windows.net/docs/${docsVersion}/scala/${props.scala}`,
    props.sourceLink,
  ]);
  assert.match(links()[0], /\/docs\/1\.1\.3\/pyspark\//);
});
