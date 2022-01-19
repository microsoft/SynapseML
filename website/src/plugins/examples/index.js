const path = require("path");
const fs = require("fs");
const { parseMarkdownString } = require("@docusaurus/utils");

function examples(folder, type) {
  return all_examples_for_type(folder, type).filter(
    (c) => c.status != "deprecated"
  );
}

function all_examples_for_type(folder, type) {
  let examples = [];
  let dir = path.join(__dirname, `../../../docs/${folder}/${type}`);
  fs.readdirSync(dir).forEach(function (file) {
    if (file.endsWith(".md")) {
      let name = file.split(".").slice(0, -1).join(".");
      let data = fs.readFileSync(path.join(dir, file));
      const { frontMatter } = parseMarkdownString(data);
      frontMatter["name"] = name;
      examples.push(frontMatter);
    }
  });
  return examples;
}

function all_examples() {
  let ex_links = [
    `features/other/AzureSearchIndex - Met Artworks.md`,
    `features/classification/Classification - Adult Census.md`,
    `features/cognitive_services/CognitiveServices - Overview.md`,
    `features/geospatial_services/GeospatialServices - Overview.md`,
    `features/other/ConditionalKNN - Exploring Art Across Cultures.md`,
    `features/other/CyberML - Anomalous Access Detection.md`,
    `features/responsible_ai/DataBalanceAnalysis - Adult Census Income.md`,
    `features/responsible_ai/Interpretability - Image Explainers.md`,
    `features/onnx/ONNX - Inference on Spark.md`,
    `features/lightgbm/LightGBM - Overview.md`,
    `features/vw/Vowpal Wabbit - Overview.md`,
  ];
  let examples = [];
  let dir = path.join(__dirname, `../../../docs`);
  ex_links.forEach(function (url) {
    let url_path = url.split(".").slice(0, -1).join(".");
    let name = url_path.split("/").slice(-1)[0];
    let data = fs.readFileSync(path.join(dir, url));
    const { frontMatter } = parseMarkdownString(data);
    frontMatter["url_path"] = url_path;
    frontMatter["name"] = name;
    examples.push(frontMatter);
  });
  return examples;
}

function listExamplePaths(folder, type) {
  let paths = [];
  let examples = all_examples_for_type(folder, type);

  examples
    .filter((c) => c.status != "deprecated")
    .sort()
    .forEach(function (info) {
      paths.push(`${folder}/${type}/${info.name}`);
    });

  let deprecatedPaths = examples
    .filter((c) => c.status == "deprecated")
    .map((c) => `${folder}/${type}/${c.name}`);

  if (deprecatedPaths.length > 0) {
    paths.push({
      type: "category",
      label: "Deprecated",
      items: deprecatedPaths,
    });
  }

  return paths;
}

module.exports = {
  all_examples: all_examples,
  listExamplePaths: listExamplePaths,
};
