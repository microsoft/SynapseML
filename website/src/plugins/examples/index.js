const path = require("path");
const fs = require("fs");
const { parseMarkdownString } = require("@docusaurus/utils");

function examples(type) {
  return all_examples_for_type(type).filter((c) => c.status != "deprecated");
}

function all_examples_for_type(type) {
  let examples = [];
  let dir = path.join(__dirname, `../../../docs/examples/${type}`);
  fs.readdirSync(dir).forEach(function (file) {
    if (!/about\.mdx?/.test(file)) {
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
    `examples/AzureSearchIndex - Met Artworks.md`,
    `examples/classification/Classification - Adult Census.md`,
    `features/Cognitive Services - Overview.md`,
    `examples/ConditionalKNN - Exploring Art Across Cultures.md`,
    `examples/CyberML - Anomalous Access Detection.md`,
    `features/ONNX - Inference on Spark.md`,
    `features/lightgbm/LightGBM - Overview.md`,
    `features/model_interpretability/Model Interpretability - Snow Leopard Detection.md`,
    `features/vw/Vowpal Wabbit - Overview.md`,
  ];
  let examples = [];
  let dir = path.join(__dirname, `../../../docs`);
  ex_links.forEach(function (url) {
    let url_path = url.split(".").slice(0, -1).join(".");;
    let name = url_path.split("/").slice(-1)[0];
    let data = fs.readFileSync(path.join(dir, url));
    const { frontMatter } = parseMarkdownString(data);
    frontMatter["url_path"] = url_path;
    frontMatter["name"] = name;
    examples.push(frontMatter);
  });
  return examples;
}

function all_examples_original() {
  let examples = [];
  let dir = path.join(__dirname, `../../../docs/examples`);
  fs.readdirSync(dir).forEach(function (sub_dir) {
    if (!/about\.mdx?/.test(sub_dir)) {
      fs.readdirSync(path.join(dir, sub_dir)).forEach(function (file) {
        if (!/about\.mdx?/.test(file)) {
          let name = file.split(".").slice(0, -1).join(".");
          let data = fs.readFileSync(path.join(dir, sub_dir, file));
          const { frontMatter } = parseMarkdownString(data);
          frontMatter["name"] = name;
          examples.push(frontMatter);
        }
      });
    }
  });
  return examples;
}

function listExamplePaths(type) {
  let paths = [];
  let examples = all_examples_for_type(type);

  examples
    .filter((c) => c.status != "deprecated")
    .forEach(function (info) {
      paths.push(`examples/${type}/${info.name}`);
    });

  let deprecatedPaths = examples
    .filter((c) => c.status == "deprecated")
    .map((c) => `examples/${type}/${c.name}`);

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
  examples: examples,
  all_examples: all_examples,
  listExamplePaths: listExamplePaths,
};
