const path = require("path");
const fs = require("fs");
const { parseMarkdownString } = require("@docusaurus/utils");

function notebooks(type) {
  return all_notebooks_for_type(type).filter((c) => c.status != "deprecated");
}

function all_notebooks_for_type(type) {
  let notebooks = [];
  let dir = path.join(__dirname, `../../../docs/notebooks/${type}`);
  fs.readdirSync(dir).forEach(function (file) {
    if (!/about\.mdx?/.test(file)) {
      let name = file.split(".").slice(0, -1).join(".");
      let data = fs.readFileSync(path.join(dir, file));
      const { frontMatter } = parseMarkdownString(data);
      frontMatter["name"] = name;
      notebooks.push(frontMatter);
    }
  });
  return notebooks;
}

function all_examples() {
  let ex_links = [
    `Azure Search/AzureSearchIndex - Met Artworks.md`,
    `Classification/Classification - Adult Census.md`,
    `Cognitive Services/Cognitive Services - Overview.md`,
    `Conditional KNN/ConditionalKNN - Exploring Art Across Cultures.md`,
    `CyberML/CyberML - Anomalous Access Detection.md`,
    `LightGBM/LightGBM - Overview.md`,
    `Model Interpretation/ModelInterpretation - Snow Leopard Detection.md`,
    `Vowpal Wabbit/Vowpal Wabbit - Overview.md`,
  ];
  let examples = [];
  let dir = path.join(__dirname, `../../../docs/notebooks`);
  ex_links.forEach(function (url) {
    let url_path = url.substr(0, -3);
    let name = url_path.split("/").slice(-1)[0];
    let data = fs.readFileSync(path.join(dir, url));
    const { frontMatter } = parseMarkdownString(data);
    frontMatter["url_path"] = url_path;
    frontMatter["name"] = name;
    examples.push(frontMatter);
  });
  return examples;
}

function all_notebooks() {
  let notebooks = [];
  let dir = path.join(__dirname, `../../../docs/notebooks`);
  fs.readdirSync(dir).forEach(function (sub_dir) {
    if (!/about\.mdx?/.test(sub_dir)) {
      fs.readdirSync(path.join(dir, sub_dir)).forEach(function (file) {
        if (!/about\.mdx?/.test(file)) {
          let name = file.split(".").slice(0, -1).join(".");
          let data = fs.readFileSync(path.join(dir, sub_dir, file));
          const { frontMatter } = parseMarkdownString(data);
          frontMatter["name"] = name;
          notebooks.push(frontMatter);
        }
      });
    }
  });
  return notebooks;
}

function listNotebookPaths(type) {
  let paths = [];
  if (
    [
      "HTTP",
      "Spark Serving",
      "LightGBM",
      "Vowpal Wabbit",
      "Model Interpretation",
    ].includes(type)
  ) {
    paths.push(`notebooks/${type}/about`);
  }

  let notebooks = all_notebooks_for_type(type);

  notebooks
    .filter((c) => c.status != "deprecated")
    .forEach(function (info) {
      paths.push(`notebooks/${type}/${info.name}`);
    });

  let deprecatedPaths = notebooks
    .filter((c) => c.status == "deprecated")
    .map((c) => `notebooks/${type}/${c.name}`);

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
  notebooks: notebooks,
  all_examples: all_examples,
  listNotebookPaths: listNotebookPaths,
};
