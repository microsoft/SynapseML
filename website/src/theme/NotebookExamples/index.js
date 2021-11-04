import React from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

import NotebookCard from "@theme/NotebookCard";


function NotebookExamples() {
  const context = useDocusaurusContext();
  const types = context.siteConfig.customFields.examples;

  return types.map((data) => (
    <NotebookCard url={data}/>
  ))

}

export default NotebookExamples;
