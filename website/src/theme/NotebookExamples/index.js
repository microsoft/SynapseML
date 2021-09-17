import React from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

import NotebookCard from "@theme/NotebookCard";


function NotebookExamples() {
  const context = useDocusaurusContext();
  const types = context.siteConfig.customFields.examples;

  return types.map((data) => (
    <NotebookCard url={data}/>
  ))

}

export default NotebookExamples;
