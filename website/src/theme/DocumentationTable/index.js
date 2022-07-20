import React from "react";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";

function DocTable(props) {
  const { className, py, scala, csharp, sourceLink } = props;
  const context = useDocusaurusContext();
  const version = context.siteConfig.customFields.version;
  let pyLink = `https://mmlspark.blob.core.windows.net/docs/${version}/pyspark/${py}`;
  let scalaLink = `https://mmlspark.blob.core.windows.net/docs/${version}/scala/${scala}`;
  let csharpLink = `https://mmlspark.blob.core.windows.net/docs/${version}/dotnet/${csharp}`;

  return (
    <table>
      <tbody>
        <tr>
          <td>
            <strong>Python API: </strong>
            <a href={pyLink}>
              {className}
            </a>
          </td>
          <td>
            <strong>Scala API: </strong>
            <a href={scalaLink}>
              {className}
            </a>
          </td>
          <td>
            <strong>.NET API: </strong>
            <a href={csharpLink}>
              {className}
            </a>
          </td>
          <td>
            <strong>Source: </strong>
            <a href={sourceLink}>
              {className}
            </a>
          </td>
        </tr>
      </tbody>
    </table>
  );
}

export default DocTable;
