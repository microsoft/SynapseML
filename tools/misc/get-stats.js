// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

let az = require("azure-storage");

let httpsGet = require("https").get;
let get = url => new Promise((resolve, reject) =>
  httpsGet(url, r => {
    r.setEncoding("utf8");
    let str = "";
    r.on("data", d => str += d);
    r.on("end", () => resolve(JSON.parse(str)));
  }));

let ghAPI = require("github");
let repo = {owner: "Azure", repo: "mmlspark"};
let repo_pg = Object.assign({}, repo, {per_page: 100});

module.exports = ctx => {
  let blob = az.createBlobService(process.env.MMLSparkStorage);
  let gh   = new ghAPI();
  let timeStamp = new Date().toISOString()
                        .replace(/:[0-9][0-9]\.[0-9]+Z/, "").replace(/T/, " ");

  let getPaged = (state, combine) => r =>
    !gh.hasNextPage(r) ? Promise.resolve(combine(state, r.data))
    : gh.getNextPage(r).then(getPaged(combine(state, r.data), combine));
  let getAllContributors = getPaged([], (st,d) => st.concat(d.map(x => x.login)))
  let getAllIssues = getPaged([[],[]], (st,d) =>
    [st[0].concat(d.filter(x => !("pull_request" in x)).map(x => "#"+x.number)),
     st[1].concat(d.filter(x =>  ("pull_request" in x)).map(x => "#"+x.number))])

  // To initialize the append blob (delete the rest when doing this to avoid races):
  // blob.createAppendBlobFromText("stats", "log", `initial text`,
  //   (err) => { if (err) ctx.done(err); else { ctx.log(">>> DONE!"); ctx.done(); }});

  let text = ([repo, [issues, pulls], contribs, docker, dtags]) => `
    |=========================
    |${timeStamp}
    |github:
    |  stars: ${       repo.stargazers_count }
    |  watchers: ${    repo.subscribers_count}
    |  forks: ${       repo.forks_count      }
    |  issues_pulls: ${repo.open_issues_count}
    |  contributors: ${contribs.length} => ${contribs.join(", ")}
    |  pulls: ${       pulls   .length} => ${pulls   .join(", ")}
    |  issues: ${      issues  .length} => ${issues  .join(", ")}
    |docker:
    |  pulls: ${docker.pull_count}
    |  stars: ${docker.star_count}
    |  tags: ${dtags.length} => ${dtags.map(x => x.name).join(", ")}
    `.replace(/(?:^|\r?(\n))\s*(?:\||$)/g, "$1");

  Promise.all([gh.repos.get(repo).then(r => Promise.resolve(r.data)),
               gh.issues.getForRepo(repo_pg).then(getAllIssues),
               gh.repos.getContributors(repo_pg).then(getAllContributors),
               get("https://hub.docker.com/v2/repositories/microsoft/mmlspark/"),
               get("https://hub.docker.com/v2/repositories/microsoft/mmlspark/tags/")
                 .then(r => Promise.resolve(r.results))])
    .then(r => blob.appendFromText("stats", "log", text(r),
                                   (err) => err ? ctx.done(err) : ctx.done()));

};
