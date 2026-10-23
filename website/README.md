# Website

This website is built using [Docusaurus 3](https://docusaurus.io/), a modern static website generator.

### Installation

```bash
npm ci
```

### Dependency security

Use `npm audit` to check the complete locked dependency tree. Security overrides
in `package.json` keep transitive dependencies on patched releases without
downgrading Docusaurus. Run `npm test` and `npm run build` after updating them;
the tests cover shell quoting, math rendering, CSS optimization, and worker-pool
compatibility. KaTeX's
stylesheet is bundled from the same package as its renderer.

`braces@3.0.3` remains affected by
[GHSA-vfj7-8cjw-p6xm](https://github.com/advisories/GHSA-vfj7-8cjw-p6xm):
deeply nested patterns can exhaust the stack. No patched release is published
as of 2026-10-23. Docusaurus uses it through file watching and glob matching.
Do not pass untrusted glob patterns to these build/development tools.
`npm audit` therefore still exits nonzero for this advisory and its dependent
packages; this is an unresolved upstream dependency, not a clean audit.

### Local Development

```bash
npm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```bash
npm run build
```

This command generates static content into the `build` directory and can be served using any static content hosting service.

### Deployment

```bash
GIT_USER=<Your GitHub username> USE_SSH=true npm run deploy
```

If you're using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.


### Adding a new versioned docs section

To add a version to the docs like `0.9.5` from the `website` directory
```bash
cd ../
sbt convertNotebooks
cd website
npm exec -- docusaurus docs:version 0.9.5
```
