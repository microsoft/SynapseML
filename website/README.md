# Website

This website is built using [Docusaurus 3](https://docusaurus.io/), a modern static website generator.

### Installation

```bash
npm ci
```

### Local Development

```bash
npm start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```bash
npm run build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

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
