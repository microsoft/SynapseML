# Website

This website is built using [Docusaurus 2](https://v2.docusaurus.io/), a modern static website generator.

### Installation

```
$ yarn
```

### Local Development

```
$ yarn start
```

This command starts a local development server and opens up a browser window. Most changes are reflected live without having to restart the server.

### Build

```
$ yarn build
```

This command generates static content into the `build` directory and can be served using any static contents hosting service.

### Deployment

```
$ GIT_USER=<Your GitHub username> USE_SSH=true yarn deploy
```

If you're using GitHub pages for hosting, this command is a convenient way to build the website and push to the `gh-pages` branch.


### Adding a new versioned docs section

To add a version to the docs like `0.9.5` from the `website` directory
```
cd ../
sbt convertNotebooks
cd website
yarn run docusaurus docs:version 0.9.5`
````
