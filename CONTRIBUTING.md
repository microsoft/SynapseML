## Interested in contributing to SynapseML?  We're excited to work with you.

### You can contribute in many ways:

-   Use the library and give feedback: report bugs, request features.
-   Add sample Jupyter notebooks, Python or Scala code examples, documentation
    pages.
-   Fix bugs and issues.
-   Add new features, such as data transformations or machine learning algorithms.
-   Review pull requests from other contributors.

### How to contribute?

You can give feedback, report bugs and request new features anytime by opening
an issue.  Also, you can up-vote or comment on existing issues.

If you want to add code, examples or documentation to the repository, follow
this process:

### Which branch should I target?

Most contributions target `master`.

This repository also maintains ports of the library to newer Spark versions on
long-lived branches (`spark4.0`, `spark4.1`).  Target one of those only when the
change exists *because of* that Spark version — for example, replacing an API
that behaves differently there.  Ordinary bug fixes and new features belong on
`master` and reach the port branches when `master` is merged into them.

If a fix applies everywhere, land it on `master` first so the port branches
inherit it on the next sync.  Fixing the same thing separately on each branch
creates a conflict that someone then has to resolve by hand.

Each port branch carries an `AGENTS_<branch>.md` describing what diverges there
and why.  Read it before changing anything on that branch — several of the
differences look like mistakes until you know the reason for them, and a
"cleanup" that reverts one tends to break the build in a way that is not obvious
from the diff.  Repository-wide guidance for automated coding agents is in
[AGENTS.md](AGENTS.md).

#### Propose a contribution

-   Preferably, get started by tackling existing issues to get yourself acquainted
    with the library source and the process.
-   Open an issue, or comment on an existing issue to discuss your contribution
    and design, to ensure your contribution is a good fit and doesn't duplicate
    on-going work.
-   Any algorithm you're planning to contribute should be well known and accepted
    for production use, and backed by research papers.
-   Algorithms should be highly scalable and suitable for very large datasets.
-   All contributions need to comply with the MIT License.  Contributors external
    to Microsoft need to sign CLA.

#### Implement your contribution

-   Fork the SynapseML repository.
-   Implement your algorithm in Scala, using our wrapper generation mechanism to
    produce PySpark bindings.
-   Use SparkML `PipelineStage`s so your algorithm can be used as a part of
    pipeline.
-   Implement model saving and loading by extending SparkML `MLReadable`.
-   Use good Scala style.
-   Binary dependencies should be on Maven Central.
-   See this [pull request](https://github.com/microsoft/SynapseML/pull/1282) for an
    example contribution.

#### Implement tests

-   Set up build environment using the [developer guide](https://microsoft.github.io/SynapseML/docs/Reference/Developer%20Setup/)
-   Test your code locally.
-   Add tests using ScalaTests — unit tests are required.
-   A sample notebook is required as an end-to-end test.

#### Implement documentation

-   Add a [sample Jupyter notebook](docs/) that shows the intended use
    case of your algorithm, with instructions in step-by-step manner.  (The same
    notebook could be used for testing the code.)
-   Add in-line ScalaDoc comments to your source code, to generate the [API
    reference documentation](https://mmlspark.blob.core.windows.net/docs/pyspark/)

#### Open a pull request

-   In most cases, you should squash your commits into one.
-   Open a pull request, and link it to the discussion issue you created earlier.
-   A SynapseML core team member will trigger a build to test your changes.
-   Fix any build failures.  (The pull request will have comments from the build
    with useful links.)
-   Wait for code reviews from core team members and others.
-   Fix issues found in code review and re-iterate.

#### Build and check-in

-   Wait for a core team member to merge your code in.
-   Your feature will be available through a Docker image and script installation
    in the next release, which typically happens around once a month.  You can try
    out your features sooner by using build artifacts for the version that has
    your changes merged in (such versions end with a `.devN`).

If in doubt about how to do something, see how it was done in existing code or
pull requests, and don't hesitate to ask.
