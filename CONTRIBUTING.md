## Interested in contributing to Microsoft Machine Learning Library for Apache Spark?  We're excited to work with you.

### You can contribute in many ways:

* Use the library and give feedback: report bugs, request features.
* Add example Jupyter notebooks.
* Add Python or Scala code examples.
* Add or improve documentation pages.
* Fix bugs and issue.
* Add new features, such as data transformations or machine learning algorithms.
* Review pull requests from other contributors.

### How to contribute?

You can give feedback, report bugs and request new features anytime by opening an issue. 
Also, you can up-vote and comment on existing issues.

If you want to add code, examples or documentation to the repository, follow this process:

#### Propose a contribution

* Preferably, get started by tackling existing issues to get yourself acquainted with the 
library source and the process.
* Open an issue, or comment on an existing issue to discuss your contribution and design, 
to ensure your contribution is a good fit and doesn't duplicate on-going work.
* Any algorithm you're planning to contribute should be well known and accepted for 
production use, and backed by research papers.
* Algorithms should be highly scalable and suitable for very large datasets.
* All contributions need to comply to MIT License. Contributors external to Microsoft need 
to sign CLA.
 
#### Implement your contribution

* Fork the MMLSpark repository.
* Implement your algorithm in Scala, using our wrapper generation mechanism to produce 
PySpark bindings.
* Use SparkML PipelineStages so your algorithm can be used as a part of pipeline.
* For parameters use MMLParams.
* Implement model saving and loading by extending SparkML MLReadable.
* Use clean ScalaStyle.
* Upload any binary dependencies to Maven Central.
* For an example contribution, see code in this 
[pull request](https://github.com/Azure/mmlspark/pull/22)

#### Implement tests

* Set up build environment. Use Ubuntu computer or VM, and install environment using 
[runme script](runme)
* Add tests using ScalaTests.
* Unit tests are required.
* Sample notebook is required as an end-to-end test.
* Test your code locally.

#### Implement documentation

* Add a [sample Jupyter notebook](notebooks/samples) that shows the intended use case of your 
algorithm, with instructions in step-by-step manner.
* Add in-line ScalaDoc comments to your source code, which are then used to generate 
[API reference documentation](https://mmlspark.azureedge.net/docs/pyspark/)

#### Open pull request

* Squash your commits into one.
* Open pull request, and link it to the issue you created in the first step.
* A MMLSpark core team member will trigger a build to validate your changes.
* Fix any build failures. At the end of pull request comments there will be a comment from 
BuildBot listing build failures.
* Wait for code reviews from core team members, and others.
* Fix issues found in code review.

#### Build and check-in

* Wait for a core team member to merge your code in.
* Your feature will be available through Docker image and script installation in next official 
release, typically once a month. You can try out your features sooner by using build artifacts 
linked by BuildBot.

If in doubt about how to do something, look how it was done in existing code or pull requests.
