<!-- 🚨 We recommend pull requests be filed from a non-master branch on a repository fork (e.g. <username>:fix-xxx). 🚨 -->

## Related Issues/PRs

<!--
Please reference any related feature requests, issues, or PRs here. For example, `#123`. To automatically close the referenced items when this PR is merged, please use a closing keyword (close, fix, or resolve). For example, `Close #123`. See https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue for more information.
-->
#xxx

## What changes are proposed in this pull request?

_Briefly describe the changes included in this Pull Request._

## How is this patch tested?

<!--
If you're unsure about what to test, where to add tests, or how to run tests, please feel free to ask. We'd be happy to help.
-->
- [ ] I have written tests (not required for typo or doc fix) and confirmed the proposed feature/bug-fix/change works.

## Does this PR change any dependencies?

- [ ] No. You can skip this section.
- [ ] Yes. Make sure the dependencies are resolved correctly, and list changes here.

## Does this PR add a new feature? If so, have you added samples on website?

- [ ] No. You can skip this section.
- [ ] Yes. Make sure you have added samples following below steps.

1. Find the corresponding markdown file for your new feature in `website/docs/documentation` folder.
   Make sure you choose the correct class `estimators/transformers` and namespace.
2. Follow the pattern in markdown file and add another section for your new API, including pyspark, scala (and .NET potentially) samples.
3. Make sure the `DocTable` points to correct API link.
4. Navigate to website folder, and run `yarn run start` to make sure the website renders correctly.
5. Don't forget to add `<!--pytest-codeblocks:cont-->` before each python code blocks to enable auto-tests for python samples.
6. Make sure the `WebsiteSamplesTests` job pass in the pipeline.
