<!-- 🚨 We recommend pull requests be filed from a non-master branch on a repository fork (e.g. <username>:fix-xxx). 🚨 -->

## Related Issues/PRs

<!--
Please reference any related feature requests, issues, or PRs here. For example, `#123`. To automatically close the referenced items when this PR is merged, please use a closing keyword (close, fix, or resolve). For example, `Close #123`. See https://docs.github.com/en/issues/tracking-your-work-with-issues/linking-a-pull-request-to-an-issue for more information.
-->
#2460, #2444

# Streamline OpenAI Services Output Structure

## What changes are proposed in this pull request?

This PR streamlines the output structure for OpenAIPrompt and OpenAIEmbedding to make it more predictable and easier to use by ensuring the output column always contains the primary data (text/vector), with metadata returned in separate, explicitly-named columns.

### Key Changes:

**1. Output Column Simplification**
- **OpenAIPrompt**: `outputCol` now **always** returns the parsed response (string/array/struct based on `postProcessing`)
- **OpenAIEmbedding**: `outputCol` now **always** returns the embedding vector (VectorType)
- Removed dynamic struct wrapping that previously changed based on `returnUsage` and `store` flags
- Output type is now predictable and consistent

**2. Replaced `returnUsage` with `usageCol`**
- Removed `returnUsage` boolean parameter entirely from both OpenAIPrompt and OpenAIEmbedding
- Added `usageCol` parameter - users explicitly set column name to enable usage tracking
- Usage statistics only included when user sets `usageCol`, e.g., `.setUsageCol("usage")`
- No usage column created if not set
- Applies to both OpenAIPrompt and OpenAIEmbedding

**3. Auto-generated `responseIdCol` for store (OpenAIPrompt only)**
- Added `responseIdCol` parameter for response ID when `store=true`
- Auto-generates column name (e.g., `OpenAIPrompt_abc123_responseId`) if not explicitly set
- Users can customize via `.setResponseIdCol("custom_name")`
- Only created when `store=true` with Responses API

**4. Follows Standard Spark ML Pattern**
- Consistent with `errorCol`, `outputCol` conventions
- Separate columns for separate concerns
- Explicit, predictable behavior

### Before (Old Behavior):
```scala
val prompt = new OpenAIPrompt()
  .setReturnUsage(true)
  .setStore(true)
  .setOutputCol("result")

// result column contains: {response: "...", usage: {...}, id: "..."}
val response = df.select("result.response").first().getString(0)
```

### After (New Behavior):
```scala
// OpenAIPrompt example
val prompt = new OpenAIPrompt()
  .setUsageCol("usage")
  .setStore(true)
  .setOutputCol("result")

// Clean: each piece of data in its own column
val response = df.select("result").first().getString(0)  // Direct access
val usage = df.select("usage").first().getAs[Row](0)
val id = df.select(responseIdColName).first().getString(0)

// OpenAIEmbedding example
val embedding = new OpenAIEmbedding()
  .setUsageCol("usage")
  .setTextCol("text")
  .setOutputCol("embeddings")

// embeddings column contains vector, usage in separate column
val vector = df.select("embeddings").first().getAs[Vector](0)
val usage = df.select("usage").first().getAs[Row](0)
```

### Breaking Changes:
- **Removed**: `returnUsage` parameter and `setReturnUsage()` method from OpenAIPrompt and OpenAIEmbedding
- **Changed**: Output structure no longer wraps response/embeddings in struct
- **Migration for OpenAIPrompt**:
  - Replace `.setReturnUsage(true)` with `.setUsageCol("usage")`
  - Access response directly from `outputCol` instead of `outputCol.response`
  - Access usage from `usageCol` instead of `outputCol.usage`
  - Access response ID from `responseIdCol` instead of `outputCol.id`
- **Migration for OpenAIEmbedding**:
  - Replace `.setReturnUsage(true)` with `.setUsageCol("usage")`
  - Access embeddings directly from `outputCol` (always returns Vector)
  - Access usage from `usageCol` instead of `outputCol.usage`

## How is this patch tested?

- [x] I have written tests (not required for typo or doc fix) and confirmed the proposed feature/bug-fix/change works.

### Test Coverage:
- **46/46 tests passing** (100% pass rate)
  - OpenAIPromptSuite: 35/35 tests passing
  - OpenAIEmbeddingsSuite: 11/11 tests passing
- Updated all existing tests that used `returnUsage` in both suites
- Added new tests for `usageCol` parameter in both OpenAIPrompt and OpenAIEmbedding
- Added new tests for auto-generated `responseIdCol` (OpenAIPrompt)
- Added new test for custom `responseIdCol` naming (OpenAIPrompt)
- Edge cases tested: null inputs, store=false, various API types

### Key Test Scenarios (OpenAIPrompt)

1. `usageCol not set keeps structured output as array` - validates default behavior
2. `usageCol set emits response in outputCol and usage in separate column` - validates separate column behavior
3. `null input returns null output without usageCol` - null handling without usage
4. `null input returns null output with usageCol set` - null handling with usage
5. `store=true with Responses API returns response in outputCol and id in auto-generated responseIdCol` - auto-generation
6. `store=true with usageCol set returns response, usage, and id in separate columns` - all metadata together
7. `store=true with custom responseIdCol uses specified column name` - custom naming
8. `store=false does not add responseIdCol` - conditional column creation

### Key Test Scenarios (OpenAIEmbedding)

1. `usageCol not set keeps vector output` - validates embeddings always return as Vector
2. `usageCol set emits vector in outputCol and usage in separate column` - validates separate usage column
3. `null input returns null output without usageCol` - null handling for embeddings
4. `null input returns null output with usageCol set` - null handling for embeddings with usage

### Code Quality

- Scala style check: 0 errors, 0 warnings
- All source and test files compile successfully
- No regressions in existing functionality

## Does this PR change any dependencies?

- [x] No. You can skip this section.
- [ ] Yes. Make sure the dependencies are resolved correctly, and list changes here.

## Does this PR add a new feature? If so, have you added samples on website?

- [ ] No. You can skip this section.
- [x] Yes. Make sure you have added samples following below steps.

### Documentation Updated:
- Updated `docs/Explore Algorithms/OpenAI/OpenAI.ipynb` with new API examples
- Added examples showing `setUsageCol()` usage for Chat Completions, Responses API, and Embeddings
- Updated store and response chaining examples to show auto-generated `responseIdCol`
- All code samples use the new API with clear comments explaining behavior
- Notebook cells properly formatted with source as list of strings

### Website Samples:
The existing OpenAI documentation notebook has been updated with the new API patterns. The changes demonstrate:
- How to use `setUsageCol()` to enable usage tracking
- How `responseIdCol` is auto-generated when `store=true`
- How to customize `responseIdCol` name
- Migration from old to new API

---

### Summary

This PR makes OpenAIPrompt and OpenAIEmbedding output predictable and easier to use by always returning primary data (text/vector) in the output column, with metadata in separate, explicitly-named columns. The changes follow standard Spark ML patterns and include comprehensive tests and documentation updates for both classes.
