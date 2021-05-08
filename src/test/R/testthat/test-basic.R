test_that("basic smoke test", {
  faithful_df <- copy_to(sc, faithful)
  cmd_model = ml_clean_missing_data(
    x=faithful_df,
    inputCols = c("eruptions", "waiting"),
    outputCols = c("eruptions_output", "waiting_output"),
    only.model=TRUE)
  new_df = ml_transform(cmd_model, faithful_df)
  expect_equal(sdf_nrow(new_df), 272)
})
