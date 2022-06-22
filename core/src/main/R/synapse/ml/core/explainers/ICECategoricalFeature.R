ICECategoricalFeature <- function(n, ntv) {
  value <- list(name = n, numTopValues = ntv)
  class(value) <- "ICECategoricalFeature"
  value
}
