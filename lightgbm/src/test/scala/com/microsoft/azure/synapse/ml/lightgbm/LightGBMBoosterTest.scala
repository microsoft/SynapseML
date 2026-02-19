import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.microsoft.azure.synapse.ml.lightgbm.booster.LightGBMBooster
import com.microsoft.azure.synapse.ml.lightgbm.dataset.LightGBMDataset
import org.apache.spark.ml.linalg.Vectors

class LightGBMBoosterTest extends AnyFlatSpec with Matchers {

  "LightGBMBooster" should "handle scoredDataOutPtr and scoredDataLengthLongPtr pointers correctly in score method" in {
    val booster = new LightGBMBooster("model string")
    val features = Vectors.dense(1.0, 2.0, 3.0)
    noException should be thrownBy booster.score(features, raw = true, classification = true, disableShapeCheck = false)
  }

  it should "handle scoredDataOutPtr and scoredDataLengthLongPtr pointers correctly in predictLeaf method" in {
    val booster = new LightGBMBooster("model string")
    val features = Vectors.dense(1.0, 2.0, 3.0)
    noException should be thrownBy booster.predictLeaf(features)
  }

  it should "handle scoredDataOutPtr and scoredDataLengthLongPtr pointers correctly in featuresShap method" in {
    val booster = new LightGBMBooster("model string")
    val features = Vectors.dense(1.0, 2.0, 3.0)
    noException should be thrownBy booster.featuresShap(features)
  }

  it should "handle scoredDataOutPtr and scoredDataLengthLongPtr pointers correctly in innerPredict method" in {
    val trainDataset = new LightGBMDataset("dataset string")
    val booster = new LightGBMBooster(Some(trainDataset), Some("parameters"), Some("model string"))
    noException should be thrownBy booster.innerPredict(0, classification = true)
  }
}
